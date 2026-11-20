use super::*;
use std::{net::Ipv4Addr, path::Path, process::Command};

use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio_rustls::TlsConnector;

fn openssl(root: &Path, args: &[&str]) -> Vec<u8> {
    let output = Command::new("openssl").current_dir(root).args(args).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    output.stdout
}

fn tls_pair() -> (Arc<CertStore>, TlsAcceptor, TlsConnector) {
    let dir = tempfile::tempdir().unwrap();
    openssl(
        dir.path(),
        &[
            "req",
            "-new",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-noenc",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-keyout",
            "key.pem",
            "-out",
            "cert.pem",
        ],
    );
    let p12 = openssl(
        dir.path(),
        &[
            "pkcs12",
            "-export",
            "-in",
            "cert.pem",
            "-inkey",
            "key.pem",
            "-keypbe",
            "PBE-SHA1-3DES",
            "-certpbe",
            "PBE-SHA1-3DES",
            "-macalg",
            "sha1",
            "-passout",
            "pass:test",
        ],
    );
    let parsed = ParsedCert::from_p12(&p12, "test").unwrap();
    let provider = Arc::new(ssl_provider());
    let store = Arc::new(CertStore::new(provider.clone(), parsed, false).unwrap());
    let acceptor = TlsAcceptor::from(Arc::new(create_ssl_config(provider.clone(), store.clone())));
    let mut roots = RootCertStore::empty();
    let der = openssl(dir.path(), &["x509", "-in", "cert.pem", "-outform", "der"]);
    roots.add(CertificateDer::from(der)).unwrap();
    let client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (store, acceptor, TlsConnector::from(Arc::new(client)))
}

async fn dial(address: SocketAddr, peer: Ipv4Addr) -> io::Result<TcpStream> {
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind(SocketAddr::from((peer, 0))).unwrap();
    socket.connect(address).await
}

async fn connect(address: SocketAddr, peer: Ipv4Addr) -> TcpStream {
    dial(address, peer).await.unwrap()
}

fn header(source: Ipv4Addr, port: u16) -> Vec<u8> {
    let mut bytes = b"\r\n\r\n\0\r\nQUIT\n\x21\x11\x00\x0c".to_vec();
    bytes.extend(source.octets());
    bytes.extend(Ipv4Addr::LOCALHOST.octets());
    bytes.extend(port.to_be_bytes());
    bytes.extend(443u16.to_be_bytes());
    bytes
}

async fn request(stream: TcpStream, connector: &TlsConnector) -> io::Result<String> {
    let socket = connector.connect(ServerName::try_from("localhost").unwrap(), stream).await?;
    let mut socket = BufReader::new(socket);
    socket
        .get_mut()
        .write_all(b"GET /peer HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await?;
    let mut line = String::new();
    let mut length = None;
    while socket.read_line(&mut line).await? != 0 && line != "\r\n" {
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = Some(value.trim().parse().unwrap());
        }
        line.clear();
    }
    let mut body = vec![0; length.ok_or_else(|| io::Error::other("missing test response length"))?];
    socket.read_exact(&mut body).await?;
    Ok(String::from_utf8(body).unwrap())
}

#[tokio::test]
async fn real_tls_uses_the_reported_peer_only_for_trusted_connections() {
    let (store, acceptor, connector) = tls_pair();
    let handle = Arc::new(ServerHandle::new(store, crate::metrics::Metrics::new().tcp));
    let listener = bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let address = listener.local_addr().unwrap();
    let trusted = Ipv4Addr::new(127, 0, 0, 2);
    let router = Router::new().route("/peer", get(|addr: ClientAddr| async move { addr.0.to_string() }));
    let ingress = TcpIngress {
        flood_control: true,
        h3_port: None,
        trusted_proxies: vec![trusted.to_string().parse().unwrap()],
    };
    let server = tokio::spawn(accept_loop(handle.clone(), listener, acceptor, http1::Builder::new(), router, ingress));

    let mut stalled = connect(address, trusted).await;
    stalled.write_all(b"\r\n\r").await.unwrap();
    let direct = connect(address, Ipv4Addr::LOCALHOST).await;
    let expected = direct.local_addr().unwrap();
    assert_eq!(timeout(Duration::from_secs(1), request(direct, &connector)).await.unwrap().unwrap(), expected.to_string());

    // More requests than the flood threshold: clients behind the same proxy must not share one allowance.
    for n in 1..=12 {
        let client = Ipv4Addr::new(192, 0, 2, n);
        let mut forwarded = connect(address, trusted).await;
        forwarded.write_all(&header(client, 12345)).await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(3), request(forwarded, &connector))
                .await
                .unwrap()
                .unwrap(),
            SocketAddr::from((client, 12345)).to_string()
        );
    }

    let mut untrusted = connect(address, Ipv4Addr::LOCALHOST).await;
    let fake = header(Ipv4Addr::new(192, 0, 2, 40), 12345);
    untrusted.write_all(&fake).await.unwrap();
    assert!(
        timeout(Duration::from_secs(1), request(untrusted, &connector))
            .await
            .unwrap()
            .is_err()
    );
    let missing = connect(address, trusted).await;
    assert!(
        timeout(Duration::from_secs(1), request(missing, &connector))
            .await
            .unwrap()
            .is_err()
    );

    let result = timeout(Duration::from_secs(3), stalled.read_u8()).await.unwrap().unwrap_err();
    assert!(peer_io_closed(result.kind()), "{result}");

    // Direct and forwarded traffic from the same effective address share the flood allowance.
    let mixed = Ipv4Addr::new(127, 0, 0, 3);
    let mixed_header = header(mixed, 12345);
    for n in 0..9 {
        let stream = if n % 2 == 0 {
            let mut stream = connect(address, trusted).await;
            stream.write_all(&mixed_header).await.unwrap();
            stream
        } else {
            connect(address, mixed).await
        };
        assert!(timeout(Duration::from_secs(1), request(stream, &connector)).await.unwrap().is_ok());
    }
    match dial(address, mixed).await {
        Ok(blocked) => assert!(
            timeout(Duration::from_secs(1), request(blocked, &connector))
                .await
                .unwrap()
                .is_err()
        ),
        Err(error) => assert!(peer_io_closed(error.kind()), "{error}"),
    }

    let mut pending_at_shutdown = connect(address, trusted).await;
    pending_at_shutdown.write_all(b"\r\n\r").await.unwrap();
    handle.shutdown();
    timeout(Duration::from_secs(1), server).await.unwrap().unwrap();
    let closed = timeout(Duration::from_secs(1), pending_at_shutdown.read_u8())
        .await
        .unwrap()
        .unwrap_err();
    assert!(peer_io_closed(closed.kind()), "{closed}");
}
