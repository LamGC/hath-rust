use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
    vec,
};

use axum::{Extension, Router, body::Body, extract::Request, http::HeaderValue, response::Response, routing::get};
use futures::pin_mut;
use h3::error::Code;
use h3_quinn::quinn;
use http_body_util::BodyExt;
use hyper::{
    header::{ALT_SVC, CONNECTION, DATE},
    server::conn::http1,
};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use log::{error, info, warn};
use parking_lot::Mutex;
use quinn::{BloomTokenLog, Endpoint, TransportConfig, ValidationTokenConfig, crypto::rustls::QuicServerConfig};
use rustls::{
    ServerConfig,
    compress::CompressionCache,
    crypto::{CryptoProvider, aws_lc_rs::Ticketer},
    server::{NoServerSessionStorage, ServerSessionMemoryCache},
    version::TLS13,
};
use tokio::{
    net::{TcpListener, TcpSocket, TcpStream},
    sync::watch,
    task::JoinHandle,
    time::timeout,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tower::{Layer, Service};
use tower_http::set_header::SetResponseHeaderLayer;

pub use crate::server::{cert::ParsedCert, util::ClientAddr};
use crate::{
    AppState,
    metrics::{TcpConnectionEvent, TcpConnectionMetrics},
    middleware, route,
    server::{cert::CertStore, util::FloodControl},
    util::{aes_support, ssl_provider},
};

mod cert;
mod date_header;
mod h3_quinn;
pub mod util;

pub struct Server {
    handle: Arc<ServerHandle>,
    accept_task: JoinHandle<()>,
    accept_task_h3: Option<JoinHandle<()>>,
}

pub struct ServerOptions {
    port: u16,
    host: IpAddr,
    cert: ParsedCert,
    data: AppState,
    flood_control: bool,
    enable_metrics: bool,
    sni_strict: bool,
    server_header: bool,
    h3: bool,
}

impl ServerOptions {
    pub fn new(port: u16, host: IpAddr, cert: ParsedCert, data: AppState) -> Self {
        Self {
            port,
            host,
            cert,
            data,
            flood_control: true,
            enable_metrics: false,
            sni_strict: false,
            server_header: true,
            h3: false,
        }
    }

    pub fn flood_control(mut self, enabled: bool) -> Self {
        self.flood_control = enabled;
        self
    }

    pub fn metrics(mut self, enabled: bool) -> Self {
        self.enable_metrics = enabled;
        self
    }

    pub fn sni_strict(mut self, enabled: bool) -> Self {
        self.sni_strict = enabled;
        self
    }

    pub fn server_header(mut self, enabled: bool) -> Self {
        self.server_header = enabled;
        self
    }

    pub fn h3(mut self, enabled: bool) -> Self {
        self.h3 = enabled;
        self
    }
}

impl Server {
    pub fn new(option: ServerOptions) -> Option<Self> {
        let port = option.port;
        let host = option.host;
        let flood_control = option.flood_control;

        let provider = Arc::new(ssl_provider());
        let cert_store = match CertStore::new(provider.clone(), option.cert, option.sni_strict) {
            Ok(store) => Arc::new(store),
            Err(_) => {
                error!("Server TLS certificate is invalid");
                return None;
            }
        };
        let handle = Arc::new(ServerHandle::new(cert_store.clone(), option.data.metrics.tcp.clone()));

        let acceptor = TlsAcceptor::from(Arc::new(create_ssl_config(provider.clone(), cert_store.clone())));
        let listener = match bind(SocketAddr::from((host, port))) {
            Ok(listener) => listener,
            Err(err) => {
                error!("Bind port error: {err}");
                return None;
            }
        };

        let mut http = http1::Builder::new();
        http.keep_alive(false)
            .max_buf_size(16384) // SSL max record size is 16KiB
            .timer(TokioTimer::new())
            .title_case_headers(true)
            .writev(true)
            .header_read_timeout(Duration::from_secs(15));

        let mut router = Router::new();
        router = route::register_route(router);
        if option.enable_metrics {
            router = router.route("/metrics", get(route::metrics));
            info!("Metrics endpoint enabled at '/metrics'");
        }
        router = middleware::register_layer(router, &option.data, option.server_header);
        let router = router.with_state(Arc::new(option.data));

        // h3, bind before the accept loop so Alt-Svc is only advertised when it works
        let endpoint = if option.h3 {
            info!("Enable experimental HTTP3 support. Please ensure UDP port {port} is open.");
            let quinn_config = create_quic_config(provider, cert_store);
            quinn::Endpoint::server(quinn_config, SocketAddr::from((host, port)))
                .inspect_err(|err| warn!("Bind UDP port error, HTTP3 disabled: {err}"))
                .ok()
        } else {
            None
        };

        // Accept loop
        let h3_port = endpoint.as_ref().map(|_| port);
        let accept_task = tokio::spawn(accept_loop(handle.clone(), listener, acceptor, http, router.clone(), flood_control, h3_port));
        let accept_task_h3 = endpoint.map(|endpoint| tokio::spawn(accept_loop_h3(handle.clone(), endpoint, router, flood_control)));

        Some(Self {
            handle,
            accept_task,
            accept_task_h3,
        })
    }

    pub fn handle(&self) -> Arc<ServerHandle> {
        self.handle.clone()
    }

    /// Graceful shutdown and wait server loop stop.
    pub async fn shutdown(self) {
        self.handle.shutdown();
        let _ = self.accept_task.await;
        if let Some(handle) = self.accept_task_h3 {
            let _ = handle.await;
        }
    }
}

fn create_ssl_config(provider: Arc<CryptoProvider>, cert_store: Arc<CertStore>) -> ServerConfig {
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_cert_resolver(cert_store);

    // Prefer ChaCha20 on non-AES hardware.
    config.ignore_client_order = true;
    config.prioritize_chacha = aes_support();
    // Only support ticket resumption.
    config.session_storage = Arc::new(NoServerSessionStorage {});
    config.ticketer = Ticketer::new().unwrap();
    config.send_tls13_tickets = 1;
    config.cert_compression_cache = CompressionCache::new(2).into(); // 1 cert * 2 compression algorithms

    config
}

fn create_quic_config(provider: Arc<CryptoProvider>, cert_store: Arc<CertStore>) -> quinn::ServerConfig {
    let mut ssl_config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&TLS13]) // QUIC requires TLS 1.3
        .unwrap()
        .with_no_client_auth()
        .with_cert_resolver(cert_store);

    // Prefer ChaCha20 on non-AES hardware.
    ssl_config.ignore_client_order = true;
    ssl_config.prioritize_chacha = aes_support();
    // Rustls QUIC 0-RTT require stateful resumption
    ssl_config.session_storage = ServerSessionMemoryCache::new(65536); // ~64 bytes per session, ~4MiB total
    ssl_config.send_tls13_tickets = 1;
    ssl_config.cert_compression_cache = CompressionCache::new(2).into(); // 1 cert * 2 compression algorithms
    // h3 settings
    ssl_config.alpn_protocols = vec![b"h3".into()];
    ssl_config.max_early_data_size = u32::MAX; // 0-RTT support
    ssl_config.send_half_rtt_data = true;

    let mut transport_config = TransportConfig::default();
    transport_config.congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default())); // TODO replace with BBRv3
    transport_config.send_fairness(false); // We don't need send fairness
    transport_config.send_window(2500000); // 100Mbps * 200ms (2.5MB per connection) // TODO check size
    transport_config.max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()));
    let mut validation_config = ValidationTokenConfig::default();
    validation_config.lifetime(Duration::from_hours(24)); // Match default alt-svc age
    validation_config.log(Arc::new(BloomTokenLog::new_expected_items(2 * 1024 * 1024, 65536))); // 2MiB & match rustls session storage
    validation_config.sent(1);

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(ssl_config).unwrap()));
    server_config.transport_config(transport_config.into());
    server_config.max_incoming(500);
    server_config
}

fn bind(addr: SocketAddr) -> io::Result<TcpListener> {
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    }?;

    let _ = socket.set_reuseaddr(true); // Without this need wait all connection close before restart
    let _ = socket.set_keepalive(true);
    let _ = socket.set_nodelay(true);

    socket.bind(addr)?;
    socket.listen(512).inspect(|_| info!("Server is listen on {addr}"))
}

async fn accept(listener: &mut TcpListener, metrics: &TcpConnectionMetrics) -> (TcpStream, SocketAddr) {
    loop {
        match listener.accept().await {
            // Unmap IPv4-mapped IPv6
            Ok((stream, addr)) => return (stream, SocketAddr::new(addr.ip().to_canonical(), addr.port())),
            Err(_) => {
                metrics.record(TcpConnectionEvent::AcceptError);
                tokio::time::sleep(Duration::from_millis(50)).await; // Too many open files, retry.
            }
        }
    }
}

fn peer_io_closed(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted | io::ErrorKind::BrokenPipe | io::ErrorKind::UnexpectedEof
    )
}

fn http_connection_event(result: Result<(), hyper::Error>) -> TcpConnectionEvent {
    use TcpConnectionEvent::*;
    let Err(error) = result else { return HttpCompleted };
    if error.is_timeout() {
        return HttpHeaderTimeout; // Describes Hyper's HTTP/1 header deadline, not the TLS or outer deadline.
    }
    if error.is_parse() {
        return HttpProtocolError;
    }
    // Producer/body I/O errors can themselves wrap UnexpectedEof. Those are
    // not evidence of a peer disconnect, even with the same std::io kind.
    if error.is_user() {
        return HttpError;
    }
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = cause {
        if current
            .downcast_ref::<io::Error>()
            .is_some_and(|source| peer_io_closed(source.kind()))
        {
            return HttpPeerClosed;
        }
        cause = current.source();
    }
    if error.is_incomplete_message() { HttpPeerClosed } else { HttpError }
}

async fn accept_loop(
    handle: Arc<ServerHandle>,
    mut listener: TcpListener,
    acceptor: TlsAcceptor,
    http: http1::Builder,
    mut router: Router,
    flood_control: bool,
    h3_port: Option<u16>,
) {
    let (shutdown_tx, _) = watch::channel(());
    let metrics = &handle.tcp_metrics;
    let mut flood_control = if flood_control { Some(FloodControl::new()) } else { None };
    // Add h3 Alt-svc header
    if let Some(port) = h3_port {
        let value = HeaderValue::from_str(&format!("h3=\":{}\"; persist=1", port)).unwrap();
        let h3_header = SetResponseHeaderLayer::appending(ALT_SVC, value);
        router = router.layer(h3_header)
    }
    loop {
        // Wait for new tcp connection
        let (stream, addr) = tokio::select! {
            biased;
            _ = handle.shutdown.cancelled() => break,
            result = accept(&mut listener, metrics) => result,
        };
        metrics.record(TcpConnectionEvent::Accepted);

        // Flood control
        if flood_control.as_mut().is_some_and(|fc| fc.hit(addr)) {
            metrics.record(TcpConnectionEvent::FloodBlocked);
            // Flood detected
            let _ = stream.set_zero_linger(); // RST connection
            drop(stream);
            continue;
        }

        // Process connection
        let acceptor = acceptor.clone();
        let http = http.clone();
        let metrics = metrics.clone();
        let service = Extension(ClientAddr(addr)).layer(router.clone());
        let mut shutdown_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            // TLS handshake
            let ssl_stream = match timeout(Duration::from_secs(10), acceptor.accept(stream)).await {
                Ok(Ok(s)) => {
                    metrics.record(TcpConnectionEvent::TlsEstablished);
                    s
                }
                Ok(Err(error)) => {
                    metrics.record(if peer_io_closed(error.kind()) {
                        TcpConnectionEvent::TlsPeerClosed
                    } else {
                        TcpConnectionEvent::TlsFailed
                    });
                    return;
                }
                Err(_) => {
                    metrics.record(TcpConnectionEvent::TlsTimeout);
                    return;
                }
            };

            // Disable nodelay after handshake
            let _ = ssl_stream.get_ref().0.set_nodelay(false);

            // Process request
            let stream = TokioIo::new(ssl_stream); // Tokio to hyper trait
            let service = TowerToHyperService::new(service); // Tower to hyper trait
            let connection = http.serve_connection(stream, service);
            pin_mut!(connection);
            let event = timeout(Duration::from_secs(181), async {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => {
                        connection.as_mut().graceful_shutdown();
                        match timeout(Duration::from_secs(10), connection).await {
                            Ok(result) => http_connection_event(result),
                            Err(_) => TcpConnectionEvent::HttpShutdownTimeout,
                        }
                    },
                    result = &mut connection => http_connection_event(result),
                }
            })
            .await
            .unwrap_or(TcpConnectionEvent::HttpConnectionTimeout);
            metrics.record(event);
        });
    }

    // Close listener
    drop(listener);
    info!("Server listener closed!");

    // Wait connection complated or timeout
    shutdown_tx.send_replace(()); // Notify all connection
    let _ = timeout(Duration::from_secs(15), shutdown_tx.closed()).await;
}

async fn accept_loop_h3(handle: Arc<ServerHandle>, endpoint: Endpoint, router: Router, flood_control: bool) {
    let (shutdown_tx, _) = watch::channel(());
    let flood_control = if flood_control {
        Some(Arc::new(Mutex::new(FloodControl::new())))
    } else {
        None
    };

    loop {
        // Wait new connection
        let new_conn = tokio::select! {
            biased;
            _ = handle.shutdown.cancelled() => break,
            Some(result) = endpoint.accept() => result,
        };

        let addr = new_conn.remote_address();
        let addr = SocketAddr::new(addr.ip().to_canonical(), addr.port()); // Unmap IPv4-mapped IPv6

        // Process connection
        let router = router.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let flood_control = flood_control.clone();
        tokio::spawn(async move {
            // QUIC handshake
            let conn = match new_conn.accept() {
                Ok(mut conn) => {
                    // We need wait handshake data before start h3 handshake
                    if conn.handshake_data().await.is_err() {
                        return;
                    }
                    conn.into_0rtt().unwrap().0 // Server side 0-RTT always success
                }
                _ => return, // Timeout or error
            };

            // H3 Handshake
            let mut h3_builder = h3::server::builder();
            h3_builder.max_field_section_size(64 * 1024); // Limit max header size
            let mut h3_conn = match h3_builder.build(h3_quinn::Connection::new(conn)).await {
                Ok(conn) => conn,
                Err(_) => return,
            };

            // Request loop
            let (close_wait, _) = watch::channel(());
            loop {
                // Accept request
                let resolver = tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => { // Graceful shutdown
                        let _ = h3_conn.shutdown(1).await; // Send GOAWAY
                        close_wait.closed().await; // Drop h3_conn will reset request, wait for request complete
                        break;
                    },
                    resolver = h3_conn.accept() => resolver,
                };
                let resolver = match resolver {
                    Ok(Some(resolver)) => resolver,
                    _ => break,
                };

                // Flood control
                let blocked = flood_control.as_ref().is_some_and(|fc| fc.lock().hit(addr));
                if blocked {
                    let _ = h3_conn.shutdown(0).await; // Send GOAWAY
                }

                // Process request
                let router = router.clone();
                let request_done = close_wait.subscribe();
                tokio::spawn(async move {
                    // Read request
                    let (req, mut stream) = match resolver.resolve_request().await {
                        Ok(req) => req,
                        Err(_) => return,
                    };

                    if blocked {
                        stream.stop_stream(Code::H3_EXCESSIVE_LOAD);
                        return;
                    }

                    let request = Request::from_parts(req.into_parts().0, Body::empty()); // We don't read body

                    // ref: https://github.com/hyperium/h3/issues/261#issuecomment-2352838801
                    // Call Service
                    let mut service = Extension(ClientAddr(addr)).layer(router.clone());
                    let (parts, mut body) = service.call(request).await.unwrap().into_parts();

                    // Build response
                    let mut response = Response::from_parts(parts, ());
                    let header = response.headers_mut();
                    header.remove(CONNECTION); // H3 not allow connection header
                    header.entry(DATE).or_insert_with(date_header::update_and_header_value); // Add date header

                    // Send header
                    if (stream.send_response(response).await).is_err() {
                        return;
                    }
                    // Send body and trailers, timeout to prevent client hold stream forever
                    let send_body = async {
                        while let Some(chunk) = body.frame().await {
                            match chunk {
                                Ok(frame) if frame.is_data() => {
                                    if stream.send_data(frame.into_data().unwrap()).await.is_err() {
                                        return false;
                                    }
                                }
                                Ok(frame) => {
                                    if stream.send_trailers(frame.into_trailers().unwrap()).await.is_err() {
                                        return false;
                                    }
                                }
                                Err(err) => {
                                    error!("{{h3/{addr}}} Failed to read stream: {err}");
                                    stream.stop_stream(Code::H3_INTERNAL_ERROR);
                                    return false;
                                }
                            }
                        }
                        true
                    };
                    match timeout(Duration::from_secs(181), send_body).await {
                        Ok(true) => {}
                        Ok(false) => return,
                        Err(_) => {
                            stream.stop_stream(Code::H3_REQUEST_CANCELLED);
                            return;
                        }
                    }
                    // All data must be read to receive ACK, timeout to prevent client hold stream forever
                    let _ = timeout(Duration::from_secs(181), async { while let Ok(Some(_)) = stream.recv_data().await {} }).await;
                    let _ = stream.finish().await;
                    drop(request_done); // Notify request complete
                });
            }
        });
    }

    // Wait connection complated or timeout
    shutdown_tx.send_replace(()); // Notify all connection
    let _ = timeout(Duration::from_secs(15), shutdown_tx.closed()).await;
    endpoint.close(0u8.into(), b"Shutdown");
}

pub struct ServerHandle {
    shutdown: CancellationToken,
    cert_store: Arc<CertStore>,
    tcp_metrics: Arc<TcpConnectionMetrics>,
}

impl ServerHandle {
    fn new(cert_store: Arc<CertStore>, tcp_metrics: Arc<TcpConnectionMetrics>) -> Self {
        Self {
            shutdown: CancellationToken::new(),
            cert_store,
            tcp_metrics,
        }
    }

    pub fn update_cert(&self, cert: ParsedCert) -> Result<(), rustls::Error> {
        self.cert_store.update(cert)
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}
