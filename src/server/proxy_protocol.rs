use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    str::FromStr,
    time::Duration,
};

use ipnet::IpNet;
use log::debug;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::TcpStream,
    time::timeout,
};

const SIGNATURE: &[u8; 12] = b"\r\n\r\n\0\r\nQUIT\n";
const HEADER_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_PAYLOAD: usize = 4096;
pub(super) const MAX_PENDING_HEADERS: usize = 128;

#[derive(Clone, Copy, Debug)]
pub(crate) struct TrustedProxy(IpNet);

impl FromStr for TrustedProxy {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let network = if value.contains('/') {
            value.parse::<IpNet>().map_err(|_| "expected an IP address or CIDR")?
        } else {
            IpNet::from(value.parse::<IpAddr>().map_err(|_| "expected an IP address or CIDR")?)
        };
        if network.prefix_len() == 0 {
            return Err("a trust-all proxy network is not allowed");
        }
        if network.addr().to_canonical() != network.addr() {
            return Err("use an IPv4 address or CIDR for an IPv4-mapped proxy");
        }
        if network.addr() != network.network() {
            return Err("CIDR has host bits set; use an exact IP or an explicit network address");
        }
        Ok(Self(network))
    }
}

impl TrustedProxy {
    pub(super) fn contains(self, address: IpAddr) -> bool {
        self.0.contains(&address.to_canonical())
    }
}

pub(super) async fn accept_proxy_header(mut stream: TcpStream) -> Option<(TcpStream, SocketAddr)> {
    match timeout(HEADER_TIMEOUT, read_source_address(&mut stream)).await {
        Ok(Ok(source)) => Some((stream, source)),
        Ok(Err(error)) => {
            debug!("Rejected PROXY v2 header from trusted peer: {:?}", error.kind());
            None
        }
        Err(_) => {
            debug!("Timed out reading PROXY v2 header from trusted peer");
            None
        }
    }
}

async fn read_source_address(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<SocketAddr> {
    fn invalid() -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, "invalid PROXY v2 TCP header")
    }

    let mut header = [0; 16];
    stream.read_exact(&mut header).await?;
    // Require the PROXY command. LOCAL or UNSPEC cannot provide a verified client address.
    if &header[..12] != SIGNATURE || header[12] != 0x21 {
        return Err(invalid());
    }
    let address_len = match header[13] {
        0x11 => 12, // AF_INET / STREAM
        0x21 => 36, // AF_INET6 / STREAM
        _ => return Err(invalid()),
    };
    let payload_len = u16::from_be_bytes([header[14], header[15]]) as usize;
    if !(address_len..=MAX_PAYLOAD).contains(&payload_len) {
        return Err(invalid());
    }

    // Typical Traefik headers contain only addresses. Read extensions in one bounded
    // operation when present, rather than performing a socket read for each small TLV.
    let mut addresses = [0u8; 36];
    let mut extended_payload;
    let payload = if payload_len == address_len {
        &mut addresses[..address_len]
    } else {
        extended_payload = vec![0; payload_len];
        extended_payload.as_mut_slice()
    };
    stream.read_exact(payload).await?;
    let ip = if address_len == 12 {
        IpAddr::from(Ipv4Addr::from(<[u8; 4]>::try_from(&payload[..4]).unwrap()))
    } else {
        IpAddr::from(Ipv6Addr::from(<[u8; 16]>::try_from(&payload[..16]).unwrap())).to_canonical()
    };
    let port = u16::from_be_bytes([payload[address_len - 4], payload[address_len - 3]]);
    if ip.is_unspecified() || ip.is_multicast() || port == 0 {
        return Err(invalid());
    }

    // Validate the framing and skip optional TLVs without interpreting them or reading into TLS bytes.
    let mut tlvs = &payload[address_len..];
    while !tlvs.is_empty() {
        let [_, hi, lo, rest @ ..] = tlvs else {
            return Err(invalid());
        };
        let len = u16::from_be_bytes([*hi, *lo]) as usize;
        tlvs = rest.get(len..).ok_or_else(invalid)?;
    }
    Ok(SocketAddr::new(ip, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::{AsyncWriteExt, ReadBuf};

    fn header(family: u8, body: &[u8]) -> Vec<u8> {
        let mut bytes = SIGNATURE.to_vec();
        bytes.extend([0x21, family]);
        bytes.extend((body.len() as u16).to_be_bytes());
        bytes.extend(body);
        bytes
    }

    #[test]
    fn trust_list_supports_exact_and_cidr_but_never_trusts_all_connections() {
        for value in [
            "*",
            "0.0.0.0/0",
            "::/0",
            "localhost",
            "::ffff:172.18.0.3",
            "172.18.0.3/24",
            "2001:db8:2::5/64",
        ] {
            assert!(value.parse::<TrustedProxy>().is_err(), "{value}");
        }
        let v4: TrustedProxy = "172.18.0.3".parse().unwrap();
        assert!(v4.contains("::ffff:172.18.0.3".parse().unwrap()));
        assert!(!v4.contains("172.18.0.4".parse().unwrap()));
        let v6: TrustedProxy = "2001:db8:2::/64".parse().unwrap();
        assert!(v6.contains("2001:db8:2::5".parse().unwrap()));
        assert!(!v6.contains("2001:db8:3::5".parse().unwrap()));
    }

    #[tokio::test]
    async fn fragmented_ipv4_header_with_tlv_leaves_tls_bytes_unread() {
        let bytes = header(0x11, &[203, 0, 113, 4, 192, 0, 2, 9, 1, 187, 1, 187, 0xe0, 0, 3, 1, 2, 3]);
        let (mut writer, mut reader) = tokio::io::duplex(8);
        let (_, actual) = tokio::join!(
            async {
                for chunk in bytes.chunks(3) {
                    writer.write_all(chunk).await.unwrap();
                }
                writer.write_all(b"tls").await.unwrap();
            },
            async {
                let addr = read_source_address(&mut reader).await.unwrap();
                let mut tls = [0; 3];
                reader.read_exact(&mut tls).await.unwrap();
                assert_eq!(&tls, b"tls");
                addr
            }
        );
        assert_eq!(actual, "203.0.113.4:443".parse().unwrap());
    }

    // With all data available, many short TLVs should not each require a read.
    #[tokio::test]
    async fn header_and_payload_are_two_reads_even_with_many_tlvs() {
        struct Counted<'a> {
            bytes: &'a [u8],
            reads: usize,
        }

        impl AsyncRead for Counted<'_> {
            fn poll_read(self: Pin<&mut Self>, _: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
                let reader = self.get_mut();
                let (head, tail) = reader.bytes.split_at(buf.remaining().min(reader.bytes.len()));
                buf.put_slice(head);
                reader.bytes = tail;
                reader.reads += 1;
                Poll::Ready(Ok(()))
            }
        }

        let mut body = vec![192, 0, 2, 5, 192, 0, 2, 9, 1, 187, 1, 187];
        for extensions in [0, 1000] {
            body.extend((0..extensions).flat_map(|_| [0xe0, 0, 0]));
            let mut bytes = header(0x11, &body);
            bytes.extend(b"tls");
            let mut reader = Counted { bytes: &bytes, reads: 0 };
            assert_eq!(read_source_address(&mut reader).await.unwrap(), "192.0.2.5:443".parse().unwrap());
            assert_eq!(reader.reads, 2);
            assert_eq!(reader.bytes, b"tls");
        }
    }

    #[tokio::test]
    async fn ipv6_source_preserves_port_and_normalizes_ipv4_mapped() {
        for (source, expected) in [("2001:db8::1", "[2001:db8::1]:11443"), ("::ffff:192.0.2.5", "192.0.2.5:11443")] {
            let mut body = source.parse::<Ipv6Addr>().unwrap().octets().to_vec();
            body.extend(Ipv6Addr::LOCALHOST.octets());
            body.extend(11443u16.to_be_bytes());
            body.extend(443u16.to_be_bytes());
            let bytes = header(0x21, &body);
            assert_eq!(read_source_address(&mut bytes.as_slice()).await.unwrap(), expected.parse().unwrap());
        }
    }

    #[tokio::test]
    async fn rejects_wrong_protocol_family_length_and_invalid_tlvs() {
        let valid = [192, 0, 2, 5, 192, 0, 2, 9, 1, 187, 1, 187];
        let mut local = header(0x11, &valid);
        local[12] = 0x20;
        let mut oversize = header(0x11, &[]);
        oversize[14..].copy_from_slice(&((MAX_PAYLOAD + 1) as u16).to_be_bytes());
        let mut bad_tlv = valid.to_vec();
        bad_tlv.extend([0xe0, 0, 3, 1]);
        for bytes in [
            local,
            oversize,
            header(0x12, &valid),
            header(0x11, &valid[..11]),
            header(0x11, &bad_tlv),
            b"not a PROXY preface".to_vec(),
        ] {
            assert_eq!(read_source_address(&mut bytes.as_slice()).await.unwrap_err().kind(), io::ErrorKind::InvalidData);
        }
        let mut truncated = header(0x11, &valid);
        truncated.pop();
        assert_eq!(read_source_address(&mut truncated.as_slice()).await.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }
}
