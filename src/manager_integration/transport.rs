use super::{
    config::{Config, Token, invalid},
    protocol::{self, Acceptance, NativeHealth, NativeIdentity, Registration, Session},
};
use futures::StreamExt;
use serde_json::json;
use std::{io, time::Duration};

pub(super) struct Transport {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    token: Token,
    instance: String,
    session: Option<Session>,
}

impl Transport {
    pub fn has_session(&self) -> bool {
        self.session.is_some()
    }

    pub fn new(config: &Config) -> io::Result<Self> {
        config.validate()?;
        let tls = rustls::ClientConfig::builder_with_provider(rustls::crypto::aws_lc_rs::default_provider().into())
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid("manager_tls_configuration"))?
            .with_root_certificates(rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()))
            .with_no_client_auth();
        let client = reqwest::ClientBuilder::new()
            .tls_backend_preconfigured(tls)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(1)
            .build()
            .map_err(|_| invalid("manager_http_configuration"))?;
        Ok(Self {
            client,
            endpoint: config.endpoint()?,
            token: config.token()?,
            instance: config.instance_id.clone(),
            session: None,
        })
    }

    pub async fn register(&mut self, identity: &NativeIdentity) -> io::Result<()> {
        self.session = None;
        let body = serde_json::to_vec(&Registration::new(&self.instance, identity)).map_err(|_| invalid("manager_registration_invalid"))?;
        let reply = self.post("agents/register", body, true).await?;
        let acceptance: Acceptance = protocol::decode(&reply).map_err(invalid)?;
        self.session = Some(acceptance.validate().map_err(invalid)?);
        Ok(())
    }

    pub async fn heartbeat(&mut self, sequence: i64, status: &str, health: Option<&NativeHealth>) -> io::Result<()> {
        if sequence < 0 || !matches!(status, "starting" | "active" | "stopping" | "degraded") {
            return Err(invalid("manager_heartbeat_invalid"));
        }
        let body = serde_json::to_vec(
            &json!({"instanceId":self.instance,"sequence":sequence,"timestamp":chrono::Utc::now(),"status":status,"nativeHealth":health}),
        )
        .map_err(|_| invalid("manager_heartbeat_invalid"))?;
        let reply = self.post("agents/heartbeat", body, false).await?;
        protocol::acknowledge(&reply, None).map_err(invalid)
    }

    #[cfg(target_os = "linux")]
    pub async fn send_events(&mut self, records: &[super::outbox::Record]) -> io::Result<()> {
        if records.is_empty() || records.len() > 32 || records.iter().any(|record| record.event.instance_id != self.instance) {
            return Err(invalid("manager_batch_invalid"));
        }
        let batch = super::new_id()?;
        let mut body = format!("{{\"instanceId\":{},\"batchId\":{},\"events\":[", json!(self.instance), json!(batch)).into_bytes();
        for (index, record) in records.iter().enumerate() {
            if index != 0 {
                body.push(b',');
            }
            body.extend_from_slice(&record.bytes);
        }
        body.extend_from_slice(b"]}");
        if body.len() > 256 * 1024 {
            return Err(invalid("manager_batch_size"));
        }
        let reply = self.post("events/batch", body, false).await?;
        protocol::acknowledge(&reply, Some(&batch)).map_err(invalid)
    }

    pub async fn telemetry(
        &mut self,
        sequence: i64,
        timestamp: chrono::DateTime<chrono::Utc>,
        payload: &serde_json::Value,
    ) -> io::Result<()> {
        let session = self.session.as_ref().ok_or_else(|| invalid("manager_session_required"))?;
        if !session.capabilities.contains("telemetry.v1") {
            return Ok(());
        }
        if sequence < 0 || !payload.is_object() {
            return Err(invalid("manager_telemetry_invalid"));
        }
        let body = serde_json::to_vec(&json!({"instanceId":self.instance,"sequence":sequence,"timestamp":timestamp,"payload":payload}))
            .map_err(|_| invalid("manager_telemetry_invalid"))?;
        if body.len() > 64 * 1024 {
            return Err(invalid("manager_telemetry_size"));
        }
        let reply = self.post("telemetry", body, false).await?;
        protocol::acknowledge(&reply, None).map_err(invalid)
    }

    async fn post(&mut self, path: &str, body: Vec<u8>, registration: bool) -> io::Result<Vec<u8>> {
        let url = self
            .endpoint
            .join(&format!("rpc/v1/{path}"))
            .map_err(|_| invalid("manager_endpoint_invalid"))?;
        let mut request = self
            .client
            .post(url)
            .bearer_auth(self.token.as_str())
            .header("Content-Type", "application/json; charset=utf-8")
            .body(body);
        if !registration {
            let session = self.session.as_ref().ok_or_else(|| invalid("manager_session_required"))?;
            request = request.header("X-Hath-Agent-Session", session.id.as_str());
        }
        let reply = request.send().await.map_err(|_| invalid("manager_transport_unavailable"))?;
        if !reply.status().is_success() {
            if matches!(reply.status().as_u16(), 401 | 403 | 409 | 426 | 503) {
                self.session = None;
            }
            return Err(invalid("manager_http_rejected"));
        }
        if reply.content_length().is_some_and(|length| length > protocol::MAX_RESPONSE as u64) {
            return Err(invalid("manager_response_size"));
        }
        let mut output = Vec::new();
        let mut stream = reply.bytes_stream();
        while let Some(part) = stream.next().await {
            let part = part.map_err(|_| invalid("manager_transport_unavailable"))?;
            if output.len() + part.len() > protocol::MAX_RESPONSE {
                return Err(invalid("manager_response_size"));
            }
            output.extend_from_slice(&part);
        }
        Ok(output)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn server(replies: Vec<(u16, String)>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, body) in replies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let header_end = loop {
                    let mut byte = [0];
                    stream.read_exact(&mut byte).await.unwrap();
                    request.push(byte[0]);
                    assert!(request.len() < 16 * 1024);
                    if request.ends_with(b"\r\n\r\n") {
                        break request.len();
                    }
                };
                let headers = String::from_utf8(request.clone()).unwrap();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(str::trim)
                            .map(str::to_owned)
                    })
                    .unwrap()
                    .parse()
                    .unwrap();
                assert!(length < 256 * 1024);
                request.resize(header_end + length, 0);
                stream.read_exact(&mut request[header_end..]).await.unwrap();
                let body = if body == "ACK_BATCH" {
                    let input: serde_json::Value = serde_json::from_slice(&request[header_end..]).unwrap();
                    json!({"ok":true,"batchId":input["batchId"]}).to_string()
                } else {
                    body
                };
                requests.push(String::from_utf8(request).unwrap());
                stream.write_all(format!("HTTP/1.1 {status} reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
            requests
        });
        (url, task)
    }

    #[tokio::test]
    async fn session_loss_requires_reregistration_and_wrong_ack_never_deletes_records() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!("fixtures/agent-registration-native-v1.json")).unwrap();
        let identity: NativeIdentity = serde_json::from_value(fixture["nativeIdentity"].clone()).unwrap();
        let accepted = json!({"status":"registered","serverTime":"2026-11-07T00:00:00Z","managerProtocol":{"major":1,"minor":0},
            "selectedProtocol":{"major":1,"minor":0},"enabledCapabilities":protocol::Offer::native().capabilities,
            "outputProfile":"gallery-v2","sessionId":"0123456789abcdef0123456789abcdef","legacy":false})
        .to_string();
        let (url, task) = server(vec![
            (200, accepted.clone()),
            (200, "{\"ok\":true,\"batchId\":\"wrong\"}".into()),
            (200, "ACK_BATCH".into()),
            (200, "{\"ok\":true}".into()),
            (503, "{\"error\":\"session_required\"}".into()),
            (200, accepted),
            (200, "{\"ok\":true}".into()),
        ])
        .await;
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let token = root.path().join("token");
        std::fs::write(&token, "A".repeat(43)).unwrap();
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
        let config = Config {
            instance_id: "11111111-1111-4111-8111-111111111111".into(),
            stable_instance_key: "primary".into(),
            rpc_url: url,
            credential_file: token,
            spool_directory: root.path().join("spool"),
            heartbeat_millis: 30_000,
            snapshot_millis: 10_000,
        };
        let spool_root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(spool_root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut spool = super::super::outbox::Outbox::open(spool_root.path(), &config.instance_id, Default::default()).unwrap();
        spool
            .append(&super::super::outbox::Event::new(&config.instance_id, 1, "gallery.download.initialized", json!({"gid":123})).unwrap())
            .unwrap();
        let mut transport = Transport::new(&config).unwrap();
        assert!(transport.heartbeat(1, "active", None).await.is_err());
        transport.register(&identity).await.unwrap();
        assert!(transport.send_events(&spool.batch().unwrap()).await.is_err());
        assert_eq!(spool.depth(), 1);
        let records = spool.batch().unwrap();
        transport.send_events(&records).await.unwrap();
        spool.acknowledge(&records).unwrap();
        assert_eq!(spool.depth(), 0);
        let sampled_at = "2026-11-07T00:01:02Z".parse().unwrap();
        transport.telemetry(8, sampled_at, &json!({"counter":123})).await.unwrap();
        assert!(transport.heartbeat(2, "active", None).await.is_err());
        assert!(transport.heartbeat(3, "active", None).await.is_err());
        transport.register(&identity).await.unwrap();
        transport.heartbeat(4, "active", None).await.unwrap();
        let requests = task.await.unwrap();
        assert_eq!(requests.len(), 7);
        let telemetry: serde_json::Value = serde_json::from_str(requests[3].split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(telemetry["timestamp"], "2026-11-07T00:01:02Z");
        assert!(requests[0].contains("\"peerKind\":\"rust_native\""));
        assert!(
            requests[1]
                .to_ascii_lowercase()
                .contains("x-hath-agent-session: 0123456789abcdef0123456789abcdef")
        );
        assert!(
            requests
                .iter()
                .all(|request| request.contains(&format!("Bearer {}", "A".repeat(43))))
        );
    }
}
