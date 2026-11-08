//! Actual executable, local TLS/control/Manager mocks, and throwaway synthetic credentials.
#![cfg(all(target_os = "linux", feature = "manager-integration"))]
use aws_lc_rs::digest;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{Response, StatusCode, Uri},
    routing::{get, post},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Notify, mpsc},
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

const KEY: &str = "BBBBBBBBBBBBBBBBBBBB";
const INSTANCE: &str = "11111111-1111-4111-8111-111111111111";
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    NoTelemetry,
    Standalone,
    Normal,
    Degraded,
    BadCertificate,
    RejectedStart,
}
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct Mock {
    address: String,
    port: u16,
    cert: Vec<u8>,
    mode: Mode,
    actions: Mutex<Vec<String>>,
    observations: mpsc::Sender<Value>,
    started: Notify,
    release_start: Notify,
}
fn sha(bytes: &[u8]) -> String {
    const_hex::encode(digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, bytes))
}
fn signed(body: &str) -> String {
    let input = if body.is_empty() {
        format!("0-{KEY}")
    } else {
        format!("0-{KEY}\n{body}")
    };
    format!("OK-0-{}{}{}", sha(input.as_bytes()), if body.is_empty() { "" } else { "\n" }, body)
}
fn certificate(root: &Path) -> Vec<u8> {
    for args in [
        vec![
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-keyout",
            "key.pem",
            "-out",
            "cert.pem",
        ],
        vec![
            "pkcs12",
            "-export",
            "-inkey",
            "key.pem",
            "-in",
            "cert.pem",
            "-keypbe",
            "PBE-SHA1-3DES",
            "-certpbe",
            "PBE-SHA1-3DES",
            "-macalg",
            "sha1",
            "-out",
            "cert.p12",
            "-passout",
            "pass:BBBBBBBBBBBBBBBBBBBB",
        ],
    ] {
        let output = Command::new("openssl")
            .current_dir(root)
            .args(args)
            .output()
            .expect("OpenSSL is required for synthetic TLS test fixtures");
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    }
    std::fs::read(root.join("cert.p12")).unwrap()
}
async fn serve(listener: tokio::net::TcpListener, router: Router, stop: CancellationToken) {
    let mut tasks = JoinSet::new();
    loop {
        let accepted = tokio::select! {biased; _=stop.cancelled()=>break,result=listener.accept()=>result.unwrap()};
        let service = hyper_util::service::TowerToHyperService::new(router.clone());
        tasks.spawn(async move {
            let _ = hyper::server::conn::http1::Builder::new()
                .keep_alive(false)
                .serve_connection(hyper_util::rt::TokioIo::new(accepted.0), service)
                .await;
        });
        while tasks.try_join_next().is_some() {}
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}
async fn control(State(mock): State<Arc<Mock>>, uri: Uri) -> Response<Body> {
    let url = reqwest::Url::parse(&format!("http://localhost{uri}")).unwrap();
    let q: HashMap<_, _> = url.query_pairs().map(|(k, v)| (k.into_owned(), v.into_owned())).collect();
    let act = q["act"].as_str();
    let add = q["add"].as_str();
    assert_eq!(q["actkey"], sha(format!("hentai@home-{act}-{add}-1001-{}-{KEY}", q["acttime"]).as_bytes()));
    mock.actions.lock().unwrap().push(act.to_owned());
    if act == "get_cert" {
        return Response::new(Body::from(if mock.mode == Mode::BadCertificate {
            b"invalid-test-certificate".to_vec()
        } else {
            mock.cert.clone()
        }));
    }
    let body = match act {
        "server_stat" => signed(&format!("server_time={}\nmin_client_build=1\ncur_client_build=1", chrono::Utc::now().timestamp())),
        "client_login" | "client_settings" => signed(&format!(
            "port={}\nhost=127.0.0.1\nverify_cache=0\nstatic_ranges=\ndisklimit_bytes=1073741824\nthrottle_bytes=0\nrpc_server_ip=127.0.0.1",
            mock.port
        )),
        "client_start" => {
            mock.started.notify_one();
            mock.release_start.notified().await;
            if mock.mode == Mode::RejectedStart {
                "FAIL_STARTUP_MOCK".into()
            } else {
                signed("")
            }
        }
        "get_blacklist" | "still_alive" | "client_stop" => signed(""),
        "fetchqueue" if add.is_empty() => {
            let row = "1 10 org unknown jpg 001";
            format!(
                "GID 123\nTITLE Example Gallery\nMINXRES org\nFILECOUNT 1\nDLHASH {}\nFILELIST\n{row}\nINFORMATION\nSynthetic test gallery",
                sha(format!("{KEY}\n{row}").as_bytes())
            )
        }
        "fetchqueue" => "NO_PENDING_DOWNLOADS".into(),
        "dlfetch" => signed(&format!("http://{}/image", mock.address)),
        "srfetch" if add.starts_with("empty;") => signed(""),
        "srfetch" if add.starts_with("bad;") => signed(&format!("http://{}/bad-image", mock.address)),
        "srfetch" => signed(&format!("http://{}/image", mock.address)),
        other => panic!("unexpected mock action {other}"),
    };
    Response::new(Body::from(body))
}
async fn manager(State(mock): State<Arc<Mock>>, uri: Uri, bytes: Bytes) -> String {
    mock.actions.lock().unwrap().push(format!("manager:{}", uri.path()));
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["instanceId"], INSTANCE);
    match uri.path() {
        "/rpc/v1/agents/register" => {
            let mut capabilities = vec![
                "events.reliable.v1",
                "gallery.lifecycle.v2",
                "gallery.metadata.v2",
                "client.native.v1",
                "native.health.v1",
            ];
            if mock.mode != Mode::NoTelemetry {
                capabilities.push("telemetry.v1");
            }
            json!({"status":"registered","serverTime":chrono::Utc::now(),"managerProtocol":{"major":1,"minor":0},
            "selectedProtocol":{"major":1,"minor":0},"enabledCapabilities":capabilities,
            "outputProfile":"gallery-v2","sessionId":"0123456789abcdef0123456789abcdef","legacy":false})
            .to_string()
        }
        "/rpc/v1/events/batch" => {
            mock.observations.send(body.clone()).await.unwrap();
            json!({"ok":true,"batchId":body["batchId"]}).to_string()
        }
        "/rpc/v1/agents/heartbeat" => {
            mock.observations.send(body).await.unwrap();
            json!({"ok":true}).to_string()
        }
        "/rpc/v1/telemetry" => {
            mock.observations.send(body).await.unwrap();
            json!({"ok":true}).to_string()
        }
        other => panic!("unexpected Manager path {other}"),
    }
}
async fn next_health(rx: &mut mpsc::Receiver<Value>, ready: bool, integration: bool) -> Value {
    timeout(Duration::from_secs(10), async {
        loop {
            let value = rx.recv().await.unwrap();
            if value["nativeHealth"]["clientReady"] == ready && value["nativeHealth"]["integrationReady"] == integration {
                return value;
            }
        }
    })
    .await
    .unwrap()
}
async fn signal_and_wait(child: &mut ChildGuard) -> std::process::ExitStatus {
    let pid = rustix::process::Pid::from_raw(child.0.id() as i32).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::TERM).unwrap();
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                return status;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
async fn scenario(mode: Mode) {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    for name in ["data", "cache", "download", "tmp", "log", "spool"] {
        let path = root.path().join(name);
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let cert = certificate(root.path());
    let port_probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = port_probe.local_addr().unwrap().port();
    drop(port_probe);
    let control_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = control_listener.local_addr().unwrap().to_string();
    let manager_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/", manager_listener.local_addr().unwrap());
    let (tx, mut rx) = mpsc::channel(64);
    let mock = Arc::new(Mock {
        address: address.clone(),
        port,
        cert,
        mode,
        actions: Mutex::new(Vec::new()),
        observations: tx,
        started: Notify::new(),
        release_start: Notify::new(),
    });
    let stop = CancellationToken::new();
    let router = Router::new()
        .route("/16/rpc", get(control))
        .route("/16/dl", get(control))
        .route("/image", get(|| async { b"image-one".to_vec() }))
        .route("/bad-image", get(|| async { b"image-two".to_vec() }))
        .with_state(mock.clone());
    let control_task: JoinHandle<()> = tokio::spawn(serve(control_listener, router, stop.clone()));
    let router = Router::new()
        .route("/rpc/v1/agents/register", post(manager))
        .route("/rpc/v1/agents/heartbeat", post(manager))
        .route("/rpc/v1/events/batch", post(manager))
        .route("/rpc/v1/telemetry", post(manager))
        .with_state(mock.clone());
    let manager_task = tokio::spawn(serve(manager_listener, router, stop.clone()));
    let spool = root.path().join("spool");
    if mode == Mode::Degraded {
        std::fs::remove_dir(&spool).unwrap();
    }
    for (name, bytes) in [
        ("token", "A".repeat(43)),
        ("data/client_login", format!("1001-{KEY}")),
        (
            "runtime.json",
            json!({"instanceId":INSTANCE,"stableInstanceKey":"primary",
        "rpcUrl":endpoint,"credentialFile":root.path().join("token"),"spoolDirectory":spool,"heartbeatMillis":1000,"snapshotMillis":1000})
            .to_string(),
        ),
    ] {
        let path = root.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let mut command = Command::new(env!("CARGO_BIN_EXE_hath-rust"));
    command.current_dir(root.path());
    if mode != Mode::Standalone {
        command.arg("--manager-config").arg(root.path().join("runtime.json"));
    } else {
        std::fs::write(root.path().join("token"), "invalid-unused-credential").unwrap();
    }
    let mut child = ChildGuard(
        command
            .arg("--data-dir")
            .arg(root.path().join("data"))
            .args(["--host=127.0.0.1", "--disable-flood-control", "--rpc-server-ip", &address])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    if mode == Mode::BadCertificate {
        let status = timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = child.0.try_wait().unwrap() {
                    return status;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!status.success());
        assert!(!mock.actions.lock().unwrap().iter().any(|act| act == "client_start"));
    } else {
        timeout(Duration::from_secs(10), mock.started.notified()).await.unwrap();
        if mode != Mode::Standalone {
            next_health(&mut rx, false, mode != Mode::Degraded).await;
        }
        mock.release_start.notify_one();
        if mode == Mode::RejectedStart {
            let status = timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(status) = child.0.try_wait().unwrap() {
                        return status;
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert!(!status.success());
        } else {
            if mode != Mode::Standalone {
                next_health(&mut rx, true, mode != Mode::Degraded).await;
            }
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            let http = reqwest::ClientBuilder::new()
                .tls_backend_rustls()
                .tls_danger_accept_invalid_certs(true)
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap();
            let time = chrono::Utc::now().timestamp();
            let url = format!(
                "https://127.0.0.1:{port}/servercmd/start_downloader/none/{time}/{}",
                sha(format!("hentai@home-servercmd-start_downloader-none-1001-{time}-{KEY}").as_bytes())
            );
            assert_eq!(http.get(url).send().await.unwrap().status(), StatusCode::OK);
            if mode == Mode::Normal || mode == Mode::NoTelemetry {
                let terminal = timeout(Duration::from_secs(10), async {
                    loop {
                        let body = rx.recv().await.unwrap();
                        if let Some(event) = body["events"]
                            .as_array()
                            .and_then(|events| events.iter().find(|event| event["eventType"] == "gallery.download.completed"))
                        {
                            return event.clone();
                        }
                    }
                })
                .await
                .unwrap();
                let path = terminal["payload"]["metadataPath"].as_str().unwrap();
                let bytes = std::fs::read(path).unwrap();
                let hash = const_hex::encode(digest::digest(&digest::SHA256, &bytes));
                assert_eq!(terminal["payload"]["metadataSha256"], hash);
                let metadata: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(metadata["schemaVersion"], 2);
                assert_eq!(metadata["status"], "completed");
                assert_eq!(metadata["files"].as_array().unwrap().len(), 1);
                if mode == Mode::NoTelemetry {
                    assert!(
                        !mock
                            .actions
                            .lock()
                            .unwrap()
                            .iter()
                            .any(|action| action == "manager:/rpc/v1/telemetry")
                    );
                } else {
                    exercise_image_http(&http, port, root.path()).await;
                    let payload = wait_for_telemetry(&mut rx).await;
                    assert_eq!(payload["nativeOperations"]["rpc.client_login"]["completed"], 1);
                    assert_eq!(payload["nativeOperations"]["servercmd.start_downloader"]["completed"], 1);
                    assert_eq!(payload["nativeCacheScanComplete"], true);
                    // Discard pre-HTTP telemetry queued while exercising the routes.
                    let payload = if payload["filesSent"].as_u64().unwrap_or(0) < 2 {
                        timeout(Duration::from_secs(5), async {
                            loop {
                                let message = rx.recv().await.unwrap();
                                if message["payload"]["filesSent"].as_u64().unwrap_or(0) >= 2 {
                                    break message["payload"].clone();
                                }
                            }
                        })
                        .await
                        .unwrap()
                    } else {
                        payload
                    };
                    assert_eq!(payload["filesSent"], 2);
                    assert_eq!(payload["filesReceived"], 2);
                    assert_eq!(payload["nativeHttpVersion"], 1);
                    assert!(payload.get("clientSuspended").is_none());
                    assert!(!payload.to_string().contains(KEY));
                    if let Ok(path) = std::env::var("HATH_NATIVE_TELEMETRY_CAPTURE_PATH") {
                        std::fs::write(path, serde_json::to_vec_pretty(&payload).unwrap()).unwrap();
                    }
                }
            } else if mode == Mode::Standalone {
                timeout(Duration::from_secs(10), async {
                    loop {
                        if mock
                            .actions
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|action| action.as_str() == "fetchqueue")
                            .count()
                            >= 2
                        {
                            break;
                        }
                        sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                let directory = root.path().join("download/Example Gallery [123]");
                assert_eq!(std::fs::read(directory.join("001.jpg")).unwrap(), b"image-one");
                assert!(!directory.join(".hath-manager.json").exists());
                assert_eq!(std::fs::read_dir(&spool).unwrap().count(), 0);
                assert!(!mock.actions.lock().unwrap().iter().any(|action| action.starts_with("manager:")));
            } else {
                let payload = wait_for_telemetry(&mut rx).await;
                assert_eq!(payload["nativePersistenceHealthy"], false);
                assert!(payload.get("agentSpoolBytes").is_none());
                assert_eq!(std::fs::read_dir(root.path().join("download")).unwrap().count(), 0);
                assert!(!mock.actions.lock().unwrap().iter().any(|act| act == "dlfetch"));
            }
            assert!(signal_and_wait(&mut child).await.success());
            assert_eq!(
                mock.actions
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|act| act.as_str() == "client_stop")
                    .count(),
                1
            );
        }
    }
    for name in ["data", "spool"] {
        let path = root.path().join(name);
        if path.exists() {
            let fd = std::fs::File::open(path).unwrap();
            rustix::fs::flock(&fd, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
        }
    }
    stop.cancel();
    control_task.await.unwrap();
    manager_task.await.unwrap();
    if mode == Mode::Normal {
        for name in ["log/log_out", "log/log_err"] {
            let text = std::fs::read_to_string(root.path().join(name)).unwrap();
            assert!(!text.contains(KEY));
            assert!(!text.contains("keystamp"));
            assert!(!text.contains("127.0.0.1"));
        }
    }
}

async fn exercise_image_http(http: &reqwest::Client, port: u16, root: &Path) {
    let hash = sha(b"image-one");
    let id = format!("{hash}-9-jpg");
    let url = |id: &str, index: &str| {
        let time = chrono::Utc::now().timestamp();
        format!(
            "https://127.0.0.1:{port}/h/{id}/keystamp={time}-{};fileindex={index}/secret-name.jpg",
            sha(format!("{time}-{id}-{KEY}-hotlinkthis").as_bytes())
        )
    };
    let path = root.join("cache").join(&hash[..2]).join(&hash[2..4]).join(&id);
    let first = http.get(url(&id, "valid")).send().await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.bytes().await.unwrap().as_ref(), b"image-one");
    timeout(Duration::from_secs(5), async {
        while !path.exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // Keep the file old so this read exercises full SHA verification, not the recent-file fast path.
    filetime::set_file_mtime(&path, filetime::FileTime::from_unix_time(1, 0)).unwrap();
    let second = http.get(url(&id, "valid")).send().await.unwrap();
    let etag = second.headers()["etag"].clone();
    assert_eq!(second.bytes().await.unwrap().as_ref(), b"image-one");
    let head = http.head(url(&id, "valid")).send().await.unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert!(head.bytes().await.unwrap().is_empty());
    let range = http.get(url(&id, "valid")).header("Range", "bytes=2-4").send().await.unwrap();
    assert_eq!(range.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(range.bytes().await.unwrap().as_ref(), b"age");
    assert_eq!(
        http.get(url(&id, "valid"))
            .header("If-None-Match", etag)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_MODIFIED
    );
    let bad_id = format!("{}-9-jpg", sha(b"wrong-one"));
    let bad = http.get(url(&bad_id, "bad")).send().await.unwrap();
    assert!(bad.bytes().await.is_err());
    // Empty source sets are ordinary misses, never a worker panic or an infinite body.
    assert_eq!(http.get(url(&bad_id, "empty")).send().await.unwrap().status(), StatusCode::NOT_FOUND);
}
async fn wait_for_telemetry(rx: &mut mpsc::Receiver<Value>) -> Value {
    timeout(Duration::from_secs(5), async {
        loop {
            let message = rx.recv().await.unwrap();
            let payload = &message["payload"];
            if payload["nativeOperations"]["dispatch.start_downloader"]["completed"]
                .as_u64()
                .is_some_and(|count| count > 0)
            {
                assert_eq!(payload["nativeTelemetryVersion"], 1);
                return payload.clone();
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_cli_normal_start_gallery_handoff_and_shutdown() {
    scenario(Mode::Normal).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_cli_without_manager_config_stays_noop_and_preserves_upstream_layout() {
    scenario(Mode::Standalone).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_cli_required_health_and_gallery_do_not_require_optional_telemetry() {
    scenario(Mode::NoTelemetry).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_cli_degraded_runtime_keeps_tls_service_but_never_downloads_images() {
    scenario(Mode::Degraded).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_cli_certificate_failure_cleans_up_without_claiming_upstream_start() {
    scenario(Mode::BadCertificate).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_cli_rejected_upstream_start_closes_the_already_bound_http_server() {
    scenario(Mode::RejectedStart).await;
}
