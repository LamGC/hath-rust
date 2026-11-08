//! Process-lifetime owner. Configuration errors and competing writers are fatal; an
//! unavailable durable outbox instead pauses Gallery and reports health without it.
use super::{
    config::{Config, invalid, open_no_follow, read_private},
    observation::ObservationHandle,
    protocol::{NativeHealth, NativeIdentity},
    runtime::{Backoff, IntegrationHandle, Runtime},
    telemetry::{FallbackSink, Sample, Sampler},
    transport::Transport,
};
use std::{
    fs::File,
    io,
    os::unix::fs::MetadataExt,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering::Relaxed},
    },
    time::Duration,
};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, sleep_until},
};

pub(crate) struct ManagedRuntime {
    observations: ObservationHandle,
    sampler: Option<Sampler>,
    snapshot_millis: u64,
    handle: IntegrationHandle,
    runtime: Option<Runtime>,
    degraded: Option<DegradedReporter>,
    // Retained even in degraded mode. Two clients must not touch one profile if
    // one of them cannot acquire/open the outbox.
    _profile: File,
}

impl ManagedRuntime {
    pub(crate) async fn start(path: &Path, data: &Path) -> io::Result<(Self, (i32, String))> {
        let path = path.to_owned();
        let data = data.to_owned();
        let (config, profile, credentials, identity) = tokio::task::spawn_blocking(move || {
            let config = Config::read(&path)?;
            // Invalid secrets are admission failures, not reasons to run untracked.
            let _ = config.token()?;
            let profile = open_no_follow(&data)?;
            let meta = profile.metadata()?;
            if !meta.is_dir() || meta.uid() != rustix::process::geteuid().as_raw() || meta.mode() & 0o7777 != 0o700 {
                return Err(invalid("manager_profile_invalid"));
            }
            rustix::fs::flock(&profile, rustix::fs::FlockOperation::NonBlockingLockExclusive)
                .map_err(|_| invalid("manager_profile_busy"))?;
            let bytes = read_private(&data.join("client_login"), 128)?;
            let text = std::str::from_utf8(&bytes)
                .map_err(|_| invalid("manager_client_credentials_invalid"))?
                .trim_end_matches(['\r', '\n']);
            let (id, key) = text.split_once('-').ok_or_else(|| invalid("manager_client_credentials_invalid"))?;
            let number: i32 = id.parse().map_err(|_| invalid("manager_client_credentials_invalid"))?;
            if number < 1000
                || !id.bytes().all(|c| c.is_ascii_digit())
                || key.len() != 20
                || !key.bytes().all(|c| c.is_ascii_alphanumeric())
            {
                return Err(invalid("manager_client_credentials_invalid"));
            }
            Ok::<_, io::Error>((config, profile, (number, key.to_owned()), super::identity()?))
        })
        .await
        .map_err(|_| invalid("manager_startup_worker"))??;
        Self::start_prepared(config, profile, credentials, identity).await
    }

    async fn start_prepared(
        config: Config,
        profile: File,
        credentials: (i32, String),
        identity: NativeIdentity,
    ) -> io::Result<(Self, (i32, String))> {
        let snapshot_millis = config.snapshot_millis;
        match Runtime::start(config.clone(), identity.clone()).await {
            Ok(runtime) => Ok((
                Self {
                    observations: ObservationHandle::new(),
                    sampler: None,
                    snapshot_millis,
                    handle: runtime.handle(),
                    runtime: Some(runtime),
                    degraded: None,
                    _profile: profile,
                },
                credentials,
            )),
            Err(error) if error.to_string() == "manager_spool_busy" => Err(error),
            Err(_) => {
                let degraded = DegradedReporter::start(config, identity).await?;
                Ok((
                    Self {
                        observations: ObservationHandle::new(),
                        sampler: None,
                        snapshot_millis,
                        handle: IntegrationHandle::unavailable(),
                        runtime: None,
                        degraded: Some(degraded),
                        _profile: profile,
                    },
                    credentials,
                ))
            }
        }
    }

    pub(crate) fn handle(&self) -> IntegrationHandle {
        self.handle.clone()
    }
    pub(crate) fn observations(&self) -> ObservationHandle {
        self.observations.clone()
    }
    pub(crate) fn observe(&mut self, metrics: std::sync::Weak<crate::metrics::Metrics>, client: std::sync::Weak<crate::rpc::RPCClient>) {
        if self.sampler.is_none() {
            self.sampler = Some(Sampler::start(
                metrics,
                client,
                self.observations.clone(),
                self.handle(),
                self.degraded.as_ref().map(|reporter| FallbackSink {
                    snapshots: reporter.snapshots.clone(),
                    link: reporter.link.clone(),
                }),
                self.snapshot_millis,
            ));
        }
    }
    pub(crate) fn is_degraded(&self) -> bool {
        self.degraded.is_some()
    }
    pub(crate) fn upstream_ready(&self) {
        self.handle.upstream_ready();
        if let Some(reporter) = &self.degraded {
            reporter.phase.store(1, Relaxed);
        }
    }
    pub(crate) fn stopping(&self) {
        self.handle.stopping();
        if let Some(reporter) = &self.degraded {
            reporter.phase.store(2, Relaxed);
        }
    }
    pub(crate) async fn shutdown(mut self) -> io::Result<()> {
        self.stopping();
        if let Some(sampler) = self.sampler.take() {
            sampler.shutdown().await;
        }
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown().await?;
        }
        if let Some(reporter) = self.degraded.take() {
            reporter.shutdown().await?;
        }
        Ok(())
    }
}

struct DegradedReporter {
    snapshots: watch::Sender<Option<Arc<Sample>>>,
    link: Arc<AtomicBool>,
    phase: Arc<AtomicU8>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}
impl DegradedReporter {
    async fn start(config: Config, identity: NativeIdentity) -> io::Result<Self> {
        let heartbeat = config.heartbeat_interval().min(Duration::from_secs(30));
        let snapshot_interval = Duration::from_millis(config.snapshot_millis);
        let mut transport = tokio::task::spawn_blocking(move || Transport::new(&config))
            .await
            .map_err(|_| invalid("manager_transport_worker"))??;
        let phase = Arc::new(AtomicU8::new(0));
        let observed_phase = phase.clone();
        let (stop, mut cancelled) = watch::channel(false);
        let (snapshots, latest) = watch::channel::<Option<Arc<Sample>>>(None);
        let link = Arc::new(AtomicBool::new(false));
        let link_observation = link.clone();
        let task = tokio::spawn(async move {
            let mut sequence = 0i64;
            let mut health_sequence = 0i64;
            let mut backoff = 1000u64;
            let mut heartbeat_at = Instant::now();
            let mut snapshot_at = Instant::now();
            let mut sent = None;
            let mut telemetry_backoff = Backoff(1000);
            loop {
                let operation = async {
                    if !transport.has_session() {
                        transport.register(&identity).await?;
                        heartbeat_at = Instant::now();
                        snapshot_at = Instant::now();
                        sent = None;
                    }
                    if Instant::now() >= heartbeat_at {
                        let health = NativeHealth {
                            health_sequence,
                            observed_at: chrono::Utc::now(),
                            integration_ready: false,
                            persistence_healthy: false,
                            client_ready: observed_phase.load(Relaxed) == 1,
                        };
                        health_sequence = health_sequence
                            .checked_add(1)
                            .ok_or_else(|| invalid("manager_health_sequence_exhausted"))?;
                        let current = sequence;
                        sequence = sequence.checked_add(1).ok_or_else(|| invalid("manager_sequence_exhausted"))?;
                        transport.heartbeat(current, "degraded", Some(&health)).await?;
                        heartbeat_at = Instant::now() + heartbeat;
                    }
                    if Instant::now() >= snapshot_at {
                        snapshot_at = Instant::now() + snapshot_interval;
                        let sample = latest.borrow().clone();
                        if let Some(sample) = sample.filter(|sample| Some(sample.revision) != sent) {
                            let current = sequence;
                            sequence = sequence.checked_add(1).ok_or_else(|| invalid("manager_sequence_exhausted"))?;
                            match transport.telemetry(current, sample.timestamp, &sample.payload).await {
                                Ok(()) => {
                                    sent = Some(sample.revision);
                                    telemetry_backoff = Backoff(1000);
                                }
                                Err(error) => {
                                    snapshot_at = Instant::now() + telemetry_backoff.next();
                                    if !transport.has_session() {
                                        return Err(error);
                                    }
                                }
                            }
                        }
                    }
                    Ok::<_, io::Error>(())
                };
                let outcome = tokio::select! { biased; _ = cancelled.wait_for(|stop| *stop) => break, result = operation => result };
                link_observation.store(outcome.is_ok(), Relaxed);
                let delay = if outcome.is_ok() {
                    backoff = 1000;
                    heartbeat_at.min(snapshot_at).saturating_duration_since(Instant::now())
                } else {
                    let delay = Duration::from_millis(fastrand::u64(backoff * 3 / 4..=backoff));
                    backoff = (backoff * 2).min(60_000);
                    delay
                };
                let until = Instant::now() + delay;
                tokio::select! { biased; _ = cancelled.wait_for(|stop| *stop) => break, _ = sleep_until(until) => () }
            }
        });
        Ok(Self {
            snapshots,
            link,
            phase,
            stop,
            task: Some(task),
        })
    }
    async fn shutdown(mut self) -> io::Result<()> {
        self.stop.send_replace(true);
        self.task.take().unwrap().await.map_err(|_| invalid("manager_degraded_worker"))
    }
}
impl Drop for DegradedReporter {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Bytes, extract::State, routing::post};
    use serde_json::{Value, json};
    use std::{os::unix::fs::PermissionsExt, path::PathBuf};
    use tokio::{sync::mpsc, time::timeout};
    use tokio_util::sync::CancellationToken;

    struct Fixture {
        _root: tempfile::TempDir,
        data: PathBuf,
        config: PathBuf,
        spool: PathBuf,
    }
    fn fixture(url: &str) -> Fixture {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let data = root.path().join("data");
        let spool = root.path().join("spool");
        for path in [&data, &spool] {
            std::fs::create_dir(path).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let token = root.path().join("token");
        let login = data.join("client_login");
        let config = root.path().join("config.json");
        for (path, bytes) in [
            (&token, "A".repeat(43)),
            (&login, format!("1001-{}", "B".repeat(20))),
            (
                &config,
                json!({"instanceId":"11111111-1111-4111-8111-111111111111","stableInstanceKey":"primary",
                "rpcUrl":url,"credentialFile":token,"spoolDirectory":spool,"heartbeatMillis":1000,"snapshotMillis":1000})
                .to_string(),
            ),
        ] {
            std::fs::write(path, bytes).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        Fixture {
            _root: root,
            data,
            config,
            spool,
        }
    }
    async fn manager() -> (String, mpsc::Receiver<Value>, CancellationToken, JoinHandle<()>) {
        async fn registration() -> String {
            json!({"status":"registered","serverTime":chrono::Utc::now(),"managerProtocol":{"major":1,"minor":0},
                "selectedProtocol":{"major":1,"minor":0},"enabledCapabilities":super::super::protocol::Offer::native().capabilities,
                "outputProfile":"gallery-v2","sessionId":"0123456789abcdef0123456789abcdef","legacy":false})
            .to_string()
        }
        async fn heartbeat(State(tx): State<mpsc::Sender<Value>>, body: Bytes) -> String {
            let _ = tx.send(serde_json::from_slice(&body).unwrap()).await;
            json!({"ok":true}).to_string()
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let (tx, rx) = mpsc::channel(16);
        let router = Router::new()
            .route("/rpc/v1/agents/register", post(registration))
            .route("/rpc/v1/agents/heartbeat", post(heartbeat))
            .with_state(tx);
        let stop = CancellationToken::new();
        let signal = stop.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let accepted = tokio::select! { biased; _ = signal.cancelled() => break, result = listener.accept() => result.unwrap() };
                let service = hyper_util::service::TowerToHyperService::new(router.clone());
                connections.spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .keep_alive(false)
                        .serve_connection(hyper_util::rt::TokioIo::new(accepted.0), service)
                        .await;
                });
            }
            while let Some(result) = connections.join_next().await {
                result.unwrap();
            }
        });
        (url, rx, stop, task)
    }

    #[tokio::test]
    async fn outbox_failure_reports_degraded_health_and_keeps_profile_fenced() {
        let (url, mut health, stop, server) = manager().await;
        let f = fixture(&url);
        std::fs::remove_dir(&f.spool).unwrap();
        let (owner, credentials) = ManagedRuntime::start(&f.config, &f.data).await.unwrap();
        assert_eq!(credentials.0, 1001);
        assert_eq!(credentials.1, "B".repeat(20));
        assert!(owner.handle().enabled());
        assert!(owner.handle().event("gallery.download.initialized", json!({})).is_err());
        let initial = timeout(Duration::from_secs(5), health.recv()).await.unwrap().unwrap();
        assert_eq!(initial["status"], "degraded");
        assert_eq!(initial["nativeHealth"]["integrationReady"], false);
        assert_eq!(initial["nativeHealth"]["persistenceHealthy"], false);
        assert_eq!(initial["nativeHealth"]["clientReady"], false);
        assert_eq!(ManagedRuntime::start(&f.config, &f.data).await.err().unwrap().to_string(), "manager_profile_busy");
        owner.upstream_ready();
        let active = timeout(Duration::from_secs(3), health.recv()).await.unwrap().unwrap();
        assert_eq!(active["nativeHealth"]["clientReady"], true);
        assert_eq!(active["nativeHealth"]["integrationReady"], false);
        assert!(active["nativeHealth"]["healthSequence"].as_i64() > initial["nativeHealth"]["healthSequence"].as_i64());
        timeout(Duration::from_secs(2), owner.shutdown()).await.unwrap().unwrap();
        let profile = File::open(&f.data).unwrap();
        rustix::fs::flock(&profile, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
        stop.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn competing_outbox_writer_is_fatal_not_a_degraded_second_client() {
        let f = fixture("http://127.0.0.1:1/");
        let writer = File::open(&f.spool).unwrap();
        rustix::fs::flock(&writer, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
        assert_eq!(ManagedRuntime::start(&f.config, &f.data).await.err().unwrap().to_string(), "manager_spool_busy");
        assert_eq!(std::fs::read_dir(&f.spool).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn credentials_are_mandatory_and_never_prompt_or_leak_the_key() {
        let f = fixture("http://127.0.0.1:1/");
        let login = f.data.join("client_login");
        for contents in [
            "1001-secret",
            "999-BBBBBBBBBBBBBBBBBBBB",
            "1001-BBBBBBBBBBBBBBBBBBBB-extra",
            "1001-BBBBBBBBBBBBBBBBBBBB\nsecret",
        ] {
            std::fs::write(&login, contents).unwrap();
            let error = ManagedRuntime::start(&f.config, &f.data).await.err().unwrap().to_string();
            assert_eq!(error, "manager_client_credentials_invalid");
            assert!(!error.contains("secret"));
        }
        std::fs::remove_file(login).unwrap();
        assert!(ManagedRuntime::start(&f.config, &f.data).await.is_err());
        assert_eq!(std::fs::read_dir(&f.spool).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn normal_runtime_readiness_requires_explicit_upstream_confirmation_and_drains_on_exit() {
        let (url, mut health, stop, server) = manager().await;
        let f = fixture(&url);
        let (owner, _) = ManagedRuntime::start(&f.config, &f.data).await.unwrap();
        let initial = timeout(Duration::from_secs(5), health.recv()).await.unwrap().unwrap();
        assert_eq!(initial["nativeHealth"]["integrationReady"], true);
        assert_eq!(initial["nativeHealth"]["clientReady"], false);
        owner.upstream_ready();
        let active = timeout(Duration::from_secs(2), health.recv()).await.unwrap().unwrap();
        assert_eq!(active["nativeHealth"]["clientReady"], true);
        let handle = owner.handle();
        let event = handle.event("gallery.download.initialized", json!({})).unwrap().unwrap();
        handle.persist(&event).await.unwrap();
        timeout(Duration::from_secs(2), owner.shutdown()).await.unwrap().unwrap();
        // No events endpoint: unsent durable data survives and the root lock is released.
        assert_eq!(
            std::fs::read_dir(f.spool.join("11111111-1111-4111-8111-111111111111"))
                .unwrap()
                .count(),
            1
        );
        let spool = File::open(f.spool).unwrap();
        rustix::fs::flock(&spool, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
        stop.cancel();
        server.await.unwrap();
    }
}
