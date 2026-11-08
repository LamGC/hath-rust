//! Bounded persistence and sending engines. No Gallery or HTTP hook performs filesystem I/O.
//! The process owner joins both engines only after Gallery writers have quiesced.
use super::{
    config::{Config, invalid},
    outbox::{Event, Limits, Outbox, Record, Reservation},
    protocol::{self, NativeIdentity},
    transport::Transport,
};
use serde_json::Value;
use std::{
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, AtomicUsize, Ordering::Relaxed},
    },
    thread,
    time::Duration,
};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    time::{Instant, sleep, sleep_until},
};

const PENDING_RECORDS: usize = 64;
const PENDING_BYTES: usize = 256 * 1024;
const MAX_SNAPSHOT: usize = 60 * 1024;

#[derive(Clone, Copy)]
pub(super) enum Phase {
    Starting = 0,
    Active = 1,
    Stopping = 2,
}

#[derive(Clone, Copy)]
pub(super) struct Health {
    pub link_healthy: bool,
    pub persistence_healthy: bool,
    pub quarantined: usize,
    pub outbox_depth: usize,
}

struct State {
    instance: String,
    sequence: AtomicI64,
    health_sequence: AtomicI64,
    health_dirty: AtomicBool,
    phase: AtomicU8,
    link_healthy: AtomicBool,
    persistence_healthy: AtomicBool,
    quarantined: AtomicUsize,
    outbox_depth: AtomicUsize,
    outbox_bytes: AtomicU64,
    write_failures: AtomicU64,
    telemetry_failures: AtomicU64,
    wake: Notify,
    snapshot: watch::Sender<Option<Arc<Snapshot>>>,
}
struct PersistenceHealth(Arc<State>);
impl Drop for PersistenceHealth {
    fn drop(&mut self) {
        self.0.persistence_healthy.store(false, Relaxed);
        self.0.health_dirty.store(true, Relaxed);
        self.0.wake.notify_one();
    }
}
impl State {
    fn native_health(&self) -> io::Result<protocol::NativeHealth> {
        let sequence = self
            .health_sequence
            .try_update(Relaxed, Relaxed, |value| value.checked_add(1))
            .map_err(|_| invalid("manager_health_sequence_exhausted"))?;
        Ok(protocol::NativeHealth {
            health_sequence: sequence,
            observed_at: chrono::Utc::now(),
            integration_ready: true,
            persistence_healthy: self.persistence_healthy.load(Relaxed) && self.quarantined.load(Relaxed) == 0,
            client_ready: self.phase.load(Relaxed) == Phase::Active as u8,
        })
    }
    fn sequence(&self) -> io::Result<i64> {
        self.sequence
            .try_update(Relaxed, Relaxed, |value| value.checked_add(1))
            .map_err(|_| invalid("manager_sequence_exhausted"))
    }
    fn status(&self) -> &'static str {
        if !self.persistence_healthy.load(Relaxed) || self.quarantined.load(Relaxed) != 0 {
            "degraded"
        } else {
            match self.phase.load(Relaxed) {
                1 => "active",
                2 => "stopping",
                _ => "starting",
            }
        }
    }
}

struct Snapshot {
    sequence: i64,
    timestamp: chrono::DateTime<chrono::Utc>,
    payload: Value,
}

#[derive(Clone, Default)]
pub(crate) struct IntegrationHandle {
    inner: Option<Arc<Enabled>>,
    unavailable: bool,
}
struct Enabled {
    state: Arc<State>,
    storage: Storage,
}
impl IntegrationHandle {
    pub(super) fn telemetry_failures(&self) -> Option<u64> {
        self.inner.as_ref().map(|enabled| enabled.state.telemetry_failures.load(Relaxed))
    }
    pub(super) fn storage_snapshot(&self) -> Option<(u64, u64, u64, u64, u64)> {
        self.inner.as_ref().map(|enabled| {
            (
                enabled.state.outbox_depth.load(Relaxed) as u64,
                enabled.state.outbox_bytes.load(Relaxed),
                enabled.state.quarantined.load(Relaxed) as u64,
                enabled.state.write_failures.load(Relaxed),
                (PENDING_RECORDS - enabled.storage.records.available_permits()) as u64,
            )
        })
    }
    // Explicit fail-closed mode for a configured integration whose runtime could not start.
    // It is not Noop: Gallery operations return backpressure instead of acknowledging unrecorded work.
    pub(crate) fn unavailable() -> Self {
        Self {
            inner: None,
            unavailable: true,
        }
    }

    fn ensure_available(&self) -> io::Result<()> {
        if self.unavailable {
            Err(invalid("manager_runtime_unavailable"))
        } else {
            Ok(())
        }
    }

    pub fn enabled(&self) -> bool {
        self.inner.is_some() || self.unavailable
    }

    pub(super) fn health(&self) -> Option<Health> {
        if self.unavailable {
            return Some(Health {
                link_healthy: false,
                persistence_healthy: false,
                quarantined: 0,
                outbox_depth: 0,
            });
        }
        self.inner.as_ref().map(|enabled| Health {
            link_healthy: enabled.state.link_healthy.load(Relaxed),
            persistence_healthy: enabled.state.persistence_healthy.load(Relaxed),
            quarantined: enabled.state.quarantined.load(Relaxed),
            outbox_depth: enabled.state.outbox_depth.load(Relaxed),
        })
    }

    pub(super) fn phase(&self, phase: Phase) {
        if let Some(enabled) = &self.inner {
            enabled.state.phase.store(phase as u8, Relaxed);
            enabled.state.health_dirty.store(true, Relaxed);
            enabled.state.wake.notify_one();
        }
    }

    /// Called only after EH's upstream startup confirmation succeeds, never on local HTTP bind/Manager registration.
    pub(crate) fn upstream_ready(&self) {
        self.phase(Phase::Active);
    }

    pub(crate) fn stopping(&self) {
        self.phase(Phase::Stopping);
    }

    // The future Gallery façade owns event creation/journaling, so retry uses the same Event.
    pub(super) fn event(&self, kind: &str, payload: Value) -> io::Result<Option<Event>> {
        self.ensure_available()?;
        self.inner
            .as_ref()
            .map(|enabled| Event::new(&enabled.state.instance, enabled.state.sequence()?, kind, payload))
            .transpose()
    }

    // Success means fsync + rename + directory sync, not just acceptance by an in-memory queue.
    pub(super) async fn persist(&self, event: &Event) -> io::Result<()> {
        self.ensure_available()?;
        if let Some(enabled) = &self.inner {
            if event.instance_id != enabled.state.instance {
                return Err(invalid("manager_event_instance"));
            }
            enabled.storage.persist(event.encode()?).await?;
            enabled.state.wake.notify_one();
        }
        Ok(())
    }

    pub(super) async fn reserve(&self, initialized: Reservation, terminal: Reservation) -> io::Result<()> {
        self.ensure_available()?;
        if let Some(enabled) = &self.inner {
            let (reply, response) = oneshot::channel();
            enabled.storage.enqueue(Command::Reserve([initialized, terminal], reply))?;
            response.await.map_err(|_| invalid("manager_persistence_stopped"))??;
        }
        Ok(())
    }

    pub(super) fn persistence_failed(&self) {
        if let Some(enabled) = &self.inner {
            enabled.state.persistence_healthy.store(false, Relaxed);
            enabled.state.health_dirty.store(true, Relaxed);
            enabled.state.wake.notify_one();
        }
    }

    pub(super) fn snapshot(&self, payload: Value) -> io::Result<()> {
        self.ensure_available()?;
        if let Some(enabled) = &self.inner {
            if !payload.is_object() {
                return Err(invalid("manager_telemetry_invalid"));
            }
            let bytes = serde_json::to_vec(&payload).map_err(|_| invalid("manager_telemetry_invalid"))?;
            if bytes.len() > MAX_SNAPSHOT {
                return Err(invalid("manager_telemetry_size"));
            }
            let payload = protocol::decode(&bytes).map_err(invalid)?;
            enabled.state.snapshot.send_replace(Some(Arc::new(Snapshot {
                sequence: enabled.state.sequence()?,
                timestamp: chrono::Utc::now(),
                payload,
            })));
        }
        Ok(())
    }
}

pub(crate) struct Runtime {
    handle: IntegrationHandle,
    stop: watch::Sender<bool>,
    sender: Option<tokio::task::JoinHandle<io::Result<()>>>,
    persistence: Option<thread::JoinHandle<()>>,
}
impl Runtime {
    pub(super) async fn start(config: Config, identity: NativeIdentity) -> io::Result<Self> {
        Self::start_with_limits(config, identity, Limits::default()).await
    }

    pub(super) async fn start_with_limits(config: Config, identity: NativeIdentity, limits: Limits) -> io::Result<Self> {
        let (snapshot, receiver) = watch::channel(None);
        let state = Arc::new(State {
            instance: config.instance_id.clone(),
            sequence: AtomicI64::new(0),
            health_sequence: AtomicI64::new(0),
            health_dirty: AtomicBool::new(true),
            phase: AtomicU8::new(Phase::Starting as u8),
            link_healthy: AtomicBool::new(false),
            persistence_healthy: AtomicBool::new(false),
            quarantined: AtomicUsize::new(0),
            outbox_depth: AtomicUsize::new(0),
            outbox_bytes: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            telemetry_failures: AtomicU64::new(0),
            wake: Notify::new(),
            snapshot,
        });
        // native.health.v1 is required even when telemetry.v1 was not negotiated.
        let heartbeat = config.heartbeat_interval().min(Duration::from_secs(30));
        let snapshot = Duration::from_millis(config.snapshot_millis);
        let root = config.spool_directory.clone();
        // Private credential reads and TLS setup also stay off the shared async executor.
        let transport = tokio::task::spawn_blocking(move || Transport::new(&config))
            .await
            .map_err(|_| invalid("manager_transport_worker"))??;
        let (storage, persistence) = Storage::start(root, state.clone(), limits).await?;
        let handle = IntegrationHandle {
            inner: Some(Arc::new(Enabled {
                state: state.clone(),
                storage: storage.clone(),
            })),
            unavailable: false,
        };
        let (stop, cancellation) = watch::channel(false);
        let sender = tokio::spawn(async move {
            run(transport, &storage, &state, &identity, receiver, cancellation, heartbeat, snapshot).await;
            storage.close().await
        });
        Ok(Self {
            handle,
            stop,
            sender: Some(sender),
            persistence: Some(persistence),
        })
    }

    pub fn handle(&self) -> IntegrationHandle {
        self.handle.clone()
    }

    pub async fn shutdown(mut self) -> io::Result<()> {
        self.stop.send_replace(true);
        let result = self.sender.take().unwrap().await.map_err(|_| invalid("manager_sender_worker"));
        if result.is_err() {
            // Also close the disk worker if the async sender unexpectedly panicked.
            let _ = self.handle.inner.as_ref().unwrap().storage.close().await;
        }
        let persistence = self.persistence.take().unwrap();
        tokio::task::spawn_blocking(move || persistence.join())
            .await
            .map_err(|_| invalid("manager_persistence_worker"))?
            .map_err(|_| invalid("manager_persistence_worker"))?;
        result?
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        // Dropping the owner cancels HTTP and lets the sender close/drain the persistence queue.
        self.stop.send_replace(true);
    }
}

enum Command {
    Reserve([Reservation; 2], oneshot::Sender<io::Result<()>>),
    Persist {
        bytes: Vec<u8>,
        _budget: OwnedSemaphorePermit,
        _record: OwnedSemaphorePermit,
        reply: oneshot::Sender<io::Result<()>>,
    },
    Batch(oneshot::Sender<io::Result<Vec<Record>>>),
    Ack(Vec<Record>, oneshot::Sender<io::Result<()>>),
    Refresh(oneshot::Sender<io::Result<()>>),
    Stop(oneshot::Sender<()>),
}
#[derive(Clone)]
struct Storage {
    sender: mpsc::Sender<Command>,
    budget: Arc<Semaphore>,
    records: Arc<Semaphore>,
}
impl Storage {
    async fn start(root: PathBuf, state: Arc<State>, limits: Limits) -> io::Result<(Self, thread::JoinHandle<()>)> {
        let (sender, mut receiver) = mpsc::channel(PENDING_RECORDS);
        let (ready, initialized) = oneshot::channel();
        let persistence = thread::Builder::new().name("manager-persistence".into()).spawn(move || {
            let _health = PersistenceHealth(state.clone());
            let mut spool = match Outbox::open(&root, &state.instance, limits) {
                Ok(spool) => spool,
                Err(error) => {
                    let code = if error.kind() == io::ErrorKind::WouldBlock {
                        "manager_spool_busy"
                    } else {
                        "manager_spool_open"
                    };
                    let _ = ready.send(Err(invalid(code)));
                    return;
                }
            };
            match spool.next_sequence() {
                Ok(sequence) => state.sequence.store(sequence, Relaxed),
                Err(_) => {
                    let _ = ready.send(Err(invalid("manager_sequence_exhausted")));
                    return;
                }
            }
            state.persistence_healthy.store(!spool.write_blocked(), Relaxed);
            state.quarantined.store(spool.quarantined(), Relaxed);
            state.outbox_depth.store(spool.depth(), Relaxed);
            state.outbox_bytes.store(spool.bytes(), Relaxed);
            if ready.send(Ok(())).is_err() {
                return;
            }
            let mut stopped = None;
            while let Some(command) = receiver.blocking_recv() {
                match command {
                    Command::Reserve(slots, reply) => {
                        let result = spool.reserve(&slots[0]).and_then(|_| spool.reserve(&slots[1]));
                        if result.is_err() {
                            state.write_failures.fetch_add(1, Relaxed);
                        }
                        state.persistence_healthy.store(result.is_ok() && !spool.write_blocked(), Relaxed);
                        let _ = reply.send(result.map_err(|_| invalid("manager_spool_reserve")));
                    }
                    Command::Persist {
                        bytes,
                        reply,
                        _budget,
                        _record,
                    } => {
                        let result = spool.append_bytes(&bytes);
                        if result.is_err() {
                            state.write_failures.fetch_add(1, Relaxed);
                        }
                        state.persistence_healthy.store(result.is_ok() && !spool.write_blocked(), Relaxed);
                        state.outbox_depth.store(spool.depth(), Relaxed);
                        let _ = reply.send(result.map_err(|_| invalid("manager_spool_append")));
                    }
                    Command::Batch(reply) => {
                        let result = spool.batch();
                        // A successful read must not clear a previous write/fsync failure.
                        if result.is_err() {
                            state.persistence_healthy.store(false, Relaxed);
                        }
                        let _ = reply.send(result.map_err(|_| invalid("manager_spool_read")));
                    }
                    Command::Ack(records, reply) => {
                        let result = spool.acknowledge(&records);
                        if result.is_err() {
                            state.write_failures.fetch_add(1, Relaxed);
                        }
                        state.persistence_healthy.store(result.is_ok() && !spool.write_blocked(), Relaxed);
                        state.outbox_depth.store(spool.depth(), Relaxed);
                        let _ = reply.send(result.map_err(|_| invalid("manager_spool_ack")));
                    }
                    Command::Refresh(reply) => {
                        let result = spool.refresh_budget();
                        state.quarantined.store(spool.quarantined(), Relaxed);
                        if result.is_err() {
                            state.persistence_healthy.store(false, Relaxed);
                        }
                        let _ = reply.send(result.map_err(|_| invalid("manager_spool_budget")));
                    }
                    Command::Stop(reply) => {
                        receiver.close();
                        stopped = Some(reply);
                    }
                }
                state.outbox_depth.store(spool.depth(), Relaxed);
                state.outbox_bytes.store(spool.bytes(), Relaxed);
                state.quarantined.store(spool.quarantined(), Relaxed);
            }
            if let Some(reply) = stopped {
                let _ = reply.send(());
            }
        })?;
        match initialized.await {
            Ok(Ok(())) => (),
            result => {
                let _ = tokio::task::spawn_blocking(move || persistence.join()).await;
                return Err(result.unwrap_or_else(|_| Err(invalid("manager_persistence_worker"))).unwrap_err());
            }
        }
        Ok((
            Self {
                sender,
                budget: Arc::new(Semaphore::new(PENDING_BYTES)),
                records: Arc::new(Semaphore::new(PENDING_RECORDS)),
            },
            persistence,
        ))
    }

    async fn persist(&self, bytes: Vec<u8>) -> io::Result<()> {
        if bytes.is_empty() || bytes.len() > 16 * 1024 {
            return Err(invalid("manager_event_size"));
        }
        let budget = self
            .budget
            .clone()
            .try_acquire_many_owned(bytes.len() as u32)
            .map_err(|_| invalid("manager_persistence_backpressure"))?;
        let record = self
            .records
            .clone()
            .try_acquire_owned()
            .map_err(|_| invalid("manager_persistence_backpressure"))?;
        let (reply, response) = oneshot::channel();
        self.enqueue(Command::Persist {
            bytes,
            _budget: budget,
            _record: record,
            reply,
        })?;
        response.await.map_err(|_| invalid("manager_persistence_stopped"))?
    }
    async fn batch(&self) -> io::Result<Vec<Record>> {
        let (reply, response) = oneshot::channel();
        self.enqueue(Command::Batch(reply))?;
        response.await.map_err(|_| invalid("manager_persistence_stopped"))?
    }
    async fn ack(&self, records: Vec<Record>) -> io::Result<()> {
        let (reply, response) = oneshot::channel();
        self.enqueue(Command::Ack(records, reply))?;
        response.await.map_err(|_| invalid("manager_persistence_stopped"))?
    }
    async fn refresh(&self) -> io::Result<()> {
        let (reply, response) = oneshot::channel();
        self.enqueue(Command::Refresh(reply))?;
        response.await.map_err(|_| invalid("manager_persistence_stopped"))?
    }
    fn enqueue(&self, command: Command) -> io::Result<()> {
        self.sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => invalid("manager_persistence_backpressure"),
            mpsc::error::TrySendError::Closed(_) => invalid("manager_persistence_stopped"),
        })
    }
    async fn close(&self) -> io::Result<()> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Command::Stop(reply))
            .await
            .map_err(|_| invalid("manager_persistence_stopped"))?;
        response.await.map_err(|_| invalid("manager_persistence_stopped"))
    }
}

pub(super) struct Backoff(pub(super) u64);
impl Backoff {
    pub(super) fn next(&mut self) -> Duration {
        let millis = fastrand::u64(self.0 * 3 / 4..=self.0);
        self.0 = self.0.saturating_mul(2).min(60_000);
        Duration::from_millis(millis)
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    mut transport: Transport,
    storage: &Storage,
    state: &State,
    identity: &NativeIdentity,
    snapshots: watch::Receiver<Option<Arc<Snapshot>>>,
    mut stop: watch::Receiver<bool>,
    heartbeat: Duration,
    snapshot: Duration,
) {
    let mut heartbeat_at = Instant::now();
    let mut snapshot_at = Instant::now();
    let mut refresh_at = Instant::now() + Duration::from_secs(30);
    let mut backoff = Backoff(1000);
    let mut last_snapshot_sent = None;
    let mut telemetry_backoff = Backoff(1000);
    loop {
        let step = async {
            if !transport.has_session() {
                transport.register(identity).await?;
                heartbeat_at = Instant::now();
                snapshot_at = Instant::now();
                last_snapshot_sent = None;
            }
            let health_dirty = state.health_dirty.swap(false, Relaxed);
            if Instant::now() >= heartbeat_at || health_dirty {
                let health = state.native_health()?;
                let result = transport.heartbeat(state.sequence()?, state.status(), Some(&health)).await;
                state.link_healthy.store(result.is_ok(), Relaxed);
                result?;
                heartbeat_at = Instant::now() + heartbeat;
            }
            if Instant::now() >= snapshot_at {
                snapshot_at = Instant::now() + snapshot;
                let latest = snapshots.borrow().clone();
                if let Some(latest) = latest.filter(|value| Some(value.sequence) != last_snapshot_sent) {
                    match transport.telemetry(latest.sequence, latest.timestamp, &latest.payload).await {
                        Ok(()) => {
                            last_snapshot_sent = Some(latest.sequence);
                            telemetry_backoff = Backoff(1000);
                        }
                        Err(error) => {
                            let _ = state
                                .telemetry_failures
                                .try_update(Relaxed, Relaxed, |value| Some(value.saturating_add(1)));
                            snapshot_at = Instant::now() + telemetry_backoff.next();
                            // Optional telemetry cannot gate reliable delivery. Lost
                            // authorization/session still requires re-registration.
                            if !transport.has_session() {
                                return Err(error);
                            }
                        }
                    }
                }
            }
            if Instant::now() >= refresh_at {
                storage.refresh().await?;
                refresh_at = Instant::now() + Duration::from_secs(30);
            }
            let records = storage.batch().await?;
            if records.is_empty() {
                return Ok(false);
            }
            let result = transport.send_events(&records).await;
            state.link_healthy.store(result.is_ok(), Relaxed);
            result?;
            storage.ack(records).await?;
            Ok::<_, io::Error>(true)
        };
        let outcome = tokio::select! {
            biased;
            _ = stop.wait_for(|stop| *stop) => break,
            result = step => result,
        };
        match outcome {
            Err(_) => {
                // Registration can fail before the heartbeat path updates this flag.
                if !transport.has_session() {
                    state.link_healthy.store(false, Relaxed);
                }
                tokio::select! {
                    biased;
                    _ = stop.wait_for(|stop| *stop) => break,
                    _ = sleep(backoff.next()) => (),
                }
            }
            Ok(more) => {
                backoff = Backoff(1000);
                if more {
                    continue;
                }
                tokio::select! {
                    biased;
                    _ = stop.wait_for(|stop| *stop) => break,
                    _ = state.wake.notified() => (),
                    _ = sleep_until(heartbeat_at.min(snapshot_at).min(refresh_at)) => (),
                }
            }
        }
    }
    state.link_healthy.store(false, Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        time::timeout,
    };

    fn config(root: &std::path::Path, url: String) -> Config {
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let token = root.join("token");
        std::fs::write(&token, "A".repeat(43)).unwrap();
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
        let spool = root.join("spool");
        std::fs::create_dir(&spool).unwrap();
        std::fs::set_permissions(&spool, std::fs::Permissions::from_mode(0o700)).unwrap();
        Config {
            instance_id: "11111111-1111-4111-8111-111111111111".into(),
            stable_instance_key: "primary".into(),
            rpc_url: url,
            credential_file: token,
            spool_directory: spool,
            heartbeat_millis: 1000,
            snapshot_millis: 1000,
        }
    }
    fn identity() -> NativeIdentity {
        let fixture: Value = serde_json::from_str(include_str!("fixtures/agent-registration-native-v1.json")).unwrap();
        serde_json::from_value(fixture["nativeIdentity"].clone()).unwrap()
    }

    #[tokio::test]
    async fn fresh_native_health_does_not_require_telemetry_or_create_reliable_history() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let (observed, mut observations) = mpsc::channel::<protocol::NativeHealth>(16);
        let server = tokio::spawn(async move {
            let mut registered_at = chrono::Utc::now();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    assert!(header.len() < 16384);
                    header.push(socket.read_u8().await.unwrap());
                }
                let headers = String::from_utf8(header).unwrap();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|value| value.trim().parse().unwrap())
                    })
                    .unwrap();
                assert!(length < 65536);
                let mut body = vec![0; length];
                socket.read_exact(&mut body).await.unwrap();
                let input: Value = serde_json::from_slice(&body).unwrap();
                let reply = if headers.starts_with("POST /rpc/v1/agents/register ") {
                    registered_at = chrono::Utc::now();
                    json!({"status":"registered","serverTime":registered_at,"managerProtocol":{"major":1,"minor":0},
                        "selectedProtocol":{"major":1,"minor":0},"enabledCapabilities":protocol::REQUIRED,
                        "outputProfile":"gallery-v2","sessionId":"0123456789abcdef0123456789abcdef","legacy":false})
                } else {
                    assert!(headers.starts_with("POST /rpc/v1/agents/heartbeat "));
                    let health: protocol::NativeHealth = serde_json::from_value(input["nativeHealth"].clone()).unwrap();
                    assert!(health.observed_at >= registered_at);
                    observed.send(health).await.unwrap();
                    json!({"ok":true})
                }
                .to_string();
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                            reply.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        let directory = tempfile::tempdir().unwrap();
        let runtime = Runtime::start(config(directory.path(), url), identity()).await.unwrap();
        let handle = runtime.handle();
        let first = timeout(Duration::from_secs(3), observations.recv()).await.unwrap().unwrap();
        assert!(first.integration_ready && first.persistence_healthy && !first.client_ready);
        handle.upstream_ready();
        let active = timeout(Duration::from_secs(3), observations.recv()).await.unwrap().unwrap();
        assert!(active.client_ready && active.health_sequence > first.health_sequence);
        handle.persistence_failed();
        let failed = timeout(Duration::from_secs(3), observations.recv()).await.unwrap().unwrap();
        assert!(!failed.persistence_healthy && failed.health_sequence > active.health_sequence);
        handle.stopping();
        let stopping = timeout(Duration::from_secs(3), observations.recv()).await.unwrap().unwrap();
        assert!(!stopping.client_ready && stopping.health_sequence > failed.health_sequence);
        assert_eq!(handle.health().unwrap().outbox_depth, 0);
        runtime.shutdown().await.unwrap();
        server.abort();
        let _ = server.await;
    }

    #[test]
    fn noop_creates_no_identity_queue_or_snapshot_and_backoff_is_bounded() {
        let handle = IntegrationHandle::default();
        assert!(!handle.enabled());
        assert!(handle.health().is_none());
        assert!(handle.event("invalid type", Value::Null).unwrap().is_none());
        assert!(handle.snapshot(Value::Null).is_ok());
        handle.phase(Phase::Active);
        let mut backoff = Backoff(1000);
        for _ in 0..100 {
            let wait = backoff.next();
            assert!((Duration::from_millis(750)..=Duration::from_secs(60)).contains(&wait));
        }
        assert_eq!(backoff.0, 60_000);
    }

    #[tokio::test]
    async fn pending_queue_bounds_count_and_bytes_without_dropping_durable_work() {
        use futures::{FutureExt, poll};
        let (sender, mut receiver) = mpsc::channel(PENDING_RECORDS);
        let storage = Storage {
            sender,
            budget: Arc::new(Semaphore::new(PENDING_BYTES)),
            records: Arc::new(Semaphore::new(PENDING_RECORDS)),
        };
        // A fake stalled persistence worker makes the admission boundary deterministic.
        let mut pending = Vec::new();
        for _ in 0..16 {
            let mut request = Box::pin(storage.persist(vec![0; 16 * 1024]));
            assert!(poll!(request.as_mut()).is_pending());
            pending.push(request);
        }
        assert_eq!(storage.budget.available_permits(), 0);
        assert!(storage.persist(vec![0; 1]).await.is_err());
        // Dropping a waiting future does not revoke the worker's already accepted command.
        drop(pending);
        assert_eq!(storage.budget.available_permits(), 0);
        for _ in 0..16 {
            drop(receiver.recv().await.unwrap());
        }
        assert_eq!(storage.budget.available_permits(), PENDING_BYTES);
        let mut pending = Vec::new();
        for _ in 0..PENDING_RECORDS {
            let mut request = storage.persist(vec![0; 1]).boxed();
            assert!(poll!(request.as_mut()).is_pending());
            pending.push(request);
        }
        assert!(storage.persist(vec![0; 1]).await.is_err());
        let in_flight = receiver.recv().await.unwrap();
        // Reading a command out of the channel does not free its in-flight admission slot.
        assert_eq!(storage.records.available_permits(), 0);
        assert!(storage.persist(vec![0; 1]).await.is_err());
        drop(in_flight);
        drop(receiver);
        for request in pending {
            assert!(request.await.is_err());
        }
        assert_eq!(storage.budget.available_permits(), PENDING_BYTES);
        assert_eq!(storage.records.available_permits(), PENDING_RECORDS);
    }

    #[tokio::test]
    async fn offline_shutdown_preserves_event_and_snapshot_slot_only_keeps_latest() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        // Keep a reserved but nonresponsive loopback listener; shutdown must cancel an in-flight HTTP request.
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path(), url);
        let spool = config.spool_directory.clone();
        let instance = config.instance_id.clone();
        let runtime = Runtime::start(config, identity()).await.unwrap();
        let handle = runtime.handle();
        let event = handle.event("gallery.download.initialized", json!({"gid":123})).unwrap().unwrap();
        handle.persist(&event).await.unwrap();
        assert_eq!(handle.health().unwrap().outbox_depth, 1);
        for index in 0..1000 {
            handle.snapshot(json!({"counter":index})).unwrap();
        }
        let last = handle.inner.as_ref().unwrap().state.snapshot.borrow().clone().unwrap();
        assert_eq!(last.payload["counter"], 999);
        assert_eq!(Arc::strong_count(&last), 2); // one slot, one test reference; no history
        assert!(handle.snapshot(json!({"huge":"x".repeat(MAX_SNAPSHOT)})).is_err());
        assert_eq!(handle.inner.as_ref().unwrap().state.snapshot.borrow().as_ref().unwrap().payload["counter"], 999);
        timeout(Duration::from_secs(2), runtime.shutdown()).await.unwrap().unwrap();
        assert!(!handle.health().unwrap().link_healthy);
        assert!(handle.persist(&event).await.is_err());
        let mut recovered = Outbox::open(&spool, &instance, Limits::default()).unwrap();
        let batch = recovered.batch().unwrap();
        assert_eq!(batch[0].event.event_id, event.event_id);
        assert_eq!(batch[0].bytes, event.encode().unwrap());
        recovered.acknowledge(&batch).unwrap();
    }

    #[tokio::test]
    async fn quarantine_reports_degraded_without_blocking_new_durable_events() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path(), url);
        let mut spool = Outbox::open(&config.spool_directory, &config.instance_id, Limits::default()).unwrap();
        let event = Event::new(&config.instance_id, 0, "gallery.download.initialized", json!({"gid":123})).unwrap();
        spool.append(&event).unwrap();
        let path = std::fs::read_dir(config.spool_directory.join(&config.instance_id))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::write(path, b"{").unwrap();
        drop(spool);
        let runtime = Runtime::start(config, identity()).await.unwrap();
        let handle = runtime.handle();
        assert_eq!(handle.health().unwrap().quarantined, 1);
        assert_eq!(handle.inner.as_ref().unwrap().state.status(), "degraded");
        assert!(!handle.inner.as_ref().unwrap().state.native_health().unwrap().persistence_healthy);
        handle.persist(&event).await.unwrap();
        assert_eq!(handle.health().unwrap().outbox_depth, 1);
        assert_eq!(handle.inner.as_ref().unwrap().state.status(), "degraded");
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn disk_worker_exit_cannot_leave_healthy_native_readiness_behind() {
        let root = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(root.path(), format!("http://{}/", listener.local_addr().unwrap()));
        let runtime = Runtime::start(config, identity()).await.unwrap();
        let handle = runtime.handle();
        handle.inner.as_ref().unwrap().storage.close().await.unwrap();
        timeout(Duration::from_secs(1), async {
            while handle.inner.as_ref().unwrap().state.native_health().unwrap().persistence_healthy {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(timeout(Duration::from_secs(2), runtime.shutdown()).await.unwrap().is_err());
    }

    #[tokio::test]
    async fn sender_retries_identical_event_after_lost_ack_and_shuts_down_with_no_background_writers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut events = Vec::new();
            loop {
                let (mut connection, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(connection.read_u8().await.unwrap());
                    assert!(request.len() < 16 * 1024);
                }
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
                let mut bytes = vec![0; length];
                connection.read_exact(&mut bytes).await.unwrap();
                let input: Value = serde_json::from_slice(&bytes).unwrap();
                let response = if headers.starts_with("POST /rpc/v1/agents/register ") {
                    json!({"status":"registered","serverTime":"2026-11-07T00:00:00Z","managerProtocol":{"major":1,"minor":0},
                        "selectedProtocol":{"major":1,"minor":0},"enabledCapabilities":protocol::Offer::native().capabilities,
                        "outputProfile":"gallery-v2","sessionId":"0123456789abcdef0123456789abcdef","legacy":false})
                } else if headers.starts_with("POST /rpc/v1/events/batch ") {
                    events.push(input["events"].clone());
                    if events.len() == 1 {
                        continue;
                    } // Server committed but its ACK never arrived.
                    json!({"ok":true,"batchId":input["batchId"]})
                } else {
                    json!({"ok":true})
                };
                let body = response.to_string();
                connection
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
                if events.len() == 2 {
                    return events;
                }
            }
        });
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path(), url);
        let spool = config.spool_directory.clone();
        let instance = config.instance_id.clone();
        let runtime = Runtime::start(config, identity()).await.unwrap();
        let handle = runtime.handle();
        handle.phase(Phase::Active);
        let event = handle.event("gallery.download.initialized", json!({"gid":123})).unwrap().unwrap();
        handle.persist(&event).await.unwrap();
        let events = timeout(Duration::from_secs(5), server).await.unwrap().unwrap();
        assert_eq!(events[0], events[1]);
        timeout(Duration::from_secs(1), async {
            while handle.health().unwrap().outbox_depth != 0 {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(handle.health().unwrap().persistence_healthy);
        handle.phase(Phase::Stopping);
        runtime.shutdown().await.unwrap();
        let recovered = Outbox::open(&spool, &instance, Limits::default()).unwrap();
        assert!(recovered.batch().unwrap().is_empty());
    }

    #[tokio::test]
    async fn optional_telemetry_failure_does_not_starve_reliable_events_or_reset_core_link_health() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let (delivered, confirmed) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut delivered = Some(delivered);
            let mut telemetry_failures = 0;
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    assert!(header.len() < 16384);
                    header.push(socket.read_u8().await.unwrap());
                }
                let header = String::from_utf8(header).unwrap();
                let length: usize = header
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|value| value.trim().parse().unwrap())
                    })
                    .unwrap();
                assert!(length < 65536);
                let mut bytes = vec![0; length];
                socket.read_exact(&mut bytes).await.unwrap();
                let input: Value = serde_json::from_slice(&bytes).unwrap();
                let mut is_delivered = false;
                let (code, reply) = if header.starts_with("POST /rpc/v1/agents/register ") {
                    (
                        200,
                        json!({"status":"registered","serverTime":chrono::Utc::now(),"managerProtocol":{"major":1,"minor":0},
                        "selectedProtocol":{"major":1,"minor":0},"enabledCapabilities":protocol::Offer::native().capabilities,
                        "outputProfile":"gallery-v2","sessionId":"0123456789abcdef0123456789abcdef","legacy":false}),
                    )
                } else if header.starts_with("POST /rpc/v1/telemetry ") {
                    telemetry_failures += 1;
                    (500, json!({"error":"optional_sink_unavailable"}))
                } else if header.starts_with("POST /rpc/v1/events/batch ") {
                    assert!(telemetry_failures > 0, "exercise the telemetry failure before durable delivery");
                    is_delivered = true;
                    (200, json!({"ok":true,"batchId":input["batchId"]}))
                } else {
                    (200, json!({"ok":true}))
                };
                let body = reply.to_string();
                socket
                    .write_all(
                        format!("HTTP/1.1 {code} reply\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes(),
                    )
                    .await
                    .unwrap();
                if is_delivered && let Some(delivered) = delivered.take() {
                    let _ = delivered.send(());
                }
            }
        });
        let root = tempfile::tempdir().unwrap();
        let runtime = Runtime::start(config(root.path(), url), identity()).await.unwrap();
        let handle = runtime.handle();
        handle.snapshot(json!({"counter":1})).unwrap();
        let event = handle.event("gallery.download.initialized", json!({"gid":123})).unwrap().unwrap();
        handle.persist(&event).await.unwrap();
        timeout(Duration::from_secs(3), confirmed).await.unwrap().unwrap();
        timeout(Duration::from_secs(2), async {
            while handle.health().unwrap().outbox_depth != 0 {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(handle.telemetry_failures().unwrap() > 0);
        assert!(handle.health().unwrap().link_healthy);
        runtime.shutdown().await.unwrap();
        server.abort();
        let _ = server.await;
    }
}
