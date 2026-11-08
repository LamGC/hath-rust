//! Fixed-size operational counters only. No URLs, identifiers, errors or dynamic labels.
use serde_json::{Map, Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering::Relaxed},
    },
    time::Instant,
};

pub(super) const NAMES: [&str; 33] = [
    "rpc.server_stat",
    "rpc.client_login",
    "rpc.get_cert",
    "rpc.get_blacklist",
    "rpc.client_start",
    "rpc.client_settings",
    "rpc.srfetch",
    "rpc.dlfails",
    "rpc.fetchqueue",
    "rpc.dlfetch",
    "rpc.client_stop",
    "rpc.still_alive",
    "rpc.client_suspend",
    "rpc.client_resume",
    "rpc.overload",
    "rpc.other",
    "servercmd.still_alive",
    "servercmd.threaded_proxy_test",
    "servercmd.speed_test",
    "servercmd.refresh_settings",
    "servercmd.start_downloader",
    "servercmd.refresh_certs",
    "servercmd.other",
    "cache.import",
    "cache.delete",
    "cache.scan",
    "cache.prune",
    "cache.blacklist",
    "dispatch.reload_cert",
    "dispatch.refresh_settings",
    "dispatch.start_downloader",
    "dispatch.overload",
    "proxy.upstream",
];
#[derive(Clone, Copy)]
pub(crate) enum Operation {
    CacheImport = 23,
    CacheDelete = 24,
    CacheScan = 25,
    CachePrune = 26,
    CacheBlacklist = 27,
    ReloadCert = 28,
    RefreshSettings = 29,
    StartDownloader = 30,
    Overload = 31,
    ProxyUpstream = 32,
}
#[derive(Clone, Copy)]
pub(crate) enum Outcome {
    Completed = 1,
    Failed = 2,
    Cancelled = 3,
    Noop = 4,
}

struct Counters {
    http: Arc<super::http::Counters>,
    values: [[AtomicU64; 6]; 33],
    last_contact: AtomicI64,
    since: Instant,
}
#[derive(Clone, Default)]
pub(crate) struct ObservationHandle(Option<Arc<Counters>>);
impl ObservationHandle {
    pub(crate) fn new() -> Self {
        Self(Some(Arc::new(Counters {
            http: Arc::new(super::http::Counters::new()),
            values: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            last_contact: AtomicI64::new(0),
            since: Instant::now(),
        })))
    }
    pub(crate) fn operation(&self, operation: Operation) -> Observation {
        self.start(operation as usize)
    }
    pub(crate) fn enabled(&self) -> bool {
        self.0.is_some()
    }
    pub(crate) fn http(&self, method: &axum::http::Method, path: &str) -> super::http::HttpObservation {
        super::http::HttpObservation::new(self.0.as_ref().map(|c| c.http.clone()), method, path)
    }
    pub(crate) fn image_received_bytes(&self, bytes: u64) {
        if let Some(c) = &self.0 {
            c.http.received(bytes);
        }
    }
    pub(crate) fn image_received_file(&self) {
        if let Some(c) = &self.0 {
            c.http.received_file();
        }
    }
    pub(crate) fn http_snapshot(&self) -> Option<Value> {
        self.0.as_ref().map(|c| c.http.snapshot())
    }
    pub(crate) fn rpc(&self, action: &str) -> Observation {
        self.start(
            NAMES[..15]
                .iter()
                .position(|name| name.strip_prefix("rpc.").is_some_and(|value| value == action))
                .unwrap_or(15),
        )
    }
    pub(crate) fn server_command(&self, command: &str) -> Observation {
        self.start(
            16 + NAMES[16..22]
                .iter()
                .position(|name| {
                    name.strip_prefix("servercmd.")
                        .is_some_and(|value| value.eq_ignore_ascii_case(command))
                })
                .unwrap_or(6),
        )
    }
    fn start(&self, index: usize) -> Observation {
        if let Some(counters) = &self.0 {
            add(&counters.values[index][0], 1);
        }
        Observation {
            counters: self.0.clone(),
            index,
            since: self.0.as_ref().map(|_| Instant::now()),
        }
    }
    pub(crate) fn contact(&self) {
        if let Some(counters) = &self.0 {
            counters.last_contact.store(chrono::Utc::now().timestamp(), Relaxed);
        }
    }
    pub(super) fn uptime(&self) -> u64 {
        self.0.as_ref().map(|c| c.since.elapsed().as_secs()).unwrap_or(0)
    }
    pub(super) fn last_contact(&self) -> Option<i64> {
        self.0.as_ref().map(|c| c.last_contact.load(Relaxed)).filter(|value| *value > 0)
    }
    pub(super) fn snapshot(&self) -> Value {
        let mut operations = Map::new();
        if let Some(counters) = &self.0 {
            for (index, name) in NAMES.iter().enumerate() {
                let values = counters.values[index].each_ref().map(|value| value.load(Relaxed));
                if values[0] > 0 {
                    operations.insert((*name).into(),json!({"started":values[0],"completed":values[1],"failed":values[2],"cancelled":values[3],"noop":values[4],"durationMicros":values[5]}));
                }
            }
        }
        Value::Object(operations)
    }
}
pub(crate) struct Observation {
    counters: Option<Arc<Counters>>,
    index: usize,
    since: Option<Instant>,
}
impl Observation {
    pub(crate) fn finish(&mut self, outcome: Outcome) {
        if let Some(counters) = self.counters.take() {
            add(&counters.values[self.index][outcome as usize], 1);
            add(&counters.values[self.index][5], self.since.take().unwrap().elapsed().as_micros().min(u64::MAX as u128) as u64);
        }
    }
    pub(crate) fn result(&mut self, success: bool) {
        self.finish(if success { Outcome::Completed } else { Outcome::Failed });
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        self.finish(Outcome::Cancelled);
    }
}
fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.try_update(Relaxed, Relaxed, |current| Some(current.saturating_add(value)));
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_actions_stay_in_a_fixed_bucket_and_cancellation_is_not_success() {
        let observer = ObservationHandle::new();
        for i in 0..10000 {
            let mut guard = observer.rpc(&format!("secret-url-{i}"));
            if i % 2 == 0 {
                guard.result(true);
            }
        }
        let payload = observer.snapshot();
        assert_eq!(payload.as_object().unwrap().len(), 1);
        assert_eq!(payload["rpc.other"]["started"], 10000);
        assert_eq!(payload["rpc.other"]["completed"], 5000);
        assert_eq!(payload["rpc.other"]["cancelled"], 5000);
        assert!(!payload.to_string().contains("secret-url"));
    }
    #[test]
    fn noop_allocates_no_counters_and_dense_snapshot_stays_bounded() {
        assert!(std::mem::size_of::<Counters>() + std::mem::size_of::<super::super::http::Counters>() < 16 * 1024);
        let noop = ObservationHandle::default();
        noop.rpc("client_login").result(true);
        assert_eq!(noop.snapshot(), json!({}));
        let observer = ObservationHandle::new();
        let counters = observer.0.as_ref().unwrap();
        for row in &counters.values {
            for value in row {
                value.store(u64::MAX, Relaxed);
            }
        }
        assert_eq!(observer.snapshot().as_object().unwrap().len(), NAMES.len());
        assert!(serde_json::to_vec(&observer.snapshot()).unwrap().len() < 12 * 1024);
        let fixture: Value = serde_json::from_str(include_str!("fixtures/native-telemetry-v1.json")).unwrap();
        assert_eq!(
            fixture["nativeOperations"].as_object().unwrap().keys().collect::<Vec<_>>(),
            observer.snapshot().as_object().unwrap().keys().collect::<Vec<_>>()
        );
        observer.rpc("client_login").result(true);
        assert_eq!(observer.snapshot()["rpc.client_login"]["started"], u64::MAX);
    }
    #[tokio::test]
    async fn cancelled_rpc_future_is_counted_once_without_claiming_server_contact() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let observer = ObservationHandle::new();
        let client = crate::rpc::RPCClient::new(1001, "not-in-metrics", false, 0, Some(&listener.local_addr().unwrap().to_string()), None)
            .with_observation(observer.clone());
        let mut login = Box::pin(client.login());
        let _connection=tokio::time::timeout(std::time::Duration::from_secs(3),async {
            tokio::select!{result=&mut login=>panic!("unexpected mock reply: {}",result.is_ok()),accepted=listener.accept()=>accepted.unwrap()}
        }).await.unwrap();
        drop(login);
        let payload = observer.snapshot();
        assert_eq!(payload["rpc.server_stat"]["started"], 1);
        assert_eq!(payload["rpc.server_stat"]["cancelled"], 1);
        assert_eq!(payload["rpc.server_stat"]["completed"], 0);
        assert!(observer.last_contact().is_none());
        assert!(!payload.to_string().contains("not-in-metrics"));
    }
}
