//! Latest-only optional sampling; sources are Weak and no filesystem or HTTP scraping is used.
use super::{IntegrationHandle, ObservationHandle};
use crate::{metrics::Metrics, rpc::RPCClient};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    time::Duration,
};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(super) struct Sample {
    pub revision: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub payload: Value,
}
pub(super) struct FallbackSink {
    pub snapshots: watch::Sender<Option<Arc<Sample>>>,
    pub link: Arc<AtomicBool>,
}
pub(super) struct Sampler {
    stop: CancellationToken,
    task: Option<JoinHandle<()>>,
}
impl Sampler {
    pub fn start(
        metrics: Weak<Metrics>,
        client: Weak<RPCClient>,
        observations: ObservationHandle,
        handle: IntegrationHandle,
        fallback: Option<FallbackSink>,
        millis: u64,
    ) -> Self {
        let stop = CancellationToken::new();
        let cancelled = stop.clone();
        let task = tokio::spawn(async move {
            let mut timer = interval(Duration::from_millis(millis));
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut revision = 0u64;
            loop {
                tokio::select! {biased; _=cancelled.cancelled()=>break,_=timer.tick()=>()}
                let sample = {
                    let (Some(metrics), Some(client)) = (metrics.upgrade(), client.upgrade()) else {
                        break;
                    };
                    sample(&metrics, &client, &observations, &handle, fallback.as_ref().map(|sink| sink.link.load(Relaxed)))
                };
                if let Some(sink) = &fallback {
                    sink.snapshots.send_replace(Some(Arc::new(Sample {
                        revision,
                        timestamp: chrono::Utc::now(),
                        payload: sample,
                    })));
                } else {
                    let _ = handle.snapshot(sample);
                }
                let Some(next) = revision.checked_add(1) else { break };
                revision = next;
            }
        });
        Self { stop, task: Some(task) }
    }
    pub async fn shutdown(mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for Sampler {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

pub(super) fn sample(
    metrics: &Metrics,
    client: &RPCClient,
    observer: &ObservationHandle,
    handle: &IntegrationHandle,
    fallback_link: Option<bool>,
) -> Value {
    let mut payload = json!({"nativeTelemetryVersion":1,"clientRunning":client.is_running(),"uptimeSeconds":observer.uptime(),
        "nativeInflightHttpRequests":metrics.connections.get(),"nativeCacheScanComplete":metrics.cache_scan_complete.load(Relaxed),
        "nativeOperations":observer.snapshot()});
    let mut supported = vec![
        "clientRunning",
        "uptimeSeconds",
        "nativeInflightHttpRequests",
        "nativeCacheScanComplete",
    ];
    if let Some(http) = metrics.observation.http_snapshot() {
        for (key, value) in http.as_object().unwrap() {
            payload[key] = value.clone();
        }
        supported.extend(["bytesSent", "filesSent", "bytesReceived", "filesReceived"]);
    }
    if let Some(failures) = handle.telemetry_failures() {
        payload["nativeTelemetryFailures"] = json!(failures);
        supported.push("nativeTelemetryFailures");
    }
    if let Some(contact) = observer.last_contact() {
        payload["lastServerContact"] = json!(contact);
        supported.push("lastServerContact");
    }
    if let Some(logs) = metrics.logs.upgrade() {
        let [dropped, redacted, failed] = logs.counters();
        payload["nativeLogDropped"] = json!(dropped);
        payload["nativeLogRedacted"] = json!(redacted);
        payload["nativeLogWriteFailures"] = json!(failed);
        supported.extend(["nativeLogDropped", "nativeLogRedacted", "nativeLogWriteFailures"]);
    }
    if let Some(health) = handle.health() {
        payload["nativePersistenceHealthy"] = json!(health.persistence_healthy && health.quarantined == 0);
        payload["nativeManagerLinkHealthy"] = json!(fallback_link.unwrap_or(health.link_healthy));
        supported.extend(["nativePersistenceHealthy", "nativeManagerLinkHealthy"]);
    }
    if let Some((depth, bytes, quarantined, failures, pending)) = handle.storage_snapshot() {
        payload["agentSpoolDepth"] = json!(depth);
        payload["agentSpoolBytes"] = json!(bytes);
        payload["nativeQuarantinedRecords"] = json!(quarantined);
        payload["agentSpoolWriteFailures"] = json!(failures);
        payload["agentPendingReliableEvents"] = json!(pending);
        supported.extend([
            "agentSpoolDepth",
            "agentSpoolBytes",
            "nativeQuarantinedRecords",
            "agentSpoolWriteFailures",
            "agentPendingReliableEvents",
        ]);
    }
    let capacity_known = metrics.cache_configured.load(Relaxed) && metrics.cache_capacity.get() != i64::MAX;
    if capacity_known {
        payload["nativeCacheCapacityBytes"] = json!(metrics.cache_capacity.get().max(0));
        supported.push("nativeCacheCapacityBytes");
    }
    if metrics.cache_scan_complete.load(Relaxed) {
        let bytes = metrics.cache_size.get();
        let capacity = metrics.cache_capacity.get().max(0) as u64;
        payload["cacheFiles"] = json!(metrics.cache_count.get());
        payload["cacheBytes"] = json!(bytes);
        supported.extend(["cacheFiles", "cacheBytes"]);
        if capacity_known {
            payload["cacheFreeBytes"] = json!(capacity.saturating_sub(bytes));
            supported.push("cacheFreeBytes");
            if capacity > 0 {
                payload["cacheFillRatio"] = json!(bytes as f64 / capacity as f64);
                supported.push("cacheFillRatio");
            }
        }
    }
    payload["supportedMetrics"] = json!(supported);
    payload
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unavailable_metrics_are_not_filled_with_fake_zero_values() {
        let metrics = Metrics::new();
        let client = RPCClient::new(1001, "never-reported", false, 0, Some("127.0.0.1:1"), None);
        let observer = ObservationHandle::new();
        let handle = IntegrationHandle::unavailable();
        let payload = sample(&metrics, &client, &observer, &handle, Some(true));
        assert_eq!(payload["nativePersistenceHealthy"], false);
        assert_eq!(payload["nativeManagerLinkHealthy"], true);
        for key in [
            "clientSuspended",
            "filesSent",
            "bytesSent",
            "filesReceived",
            "bytesReceived",
            "openConnections",
            "cacheFiles",
            "agentSpoolBytes",
            "lastServerContact",
        ] {
            assert!(payload.get(key).is_none(), "{key}");
        }
        assert!(!payload.to_string().contains("never-reported"));
        metrics.cache_scan_complete.store(true, Relaxed);
        metrics.cache_configured.store(true, Relaxed);
        metrics.cache_count.set(4);
        metrics.cache_size.set(150);
        metrics.cache_capacity.set(100);
        let payload = sample(&metrics, &client, &observer, &handle, None);
        assert_eq!(payload["cacheFreeBytes"], 0);
        assert_eq!(payload["cacheFillRatio"], 1.5);
        metrics.cache_capacity.set(i64::MAX);
        let payload = sample(&metrics, &client, &observer, &handle, None);
        for key in ["nativeCacheCapacityBytes", "cacheFreeBytes", "cacheFillRatio"] {
            assert!(payload.get(key).is_none());
        }
    }
    #[tokio::test]
    async fn sampler_does_not_keep_sources_or_the_client_alive() {
        let metrics = Arc::new(Metrics::new());
        let client = Arc::new(RPCClient::new(1001, "private-key", false, 0, Some("127.0.0.1:1"), None));
        let weak_metrics = Arc::downgrade(&metrics);
        let weak_client = Arc::downgrade(&client);
        let sampler =
            Sampler::start(weak_metrics.clone(), weak_client.clone(), ObservationHandle::new(), IntegrationHandle::default(), None, 1000);
        tokio::task::yield_now().await;
        drop(metrics);
        drop(client);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !sampler.task.as_ref().unwrap().is_finished() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(weak_metrics.upgrade().is_none());
        assert!(weak_client.upgrade().is_none());
        sampler.shutdown().await;
    }
}
