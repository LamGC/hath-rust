//! Own cache verification/proxy writers until they have stopped, including error paths.
use parking_lot::Mutex;
use std::future::Future;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
pub(crate) struct CacheJobs {
    gate: Mutex<()>,
    stop: CancellationToken,
    tasks: TaskTracker,
}
impl CacheJobs {
    pub fn new() -> Self {
        Self {
            gate: Mutex::new(()),
            stop: CancellationToken::new(),
            tasks: TaskTracker::new(),
        }
    }
    pub fn token(&self) -> CancellationToken {
        self.stop.clone()
    }
    pub fn stopped(&self) -> bool {
        self.stop.is_cancelled()
    }
    pub fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> bool {
        let _gate = self.gate.lock();
        if self.stop.is_cancelled() {
            return false;
        }
        self.tasks.spawn(future);
        true
    }
    pub fn close(&self) {
        let _gate = self.gate.lock();
        self.stop.cancel();
        self.tasks.close();
    }
    pub async fn wait(&self) {
        self.tasks.wait().await;
    }
}
impl Drop for CacheJobs {
    fn drop(&mut self) {
        self.stop.cancel();
        self.tasks.close();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn close_fences_new_tasks_and_waits_for_existing_writers() {
        let jobs = CacheJobs::new();
        let stop = jobs.token();
        let (tx, rx) = tokio::sync::oneshot::channel();
        assert!(jobs.spawn(async move {
            stop.cancelled().await;
            tx.send(()).unwrap();
        }));
        jobs.close();
        assert!(!jobs.spawn(async { panic!("closed job scope must not poll new work") }));
        tokio::time::timeout(std::time::Duration::from_secs(1), jobs.wait()).await.unwrap();
        rx.await.unwrap();
    }
}
