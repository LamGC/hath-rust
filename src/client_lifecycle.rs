//! Explicit ownership of startup/steady-state tasks and cleanup on every Result path.
use crate::{cache_manager::CacheManager, logger::Logger, rpc::RPCClient, server::Server};
use parking_lot::Mutex;
use std::{io, sync::Arc};
use tokio::{sync::mpsc::UnboundedReceiver, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(crate) struct ClientLifetime {
    pub stop: CancellationToken,
    pub client: Option<Arc<RPCClient>>,
    pub cache: Option<Arc<CacheManager>>,
    pub server: Option<Server>,
    pub dispatcher: Option<JoinHandle<()>>,
    pub keepalive: Option<JoinHandle<()>>,
    pub downloader: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub logger: Option<Logger>,
    pub signals: Option<JoinHandle<()>>,
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    pub managed: Option<crate::manager_integration::ManagedRuntime>,
}
impl ClientLifetime {
    pub(crate) async fn wait(&mut self) -> io::Result<()> {
        enum End {
            Stop,
            Dispatcher,
            Keepalive,
        }
        let end = {
            let dispatcher = self
                .dispatcher
                .as_mut()
                .ok_or_else(|| io::Error::other("client_dispatcher_missing"))?;
            let keepalive = self
                .keepalive
                .as_mut()
                .ok_or_else(|| io::Error::other("client_keepalive_missing"))?;
            tokio::select! { biased;
                _ = self.stop.cancelled() => End::Stop,
                _ = dispatcher => End::Dispatcher,
                _ = keepalive => End::Keepalive,
            }
        };
        match end {
            End::Stop => Ok(()),
            End::Dispatcher => {
                self.dispatcher.take();
                Err(io::Error::other("client_dispatcher_stopped"))
            }
            End::Keepalive => {
                self.keepalive.take();
                Err(io::Error::other("client_keepalive_stopped"))
            }
        }
    }
    pub(crate) async fn shutdown(&mut self) {
        crate::logger::event(crate::logger::Diagnostic::Stopping);
        self.stop.cancel();
        if let Some(cache) = &self.cache {
            cache.stop_io();
        }
        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
        if let Some(managed) = &self.managed {
            managed.stopping();
        }
        for task in [self.signals.take(), self.keepalive.take(), self.dispatcher.take()]
            .into_iter()
            .flatten()
        {
            task.abort();
            let _ = task.await;
        }
        // Dispatcher cannot start another Gallery now. Await it rather than aborting
        // its parent and leaving image writers detached from their owner.
        let gallery = self.downloader.lock().take();
        let server = self.server.take();
        let client = self.client.take();
        tokio::join!(
            async {
                if let Some(task) = gallery
                    && task.await.is_err()
                {
                    log::warn!("Gallery task failed during shutdown");
                }
            },
            async {
                if let Some(server) = server {
                    server.shutdown().await;
                }
            },
            async {
                if let Some(client) = client {
                    client.shutdown().await;
                }
            },
        );
        if let Some(cache) = self.cache.take() {
            cache.shutdown_background().await;
            cache.save_state().await;
        }
        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
        if let Some(managed) = self.managed.take()
            && managed.shutdown().await.is_err()
        {
            log::error!("Manager runtime shutdown failed; durable evidence retained");
        }
        if let Some(mut logger) = self.logger.take() {
            crate::logger::event(crate::logger::Diagnostic::Stopped);
            logger.shutdown().await;
        }
    }
}

// Signal handlers are installed before login, not only after successful startup.
pub(crate) struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(windows)]
    close: tokio::signal::windows::CtrlClose,
    #[cfg(windows)]
    shutdown: tokio::signal::windows::CtrlShutdown,
}
impl ShutdownSignals {
    pub(crate) fn install() -> io::Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                interrupt: signal(SignalKind::interrupt())?,
                terminate: signal(SignalKind::terminate())?,
            })
        }
        #[cfg(windows)]
        {
            use tokio::signal::windows::{ctrl_close, ctrl_shutdown};
            Ok(Self {
                close: ctrl_close()?,
                shutdown: ctrl_shutdown()?,
            })
        }
    }
    pub(crate) async fn wait(mut self, mut shutdown: UnboundedReceiver<()>) {
        #[cfg(unix)]
        tokio::select! { _ = self.interrupt.recv() => (), _ = self.terminate.recv() => (), _ = shutdown.recv() => () }
        #[cfg(windows)]
        tokio::select! { _ = tokio::signal::ctrl_c() => (), _ = self.close.recv() => (), _ = self.shutdown.recv() => (), _ = shutdown.recv() => () }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    #[tokio::test]
    async fn shutdown_joins_gallery_writers_and_is_idempotent() {
        let mut lifetime = ClientLifetime::default();
        let finished = Arc::new(AtomicBool::new(false));
        let observed = finished.clone();
        let stop = lifetime.stop.clone();
        *lifetime.downloader.lock() = Some(tokio::spawn(async move {
            stop.cancelled().await;
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            observed.store(true, Ordering::SeqCst);
        }));
        lifetime.dispatcher = Some(tokio::spawn(std::future::pending()));
        lifetime.keepalive = Some(tokio::spawn(std::future::pending()));
        tokio::time::timeout(std::time::Duration::from_secs(2), lifetime.shutdown())
            .await
            .unwrap();
        assert!(finished.load(Ordering::SeqCst));
        assert!(lifetime.downloader.lock().is_none());
        assert!(lifetime.dispatcher.is_none());
        assert!(lifetime.keepalive.is_none());
        lifetime.shutdown().await;
    }

    #[tokio::test]
    async fn unexpected_dispatcher_exit_is_not_silently_detached_or_awaited_twice() {
        let mut lifetime = ClientLifetime {
            dispatcher: Some(tokio::spawn(async {})),
            keepalive: Some(tokio::spawn(std::future::pending())),
            ..Default::default()
        };
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), lifetime.wait())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.to_string(), "client_dispatcher_stopped");
        assert!(lifetime.dispatcher.is_none());
        lifetime.shutdown().await;
    }
}
