//! External-only recovery coordinator. Filesystem/hash work runs one at a time
//! outside the HTTP sender and persistence worker. Durable queue pressure is
//! retried while the sender can drain both old and new events.
use super::{
    external::{Boot, RecoverySource},
    gallery,
    runtime::{Backoff, Storage},
};
use std::{collections::VecDeque, io, sync::Arc};
use tokio::sync::{Notify, mpsc, watch};

pub(super) async fn run(
    source: RecoverySource,
    boots: Arc<Vec<Boot>>,
    storage: Storage,
    ready: mpsc::Sender<usize>,
    wake: Arc<Notify>,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut backoff = Backoff(1000);
    let mut pending = loop {
        let source = source.clone();
        let boots = boots.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _fence = source.fence;
            gallery::discover_external_handoffs(&source.download_root, &boots)
        });
        let result = tokio::select! { biased;
            _ = stop.wait_for(|value| *value) => return Ok(()),
            result = worker => result,
        };
        if let Ok(Ok(found)) = result {
            break VecDeque::from(found);
        }
        if cancelled_delay(&mut stop, &mut backoff).await {
            return Ok(());
        }
    };
    backoff = Backoff(1000);
    while let Some((index, path)) = pending.pop_front() {
        let boot = boots[index].clone();
        let fence = source.fence.clone();
        let candidate = path.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _fence = fence;
            match gallery::recover_external_attempt(&candidate, &boot) {
                Err(error)
                    if error.kind() == io::ErrorKind::NotFound
                        && candidate
                            .symlink_metadata()
                            .is_err_and(|missing| missing.kind() == io::ErrorKind::NotFound) =>
                {
                    Ok(None)
                }
                result => result.map(Some),
            }
        });
        let result = tokio::select! { biased;
            _ = stop.wait_for(|value| *value) => return Ok(()),
            result = worker => result,
        };
        let events = match result {
            Ok(Ok(Some(events))) => events,
            // An already handed-off attempt may be removed by the node during
            // recovery. No success is inferred and no event is invented for it.
            Ok(Ok(None)) => continue,
            _ => {
                pending.push_back((index, path));
                if cancelled_delay(&mut stop, &mut backoff).await {
                    return Ok(());
                }
                continue;
            }
        };
        backoff = Backoff(1000);
        // Keep the exact recovered event bytes in memory during queue retries;
        // never hash the gallery again merely because the outbox is full.
        for event in events {
            loop {
                let result = tokio::select! { biased;
                    _ = stop.wait_for(|value| *value) => return Ok(()),
                    result = storage.historical_persist(vec![event.clone()]) => result,
                };
                // Even a partial append must become eligible for transmission so
                // a one-free-slot queue cannot deadlock waiting for two free slots.
                tokio::select! { biased;
                    _ = stop.wait_for(|value| *value) => return Ok(()),
                    result = ready.send(index) => result.map_err(|_| io::Error::other("manager_replay_stopped"))?,
                }
                wake.notify_one();
                if result.is_ok() {
                    break;
                }
                if cancelled_delay(&mut stop, &mut backoff).await {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

async fn cancelled_delay(stop: &mut watch::Receiver<bool>, backoff: &mut Backoff) -> bool {
    tokio::select! { biased;
        _ = stop.wait_for(|value| *value) => true,
        _ = tokio::time::sleep(backoff.next()) => false,
    }
}
