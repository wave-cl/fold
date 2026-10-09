//! The tail: one `SubscribeAll` stream from the database feeding a bounded
//! ring of recent events and the head watch. Runners read from the ring
//! when their next position is inside its window and from `ReadAll` when
//! they are behind. The status items carry the log's identity, role and
//! generation: a generation change resets derived data past the cut.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fold_core::RecordedEvent;
use fold_proto::database::v1::{SubscribeAllRequest, log_item};
use tokio::task::JoinHandle;

use crate::db::wire_to_core;
use crate::state::Shared;

/// The most recent events, by position, contiguous.
pub struct Ring {
    inner: Mutex<VecDeque<RecordedEvent>>,
    cap: usize,
}

impl Ring {
    pub fn new(cap: usize) -> Self {
        Ring {
            inner: Mutex::new(VecDeque::with_capacity(cap.min(1024))),
            cap,
        }
    }

    fn push(&self, ev: RecordedEvent) {
        let mut q = self.inner.lock().expect("ring");
        // Contiguity: a gap (a reconnect further along) empties the ring.
        if q.back().is_some_and(|b| b.position.0 + 1 != ev.position.0) {
            q.clear();
        }
        if q.len() == self.cap {
            q.pop_front();
        }
        q.push_back(ev);
    }

    fn clear(&self) {
        self.inner.lock().expect("ring").clear();
    }

    /// Up to `max` events from `from` on, when the ring holds `from` (or
    /// is exactly at it with nothing after: an empty page).
    pub fn range(&self, from: u64, max: usize) -> Option<Vec<RecordedEvent>> {
        let q = self.inner.lock().expect("ring");
        let first = q.front()?.position.0;
        let after_last = q.back()?.position.0 + 1;
        if from < first || from > after_last {
            return None;
        }
        let start = (from - first) as usize;
        Some(q.iter().skip(start).take(max).cloned().collect())
    }
}

/// Follows the database until shutdown, reconnecting with a backoff.
pub fn spawn(shared: Arc<Shared>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(100);
        loop {
            if shared.cancel.is_cancelled() {
                return;
            }
            match tail_once(&shared).await {
                Ok(()) => return,
                Err(e) => {
                    tracing::warn!(error = %e, "tail interrupted; reconnecting");
                    shared.head.send_modify(|h| {
                        h.connected = false;
                        h.error = Some(format!("{e:#}"));
                    });
                    shared.ring.clear();
                }
            }
            tokio::select! {
                _ = shared.cancel.cancelled() => return,
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
    })
}

/// One subscription from the database's current head: status items keep
/// the head watch current; events fill the ring. `Ok` only on shutdown.
async fn tail_once(shared: &Arc<Shared>) -> anyhow::Result<()> {
    let from = shared.db.health().await?.head;
    let mut stream = shared
        .db
        .log()
        .subscribe_all(SubscribeAllRequest {
            from_position: from,
            last_event_id: String::new(),
        })
        .await?
        .into_inner();
    loop {
        let item = tokio::select! {
            _ = shared.cancel.cancelled() => return Ok(()),
            m = stream.message() => m?,
        };
        let Some(item) = item else {
            anyhow::bail!("the database closed the subscription");
        };
        match item.item {
            Some(log_item::Item::Status(s)) => {
                anyhow::ensure!(
                    s.log_id == shared.log_id.to_string(),
                    "the database now serves log {}, not {}; restart this node to rebind it",
                    s.log_id,
                    shared.log_id
                );
                let known = shared.head.borrow().generation;
                if s.generation != known {
                    reset_past(shared, s.generation, s.cut)?;
                }
                shared.head.send_modify(|h| {
                    h.position = h.position.max(s.head);
                    h.epoch = s.epoch;
                    h.role = s.role.clone();
                    h.generation = s.generation;
                    h.cut = s.cut;
                    h.fenced_by = s.fenced_by;
                    h.connected = true;
                    h.error = None;
                });
            }
            Some(log_item::Item::Event(e)) => {
                let ev = wire_to_core(&e)?;
                let next = ev.position.0 + 1;
                shared.ring.push(ev);
                shared.head.send_modify(|h| {
                    h.position = h.position.max(next);
                    h.connected = true;
                });
            }
            None => {}
        }
    }
}

/// The log moved backwards under this node: drop what was derived past
/// the cut and tell the runners.
fn reset_past(shared: &Arc<Shared>, generation: u64, cut: u64) -> anyhow::Result<()> {
    let report = shared.store.reset_past(fold_core::GlobalPosition(cut))?;
    crate::state::prune_snapshot_files(&shared.derived_dir, cut);
    shared.store.set_generation(generation)?;
    shared.aggregates.clear();
    shared.ring.clear();
    tracing::warn!(
        generation,
        cut,
        runners_reset = ?report.runners_reset,
        snapshots_dropped = report.snapshots_dropped,
        "the log moved backwards; derived data past the cut dropped"
    );
    *shared.last_reset.lock().expect("last_reset") =
        Some(format!("reset past {cut} (generation {generation})"));
    shared.generation_changed.send_modify(|n| *n += 1);
    Ok(())
}
