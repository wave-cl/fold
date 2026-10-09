//! The tail: one `SubscribeAll` stream from the database feeding a bounded
//! ring of recent events and the head watch, for the process managers.
//! Status items carry the log's identity, role and generation.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fold_core::RecordedEvent;
use fold_proto::database::v1::{SubscribeAllRequest, log_item};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::peers::{PeerError, wire_to_core};
use crate::state::Shared;

/// Events read per catch-up batch.
pub const BATCH: usize = 256;

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

/// The next batch from `next`: the ring when it holds it, else the
/// database.
pub async fn fetch(shared: &Arc<Shared>, next: u64) -> Result<Vec<RecordedEvent>, PeerError> {
    if let Some(page) = shared.ring.range(next, BATCH) {
        return Ok(page);
    }
    if next >= shared.db_head() {
        return Ok(Vec::new());
    }
    shared.db.read_all(next, BATCH as u32).await
}

/// Resolves once the database's head is past `next`, the tail reports a
/// reset, or a bounded wait passes (a reconnecting tail may have missed
/// the head moving).
pub async fn wait_past(shared: &Arc<Shared>, next: u64, gen_rx: &mut watch::Receiver<u64>) {
    let mut head = shared.head.subscribe();
    loop {
        if head.borrow_and_update().position > next {
            return;
        }
        tokio::select! {
            _ = shared.cancel.cancelled() => return,
            r = head.changed() => if r.is_err() { return; },
            _ = gen_rx.changed() => return,
            _ = tokio::time::sleep(Duration::from_secs(2)) => return,
        }
    }
}

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
                let (known, was_primary) = {
                    let h = shared.head.borrow();
                    (h.generation, h.role == "primary")
                };
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
                if !was_primary && s.role == "primary" {
                    shared.drain_processes().await;
                }
            }
            Some(log_item::Item::Event(e)) => {
                let ev = wire_to_core(shared.db.url(), &e)?;
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

fn reset_past(shared: &Arc<Shared>, generation: u64, cut: u64) -> anyhow::Result<()> {
    let report = shared.store.reset_past(fold_core::GlobalPosition(cut))?;
    crate::state::prune_snapshot_files(&shared.derived_dir, cut);
    shared.store.set_generation(generation)?;
    shared.ring.clear();
    tracing::warn!(
        generation,
        cut,
        runners_reset = ?report.runners_reset,
        "the log moved backwards; process data past the cut dropped"
    );
    *shared.last_reset.lock().expect("last_reset") =
        Some(format!("reset past {cut} (generation {generation})"));
    shared.generation_changed.send_modify(|n| *n += 1);
    Ok(())
}
