//! What a projection or process runner reports and what an operator may
//! ask of it.

use std::collections::HashMap;

use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Starting,
    CatchingUp,
    Live,
    Failed,
    Stopped,
    /// Reset and replaying; queries see a partial read model until live.
    Rebuilding,
}

/// What an operator may ask a running projection or process to do.
pub enum Control {
    /// Write a snapshot of the tables as of the current checkpoint.
    Snapshot {
        reply: tokio::sync::oneshot::Sender<
            Result<crate::snapshot::SnapshotMeta, crate::snapshot::SnapshotError>,
        >,
    },
    /// Drop the tables and checkpoint, restore `snapshot` if given, and
    /// replay from there. Replies once the reset is committed.
    Rebuild {
        snapshot: Option<String>,
        force: bool,
        reply: tokio::sync::oneshot::Sender<Result<Option<u64>, crate::snapshot::RebuildError>>,
    },
    /// Dispatch the held outbox now (a process manager after a promotion).
    /// Nothing for a projection.
    Drain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub state: State,
    /// Last position applied, `None` before the first commit.
    pub checkpoint: Option<u64>,
    /// Log head (next position) when last sampled.
    pub head: u64,
    pub error: Option<String>,
    pub tables: Vec<String>,
}

impl Status {
    pub fn starting(tables: Vec<String>) -> Self {
        Status {
            state: State::Starting,
            checkpoint: None,
            head: 0,
            error: None,
            tables,
        }
    }
}

/// Projection name → its live status.
pub type StatusBook = HashMap<String, watch::Receiver<Status>>;

/// Whether `cp` still describes `log`: the event before `cp.next` is the
/// one the checkpoint remembers. A checkpoint without a fingerprint, or at
/// the start, is taken at its word. `false` means the log moved backwards
/// (a truncation, a restore, a failover to a shorter primary) and whatever
/// was derived up to this checkpoint must be rebuilt.
pub fn checkpoint_matches(
    log: &fold_core::Log,
    cp: &fold_store::Checkpoint,
) -> fold_core::Result<bool> {
    let Some(id) = cp.last_event_id else {
        return Ok(true);
    };
    let Some(at) = cp.next.0.checked_sub(1) else {
        return Ok(true);
    };
    if at >= log.head().0 {
        return Ok(false);
    }
    let page = log.read_all(fold_core::GlobalPosition(at), 1)?;
    Ok(page.first().is_some_and(|e| e.id == id))
}
