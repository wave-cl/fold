//! Replication: a replica tails a primary's log as raw records.
//!
//! A chunk is a byte range of the primary's segments ending on a batch
//! boundary, plus the idempotency keys first used inside it. The replica
//! appends it through the same path an incremental backup uses, so ids,
//! timestamps, versions and CRCs are the primary's, and the keys travel so
//! that a replica promoted to primary finds its process managers' commands
//! already executed.

use uuid::Uuid;

use crate::error::{Error, Result};
use crate::event::FLAG_LAST_IN_BATCH;
use crate::ids::GlobalPosition;
use crate::log::Log;

/// Records `from..to` of a log, as framed on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationChunk {
    pub log_id: Uuid,
    pub from: GlobalPosition,
    /// Exclusive; a batch boundary.
    pub to: GlobalPosition,
    pub frames: Vec<u8>,
    /// Idempotency keys first used at a position in `from..to`.
    pub keys: Vec<(Vec<u8>, u64)>,
}

impl Log {
    /// The next chunk from `from`: about `max_events` records, rounded to a
    /// batch boundary (a batch longer than `max_events` goes whole), or
    /// `None` when `from` is the head. `from` past the head is an error.
    pub fn replication_chunk(
        &self,
        from: GlobalPosition,
        max_events: usize,
    ) -> Result<Option<ReplicationChunk>> {
        let head = self.head().0;
        if from.0 > head {
            return Err(Error::PositionOutOfRange {
                position: from,
                head: GlobalPosition(head),
            });
        }
        if from.0 == head {
            return Ok(None);
        }
        let flags_at = |p: u64| -> Result<u8> {
            let ev = self.read_all(GlobalPosition(p), 1)?;
            ev.first()
                .map(|e| e.flags)
                .ok_or_else(|| Error::PositionOutOfRange {
                    position: GlobalPosition(p),
                    head: GlobalPosition(head),
                })
        };
        let mut to = head.min(from.0 + max_events.max(1) as u64);
        while to > from.0 && flags_at(to - 1)? & FLAG_LAST_IN_BATCH == 0 {
            to -= 1;
        }
        if to == from.0 {
            // One batch longer than the window: send it whole.
            to = from.0 + 1;
            while flags_at(to - 1)? & FLAG_LAST_IN_BATCH == 0 {
                to += 1;
            }
        }
        let frames = self.frames_between(from.0, to)?;
        let keys = self.inner().index.idempotency_in(from.0, to)?;
        Ok(Some(ReplicationChunk {
            log_id: self.log_id(),
            from,
            to: GlobalPosition(to),
            frames,
            keys,
        }))
    }

    /// Appends a chunk of the primary's records. The chunk must be of this
    /// log (same identity) and start at the head; batches are committed one
    /// by one, so a failure part-way leaves whole batches behind and the
    /// next chunk starts from the new head.
    pub fn apply_replication_chunk(&self, chunk: &ReplicationChunk) -> Result<GlobalPosition> {
        if chunk.log_id != self.log_id() {
            return Err(Error::corrupt(
                self.path(),
                0,
                format!(
                    "replication chunk is of log {} but this is log {}",
                    chunk.log_id,
                    self.log_id()
                ),
            ));
        }
        let head = self.head();
        if chunk.from != head {
            return Err(Error::PositionOutOfRange {
                position: chunk.from,
                head,
            });
        }
        let new_head = self.import_frames(&chunk.frames)?;
        if new_head != chunk.to {
            return Err(Error::corrupt(
                self.path(),
                0,
                format!(
                    "replication chunk said it ends at {} but its records end at {new_head}",
                    chunk.to
                ),
            ));
        }
        self.inner().index.import_idempotency(&chunk.keys)?;
        Ok(new_head)
    }
}
