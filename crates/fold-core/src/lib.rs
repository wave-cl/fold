//! `fold-core`: an append-only, segmented event log with redb indexes, a
//! read-model store and an aggregate snapshot store.
//!
//! The crate knows nothing about schemas. It stores opaque payload and
//! metadata bytes under a [`StreamId`] and an [`EventType`], assigns dense
//! 0-based [`GlobalPosition`]s and per-stream [`StreamVersion`]s, and serves
//! reads by stream, by global position and by event-type family.
//!
//! Durability model: records are written to the current segment file and
//! (under [`FsyncPolicy::Always`]) fdatasync'd, then one redb write
//! transaction records the positions and advances `META.head`. **The redb
//! commit is the commit point**: on open, anything in the segment files at or
//! past `META.head` was never acknowledged and is truncated.

pub mod backup;
pub use backup::{
    BackupKind, BackupMeta, apply as apply_backup, apply_to as apply_backup_to,
    inspect as inspect_backup, restore as restore_backup, restore_to as restore_backup_to,
};
pub mod keyenc;

mod dir;
mod error;
mod event;
mod ids;
mod index;
mod log;
mod options;
mod readmodel;
mod recover;
mod segment;
mod snapshots;
mod subscribe;
mod truncate;

pub use error::{Closed, Error, Result};
pub use event::{ENCODING_MASK, FLAG_LAST_IN_BATCH, NewEvent, RecordedEvent};
pub use ids::{EventId, EventType, GlobalPosition, StreamId, StreamVersion};
pub use log::{AppendResult, Direction, ExpectedVersion, Log};
pub use options::{FsyncPolicy, OpenOptions};
pub use readmodel::{ReadModelSnapshot, ReadModelStore};
pub use snapshots::{Snapshot, SnapshotStore};
pub use subscribe::Subscription;
pub use truncate::{Truncated, truncate_log};
