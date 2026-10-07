use std::path::PathBuf;

use crate::ids::{GlobalPosition, StreamVersion};
use crate::log::ExpectedVersion;

/// Every fallible operation in this crate returns this.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors raised by the log, the index and the stores.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A filesystem call failed. `op` names the call (`open`, `write`,
    /// `fdatasync`, ...) so the message says what the log was doing.
    #[error("io error during {op} on {path}: {source}")]
    Io {
        path: PathBuf,
        op: &'static str,
        #[source]
        source: std::io::Error,
    },

    /// A segment file or the index disagrees with itself in a way recovery
    /// cannot repair by truncation (for example the index knows positions the
    /// segment files do not contain).
    #[error("corrupt log: {segment} at offset {offset}: {reason}")]
    Corrupt {
        segment: PathBuf,
        offset: u64,
        reason: String,
    },

    /// Another process (or another `Log` in this process) holds `LOCK`.
    #[error("log at {path} is locked by another writer")]
    Locked { path: PathBuf },

    /// `create` found a log already there.
    #[error("log already exists at {path}")]
    AlreadyExists { path: PathBuf },

    /// `open` found no log there.
    #[error("no log at {path}")]
    NotFound { path: PathBuf },

    /// The stream's current version does not satisfy the caller's expectation.
    #[error("wrong expected version on stream {stream}: expected {expected:?}, actual {actual:?}")]
    WrongExpectedVersion {
        stream: String,
        expected: ExpectedVersion,
        /// `None` when the stream does not exist yet.
        actual: Option<StreamVersion>,
    },

    /// Empty, longer than 255 bytes, or containing a control character.
    #[error("invalid stream id: {0}")]
    InvalidStreamId(String),

    /// An event type with an empty or over-long (> 65535 bytes) context or
    /// name.
    #[error("invalid event type: {0}")]
    InvalidEventType(String),

    /// `append` was called with no events.
    #[error("empty batch")]
    EmptyBatch,

    /// One encoded record exceeds `OpenOptions::max_record_bytes`.
    #[error("record of {size} bytes exceeds the maximum of {max}")]
    RecordTooLarge { size: usize, max: usize },

    /// A read asked for a position past the head.
    #[error("position {position} is out of range (head is {head})")]
    PositionOutOfRange {
        position: GlobalPosition,
        head: GlobalPosition,
    },

    /// A key part could not be encoded (for example a string with an
    /// interior NUL).
    #[error("invalid key: {0}")]
    InvalidKey(String),

    /// `append_idempotent` saw a key already used; nothing was appended.
    /// `position` is where the earlier append started.
    #[error("idempotency key already used by the append at position {position}")]
    DuplicateKey { position: GlobalPosition },

    /// redb reported an error.
    #[error("index error: {0}")]
    Index(#[from] redb::Error),
}

macro_rules! from_redb {
    ($($t:ty),* $(,)?) => {
        $(impl From<$t> for Error {
            fn from(e: $t) -> Self {
                Error::Index(redb::Error::from(e))
            }
        })*
    };
}

from_redb!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    redb::SetDurabilityError,
);

impl Error {
    pub(crate) fn io(path: impl Into<PathBuf>, op: &'static str, source: std::io::Error) -> Self {
        Error::Io {
            path: path.into(),
            op,
            source,
        }
    }

    pub(crate) fn corrupt(
        segment: impl Into<PathBuf>,
        offset: u64,
        reason: impl Into<String>,
    ) -> Self {
        Error::Corrupt {
            segment: segment.into(),
            offset,
            reason: reason.into(),
        }
    }
}

/// The log behind a [`crate::Subscription`] has been dropped; no further
/// appends can happen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("log closed")]
pub struct Closed;
