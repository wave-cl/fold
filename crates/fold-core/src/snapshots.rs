//! Aggregate state snapshots: a cache, never a source of truth. One row per
//! `(aggregate, stream)`, overwritten by every `put`.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::ids::{StreamId, StreamVersion};
use crate::index::SNAPSHOTS;
use crate::log::Inner;

/// Aggregate state as of `version`, produced by the evolve module whose
/// hash is `module_hash`. A reader ignores a snapshot whose hash differs
/// from the module it is about to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// The stream version the state includes.
    pub version: StreamVersion,
    pub module_hash: [u8; 32],
    pub state: Vec<u8>,
}

const PREFIX: usize = 8 + 32;

impl Snapshot {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(PREFIX + self.state.len());
        out.extend_from_slice(&self.version.0.to_be_bytes());
        out.extend_from_slice(&self.module_hash);
        out.extend_from_slice(&self.state);
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < PREFIX {
            return None;
        }
        Some(Snapshot {
            version: StreamVersion(u64::from_be_bytes(bytes[0..8].try_into().unwrap())),
            module_hash: bytes[8..PREFIX].try_into().unwrap(),
            state: bytes[PREFIX..].to_vec(),
        })
    }
}

/// Handle on the snapshot table of a log. Cheap to clone.
#[derive(Clone)]
pub struct SnapshotStore {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for SnapshotStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SnapshotStore")
    }
}

impl SnapshotStore {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        SnapshotStore { inner }
    }

    pub fn get(&self, aggregate: &str, stream: &StreamId) -> Result<Option<Snapshot>> {
        let txn = self.inner.index.begin_read()?;
        let t = txn.open_table(SNAPSHOTS)?;
        match t.get((aggregate, stream.as_str()))? {
            None => Ok(None),
            Some(g) => Snapshot::decode(g.value()).map(Some).ok_or_else(|| {
                Error::corrupt(
                    self.inner_index_path(),
                    0,
                    format!("snapshot row for ({aggregate}, {stream}) is shorter than its header"),
                )
            }),
        }
    }

    /// Stores or replaces the snapshot. Idempotent.
    pub fn put(&self, aggregate: &str, stream: &StreamId, snapshot: Snapshot) -> Result<()> {
        let txn = self.inner.index.begin_write()?;
        {
            let mut t = txn.open_table(SNAPSHOTS)?;
            t.insert((aggregate, stream.as_str()), snapshot.encode().as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Every snapshot of `aggregate`, by stream id in key order.
    pub fn list(&self, aggregate: &str) -> Result<Vec<(StreamId, Snapshot)>> {
        let txn = self.inner.index.begin_read()?;
        let t = txn.open_table(SNAPSHOTS)?;
        let mut out = Vec::new();
        for entry in t.range((aggregate, "")..)? {
            let (k, v) = entry?;
            let (agg, stream) = k.value();
            if agg != aggregate {
                break;
            }
            let snapshot = Snapshot::decode(v.value()).ok_or_else(|| {
                Error::corrupt(
                    self.inner_index_path(),
                    0,
                    format!("snapshot row for ({aggregate}, {stream}) is shorter than its header"),
                )
            })?;
            out.push((StreamId::new(stream)?, snapshot));
        }
        Ok(out)
    }

    /// Stores many snapshots in one transaction, replacing any present.
    pub fn put_many(&self, aggregate: &str, snapshots: Vec<(StreamId, Snapshot)>) -> Result<()> {
        let txn = self.inner.index.begin_write()?;
        {
            let mut t = txn.open_table(SNAPSHOTS)?;
            for (stream, snapshot) in &snapshots {
                t.insert((aggregate, stream.as_str()), snapshot.encode().as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Removes every snapshot of `aggregate`; returns how many there were.
    pub fn clear(&self, aggregate: &str) -> Result<usize> {
        let keys: Vec<String> = self
            .list(aggregate)?
            .into_iter()
            .map(|(s, _)| s.to_string())
            .collect();
        let txn = self.inner.index.begin_write()?;
        {
            let mut t = txn.open_table(SNAPSHOTS)?;
            for k in &keys {
                t.remove((aggregate, k.as_str()))?;
            }
        }
        txn.commit()?;
        Ok(keys.len())
    }

    /// Removes the snapshot if present.
    pub fn delete(&self, aggregate: &str, stream: &StreamId) -> Result<bool> {
        let txn = self.inner.index.begin_write()?;
        let existed = {
            let mut t = txn.open_table(SNAPSHOTS)?;
            t.remove((aggregate, stream.as_str()))?.is_some()
        };
        txn.commit()?;
        Ok(existed)
    }

    fn inner_index_path(&self) -> std::path::PathBuf {
        self.inner.index_path()
    }
}
