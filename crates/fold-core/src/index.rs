//! The redb index: positions, stream versions, event types, checkpoints and
//! snapshots. Read-model tables are opened dynamically by name in
//! `readmodel.rs`.

use std::path::Path;

use redb::{
    Database, Durability, MultimapTableDefinition, ReadTransaction, ReadableDatabase,
    TableDefinition, WriteTransaction,
};

use crate::error::Result;
use crate::options::FsyncPolicy;

/// `"head"` → next global position.
pub(crate) const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
/// position → (segment base, byte offset of the record frame).
pub(crate) const POSITIONS: TableDefinition<u64, (u64, u64)> = TableDefinition::new("positions");
/// (stream id, stream version) → position.
pub(crate) const STREAMS: TableDefinition<(&str, u64), u64> = TableDefinition::new("streams");
/// stream id → last version.
pub(crate) const STREAM_HEADS: TableDefinition<&str, u64> = TableDefinition::new("stream_heads");
/// `Context.Name` family → positions.
pub(crate) const EVENT_TYPES: MultimapTableDefinition<&str, u64> =
    MultimapTableDefinition::new("event_types");
/// projection → next position it has to process.
pub(crate) const CHECKPOINTS: TableDefinition<&str, u64> = TableDefinition::new("checkpoints");
/// (aggregate, stream id) → `u64 version BE ++ [u8; 32] module hash ++ state`.
pub(crate) const SNAPSHOTS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("snapshots");

pub(crate) const META_HEAD: &str = "head";

/// Prefix of a read-model table name: `rm:<projection>:<table>`.
pub(crate) fn read_model_table_name(projection: &str, table: &str) -> String {
    format!("rm:{projection}:{table}")
}

/// One record's index entries, as `commit_batch` needs them.
pub(crate) struct IndexEntry<'a> {
    pub position: u64,
    pub segment_base: u64,
    pub offset: u64,
    pub stream: &'a str,
    pub version: u64,
    pub family: &'a str,
}

pub(crate) struct Index {
    db: Database,
    durability: Durability,
}

impl Index {
    fn durability_for(policy: FsyncPolicy) -> Durability {
        match policy {
            FsyncPolicy::Always => Durability::Immediate,
            FsyncPolicy::Never => Durability::None,
        }
    }

    /// Creates a new index file with every fixed table present and
    /// `META.head = head`.
    pub(crate) fn create(path: &Path, policy: FsyncPolicy, head: u64) -> Result<Self> {
        let db = Database::create(path)?;
        let index = Index {
            db,
            durability: Self::durability_for(policy),
        };
        {
            let txn = index.db.begin_write()?;
            {
                let mut meta = txn.open_table(META)?;
                meta.insert(META_HEAD, head)?;
                txn.open_table(POSITIONS)?;
                txn.open_table(STREAMS)?;
                txn.open_table(STREAM_HEADS)?;
                txn.open_multimap_table(EVENT_TYPES)?;
                txn.open_table(CHECKPOINTS)?;
                txn.open_table(SNAPSHOTS)?;
            }
            txn.commit()?;
        }
        Ok(index)
    }

    pub(crate) fn open(path: &Path, policy: FsyncPolicy) -> Result<Self> {
        let db = Database::open(path)?;
        Ok(Index {
            db,
            durability: Self::durability_for(policy),
        })
    }

    pub(crate) fn begin_read(&self) -> Result<ReadTransaction> {
        Ok(self.db.begin_read()?)
    }

    /// A write transaction with the configured durability.
    pub(crate) fn begin_write(&self) -> Result<WriteTransaction> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(self.durability)?;
        Ok(txn)
    }

    /// A write transaction that is durable regardless of policy.
    pub(crate) fn begin_write_durable(&self) -> Result<WriteTransaction> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        Ok(txn)
    }

    pub(crate) fn head(&self) -> Result<u64> {
        let txn = self.begin_read()?;
        let meta = txn.open_table(META)?;
        Ok(meta.get(META_HEAD)?.map(|g| g.value()).unwrap_or(0))
    }

    pub(crate) fn stream_head(&self, stream: &str) -> Result<Option<u64>> {
        let txn = self.begin_read()?;
        let heads = txn.open_table(STREAM_HEADS)?;
        Ok(heads.get(stream)?.map(|g| g.value()))
    }

    pub(crate) fn locate(&self, position: u64) -> Result<Option<(u64, u64)>> {
        let txn = self.begin_read()?;
        let positions = txn.open_table(POSITIONS)?;
        Ok(positions.get(position)?.map(|g| g.value()))
    }

    /// Positions of `stream` at versions `>= from` (forward) or `<= from`
    /// (backward), at most `limit`.
    pub(crate) fn stream_positions(
        &self,
        stream: &str,
        from: u64,
        forward: bool,
        limit: usize,
    ) -> Result<Vec<(u64, u64)>> {
        let txn = self.begin_read()?;
        let streams = txn.open_table(STREAMS)?;
        let mut out = Vec::new();
        if forward {
            let range = streams.range((stream, from)..=(stream, u64::MAX))?;
            for item in range.take(limit) {
                let (k, v) = item?;
                out.push((k.value().1, v.value()));
            }
        } else {
            let range = streams.range((stream, 0)..=(stream, from))?;
            for item in range.rev().take(limit) {
                let (k, v) = item?;
                out.push((k.value().1, v.value()));
            }
        }
        Ok(out)
    }

    /// Positions of events in `family` at `>= from`, ascending, at most
    /// `limit`. The multimap's value set is iterated from its start, so this
    /// is linear in the number of earlier events of that type.
    pub(crate) fn type_positions(&self, family: &str, from: u64, limit: usize) -> Result<Vec<u64>> {
        let txn = self.begin_read()?;
        let types = txn.open_multimap_table(EVENT_TYPES)?;
        let mut out = Vec::new();
        for item in types.get(family)? {
            let p = item?.value();
            if p < from {
                continue;
            }
            out.push(p);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// The commit point of an append: all index entries and the new head in
    /// one transaction.
    pub(crate) fn commit_batch(&self, entries: &[IndexEntry<'_>], new_head: u64) -> Result<()> {
        let txn = self.begin_write()?;
        write_entries(&txn, entries, new_head)?;
        txn.commit()?;
        Ok(())
    }

    /// Forces everything committed so far to disk.
    pub(crate) fn flush(&self) -> Result<()> {
        let txn = self.begin_write_durable()?;
        txn.commit()?;
        Ok(())
    }
}

/// Writes a batch's entries into an open transaction (shared by `append` and
/// the rebuild).
pub(crate) fn write_entries(
    txn: &WriteTransaction,
    entries: &[IndexEntry<'_>],
    new_head: u64,
) -> Result<()> {
    let mut positions = txn.open_table(POSITIONS)?;
    let mut streams = txn.open_table(STREAMS)?;
    let mut heads = txn.open_table(STREAM_HEADS)?;
    let mut types = txn.open_multimap_table(EVENT_TYPES)?;
    let mut meta = txn.open_table(META)?;
    for e in entries {
        positions.insert(e.position, (e.segment_base, e.offset))?;
        streams.insert((e.stream, e.version), e.position)?;
        heads.insert(e.stream, e.version)?;
        types.insert(e.family, e.position)?;
    }
    meta.insert(META_HEAD, new_head)?;
    Ok(())
}
