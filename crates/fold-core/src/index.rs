//! The redb index: positions, stream versions, event types, checkpoints and
//! snapshots. Read-model tables are opened dynamically by name in
//! `readmodel.rs`.

use std::path::Path;

use redb::{
    Database, Durability, MultimapTableDefinition, ReadTransaction, ReadableDatabase,
    ReadableMultimapTable, ReadableTable, TableDefinition, TableHandle, WriteTransaction,
};

use crate::Error;
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
/// Idempotency keys of appends → the first position they produced. Lives in
/// the index, so like checkpoints it is lost on a rebuild.
pub(crate) const IDEMPOTENCY: TableDefinition<&[u8], u64> = TableDefinition::new("idempotency");
/// The same keys by the position they were first used at, for ranges (an
/// incremental backup, a replication chunk).
pub(crate) const IDEMPOTENCY_BY_POS: TableDefinition<u64, &[u8]> =
    TableDefinition::new("idempotency_by_pos");
/// (aggregate, stream id) → `u64 version BE ++ [u8; 32] module hash ++ state`.
pub(crate) const SNAPSHOTS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("snapshots");

pub(crate) const META_HEAD: &str = "head";
/// The fencing epoch: bumped by every promotion, carried by writes.
pub(crate) const META_EPOCH: &str = "epoch";

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
                txn.open_table(IDEMPOTENCY)?;
                txn.open_table(IDEMPOTENCY_BY_POS)?;
            }
            txn.commit()?;
        }
        Ok(index)
    }

    pub(crate) fn open(path: &Path, policy: FsyncPolicy) -> Result<Self> {
        let db = Database::open(path)?;
        let index = Index {
            db,
            durability: Self::durability_for(policy),
        };
        index.backfill_idempotency_by_pos()?;
        Ok(index)
    }

    /// An index written before the by-position table existed gets it built
    /// from the keys it has; a no-op afterwards.
    fn backfill_idempotency_by_pos(&self) -> Result<()> {
        let has_by_pos = self
            .begin_read()?
            .list_tables()?
            .any(|t| t.name() == IDEMPOTENCY_BY_POS.name());
        if has_by_pos {
            return Ok(());
        }
        let txn = self.begin_write_durable()?;
        {
            let keys: Vec<(Vec<u8>, u64)> = match txn.open_table(IDEMPOTENCY) {
                Ok(t) => t
                    .iter()?
                    .map(|r| r.map(|(k, v)| (k.value().to_vec(), v.value())))
                    .collect::<std::result::Result<_, _>>()?,
                Err(redb::TableError::TableDoesNotExist(_)) => Vec::new(),
                Err(e) => return Err(e.into()),
            };
            let mut by_pos = txn.open_table(IDEMPOTENCY_BY_POS)?;
            for (k, p) in &keys {
                by_pos.insert(*p, k.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
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

    pub(crate) fn epoch(&self) -> Result<u64> {
        let txn = self.begin_read()?;
        let meta = txn.open_table(META)?;
        Ok(meta.get(META_EPOCH)?.map(|g| g.value()).unwrap_or(0))
    }

    pub(crate) fn set_epoch(&self, epoch: u64) -> Result<()> {
        let txn = self.begin_write_durable()?;
        txn.open_table(META)?.insert(META_EPOCH, epoch)?;
        txn.commit()?;
        Ok(())
    }

    pub(crate) fn stream_head(&self, stream: &str) -> Result<Option<u64>> {
        let txn = self.begin_read()?;
        let heads = txn.open_table(STREAM_HEADS)?;
        Ok(heads.get(stream)?.map(|g| g.value()))
    }

    /// Every stream id, in key order.
    pub(crate) fn stream_ids(&self) -> Result<Vec<String>> {
        let txn = self.begin_read()?;
        let heads = txn.open_table(STREAM_HEADS)?;
        let mut out = Vec::new();
        for entry in heads.iter()? {
            let (k, _) = entry?;
            out.push(k.value().to_string());
        }
        Ok(out)
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
    pub(crate) fn commit_batch(
        &self,
        entries: &[IndexEntry<'_>],
        new_head: u64,
        idempotency_key: Option<&[u8]>,
    ) -> Result<()> {
        let txn = self.begin_write()?;
        if let Some(key) = idempotency_key {
            let mut keys = txn.open_table(IDEMPOTENCY)?;
            let previous = keys.get(key)?.map(|v| v.value());
            if let Some(position) = previous {
                drop(keys);
                // Dropping the transaction aborts it; the records written past
                // head are overwritten by the next append.
                return Err(Error::DuplicateKey {
                    position: crate::GlobalPosition(position),
                });
            }
            let first = entries.first().map(|e| e.position).unwrap_or(new_head);
            keys.insert(key, first)?;
            txn.open_table(IDEMPOTENCY_BY_POS)?.insert(first, key)?;
        }
        write_entries(&txn, entries, new_head)?;
        txn.commit()?;
        Ok(())
    }

    /// The position an idempotency key was first used at, if any.
    pub(crate) fn idempotency_position(&self, key: &[u8]) -> Result<Option<u64>> {
        let txn = self.begin_read()?;
        match txn.open_table(IDEMPOTENCY) {
            Ok(t) => Ok(t.get(key)?.map(|v| v.value())),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
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

// ---------------------------------------------------------------------------
// Dump and load, for whole-log backups

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_be_bytes());
    out.extend_from_slice(b);
}

fn put_entry(out: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    put_bytes(out, key);
    put_bytes(out, value);
}

fn take_bytes<'a>(src: &mut &'a [u8]) -> Option<&'a [u8]> {
    if src.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(src[..4].try_into().ok()?) as usize;
    let rest = &src[4..];
    if rest.len() < len {
        return None;
    }
    let (head, tail) = rest.split_at(len);
    *src = tail;
    Some(head)
}

fn str_u64_key(s: &str, n: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(s.len() + 12);
    put_bytes(&mut k, s.as_bytes());
    k.extend_from_slice(&n.to_be_bytes());
    k
}

fn two_str_key(a: &str, b: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(a.len() + b.len() + 8);
    put_bytes(&mut k, a.as_bytes());
    put_bytes(&mut k, b.as_bytes());
    k
}

fn u64_of(b: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(b.try_into().ok()?))
}

fn str_of(b: &[u8]) -> Option<&str> {
    std::str::from_utf8(b).ok()
}

/// A table's entries as `(key, value)` byte pairs in the dump encoding.
pub(crate) struct TableDump {
    pub name: String,
    pub entries: Vec<u8>,
}

impl Index {
    /// Every table, as of one read transaction, in a type-aware byte
    /// encoding. `head` is `META.head` in that same transaction.
    pub(crate) fn dump(&self) -> Result<(u64, Vec<TableDump>)> {
        let txn = self.begin_read()?;
        let head = txn
            .open_table(META)?
            .get(META_HEAD)?
            .map(|g| g.value())
            .unwrap_or(0);
        let mut out = Vec::new();

        let mut e = Vec::new();
        for r in txn.open_table(META)?.iter()? {
            let (k, v) = r?;
            put_entry(&mut e, k.value().as_bytes(), &v.value().to_be_bytes());
        }
        out.push(TableDump {
            name: "meta".into(),
            entries: e,
        });

        let mut e = Vec::new();
        for r in txn.open_table(POSITIONS)?.iter()? {
            let (k, v) = r?;
            let (a, b) = v.value();
            let mut val = a.to_be_bytes().to_vec();
            val.extend_from_slice(&b.to_be_bytes());
            put_entry(&mut e, &k.value().to_be_bytes(), &val);
        }
        out.push(TableDump {
            name: "positions".into(),
            entries: e,
        });

        let mut e = Vec::new();
        for r in txn.open_table(STREAMS)?.iter()? {
            let (k, v) = r?;
            let (s, n) = k.value();
            put_entry(&mut e, &str_u64_key(s, n), &v.value().to_be_bytes());
        }
        out.push(TableDump {
            name: "streams".into(),
            entries: e,
        });

        let mut e = Vec::new();
        for r in txn.open_table(STREAM_HEADS)?.iter()? {
            let (k, v) = r?;
            put_entry(&mut e, k.value().as_bytes(), &v.value().to_be_bytes());
        }
        out.push(TableDump {
            name: "stream_heads".into(),
            entries: e,
        });

        let mut e = Vec::new();
        for r in txn.open_multimap_table(EVENT_TYPES)?.iter()? {
            let (k, values) = r?;
            for v in values {
                let v = v?;
                put_entry(&mut e, k.value().as_bytes(), &v.value().to_be_bytes());
            }
        }
        out.push(TableDump {
            name: "event_types".into(),
            entries: e,
        });

        let mut e = Vec::new();
        for r in txn.open_table(CHECKPOINTS)?.iter()? {
            let (k, v) = r?;
            put_entry(&mut e, k.value().as_bytes(), &v.value().to_be_bytes());
        }
        out.push(TableDump {
            name: "checkpoints".into(),
            entries: e,
        });

        let mut e = Vec::new();
        match txn.open_table(IDEMPOTENCY) {
            Ok(t) => {
                for r in t.iter()? {
                    let (k, v) = r?;
                    put_entry(&mut e, k.value(), &v.value().to_be_bytes());
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(err) => return Err(err.into()),
        }
        out.push(TableDump {
            name: "idempotency".into(),
            entries: e,
        });

        let mut e = Vec::new();
        match txn.open_table(IDEMPOTENCY_BY_POS) {
            Ok(t) => {
                for r in t.iter()? {
                    let (k, v) = r?;
                    put_entry(&mut e, &k.value().to_be_bytes(), v.value());
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(err) => return Err(err.into()),
        }
        out.push(TableDump {
            name: "idempotency_by_pos".into(),
            entries: e,
        });

        let mut e = Vec::new();
        for r in txn.open_table(SNAPSHOTS)?.iter()? {
            let (k, v) = r?;
            let (a, b) = k.value();
            put_entry(&mut e, &two_str_key(a, b), v.value());
        }
        out.push(TableDump {
            name: "snapshots".into(),
            entries: e,
        });

        for handle in txn.list_tables()? {
            let name = handle.name().to_string();
            if !name.starts_with("rm:") {
                continue;
            }
            let def: TableDefinition<&[u8], &[u8]> = TableDefinition::new(&name);
            let mut e = Vec::new();
            for r in txn.open_table(def)?.iter()? {
                let (k, v) = r?;
                put_entry(&mut e, k.value(), v.value());
            }
            out.push(TableDump { name, entries: e });
        }
        Ok((head, out))
    }

    /// Idempotency keys first used at positions in `from..to`, by position.
    pub(crate) fn idempotency_in(&self, from: u64, to: u64) -> Result<Vec<(Vec<u8>, u64)>> {
        let txn = self.begin_read()?;
        let mut out = Vec::new();
        match txn.open_table(IDEMPOTENCY_BY_POS) {
            Ok(t) => {
                for r in t.range(from..to)? {
                    let (p, k) = r?;
                    out.push((k.value().to_vec(), p.value()));
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(e) => return Err(e.into()),
        }
        Ok(out)
    }

    /// Records idempotency keys (an incremental restore or a replication
    /// chunk brings them along).
    pub(crate) fn import_idempotency(&self, entries: &[(Vec<u8>, u64)]) -> Result<()> {
        let txn = self.begin_write_durable()?;
        {
            let mut t = txn.open_table(IDEMPOTENCY)?;
            let mut by_pos = txn.open_table(IDEMPOTENCY_BY_POS)?;
            for (k, v) in entries {
                t.insert(k.as_slice(), *v)?;
                by_pos.insert(*v, k.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Creates a fresh index at `path` holding `tables` from a dump.
    pub(crate) fn load(
        path: &Path,
        policy: FsyncPolicy,
        head: u64,
        tables: &[TableDump],
    ) -> Result<Self> {
        let index = Self::create(path, policy, head)?;
        let bad =
            |table: &str| Error::corrupt(path, 0, format!("backup table {table} is malformed"));
        let txn = index.begin_write_durable()?;
        {
            for t in tables {
                let mut src: &[u8] = &t.entries;
                match t.name.as_str() {
                    "meta" => {
                        let mut tbl = txn.open_table(META)?;
                        while let Some(k) = take_bytes(&mut src) {
                            let v = take_bytes(&mut src).ok_or_else(|| bad(&t.name))?;
                            tbl.insert(
                                str_of(k).ok_or_else(|| bad(&t.name))?,
                                u64_of(v).ok_or_else(|| bad(&t.name))?,
                            )?;
                        }
                        tbl.insert(META_HEAD, head)?;
                    }
                    "positions" => {
                        let mut tbl = txn.open_table(POSITIONS)?;
                        while let Some(k) = take_bytes(&mut src) {
                            let v = take_bytes(&mut src).ok_or_else(|| bad(&t.name))?;
                            if v.len() != 16 {
                                return Err(bad(&t.name));
                            }
                            let a = u64_of(&v[..8]).ok_or_else(|| bad(&t.name))?;
                            let b = u64_of(&v[8..]).ok_or_else(|| bad(&t.name))?;
                            tbl.insert(u64_of(k).ok_or_else(|| bad(&t.name))?, (a, b))?;
                        }
                    }
                    "streams" => {
                        let mut tbl = txn.open_table(STREAMS)?;
                        while let Some(k) = take_bytes(&mut src) {
                            let v = take_bytes(&mut src).ok_or_else(|| bad(&t.name))?;
                            let mut kk = k;
                            let s = take_bytes(&mut kk)
                                .and_then(str_of)
                                .ok_or_else(|| bad(&t.name))?;
                            let n = u64_of(kk).ok_or_else(|| bad(&t.name))?;
                            tbl.insert((s, n), u64_of(v).ok_or_else(|| bad(&t.name))?)?;
                        }
                    }
                    "stream_heads" | "checkpoints" => {
                        let def = if t.name == "stream_heads" {
                            STREAM_HEADS
                        } else {
                            CHECKPOINTS
                        };
                        let mut tbl = txn.open_table(def)?;
                        while let Some(k) = take_bytes(&mut src) {
                            let v = take_bytes(&mut src).ok_or_else(|| bad(&t.name))?;
                            tbl.insert(
                                str_of(k).ok_or_else(|| bad(&t.name))?,
                                u64_of(v).ok_or_else(|| bad(&t.name))?,
                            )?;
                        }
                    }
                    "event_types" => {
                        let mut tbl = txn.open_multimap_table(EVENT_TYPES)?;
                        while let Some(k) = take_bytes(&mut src) {
                            let v = take_bytes(&mut src).ok_or_else(|| bad(&t.name))?;
                            tbl.insert(
                                str_of(k).ok_or_else(|| bad(&t.name))?,
                                u64_of(v).ok_or_else(|| bad(&t.name))?,
                            )?;
                        }
                    }
                    "idempotency" => {
                        let mut tbl = txn.open_table(IDEMPOTENCY)?;
                        while let Some(k) = take_bytes(&mut src) {
                            let v = take_bytes(&mut src).ok_or_else(|| bad(&t.name))?;
                            tbl.insert(k, u64_of(v).ok_or_else(|| bad(&t.name))?)?;
                        }
                    }
                    "idempotency_by_pos" => {
                        let mut tbl = txn.open_table(IDEMPOTENCY_BY_POS)?;
                        while let Some(k) = take_bytes(&mut src) {
                            let v = take_bytes(&mut src).ok_or_else(|| bad(&t.name))?;
                            tbl.insert(u64_of(k).ok_or_else(|| bad(&t.name))?, v)?;
                        }
                    }
                    "snapshots" => {
                        let mut tbl = txn.open_table(SNAPSHOTS)?;
                        while let Some(k) = take_bytes(&mut src) {
                            let v = take_bytes(&mut src).ok_or_else(|| bad(&t.name))?;
                            let mut kk = k;
                            let a = take_bytes(&mut kk)
                                .and_then(str_of)
                                .ok_or_else(|| bad(&t.name))?;
                            let b = take_bytes(&mut kk)
                                .and_then(str_of)
                                .ok_or_else(|| bad(&t.name))?;
                            tbl.insert((a, b), v)?;
                        }
                    }
                    name if name.starts_with("rm:") => {
                        let def: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name);
                        let mut tbl = txn.open_table(def)?;
                        while let Some(k) = take_bytes(&mut src) {
                            let v = take_bytes(&mut src).ok_or_else(|| bad(&t.name))?;
                            tbl.insert(k, v)?;
                        }
                    }
                    other => {
                        return Err(Error::corrupt(
                            path,
                            0,
                            format!("backup holds an unknown table {other}"),
                        ));
                    }
                }
                if !src.is_empty() {
                    return Err(bad(&t.name));
                }
            }
        }
        txn.commit()?;
        Ok(index)
    }
}
