//! The derived store: everything a derivation or application node computes
//! from a log and keeps beside it, in its own redb file (`derived.redb`).
//!
//! Tables: `meta` (the log this store derives from, its generation, the
//! format), `checkpoints` (runner → next position + the id of the last event
//! applied), `rm:<name>:<table>` (read-model rows, process state, outbox,
//! timers; an encoded key → an opaque row) and `snapshots` (aggregate
//! instance snapshots, a cache).
//!
//! A runner commits its rows and its checkpoint in **one** transaction, so a
//! crash never leaves the checkpoint ahead of or behind the rows. Checkpoints
//! and snapshots carry the **id of the last event they include**: that
//! fingerprint is how a node notices a log that went backwards (a
//! truncation, a restore, a failover to a shorter primary) with nothing but
//! ordinary reads of the log.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fold_core::{EventId, FsyncPolicy, GlobalPosition, StreamId, StreamVersion};
use redb::{
    Database, Durability, ReadTransaction, ReadableDatabase, ReadableTable, ReadableTableMetadata,
    TableDefinition, TableHandle, WriteTransaction,
};
use uuid::Uuid;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("io error during {op} on {path}: {source}")]
    Io {
        path: PathBuf,
        op: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("corrupt derived store {path}: {reason}")]
    Corrupt { path: PathBuf, reason: String },
    #[error("store error: {0}")]
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

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const CHECKPOINTS: TableDefinition<&str, &[u8]> = TableDefinition::new("checkpoints");
const SNAPSHOTS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("snapshots");
type RowTable<'a> = TableDefinition<'a, &'static [u8], &'static [u8]>;

const META_LOG_ID: &str = "log_id";
const META_GENERATION: &str = "generation";
const META_FORMAT: &str = "format";
const META_SCHEMA: &str = "schema_source";
const FORMAT: u32 = 1;

/// `rm:<name>:<table>`: the redb table holding one read-model table.
pub fn read_model_table_name(name: &str, table: &str) -> String {
    format!("rm:{name}:{table}")
}

/// Where a runner is: the next position it has to process and the id of
/// the event it applied last (`None` before the first).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpoint {
    pub next: GlobalPosition,
    pub last_event_id: Option<EventId>,
}

impl Checkpoint {
    /// A checkpoint at `next` with no fingerprint (nothing applied yet, or
    /// a checkpoint advanced past positions nobody reacted to).
    pub fn at(next: u64) -> Self {
        Checkpoint {
            next: GlobalPosition(next),
            last_event_id: None,
        }
    }

    pub fn with_event(mut self, id: EventId) -> Self {
        self.last_event_id = Some(id);
        self
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(24);
        out.extend_from_slice(&self.next.0.to_be_bytes());
        let id = self.last_event_id.map_or([0u8; 16], |id| *id.0.as_bytes());
        out.extend_from_slice(&id);
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 24 {
            return None;
        }
        let next = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
        let id = Uuid::from_bytes(bytes[8..24].try_into().ok()?);
        Some(Checkpoint {
            next: GlobalPosition(next),
            last_event_id: (!id.is_nil()).then_some(EventId(id)),
        })
    }
}

/// Aggregate state as of `version`, produced by the evolve module whose
/// hash is `module_hash`, from a stream whose event at `version` has id
/// `event_id`. A reader ignores a snapshot whose hash differs from the
/// module it is about to run, or whose event id the log does not confirm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub version: StreamVersion,
    pub module_hash: [u8; 32],
    pub event_id: EventId,
    pub state: Vec<u8>,
}

const SNAP_PREFIX: usize = 8 + 32 + 16;

impl Snapshot {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(SNAP_PREFIX + self.state.len());
        out.extend_from_slice(&self.version.0.to_be_bytes());
        out.extend_from_slice(&self.module_hash);
        out.extend_from_slice(self.event_id.0.as_bytes());
        out.extend_from_slice(&self.state);
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < SNAP_PREFIX {
            return None;
        }
        Some(Snapshot {
            version: StreamVersion(u64::from_be_bytes(bytes[0..8].try_into().ok()?)),
            module_hash: bytes[8..40].try_into().ok()?,
            event_id: EventId(Uuid::from_bytes(bytes[40..SNAP_PREFIX].try_into().ok()?)),
            state: bytes[SNAP_PREFIX..].to_vec(),
        })
    }
}

/// What a reset removed.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct ResetReport {
    /// Runners whose checkpoint and tables were dropped.
    pub runners_reset: Vec<String>,
    pub tables_dropped: u64,
    pub snapshots_dropped: u64,
}

struct Inner {
    db: Database,
    path: PathBuf,
    durability: Durability,
}

/// Handle on a derived store. Cheap to clone.
#[derive(Clone)]
pub struct DerivedStore {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for DerivedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DerivedStore({})", self.inner.path.display())
    }
}

impl DerivedStore {
    /// Opens the store at `path`, creating it (and its directory) if needed.
    pub fn open_or_create(path: &Path, fsync: FsyncPolicy) -> Result<DerivedStore> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| Error::Io {
                path: parent.to_path_buf(),
                op: "create_dir",
                source,
            })?;
        }
        let db = Database::create(path)?;
        let inner = Arc::new(Inner {
            db,
            path: path.to_path_buf(),
            durability: match fsync {
                FsyncPolicy::Always => Durability::Immediate,
                FsyncPolicy::Never => Durability::None,
            },
        });
        let store = DerivedStore { inner };
        // Every fixed table exists from the start; the format is recorded.
        let txn = store.begin_write_durable()?;
        {
            txn.open_table(CHECKPOINTS)?;
            txn.open_table(SNAPSHOTS)?;
            let mut meta = txn.open_table(META)?;
            let found = meta
                .get(META_FORMAT)?
                .map(|v| u32::from_be_bytes(v.value().try_into().unwrap_or([0; 4])));
            match found {
                None => {
                    meta.insert(META_FORMAT, FORMAT.to_be_bytes().as_slice())?;
                }
                Some(found) if found != FORMAT => {
                    return Err(store.corrupt(format!(
                        "format {found} is not the supported format {FORMAT}"
                    )));
                }
                Some(_) => {}
            }
        }
        txn.commit()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    fn corrupt(&self, reason: impl Into<String>) -> Error {
        Error::Corrupt {
            path: self.inner.path.clone(),
            reason: reason.into(),
        }
    }

    fn begin_read(&self) -> Result<ReadTransaction> {
        Ok(self.inner.db.begin_read()?)
    }

    fn begin_write(&self) -> Result<WriteTransaction> {
        let mut txn = self.inner.db.begin_write()?;
        txn.set_durability(self.inner.durability)?;
        Ok(txn)
    }

    /// A write transaction that is durable regardless of policy: for the
    /// identity of the store, which must survive a crash.
    fn begin_write_durable(&self) -> Result<WriteTransaction> {
        let mut txn = self.inner.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        Ok(txn)
    }

    fn meta_u64(&self, key: &str) -> Result<Option<u64>> {
        let txn = self.begin_read()?;
        let t = txn.open_table(META)?;
        match t.get(key)? {
            None => Ok(None),
            Some(v) => {
                Ok(Some(u64::from_be_bytes(v.value().try_into().map_err(
                    |_| self.corrupt(format!("meta {key} is not 8 bytes")),
                )?)))
            }
        }
    }

    /// The log this store was built from, if bound.
    pub fn log_id(&self) -> Result<Option<Uuid>> {
        let txn = self.begin_read()?;
        let t = txn.open_table(META)?;
        match t.get(META_LOG_ID)? {
            None => Ok(None),
            Some(v) => {
                Ok(Some(Uuid::from_bytes(v.value().try_into().map_err(
                    |_| self.corrupt("meta log_id is not 16 bytes"),
                )?)))
            }
        }
    }

    /// Binds the store to `log_id`. A store bound to another log is reset
    /// first (every table dropped); returns whether that happened. Durable.
    pub fn bind(&self, log_id: Uuid) -> Result<bool> {
        let was_reset = match self.log_id()? {
            Some(current) if current == log_id => return Ok(false),
            Some(_) => {
                self.reset_all()?;
                true
            }
            None => false,
        };
        let txn = self.begin_write_durable()?;
        {
            let mut t = txn.open_table(META)?;
            t.insert(META_LOG_ID, log_id.as_bytes().as_slice())?;
        }
        txn.commit()?;
        Ok(was_reset)
    }

    /// The log generation this store last reconciled with (see
    /// `Log::generation`), if any.
    pub fn generation(&self) -> Result<Option<u64>> {
        self.meta_u64(META_GENERATION)
    }

    pub fn set_generation(&self, generation: u64) -> Result<()> {
        let txn = self.begin_write_durable()?;
        {
            let mut t = txn.open_table(META)?;
            t.insert(META_GENERATION, generation.to_be_bytes().as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// The schema text this store's tables were derived under, if recorded.
    pub fn schema_source(&self) -> Result<Option<String>> {
        let txn = self.begin_read()?;
        let t = txn.open_table(META)?;
        match t.get(META_SCHEMA)? {
            None => Ok(None),
            Some(v) => Ok(Some(
                String::from_utf8(v.value().to_vec())
                    .map_err(|_| self.corrupt("meta schema_source is not UTF-8"))?,
            )),
        }
    }

    pub fn set_schema_source(&self, text: &str) -> Result<()> {
        let txn = self.begin_write_durable()?;
        {
            let mut t = txn.open_table(META)?;
            t.insert(META_SCHEMA, text.as_bytes())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// A consistent view of every table and checkpoint as of now.
    pub fn snapshot(&self) -> Result<DerivedSnapshot> {
        Ok(DerivedSnapshot {
            txn: self.begin_read()?,
            path: self.inner.path.clone(),
        })
    }

    /// Where `name` is, `None` if it has never committed.
    pub fn checkpoint(&self, name: &str) -> Result<Option<Checkpoint>> {
        self.snapshot()?.checkpoint(name)
    }

    /// Applies `puts` and `deletes` and sets `name`'s checkpoint in one
    /// transaction. `puts` are `(table, key, row)`, `deletes` are
    /// `(table, key)`; a delete of an absent key is a no-op. Within one
    /// commit, later operations on a key win over earlier ones.
    pub fn commit(
        &self,
        name: &str,
        checkpoint: Checkpoint,
        puts: Vec<(String, Vec<u8>, Vec<u8>)>,
        deletes: Vec<(String, Vec<u8>)>,
    ) -> Result<()> {
        let txn = self.begin_write()?;
        {
            let mut names: HashMap<&str, String> = HashMap::new();
            for (t, _, _) in &puts {
                names
                    .entry(t.as_str())
                    .or_insert_with(|| read_model_table_name(name, t));
            }
            for (t, _) in &deletes {
                names
                    .entry(t.as_str())
                    .or_insert_with(|| read_model_table_name(name, t));
            }
            let mut tables: HashMap<&str, redb::Table<'_, &[u8], &[u8]>> = HashMap::new();
            for (short, full) in &names {
                let def: RowTable<'_> = TableDefinition::new(full);
                tables.insert(short, txn.open_table(def)?);
            }
            for (t, key, row) in &puts {
                tables
                    .get_mut(t.as_str())
                    .expect("table opened above")
                    .insert(key.as_slice(), row.as_slice())?;
            }
            for (t, key) in &deletes {
                tables
                    .get_mut(t.as_str())
                    .expect("table opened above")
                    .remove(key.as_slice())?;
            }
            let mut checkpoints = txn.open_table(CHECKPOINTS)?;
            checkpoints.insert(name, checkpoint.encode().as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Drops every row of `name`'s `tables` and its checkpoint in one
    /// transaction: the state before the runner ever ran. Tables that were
    /// never written are skipped.
    pub fn reset(&self, name: &str, tables: &[&str]) -> Result<()> {
        let txn = self.begin_write()?;
        {
            for t in tables {
                delete_table_if_exists(&txn, &read_model_table_name(name, t))?;
            }
            let mut checkpoints = txn.open_table(CHECKPOINTS)?;
            checkpoints.remove(name)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Deletes `rows` (`(table, key)`) of `name`, keeping its checkpoint.
    /// Absent keys are no-ops.
    pub fn delete_rows(&self, name: &str, rows: &[(String, Vec<u8>)]) -> Result<()> {
        let txn = self.begin_write()?;
        {
            let mut names: HashMap<&str, String> = HashMap::new();
            for (t, _) in rows {
                names
                    .entry(t.as_str())
                    .or_insert_with(|| read_model_table_name(name, t));
            }
            for (t, key) in rows {
                let def: RowTable<'_> = TableDefinition::new(&names[t.as_str()]);
                let mut table = txn.open_table(def)?;
                table.remove(key.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Drops `name`'s `tables` (rows and all), keeping its checkpoint: for
    /// tables the schema no longer declares.
    pub fn drop_tables(&self, name: &str, tables: &[&str]) -> Result<()> {
        let txn = self.begin_write()?;
        for t in tables {
            delete_table_if_exists(&txn, &read_model_table_name(name, t))?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Drops every runner whose checkpoint is past `cut` (its checkpoint and
    /// all its tables) and every aggregate snapshot, in one transaction: what
    /// a log truncated to `cut` invalidates. Runners at or before the cut are
    /// untouched.
    pub fn reset_past(&self, cut: GlobalPosition) -> Result<ResetReport> {
        let mut report = ResetReport::default();
        let txn = self.begin_write()?;
        {
            let doomed: Vec<String> = {
                let checkpoints = txn.open_table(CHECKPOINTS)?;
                let mut doomed = Vec::new();
                for entry in checkpoints.iter()? {
                    let (k, v) = entry?;
                    let cp = Checkpoint::decode(v.value()).ok_or_else(|| {
                        self.corrupt(format!("checkpoint of {} is malformed", k.value()))
                    })?;
                    if cp.next > cut {
                        doomed.push(k.value().to_string());
                    }
                }
                doomed
            };
            let all_tables: Vec<String> =
                txn.list_tables()?.map(|t| t.name().to_string()).collect();
            {
                let mut checkpoints = txn.open_table(CHECKPOINTS)?;
                for name in &doomed {
                    checkpoints.remove(name.as_str())?;
                }
            }
            for name in &doomed {
                let prefix = format!("rm:{name}:");
                for t in all_tables.iter().filter(|t| t.starts_with(&prefix)) {
                    if delete_table_if_exists(&txn, t)? {
                        report.tables_dropped += 1;
                    }
                }
            }
            report.runners_reset = doomed;
            {
                let snapshots = txn.open_table(SNAPSHOTS)?;
                report.snapshots_dropped = snapshots.len()?;
            }
            txn.delete_table(SNAPSHOTS)?;
            txn.open_table(SNAPSHOTS)?;
        }
        txn.commit()?;
        Ok(report)
    }

    /// Everything but the store's identity.
    pub fn reset_all(&self) -> Result<ResetReport> {
        let mut report = ResetReport::default();
        let txn = self.begin_write_durable()?;
        {
            {
                let checkpoints = txn.open_table(CHECKPOINTS)?;
                for entry in checkpoints.iter()? {
                    let (k, _) = entry?;
                    report.runners_reset.push(k.value().to_string());
                }
                let snapshots = txn.open_table(SNAPSHOTS)?;
                report.snapshots_dropped = snapshots.len()?;
            }
            let all_tables: Vec<String> =
                txn.list_tables()?.map(|t| t.name().to_string()).collect();
            for t in all_tables {
                if t.starts_with("rm:") && delete_table_if_exists(&txn, &t)? {
                    report.tables_dropped += 1;
                }
            }
            txn.delete_table(CHECKPOINTS)?;
            txn.open_table(CHECKPOINTS)?;
            txn.delete_table(SNAPSHOTS)?;
            txn.open_table(SNAPSHOTS)?;
            let mut meta = txn.open_table(META)?;
            meta.remove(META_GENERATION)?;
            meta.remove(META_SCHEMA)?;
        }
        txn.commit()?;
        Ok(report)
    }

    /// The aggregate instance snapshots.
    pub fn snapshots(&self) -> SnapshotStore {
        SnapshotStore {
            store: self.clone(),
        }
    }

    pub fn flush(&self) -> Result<()> {
        let txn = self.begin_write_durable()?;
        txn.commit()?;
        Ok(())
    }
}

fn delete_table_if_exists(txn: &WriteTransaction, name: &str) -> Result<bool> {
    let def: RowTable<'_> = TableDefinition::new(name);
    match txn.delete_table(def) {
        Ok(existed) => Ok(existed),
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(false),
        Err(e) => Err(Error::from(e)),
    }
}

/// A point-in-time view of the store.
pub struct DerivedSnapshot {
    txn: ReadTransaction,
    path: PathBuf,
}

impl std::fmt::Debug for DerivedSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DerivedSnapshot")
    }
}

impl DerivedSnapshot {
    fn open(
        &self,
        name: &str,
        table: &str,
    ) -> Result<Option<redb::ReadOnlyTable<&'static [u8], &'static [u8]>>> {
        let full = read_model_table_name(name, table);
        let def: RowTable<'_> = TableDefinition::new(&full);
        match self.txn.open_table(def) {
            Ok(t) => Ok(Some(t)),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(Error::from(e)),
        }
    }

    /// The row at `key`, if any. A table nobody has written to yet reads as
    /// empty.
    pub fn get(&self, name: &str, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(t) = self.open(name, table)? else {
            return Ok(None);
        };
        Ok(t.get(key)?.map(|g| g.value().to_vec()))
    }

    /// Rows whose key starts with `prefix`, in key order, at most `limit`.
    pub fn scan(
        &self,
        name: &str,
        table: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let Some(t) = self.open(name, table)? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        if limit == 0 {
            return Ok(out);
        }
        for item in t.range(prefix..)? {
            let (k, v) = item?;
            let k = k.value();
            if !k.starts_with(prefix) {
                break;
            }
            out.push((k.to_vec(), v.value().to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// `name`'s checkpoint as of this snapshot.
    pub fn checkpoint(&self, name: &str) -> Result<Option<Checkpoint>> {
        let t = self.txn.open_table(CHECKPOINTS)?;
        match t.get(name)? {
            None => Ok(None),
            Some(v) => Checkpoint::decode(v.value())
                .map(Some)
                .ok_or_else(|| Error::Corrupt {
                    path: self.path.clone(),
                    reason: format!("checkpoint of {name} is malformed"),
                }),
        }
    }

    /// Every checkpoint, by runner name.
    pub fn checkpoints(&self) -> Result<Vec<(String, Checkpoint)>> {
        let t = self.txn.open_table(CHECKPOINTS)?;
        let mut out = Vec::new();
        for entry in t.iter()? {
            let (k, v) = entry?;
            let cp = Checkpoint::decode(v.value()).ok_or_else(|| Error::Corrupt {
                path: self.path.clone(),
                reason: format!("checkpoint of {} is malformed", k.value()),
            })?;
            out.push((k.value().to_string(), cp));
        }
        Ok(out)
    }
}

/// Aggregate instance snapshots: a cache, never a source of truth. One row
/// per `(aggregate, stream)`, overwritten by every `put`.
#[derive(Clone)]
pub struct SnapshotStore {
    store: DerivedStore,
}

impl std::fmt::Debug for SnapshotStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SnapshotStore")
    }
}

impl SnapshotStore {
    pub fn get(&self, aggregate: &str, stream: &StreamId) -> Result<Option<Snapshot>> {
        let txn = self.store.begin_read()?;
        let t = txn.open_table(SNAPSHOTS)?;
        match t.get((aggregate, stream.as_str()))? {
            None => Ok(None),
            Some(g) => Snapshot::decode(g.value()).map(Some).ok_or_else(|| {
                self.store.corrupt(format!(
                    "snapshot row for ({aggregate}, {stream}) is shorter than its header"
                ))
            }),
        }
    }

    /// Stores or replaces the snapshot. Idempotent.
    pub fn put(&self, aggregate: &str, stream: &StreamId, snapshot: Snapshot) -> Result<()> {
        let txn = self.store.begin_write()?;
        {
            let mut t = txn.open_table(SNAPSHOTS)?;
            t.insert((aggregate, stream.as_str()), snapshot.encode().as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Every snapshot of `aggregate`, by stream id in key order.
    pub fn list(&self, aggregate: &str) -> Result<Vec<(StreamId, Snapshot)>> {
        let txn = self.store.begin_read()?;
        let t = txn.open_table(SNAPSHOTS)?;
        let mut out = Vec::new();
        for entry in t.range((aggregate, "")..)? {
            let (k, v) = entry?;
            let (agg, stream) = k.value();
            if agg != aggregate {
                break;
            }
            let snapshot = Snapshot::decode(v.value()).ok_or_else(|| {
                self.store.corrupt(format!(
                    "snapshot row for ({aggregate}, {stream}) is shorter than its header"
                ))
            })?;
            let stream = StreamId::new(stream)
                .map_err(|e| self.store.corrupt(format!("snapshot stream id: {e}")))?;
            out.push((stream, snapshot));
        }
        Ok(out)
    }

    /// Stores many snapshots in one transaction, replacing any present.
    pub fn put_many(&self, aggregate: &str, snapshots: Vec<(StreamId, Snapshot)>) -> Result<()> {
        let txn = self.store.begin_write()?;
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
        let txn = self.store.begin_write()?;
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
        let txn = self.store.begin_write()?;
        let existed = {
            let mut t = txn.open_table(SNAPSHOTS)?;
            t.remove((aggregate, stream.as_str()))?.is_some()
        };
        txn.commit()?;
        Ok(existed)
    }
}
