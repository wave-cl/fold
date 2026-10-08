//! Projection runners: one task per projection, catching up from its
//! checkpoint and then following the log live.
//!
//! A batch is applied in one read-model transaction together with the
//! checkpoint, so every position is applied exactly once. Within a batch the
//! guest sees its own earlier writes through `get_row`, so a placed-then-
//! cancelled order replays correctly even when both land in one batch.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use fold_core::{GlobalPosition, RecordedEvent};
use fold_schema::{ColumnOp, Projection, Table};
use fold_wasm::{Event, Guest, Mutation, Op, ProjectionInput, RowReader};
use serde_json::Value;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::codec;
use crate::keys;
use crate::state::Shared;

/// Events read per catch-up batch.
pub const BATCH: usize = 256;

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

/// What an operator may ask a running projection to do.
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

#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    #[error("log: {0}")]
    Core(#[from] fold_core::Error),
    #[error("wasm: {0}")]
    Wasm(#[from] fold_wasm::WasmError),
    #[error("event {position} payload is not JSON: {source}")]
    Payload {
        position: u64,
        #[source]
        source: serde_json::Error,
    },
    #[error("mutation names table {0}, which this projection does not declare")]
    UnknownTable(String),
    #[error("mutation key for table {table}: {source}")]
    Key {
        table: String,
        #[source]
        source: keys::KeyError,
    },
    #[error("mutation on table {table}: {reason}")]
    Invalid { table: String, reason: String },
    #[error("row of table {table} does not match the schema: {reasons}")]
    Row { table: String, reasons: String },
    #[error("stored row of table {table} is not JSON: {source}")]
    StoredRow {
        table: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("snapshot: {0}")]
    Snapshot(#[from] crate::snapshot::SnapshotError),
}

pub fn spawn_all(shared: Arc<Shared>) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();
    for (ctx, proj) in shared.schema.projections() {
        let name = format!("{}.{}", ctx.name, proj.name);
        let tx = shared.status_senders[&name].clone();
        let control = shared
            .projection_control_receivers
            .lock()
            .expect("control receivers")
            .remove(&name)
            .expect("one receiver per projection, taken once");
        let shared = shared.clone();
        let ctx_name = ctx.name.clone();
        let proj_name = proj.name.clone();
        handles.push(tokio::spawn(async move {
            run(shared, ctx_name, proj_name, name, tx, control).await;
        }));
    }
    handles
}

async fn run(
    shared: Arc<Shared>,
    ctx: String,
    proj: String,
    name: String,
    tx: watch::Sender<Status>,
    control: tokio::sync::mpsc::Receiver<Control>,
) {
    let set = |f: &dyn Fn(&mut Status)| tx.send_modify(|s| f(s));
    match run_inner(&shared, &ctx, &proj, &name, &tx, control).await {
        Ok(()) => set(&|s| s.state = State::Stopped),
        Err(e) => {
            tracing::error!(projection = %name, error = %e, "projection failed; it will not advance until restarted");
            set(&|s| {
                s.state = State::Failed;
                s.error = Some(e.to_string());
            });
        }
    }
}

/// Handles one control request. Returns the position to continue from
/// after a rebuild, or `None` when the checkpoint is unchanged.
async fn handle_control(
    shared: &Arc<Shared>,
    projection: &Projection,
    name: &str,
    guest: &Guest,
    models: &fold_core::ReadModelStore,
    tx: &watch::Sender<Status>,
    control: Control,
) -> Result<Option<u64>, ApplyError> {
    let tables: Vec<String> = projection.tables.keys().cloned().collect();
    match control {
        Control::Snapshot { reply } => {
            let result = {
                let shared = shared.clone();
                let name = name.to_string();
                let models = models.clone();
                let hash = guest.hash();
                tokio::task::spawn_blocking(move || {
                    crate::snapshot::take(shared.log.path(), &name, &tables, hash, &models)
                })
                .await
                .expect("snapshot task")
            };
            let _ = reply.send(result);
            Ok(None)
        }
        Control::Rebuild {
            snapshot,
            force,
            reply,
        } => {
            tx.send_modify(|s| {
                s.state = State::Rebuilding;
                s.checkpoint = None;
            });
            let result = {
                let shared = shared.clone();
                let name = name.to_string();
                let models = models.clone();
                let hash = crate::snapshot::hex(&guest.hash());
                tokio::task::spawn_blocking(move || {
                    crate::snapshot::rebuild(
                        shared.log.path(),
                        &name,
                        &tables,
                        &hash,
                        snapshot,
                        force,
                        &models,
                    )
                })
                .await
                .expect("rebuild task")
            };
            match result {
                Ok(from) => {
                    tx.send_modify(|s| {
                        s.checkpoint = from;
                        s.head = shared.log.head().0;
                    });
                    let _ = reply.send(Ok(from));
                    Ok(Some(from.map_or(0, |c| c + 1)))
                }
                Err(e) => {
                    // The stored checkpoint is still the truth: carry on from it.
                    let cp = models.checkpoint(name)?.map(|p| p.0);
                    tx.send_modify(|s| {
                        s.state = State::CatchingUp;
                        s.checkpoint = cp.and_then(|c| c.checked_sub(1));
                    });
                    let _ = reply.send(Err(e));
                    Ok(cp)
                }
            }
        }
    }
}

async fn run_inner(
    shared: &Arc<Shared>,
    ctx: &str,
    proj: &str,
    name: &str,
    tx: &watch::Sender<Status>,
    mut control: tokio::sync::mpsc::Receiver<Control>,
) -> Result<(), ApplyError> {
    let projection = shared
        .schema
        .projection(ctx, proj)
        .expect("projection exists");
    let export = projection
        .fold
        .export_or(&format!("project_{}", projection.name))
        .to_string();
    let guest = shared.guest(&projection.fold.module);
    let families: HashSet<(String, String)> = projection
        .from
        .iter()
        .map(|r| (r.context.clone(), r.name.clone()))
        .collect();

    let models = shared.log.read_models();
    let mut next: u64 = {
        let shared = shared.clone();
        let name = name.to_string();
        tokio::task::spawn_blocking(move || shared.log.read_models().checkpoint(&name))
            .await
            .expect("checkpoint task")?
            .map(|p| p.0)
            .unwrap_or(0)
    };
    tx.send_modify(|s| {
        s.checkpoint = next.checked_sub(1);
        s.head = shared.log.head().0;
    });

    let mut sub = shared.log.subscribe();
    let mut since_snapshot: u64 = 0;
    loop {
        // Catch up: the log is the queue, read it in batches.
        loop {
            if shared.cancel.is_cancelled() {
                return Ok(());
            }
            while let Ok(c) = control.try_recv() {
                if let Some(from) =
                    handle_control(shared, projection, name, &guest, &models, tx, c).await?
                {
                    next = from;
                    since_snapshot = 0;
                }
            }
            let batch = {
                let log = shared.log.clone();
                tokio::task::spawn_blocking(move || log.read_all(GlobalPosition(next), BATCH))
                    .await
                    .expect("read task")?
            };
            if batch.is_empty() {
                break;
            }
            tx.send_modify(|s| {
                s.state = State::CatchingUp;
                s.head = shared.log.head().0;
            });
            let last = batch.last().expect("non-empty").position.0;
            let batch_len = batch.len() as u64;
            {
                let shared = shared.clone();
                let guest = guest.clone();
                let families = families.clone();
                let name = name.to_string();
                let export = export.clone();
                let projection = projection.clone();
                let models = models.clone();
                tokio::task::spawn_blocking(move || {
                    apply_batch(
                        &shared,
                        &projection,
                        &name,
                        &guest,
                        &export,
                        &families,
                        &models,
                        &batch,
                    )
                })
                .await
                .expect("apply task")?;
            }
            next = last + 1;
            since_snapshot += batch_len;
            tx.send_modify(|s| {
                s.checkpoint = Some(last);
                s.head = shared.log.head().0;
            });
            if projection.snapshot_every > 0
                && since_snapshot >= u64::from(projection.snapshot_every)
            {
                since_snapshot = 0;
                let (reply, rx) = tokio::sync::oneshot::channel();
                handle_control(
                    shared,
                    projection,
                    name,
                    &guest,
                    &models,
                    tx,
                    Control::Snapshot { reply },
                )
                .await?;
                match rx.await {
                    Ok(Ok(meta)) => {
                        tracing::info!(projection = %name, id = %meta.id, rows = meta.rows, "snapshot written")
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(projection = %name, error = %e, "automatic snapshot failed")
                    }
                    Err(_) => {}
                }
            }
        }

        tx.send_modify(|s| {
            s.state = State::Live;
            s.head = shared.log.head().0;
        });
        tokio::select! {
            _ = shared.cancel.cancelled() => return Ok(()),
            c = control.recv() => match c {
                Some(c) => {
                    if let Some(from) =
                        handle_control(shared, projection, name, &guest, &models, tx, c).await?
                    {
                        next = from;
                        since_snapshot = 0;
                    }
                }
                None => return Ok(()),
            },
            r = sub.wait_past(GlobalPosition(next)) => {
                if r.is_err() {
                    return Ok(());
                }
            }
        }
    }
}

/// A row address inside one projection.
type RowKey = (String, Vec<u8>);

/// Pending rows of the batch being applied: `None` means deleted. Stored
/// values are the *columns* only; key fields are merged back on commit.
#[derive(Default)]
struct Pending {
    rows: HashMap<RowKey, Option<Value>>,
    /// The JSON key object for every pending row, to rebuild the stored row.
    keys: HashMap<RowKey, Value>,
}

/// What the guest reads through `fold.get_row`: this batch's pending writes
/// first, then the snapshot taken before the batch.
struct BatchRows {
    projection: Projection,
    name: String,
    schema: Arc<fold_schema::Schema>,
    snapshot: fold_core::ReadModelSnapshot,
    pending: Mutex<Pending>,
}

impl BatchRows {
    fn table(&self, table: &str) -> Result<&Table, ApplyError> {
        self.projection
            .tables
            .get(table)
            .ok_or_else(|| ApplyError::UnknownTable(table.to_string()))
    }

    /// The current columns of a row, or `None` if absent/deleted.
    fn current(&self, table: &Table, key_bytes: &[u8]) -> Result<Option<Value>, ApplyError> {
        let rk = (table.name.clone(), key_bytes.to_vec());
        if let Some(p) = self.pending.lock().expect("pending").rows.get(&rk) {
            return Ok(p.clone());
        }
        match self.snapshot.get(&self.name, &table.name, key_bytes)? {
            None => Ok(None),
            Some(bytes) => {
                let stored: Value =
                    serde_json::from_slice(&bytes).map_err(|source| ApplyError::StoredRow {
                        table: table.name.clone(),
                        source,
                    })?;
                Ok(Some(columns_of(table, stored)))
            }
        }
    }
}

impl RowReader for BatchRows {
    fn get_row(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let t = self.table(table).map_err(|e| e.to_string())?;
        let key_json: Value =
            serde_json::from_slice(key).map_err(|e| format!("key is not JSON: {e}"))?;
        let bytes = keys::encode(&self.schema, t, &key_json).map_err(|e| e.to_string())?;
        match self.current(t, &bytes).map_err(|e| e.to_string())? {
            None => Ok(None),
            Some(cols) => {
                let full = join_row(t, &key_json, cols);
                Ok(Some(serde_json::to_vec(&full).map_err(|e| e.to_string())?))
            }
        }
    }
}

/// Read-only access to one projection's tables as last committed, for
/// invariant checks. Unlike [`BatchRows`] there is no pending batch.
pub struct ProjectionReader {
    projection: Projection,
    name: String,
    schema: Arc<fold_schema::Schema>,
    snapshot: fold_core::ReadModelSnapshot,
}

impl ProjectionReader {
    pub fn new(shared: &Shared, ctx: &str, proj: &str) -> Result<Self, fold_core::Error> {
        let projection = shared
            .schema
            .projection(ctx, proj)
            .expect("resolved projection exists")
            .clone();
        Ok(ProjectionReader {
            projection,
            name: format!("{ctx}.{proj}"),
            schema: shared.schema.clone(),
            snapshot: shared.log.read_models().snapshot()?,
        })
    }
}

impl RowReader for ProjectionReader {
    fn get_row(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let t = self
            .projection
            .tables
            .get(table)
            .ok_or_else(|| format!("projection {} has no table {table}", self.name))?;
        let key_json: Value =
            serde_json::from_slice(key).map_err(|e| format!("key is not JSON: {e}"))?;
        let bytes = keys::encode(&self.schema, t, &key_json).map_err(|e| e.to_string())?;
        self.snapshot
            .get(&self.name, &t.name, &bytes)
            .map_err(|e| e.to_string())
    }
}

/// Waits until `projection` has applied `min_position`, bounded by `wait`.
/// `Ok(checkpoint)` on success; the error names why not.
pub async fn wait_for_checkpoint(
    statuses: &crate::state::StatusBook,
    projection: &str,
    min_position: Option<u64>,
    wait: std::time::Duration,
) -> Result<Option<u64>, CheckpointWait> {
    let rx = statuses
        .get(projection)
        .ok_or_else(|| CheckpointWait::NotRunning(projection.to_string()))?;
    let check = |s: &Status| -> Result<bool, CheckpointWait> {
        if s.state == State::Failed {
            return Err(CheckpointWait::Failed {
                projection: projection.to_string(),
                error: s.error.clone().unwrap_or_default(),
            });
        }
        Ok(match min_position {
            None => true,
            Some(p) => s.checkpoint.is_some_and(|c| c >= p),
        })
    };
    let mut rx = rx.clone();
    if check(&rx.borrow())? {
        return Ok(rx.borrow().checkpoint);
    }
    let result = tokio::time::timeout(wait, async {
        loop {
            if rx.changed().await.is_err() {
                return Err(CheckpointWait::NotRunning(projection.to_string()));
            }
            let s = rx.borrow().clone();
            if check(&s)? {
                return Ok(s.checkpoint);
            }
        }
    })
    .await;
    match result {
        Ok(r) => r,
        Err(_) => Err(CheckpointWait::Behind {
            projection: projection.to_string(),
            checkpoint: rx.borrow().checkpoint,
            wanted: min_position.unwrap_or(0),
        }),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CheckpointWait {
    #[error("projection {0} is not running")]
    NotRunning(String),
    #[error("projection {projection} has failed: {error}")]
    Failed { projection: String, error: String },
    #[error("projection {projection} has applied up to {} but {wanted} was asked for", checkpoint.map(|c| c.to_string()).unwrap_or_else(|| "nothing".into()))]
    Behind {
        projection: String,
        checkpoint: Option<u64>,
        wanted: u64,
    },
}

impl From<CheckpointWait> for tonic::Status {
    fn from(w: CheckpointWait) -> Self {
        match &w {
            CheckpointWait::NotRunning(_) => tonic::Status::not_found(w.to_string()),
            CheckpointWait::Failed { .. } => tonic::Status::failed_precondition(w.to_string()),
            CheckpointWait::Behind { .. } => tonic::Status::unavailable(w.to_string()),
        }
    }
}

/// Strips the key fields from a stored row, leaving the columns.
pub fn columns_of(table: &Table, mut stored: Value) -> Value {
    if let Some(obj) = stored.as_object_mut() {
        for k in &table.keys {
            obj.remove(&k.name);
        }
    }
    stored
}

/// The stored form: key fields followed by the columns.
pub fn join_row(table: &Table, key: &Value, columns: Value) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(k) = key.as_object() {
        for f in &table.keys {
            if let Some(v) = k.get(&f.name) {
                out.insert(f.name.clone(), v.clone());
            }
        }
    }
    if let Some(c) = columns.as_object() {
        for (k, v) in c {
            out.insert(k.clone(), v.clone());
        }
    }
    Value::Object(out)
}

#[allow(clippy::too_many_arguments)]
fn apply_batch(
    shared: &Shared,
    projection: &Projection,
    name: &str,
    guest: &Guest,
    export: &str,
    families: &HashSet<(String, String)>,
    models: &fold_core::ReadModelStore,
    batch: &[RecordedEvent],
) -> Result<(), ApplyError> {
    let rows = Arc::new(BatchRows {
        projection: projection.clone(),
        name: name.to_string(),
        schema: shared.schema.clone(),
        snapshot: models.snapshot()?,
        pending: Mutex::new(Pending::default()),
    });

    for ev in batch {
        let fam = (ev.event_type.context.clone(), ev.event_type.name.clone());
        if !families.contains(&fam) {
            continue;
        }
        let input = ProjectionInput {
            abi: fold_wasm::ABI_VERSION,
            projection: name.to_string(),
            event: to_guest_event(ev)?,
        };
        let mutations = guest.apply(export, &input, rows.clone())?;
        for m in mutations {
            apply_mutation(shared, &rows, m)?;
        }
    }

    let last = batch.last().expect("non-empty batch").position;
    let pending = std::mem::take(&mut *rows.pending.lock().expect("pending"));
    let mut puts = Vec::new();
    let mut deletes = Vec::new();
    for (rk, row) in pending.rows {
        match row {
            None => deletes.push(rk),
            Some(cols) => {
                let table = rows.table(&rk.0)?;
                let key = &pending.keys[&rk];
                let stored = join_row(table, key, cols);
                puts.push((
                    rk.0,
                    rk.1,
                    serde_json::to_vec(&stored).expect("row serializes"),
                ));
            }
        }
    }
    models.commit(name, GlobalPosition(last.0 + 1), puts, deletes)?;
    Ok(())
}

pub fn to_guest_event(ev: &RecordedEvent) -> Result<Event, ApplyError> {
    let payload: Value =
        serde_json::from_slice(&ev.payload).map_err(|source| ApplyError::Payload {
            position: ev.position.0,
            source,
        })?;
    let metadata: Value = if ev.metadata.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&ev.metadata).unwrap_or(Value::Null)
    };
    Ok(Event {
        stream: ev.stream_id.to_string(),
        r#type: codec::type_string(&ev.event_type),
        version: ev.stream_version.0,
        position: ev.position.0,
        payload,
        metadata,
    })
}

fn apply_mutation(shared: &Shared, rows: &BatchRows, m: Mutation) -> Result<(), ApplyError> {
    let table = rows.table(&m.table)?.clone();
    let key_bytes =
        keys::encode(&shared.schema, &table, &m.key).map_err(|source| ApplyError::Key {
            table: table.name.clone(),
            source,
        })?;
    let rk = (table.name.clone(), key_bytes.clone());
    let current = rows.current(&table, &key_bytes)?;
    let next: Option<Value> = match m.op {
        Op::Delete => None,
        Op::Upsert { row } => {
            shared
                .schema
                .validate_row(&table, &row)
                .map_err(|errs| ApplyError::Row {
                    table: table.name.clone(),
                    reasons: errs
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join("; "),
                })?;
            Some(row)
        }
        other => {
            // Same tagged shape on both sides of the ABI.
            let op: ColumnOp = serde_json::to_value(&other)
                .and_then(serde_json::from_value)
                .map_err(|e| ApplyError::Invalid {
                    table: table.name.clone(),
                    reason: e.to_string(),
                })?;
            let applied = fold_schema::rows::apply(&shared.schema, &table, current.as_ref(), &[op])
                .map_err(|e| ApplyError::Invalid {
                    table: table.name.clone(),
                    reason: e.to_string(),
                })?;
            Some(applied)
        }
    };
    let mut pending = rows.pending.lock().expect("pending");
    pending.keys.insert(rk.clone(), m.key);
    pending.rows.insert(rk, next);
    Ok(())
}
