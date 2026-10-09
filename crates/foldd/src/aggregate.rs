//! Aggregate state: snapshot + replay, with an in-memory cache the single
//! writer keeps current.

use std::num::NonZeroUsize;
use std::sync::Mutex;

use fold_core::{Direction, RecordedEvent, Snapshot, StreamId, StreamVersion};
use fold_schema::{Aggregate, Context};
use fold_wasm::EvolveInput;
use lru::LruCache;
use serde_json::Value;

use crate::projection::to_guest_event;
use crate::state::Shared;

/// Events read per replay page.
const PAGE: usize = 256;

#[derive(Debug, Clone, PartialEq)]
pub struct Cached {
    /// `None` for a stream with no events yet.
    pub version: Option<u64>,
    pub state: Option<Value>,
}

pub struct AggregateCache {
    lru: Mutex<LruCache<String, Cached>>,
}

impl AggregateCache {
    pub fn new(capacity: usize) -> Self {
        AggregateCache {
            lru: Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity.max(1)).expect("non-zero"),
            )),
        }
    }

    pub fn get(&self, stream: &str) -> Option<Cached> {
        self.lru.lock().expect("lru").get(stream).cloned()
    }

    pub fn put(&self, stream: &str, cached: Cached) {
        self.lru
            .lock()
            .expect("lru")
            .put(stream.to_string(), cached);
    }

    pub fn evict(&self, stream: &str) {
        self.lru.lock().expect("lru").pop(stream);
    }

    /// Forgets every cached instance.
    pub fn clear(&self) {
        self.lru.lock().expect("lru").clear();
    }

    pub fn len(&self) -> usize {
        self.lru.lock().expect("lru").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Loaded {
    pub context: String,
    pub aggregate: String,
    pub key: Value,
    pub version: Option<u64>,
    pub state: Option<Value>,
    pub snapshot_version: Option<u64>,
    pub replayed: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("stream {0} does not belong to any aggregate")]
    NoAggregate(String),
    #[error(transparent)]
    Core(#[from] fold_core::Error),
    #[error(transparent)]
    Wasm(#[from] fold_wasm::WasmError),
    #[error("event at position {position} has a non-JSON payload")]
    Payload { position: u64 },
    #[error("event at position {position}: {reason}")]
    Upcast { position: u64, reason: String },
    #[error("evolved state of {aggregate} does not match its declared state: {reasons}")]
    StateInvalid { aggregate: String, reasons: String },
}

/// Resolves the aggregate a stream id belongs to.
pub fn resolve<'a>(
    shared: &'a Shared,
    stream: &str,
) -> Result<(&'a Context, &'a Aggregate, Value), LoadError> {
    shared
        .schema
        .aggregate_for_stream(stream)
        .ok_or_else(|| LoadError::NoAggregate(stream.to_string()))
}

#[allow(clippy::too_many_arguments)]
fn evolve_one(
    shared: &Shared,
    ctx: &Context,
    agg: &Aggregate,
    stream: &str,
    key: &Value,
    prev_version: Option<u64>,
    state: Option<Value>,
    ev: &RecordedEvent,
) -> Result<Value, LoadError> {
    let event = to_guest_event(shared, ev).map_err(|e| match e {
        crate::projection::ApplyError::Upcast { position, source } => LoadError::Upcast {
            position,
            reason: source.to_string(),
        },
        _ => LoadError::Payload {
            position: ev.position.0,
        },
    })?;
    evolve_event(shared, ctx, agg, stream, key, prev_version, state, &event)
}

#[allow(clippy::too_many_arguments)]
fn evolve_event(
    shared: &Shared,
    ctx: &Context,
    agg: &Aggregate,
    stream: &str,
    key: &Value,
    prev_version: Option<u64>,
    state: Option<Value>,
    event: &fold_wasm::Event,
) -> Result<Value, LoadError> {
    let guest = shared.guest(&agg.evolve.module);
    let default_export = format!("evolve_{}", agg.name);
    let export = agg.evolve.export_or(&default_export);
    let input = EvolveInput {
        abi: fold_wasm::ABI_VERSION,
        aggregate: format!("{}.{}", ctx.name, agg.name),
        stream: stream.to_string(),
        key: key.clone(),
        version: prev_version,
        state,
        event: event.clone(),
    };
    let state = guest.evolve(export, &input)?;
    let state = shared
        .schema
        .canonicalize_state(agg, &state)
        .map_err(|errs| LoadError::StateInvalid {
            aggregate: format!("{}.{}", ctx.name, agg.name),
            reasons: errs
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("; "),
        })?;
    Ok(state)
}

/// Loads an aggregate instance: cache, else snapshot plus replay. Blocking.
pub fn load(shared: &Shared, stream: &StreamId) -> Result<Loaded, LoadError> {
    let (ctx, agg, key) = resolve(shared, stream)?;
    let full = format!("{}.{}", ctx.name, agg.name);

    if let Some(c) = shared.aggregates.get(stream) {
        return Ok(Loaded {
            context: ctx.name.clone(),
            aggregate: agg.name.clone(),
            key,
            version: c.version,
            state: c.state,
            snapshot_version: None,
            replayed: 0,
        });
    }

    let guest_hash = shared.guest(&agg.evolve.module).hash();
    let snapshot = shared
        .log
        .snapshots()
        .get(&full, stream)?
        .filter(|s| s.module_hash == guest_hash);
    let (mut version, mut state, snapshot_version) = match snapshot {
        Some(s) => {
            let v: Value =
                serde_json::from_slice(&s.state).map_err(|_| LoadError::Payload { position: 0 })?;
            (Some(s.version.0), Some(v), Some(s.version.0))
        }
        None => (None, None, None),
    };

    let mut replayed = 0u64;
    let mut from = version.map(|v| v + 1).unwrap_or(0);
    loop {
        let page = shared
            .log
            .read_stream(stream, StreamVersion(from), Direction::Forward, PAGE)?;
        if page.is_empty() {
            break;
        }
        for ev in &page {
            state = Some(evolve_one(
                shared,
                ctx,
                agg,
                stream,
                &key,
                version,
                state.take(),
                ev,
            )?);
            version = Some(ev.stream_version.0);
            replayed += 1;
        }
        from = version.expect("set") + 1;
        if page.len() < PAGE {
            break;
        }
    }

    if agg.snapshot_every > 0
        && replayed >= u64::from(agg.snapshot_every)
        && let (Some(v), Some(s)) = (version, &state)
    {
        shared.log.snapshots().put(
            &full,
            stream,
            Snapshot {
                version: StreamVersion(v),
                module_hash: guest_hash,
                state: serde_json::to_vec(s).expect("state serializes"),
            },
        )?;
    }

    shared.aggregates.put(
        stream,
        Cached {
            version,
            state: state.clone(),
        },
    );
    Ok(Loaded {
        context: ctx.name.clone(),
        aggregate: agg.name.clone(),
        key,
        version,
        state,
        snapshot_version,
        replayed,
    })
}

/// Folds events that are about to be appended onto `state`, returning the
/// state the aggregate would have. Nothing is cached or persisted here.
#[allow(clippy::too_many_arguments)]
pub fn evolve_pending(
    shared: &Shared,
    ctx: &Context,
    agg: &Aggregate,
    stream: &str,
    key: &Value,
    mut version: Option<u64>,
    mut state: Option<Value>,
    events: &[fold_wasm::Event],
) -> Result<Value, LoadError> {
    for ev in events {
        state = Some(evolve_event(
            shared,
            ctx,
            agg,
            stream,
            key,
            version,
            state.take(),
            ev,
        )?);
        version = Some(ev.version);
    }
    state.ok_or(LoadError::Payload { position: 0 })
}

/// The table name an aggregate's instance snapshots use inside a snapshot file.
pub const SNAPSHOT_TABLE: &str = "snapshots";

/// One instance snapshot as stored in a snapshot file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct FileSnapshot {
    version: u64,
    module_hash: String,
    state: Value,
}

/// Writes every instance snapshot of `Ctx.Agg` to a snapshot file. The
/// checkpoint recorded is the log head at the time, informational only.
pub fn snapshot_all(
    shared: &Shared,
    ctx: &str,
    agg: &str,
) -> Result<crate::snapshot::SnapshotMeta, crate::snapshot::SnapshotError> {
    let name = format!("{ctx}.{agg}");
    let aggregate = shared
        .schema
        .aggregate(ctx, agg)
        .expect("resolved aggregate exists");
    let hash = shared.guest(&aggregate.evolve.module).hash();
    let mut rows = Vec::new();
    for (stream, snap) in shared.log.snapshots().list(&name)? {
        let state: Value = serde_json::from_slice(&snap.state).unwrap_or(Value::Null);
        let row = FileSnapshot {
            version: snap.version.0,
            module_hash: crate::snapshot::hex(&snap.module_hash),
            state,
        };
        rows.push((
            SNAPSHOT_TABLE.to_string(),
            stream.to_string().into_bytes(),
            serde_json::to_vec(&row).expect("json"),
        ));
    }
    let checkpoint = shared.log.head().0.saturating_sub(1);
    crate::snapshot::write_rows(
        shared.log.path(),
        &name,
        &[SNAPSHOT_TABLE.to_string()],
        hash,
        checkpoint,
        &rows,
    )
}

/// Drops every instance snapshot of `Ctx.Agg` and the in-memory cache, then
/// restores `snapshot` if given; then loads every instance of the aggregate
/// from its events so each is re-evolved by the current module and
/// re-snapshotted where its policy says so. Returns the file's checkpoint
/// when restored, `None` for scratch, and the number of instances warmed.
pub fn rebuild(
    shared: &Shared,
    ctx: &str,
    agg: &str,
    snapshot: Option<String>,
    force: bool,
) -> Result<(Option<u64>, usize), crate::snapshot::RebuildError> {
    use crate::snapshot::{RebuildError, SnapshotError};
    let name = format!("{ctx}.{agg}");
    let aggregate = shared
        .schema
        .aggregate(ctx, agg)
        .expect("resolved aggregate exists");
    let hash = crate::snapshot::hex(&shared.guest(&aggregate.evolve.module).hash());

    let restored = match snapshot {
        None => None,
        Some(id) => {
            let path = crate::snapshot::path_of(shared.log.path(), &name, &id)?;
            let (meta, rows) = crate::snapshot::read(&path)?;
            if meta.projection != name {
                return Err(SnapshotError::WrongProjection {
                    found: meta.projection,
                    wanted: name,
                }
                .into());
            }
            if meta.module_hash != hash && !force {
                return Err(RebuildError::ModuleMismatch { id });
            }
            let mut snaps = Vec::with_capacity(rows.len());
            for (table, key, row) in rows {
                if table != SNAPSHOT_TABLE {
                    return Err(SnapshotError::UnknownTable(table).into());
                }
                let stream = StreamId::new(&String::from_utf8_lossy(&key))?;
                let fs: FileSnapshot =
                    serde_json::from_slice(&row).map_err(|e| SnapshotError::Format {
                        path: path.clone(),
                        reason: format!("snapshot row: {e}"),
                    })?;
                let mut module_hash = [0u8; 32];
                for (i, b) in module_hash.iter_mut().enumerate() {
                    *b = u8::from_str_radix(
                        fs.module_hash.get(2 * i..2 * i + 2).unwrap_or("00"),
                        16,
                    )
                    .unwrap_or(0);
                }
                snaps.push((
                    stream,
                    Snapshot {
                        version: StreamVersion(fs.version),
                        module_hash,
                        state: serde_json::to_vec(&fs.state).expect("json"),
                    },
                ));
            }
            Some((meta.checkpoint, snaps))
        }
    };

    shared.log.snapshots().clear(&name)?;
    shared.aggregates.clear();
    let from = match restored {
        None => None,
        Some((checkpoint, snaps)) => {
            shared.log.snapshots().put_many(&name, snaps)?;
            Some(checkpoint)
        }
    };

    // Warm up: every instance of this aggregate is re-derived now, so a
    // changed evolve module shows its errors here rather than on first use.
    let mut warmed = 0usize;
    for stream in shared.log.stream_ids()? {
        if aggregate.stream.matches(&stream).is_none() {
            continue;
        }
        load(shared, &stream).map_err(|e| match e {
            LoadError::Core(c) => RebuildError::Core(c),
            other => RebuildError::Snapshot(SnapshotError::Format {
                path: shared.log.path().join(&name),
                reason: format!("instance {stream}: {other}"),
            }),
        })?;
        warmed += 1;
    }
    Ok((from, warmed))
}
