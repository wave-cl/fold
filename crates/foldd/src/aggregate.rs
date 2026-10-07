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
    let event = to_guest_event(ev).map_err(|_| LoadError::Payload {
        position: ev.position.0,
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
    shared
        .schema
        .validate_state(agg, &state)
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
