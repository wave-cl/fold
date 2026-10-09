//! The write side: `Command.Execute` and `Command.Append`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use fold_core::{
    EventType, ExpectedVersion, GlobalPosition, NewEvent, RecordedEvent, StreamId, StreamVersion,
};
use fold_proto::v1::command_server::Command as CommandSvc;
use fold_proto::v1::{
    AppendRequest, AppendResponse, ExecuteRequest, ExecuteResponse, expected_version,
};
use fold_schema::{Aggregate, Context};
use fold_wasm::{CommandInput, CommandReply};
use serde_json::Value;
use tonic::{Request, Response, Status};

use crate::aggregate::{self, LoadError};
use crate::codec;
use crate::projection;
use crate::state::Shared;

/// One mutex per active stream, so load → handle → append is atomic per
/// stream without a global lock. Dead entries are swept opportunistically.
#[derive(Default)]
pub struct StreamLocks {
    inner: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
}

impl StreamLocks {
    pub fn get(&self, stream: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self.inner.lock().expect("locks");
        if let Some(existing) = map.get(stream).and_then(Weak::upgrade) {
            return existing;
        }
        if map.len() > 1024 {
            map.retain(|_, w| w.strong_count() > 0);
        }
        let fresh = Arc::new(tokio::sync::Mutex::new(()));
        map.insert(stream.to_string(), Arc::downgrade(&fresh));
        fresh
    }
}

pub struct Service {
    shared: Arc<Shared>,
}

impl Service {
    pub fn new(shared: Arc<Shared>) -> Self {
        Service { shared }
    }
}

/// A command to execute, from gRPC or from a process manager.
#[derive(Debug, Clone)]
pub struct ExecuteParams {
    /// `Context.Aggregate.Command`.
    pub command: String,
    pub stream_id: String,
    pub payload: Vec<u8>,
    pub metadata: Vec<u8>,
    /// When set, the append is refused if the key was used before, and the
    /// outcome is `AlreadyExecuted`: how a process manager retries safely.
    pub idempotency_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub enum ExecuteOutcome {
    Done(ExecuteResponse),
    /// The idempotency key had been used: the command's effects are already
    /// in the log, starting at `position`.
    AlreadyExecuted {
        position: u64,
    },
}

/// The whole write path for one command: resolve, validate, lock the stream,
/// load state, run the handler, check invariants, append.
pub async fn execute(shared: &Arc<Shared>, req: ExecuteParams) -> Result<ExecuteOutcome, Status> {
    ServiceView { shared }.execute_inner(req).await
}

struct ServiceView<'a> {
    shared: &'a Arc<Shared>,
}

fn load_error(e: LoadError) -> Status {
    match e {
        LoadError::NoAggregate(s) => {
            Status::not_found(format!("stream {s} does not belong to any aggregate"))
        }
        LoadError::NoState(a) => Status::failed_precondition(format!(
            "aggregate {a} has no `state` in the derivation layer; its streams cannot be folded"
        )),
        LoadError::Core(e) => codec::core_error(e),
        LoadError::Wasm(e) => codec::wasm_error(e),
        other => {
            tracing::error!(error = %other, "aggregate load failed");
            Status::internal(other.to_string())
        }
    }
}

/// Resolves and validates one event to append, enforcing the stream id
/// against the owning aggregate's template.
fn prepare_event(
    shared: &Shared,
    stream: &StreamId,
    type_ref: &str,
    payload: &[u8],
    metadata: &[u8],
    allowed: Option<(&Context, &Aggregate)>,
) -> Result<NewEvent, Status> {
    let (ctx, name, version) = fold_schema::parse_event_ref(type_ref)
        .map_err(|e| codec::invalid(format!("event type {type_ref:?}: {e}")))?;
    if ctx == fold_schema::RESERVED_CONTEXT {
        return Err(Status::permission_denied(format!(
            "context {} is reserved for the daemon's own events; {type_ref} cannot be appended",
            fold_schema::RESERVED_CONTEXT
        )));
    }
    let ty = match version {
        Some(v) => shared.schema.event_type(&ctx, &name, v),
        None => shared.schema.latest_event_type(&ctx, &name),
    }
    .ok_or_else(|| Status::not_found(format!("event type {type_ref} is not in the schema")))?;

    let payload_json = codec::parse_json(payload, "payload")?;
    let payload_json = shared
        .schema
        .canonicalize_event(ty, &payload_json)
        .map_err(|errs| codec::validation(errs, &format!("payload of {type_ref}")))?;
    if let Some((c, a)) = allowed {
        let latest = shared
            .schema
            .latest_event_type(&ctx, &name)
            .map_or(ty.id.version, |t| t.id.version);
        if ty.id.version != latest {
            return Err(Status::internal(format!(
                "handler of aggregate {}.{} emitted {type_ref}, but the latest version is v{latest}; handlers emit the latest version",
                c.name, a.name
            )));
        }
    }
    if !metadata.is_empty() {
        let m = codec::parse_json(metadata, "metadata")?;
        if !m.is_object() {
            return Err(codec::invalid("metadata must be a JSON object"));
        }
    }

    let owner = match allowed {
        Some((c, a)) => {
            if !a.events.iter().any(|r| r.context == ctx && r.name == name) {
                return Err(Status::internal(format!(
                    "handler emitted {type_ref}, which aggregate {}.{} does not declare",
                    c.name, a.name
                )));
            }
            Some((c, a))
        }
        None => shared.schema.aggregate_for_event(&ctx, &name),
    };
    if let Some((_, agg)) = owner {
        let key = payload_json.get(&agg.key.name).ok_or_else(|| {
            codec::invalid(format!(
                "payload lacks the aggregate key field {}",
                agg.key.name
            ))
        })?;
        let rendered = agg
            .stream
            .render(key)
            .map_err(|e| codec::invalid(format!("aggregate key {}: {e}", agg.key.name)))?;
        if rendered != **stream {
            return Err(codec::invalid(format!(
                "event {type_ref} belongs to stream {rendered}, not {stream}"
            )));
        }
    }

    Ok(NewEvent {
        id: None,
        event_type: EventType {
            context: ty.id.context.clone(),
            name: ty.id.name.clone(),
            version: ty.id.version,
        },
        payload: serde_json::to_vec(&payload_json).expect("json").into(),
        metadata: metadata.to_vec().into(),
    })
}

fn parse_stream(s: &str) -> Result<StreamId, Status> {
    StreamId::new(s).map_err(|e| codec::invalid(format!("stream id: {e}")))
}

/// How long a command waits for a guarding projection to catch up.
const INVARIANT_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// A rejection by a handler or an invariant: `FAILED_PRECONDITION` with the
/// code in `fold-rejection-code` and, for an invariant, its name in
/// `fold-invariant`.
fn rejection(r: &fold_wasm::Rejected, invariant: Option<&str>) -> Status {
    let mut status = match invariant {
        Some(name) => Status::failed_precondition(format!(
            "invariant {name} violated, {}: {}",
            r.code, r.message
        )),
        None => Status::failed_precondition(format!("rejected {}: {}", r.code, r.message)),
    };
    if let Ok(v) = r.code.parse() {
        status.metadata_mut().insert("fold-rejection-code", v);
    }
    if let Some(name) = invariant
        && let Ok(v) = name.parse()
    {
        status.metadata_mut().insert("fold-invariant", v);
    }
    status
}

/// Why `commit` did not append.
enum Committed {
    Status(Status),
    /// The idempotency key was already used by the append at `position`.
    Duplicate {
        position: u64,
    },
}

impl From<Status> for Committed {
    fn from(s: Status) -> Self {
        Committed::Status(s)
    }
}

/// Checks every invariant that guards `agg`, then appends. The caller holds
/// the stream lock. `events` have passed `prepare_event`.
///
/// 1. The candidate state is the loaded state evolved over the new events.
/// 2. The aggregate's state invariants run against it.
/// 3. For each context invariant on this aggregate, the scope value is read
///    from the candidate state, a lock per (invariant, scope) is taken in a
///    fixed order, the guarding projection is caught up to the log head, and
///    the check runs over its read model.
/// 4. The events are appended and the candidate state becomes the cached one.
#[allow(clippy::too_many_arguments)]
async fn commit(
    shared: &Arc<Shared>,
    ctx: &Context,
    agg: &Aggregate,
    stream: StreamId,
    key: Value,
    version: Option<u64>,
    state: Option<Value>,
    events: Vec<NewEvent>,
    idempotency_key: Option<&[u8]>,
) -> Result<Vec<RecordedEvent>, Committed> {
    let locks = &shared.locks;
    let aggregate_name = format!("{}.{}", ctx.name, agg.name);
    let head = shared.log.head().0;
    let first_version = version.map_or(0, |v| v + 1);
    let mut guest_events = Vec::with_capacity(events.len());
    let mut pending = Vec::with_capacity(events.len());
    for (i, e) in events.iter().enumerate() {
        let payload: Value = serde_json::from_slice(&e.payload).expect("validated JSON");
        // Guests (evolve and the invariants) see the latest version, as
        // they do on replay; a raw append may carry an older one.
        let (id, payload) = crate::upcast::to_latest(
            &shared.schema,
            shared.guests(),
            &crate::upcast::type_id(&e.event_type),
            payload,
        )
        .map_err(|e| Committed::Status(Status::internal(e.to_string())))?;
        let metadata: Value = if e.metadata.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&e.metadata).unwrap_or(Value::Null)
        };
        let ty = crate::upcast::type_string(&id);
        guest_events.push(fold_wasm::Event {
            stream: stream.to_string(),
            r#type: ty.clone(),
            version: first_version + i as u64,
            position: head + i as u64,
            payload: payload.clone(),
            metadata: metadata.clone(),
        });
        pending.push(fold_wasm::PendingEvent {
            r#type: ty,
            version: first_version + i as u64,
            payload,
            metadata,
        });
    }
    let last_version = first_version + events.len() as u64 - 1;

    // 1. candidate state
    let candidate = {
        let shared = shared.clone();
        let ctx = ctx.clone();
        let agg = agg.clone();
        let stream = stream.clone();
        let key = key.clone();
        let state = state.clone();
        let guest_events = guest_events.clone();
        tokio::task::spawn_blocking(move || {
            aggregate::evolve_pending(
                &shared,
                &ctx,
                &agg,
                &stream,
                &key,
                version,
                state,
                &guest_events,
            )
        })
        .await
        .map_err(|e| Status::internal(format!("evolve task: {e}")))?
        .map_err(load_error)?
    };

    // 2. state invariants
    let block = shared
        .schema
        .commands_of(&fold_schema::AggRef::new(&ctx.name, &agg.name));
    for inv in block.into_iter().flat_map(|b| b.invariants.values()) {
        let name = format!("{aggregate_name}.{}", inv.name);
        let check = match &inv.check {
            fold_schema::InvariantCheck::Wasm(w) => w,
            fold_schema::InvariantCheck::Expr { expr, text } => {
                if !fold_schema::rules::eval(expr, &candidate) {
                    let r = fold_wasm::Rejected {
                        code: inv.name.clone(),
                        message: text.clone(),
                    };
                    return Err(rejection(&r, Some(&name)).into());
                }
                continue;
            }
        };
        let guest = shared.guest(&check.module);
        let export = check.export_or(&format!("check_{}", inv.name)).to_string();
        let input = fold_wasm::CheckInput {
            abi: fold_wasm::ABI_VERSION,
            ctx: fold_wasm::InvCtx {
                invariant: name.clone(),
                aggregate: aggregate_name.clone(),
                stream: stream.to_string(),
                key: key.clone(),
                version: last_version,
                projection: None,
                scope: None,
            },
            state: candidate.clone(),
            events: pending.clone(),
        };
        let reply = tokio::task::spawn_blocking(move || {
            guest.check(&export, &input, fold_wasm::Guest::no_rows())
        })
        .await
        .map_err(|e| Status::internal(format!("check task: {e}")))?
        .map_err(codec::wasm_error)?;
        if let fold_wasm::CheckReply::Violation(v) = reply {
            return Err(rejection(&v, Some(&name)).into());
        }
    }

    // 3. context invariants, serialized per scope value
    let mut guards: Vec<(String, Value, &fold_schema::ContextInvariant)> = Vec::new();
    for inv in shared.schema.invariants_on(&ctx.name, &agg.name) {
        let scope = candidate
            .get(&inv.scope.name)
            .cloned()
            .unwrap_or(Value::Null);
        if scope.is_null() {
            return Err(Status::internal(format!(
                "invariant {}.{}: candidate state has no scope field {}",
                ctx.name, inv.name, inv.scope.name
            ))
            .into());
        }
        let lock_key = format!(
            "inv:{}.{}:{}",
            ctx.name,
            inv.name,
            serde_json::to_string(&scope).expect("json")
        );
        guards.push((lock_key, scope, inv));
    }
    guards.sort_by(|a, b| a.0.cmp(&b.0));
    let mut held = Vec::with_capacity(guards.len());
    for (lock_key, _, _) in &guards {
        let lock = locks.get(lock_key);
        held.push(lock);
    }
    let mut held_guards = Vec::with_capacity(held.len());
    for lock in &held {
        held_guards.push(lock.lock().await);
    }
    for (_, scope, inv) in &guards {
        let projection = inv.projection.to_string();
        // Every committed event must be visible to the check.
        let caught_up = shared.log.head().0.checked_sub(1);
        projection::wait_for_checkpoint(&shared.statuses, &projection, caught_up, INVARIANT_WAIT)
            .await
            .map_err(Status::from)?;
        let reader = projection::ProjectionReader::new(
            shared,
            &inv.projection.context,
            &inv.projection.name,
        )
        .map_err(codec::store_error)?;
        let guest = shared.guest(&inv.check.module);
        let export = inv
            .check
            .export_or(&format!("check_{}", inv.name))
            .to_string();
        let name = format!("{}.{}", ctx.name, inv.name);
        let input = fold_wasm::CheckInput {
            abi: fold_wasm::ABI_VERSION,
            ctx: fold_wasm::InvCtx {
                invariant: name.clone(),
                aggregate: aggregate_name.clone(),
                stream: stream.to_string(),
                key: key.clone(),
                version: last_version,
                projection: Some(projection),
                scope: Some(scope.clone()),
            },
            state: candidate.clone(),
            events: pending.clone(),
        };
        let reply =
            tokio::task::spawn_blocking(move || guest.check(&export, &input, Arc::new(reader)))
                .await
                .map_err(|e| Status::internal(format!("check task: {e}")))?
                .map_err(codec::wasm_error)?;
        if let fold_wasm::CheckReply::Violation(v) = reply {
            return Err(rejection(&v, Some(&name)).into());
        }
    }

    // 4. append, then the candidate is the cached state
    let expected = match version {
        Some(v) => ExpectedVersion::Exact(StreamVersion(v)),
        None => ExpectedVersion::NoStream,
    };
    let shared2 = shared.clone();
    let stream2 = stream.clone();
    let idem = idempotency_key.map(|k| k.to_vec());
    let appended =
        tokio::task::spawn_blocking(move || -> Result<Vec<RecordedEvent>, fold_core::Error> {
            let n = events.len();
            let r = match &idem {
                Some(k) => shared2
                    .log
                    .append_idempotent(&stream2, expected, events, k)?,
                None => shared2.log.append(&stream2, expected, events)?,
            };
            let recorded = shared2.log.read_all(r.first, n)?;
            shared2.aggregates.put(
                &stream2,
                aggregate::Cached {
                    version: Some(r.stream_version.0),
                    state: Some(candidate),
                },
            );
            Ok(recorded)
        })
        .await
        .map_err(|e| Status::internal(format!("append task: {e}")))?;
    match appended {
        Ok(recorded) => Ok(recorded),
        Err(fold_core::Error::DuplicateKey { position }) => Err(Committed::Duplicate {
            position: position.0,
        }),
        Err(e) => Err(codec::core_error(e).into()),
    }
}

/// Appends events that belong to no aggregate: nothing to evolve or check.
async fn append_plain(
    shared: &Arc<Shared>,
    stream: StreamId,
    expected: ExpectedVersion,
    events: Vec<NewEvent>,
) -> Result<Vec<RecordedEvent>, Status> {
    let shared2 = shared.clone();
    tokio::task::spawn_blocking(move || -> Result<Vec<RecordedEvent>, fold_core::Error> {
        let n = events.len();
        let r = shared2.log.append(&stream, expected, events)?;
        shared2.log.read_all(r.first, n)
    })
    .await
    .map_err(|e| Status::internal(format!("append task: {e}")))?
    .map_err(codec::core_error)
}

impl ServiceView<'_> {
    async fn execute_inner(&self, req: ExecuteParams) -> Result<ExecuteOutcome, Status> {
        let parts: Vec<&str> = req.command.split('.').collect();
        let [ctx_name, agg_name, cmd_name] = parts.as_slice() else {
            return Err(codec::invalid("command must be Context.Aggregate.Command"));
        };
        let schema = &self.shared.schema;
        let (ctx, agg) = schema
            .aggregate(ctx_name, agg_name)
            .map(|a| (schema.contexts.get(*ctx_name).expect("context"), a))
            .ok_or_else(|| {
                Status::not_found(format!(
                    "aggregate {ctx_name}.{agg_name} is not in the schema"
                ))
            })?;
        let cmd = schema
            .command(&fold_schema::AggRef::new(*ctx_name, *agg_name), cmd_name)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "aggregate {ctx_name}.{agg_name} has no command {cmd_name}"
                ))
            })?;

        let stream = parse_stream(&req.stream_id)?;
        let key = agg.stream.matches(&stream).ok_or_else(|| {
            codec::invalid(format!(
                "stream {} does not match aggregate {ctx_name}.{agg_name} ({})",
                stream, agg.stream
            ))
        })?;

        let payload = codec::parse_json(&req.payload, "payload")?;
        let payload = if payload.is_null() {
            Value::Object(Default::default())
        } else {
            payload
        };
        let payload = schema
            .canonicalize_command(cmd, &payload)
            .map_err(|errs| codec::validation(errs, &format!("command {}", req.command)))?;
        if !req.metadata.is_empty() && !codec::parse_json(&req.metadata, "metadata")?.is_object() {
            return Err(codec::invalid("metadata must be a JSON object"));
        }

        let lock = self.shared.locks.get(&stream);
        let _guard = lock.lock().await;

        // A key used before means the command's effects are in the log
        // already (a process manager's retry after a crash, or a promoted
        // replica draining an outbox the old primary had dispatched):
        // answer before the handler runs, whose view of the state would
        // now reject the command.
        if let Some(key) = req.idempotency_key.as_deref()
            && let Some(position) = self
                .shared
                .log
                .idempotency_position(key)
                .map_err(codec::core_error)?
        {
            return Ok(ExecuteOutcome::AlreadyExecuted {
                position: position.0,
            });
        }

        let shared = self.shared.clone();
        let stream_b = stream.clone();
        let loaded = tokio::task::spawn_blocking(move || aggregate::load(&shared, &stream_b))
            .await
            .map_err(|e| Status::internal(format!("load task: {e}")))?
            .map_err(load_error)?;

        for g in &cmd.requires {
            if !fold_schema::rules::eval_guard(&g.expr, loaded.state.as_ref(), &payload) {
                let mut message = g.text.clone();
                if loaded.state.is_none() {
                    message.push_str(" (the stream has no state yet)");
                }
                let r = fold_wasm::Rejected {
                    code: g.name.clone(),
                    message,
                };
                let name = format!("{ctx_name}.{agg_name}.{}.{}", cmd.name, g.name);
                return Err(rejection(&r, Some(&name)));
            }
        }

        let input = CommandInput {
            abi: fold_wasm::ABI_VERSION,
            aggregate: format!("{ctx_name}.{agg_name}"),
            stream: stream.to_string(),
            key: key.clone(),
            version: loaded.version,
            state: loaded.state.clone(),
            now: self.shared.now_rfc3339(),
            command: fold_wasm::Command {
                r#type: req.command.clone(),
                payload,
            },
        };
        let guest = self.shared.guest(&cmd.handler.module);
        let export = cmd
            .handler
            .export_or(&format!("handle_{}", cmd.name))
            .to_string();
        let reply = tokio::task::spawn_blocking(move || guest.handle(&export, &input))
            .await
            .map_err(|e| Status::internal(format!("handler task: {e}")))?
            .map_err(codec::wasm_error)?;

        let emitted = match reply {
            CommandReply::Rejected(r) => {
                let mut status =
                    Status::failed_precondition(format!("rejected {}: {}", r.code, r.message));
                if let Ok(v) = r.code.parse() {
                    status.metadata_mut().insert("fold-rejection-code", v);
                }
                return Err(status);
            }
            CommandReply::Events(events) => events,
        };

        let head_before = self.shared.log.head().0;
        if emitted.is_empty() {
            let last_position = head_before.saturating_sub(1);
            return Ok(ExecuteOutcome::Done(ExecuteResponse {
                events: vec![],
                first_position: 0,
                last_position,
                version: loaded.version,
                token: self.token(last_position)?,
            }));
        }

        let mut new_events = Vec::with_capacity(emitted.len());
        for e in &emitted {
            let payload = serde_json::to_vec(&e.payload).expect("json");
            let metadata = if e.metadata.is_null() {
                req.metadata.clone()
            } else {
                serde_json::to_vec(&e.metadata).expect("json")
            };
            new_events.push(prepare_event(
                self.shared,
                &stream,
                &e.r#type,
                &payload,
                &metadata,
                Some((ctx, agg)),
            )?);
        }
        let recorded = match commit(
            self.shared,
            ctx,
            agg,
            stream,
            key,
            loaded.version,
            loaded.state,
            new_events,
            req.idempotency_key.as_deref(),
        )
        .await
        {
            Ok(r) => r,
            Err(Committed::Duplicate { position }) => {
                return Ok(ExecuteOutcome::AlreadyExecuted { position });
            }
            Err(Committed::Status(s)) => return Err(s),
        };
        let first = recorded.first().map(|e| e.position.0).unwrap_or(0);
        let last = recorded.last().map(|e| e.position.0).unwrap_or(0);
        let version = recorded.last().map(|e| e.stream_version.0);
        Ok(ExecuteOutcome::Done(ExecuteResponse {
            events: recorded.iter().map(codec::event_to_wire).collect(),
            first_position: first,
            last_position: last,
            version,
            token: self.token(last)?,
        }))
    }

    /// The position token a write hands back.
    fn token(&self, position: u64) -> Result<String, Status> {
        let epoch = self.shared.log.epoch().map_err(codec::core_error)?;
        Ok(codec::position_token(
            self.shared.log.log_id(),
            epoch,
            position,
        ))
    }
}

#[tonic::async_trait]
impl CommandSvc for Service {
    async fn execute(
        &self,
        req: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let req = req.into_inner();
        self.shared.check_fencing_token(req.fencing_token)?;
        if let Some(refusal) = self.shared.write_refusal() {
            return Err(refusal);
        }
        match execute(
            &self.shared,
            ExecuteParams {
                command: req.command,
                stream_id: req.stream_id,
                payload: req.payload,
                metadata: req.metadata,
                idempotency_key: None,
            },
        )
        .await?
        {
            ExecuteOutcome::Done(resp) => Ok(Response::new(resp)),
            ExecuteOutcome::AlreadyExecuted { .. } => {
                unreachable!("no idempotency key was passed")
            }
        }
    }

    async fn append(
        &self,
        req: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let req = req.into_inner();
        self.shared.check_fencing_token(req.fencing_token)?;
        if let Some(refusal) = self.shared.write_refusal() {
            return Err(refusal);
        }
        let stream = parse_stream(&req.stream_id)?;
        if req.events.is_empty() {
            return Err(codec::invalid("at least one event is required"));
        }
        let expected = match req.expected.and_then(|e| e.kind) {
            None | Some(expected_version::Kind::Any(_)) => ExpectedVersion::Any,
            Some(expected_version::Kind::NoStream(_)) => ExpectedVersion::NoStream,
            Some(expected_version::Kind::StreamExists(_)) => ExpectedVersion::StreamExists,
            Some(expected_version::Kind::Exact(v)) => ExpectedVersion::Exact(StreamVersion(v)),
        };
        let mut events = Vec::with_capacity(req.events.len());
        for e in &req.events {
            events.push(prepare_event(
                &self.shared,
                &stream,
                &e.r#type,
                &e.payload,
                &e.metadata,
                None,
            )?);
        }
        let lock = self.shared.locks.get(&stream);
        let _guard = lock.lock().await;
        let owner = events.first().and_then(|e| {
            self.shared
                .schema
                .aggregate_for_event(&e.event_type.context, &e.event_type.name)
        });
        let recorded = match owner {
            None => append_plain(&self.shared, stream, expected, events).await?,
            Some((ctx, agg)) => {
                let shared = self.shared.clone();
                let stream_b = stream.clone();
                let loaded =
                    tokio::task::spawn_blocking(move || aggregate::load(&shared, &stream_b))
                        .await
                        .map_err(|e| Status::internal(format!("load task: {e}")))?
                        .map_err(load_error)?;
                // The caller's expectation is checked here, against the
                // loaded version, so a stale client fails before any check.
                let ok = match expected {
                    ExpectedVersion::Any => true,
                    ExpectedVersion::NoStream => loaded.version.is_none(),
                    ExpectedVersion::StreamExists => loaded.version.is_some(),
                    ExpectedVersion::Exact(v) => loaded.version == Some(v.0),
                };
                if !ok {
                    return Err(Status::failed_precondition(format!(
                        "stream {stream}: expected {expected:?}, actual version {}",
                        loaded
                            .version
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "none".into())
                    )));
                }
                commit(
                    &self.shared,
                    ctx,
                    agg,
                    stream,
                    loaded.key,
                    loaded.version,
                    loaded.state,
                    events,
                    None,
                )
                .await
                .map_err(|e| match e {
                    Committed::Status(s) => s,
                    Committed::Duplicate { .. } => unreachable!("no idempotency key"),
                })?
            }
        };
        let first = recorded
            .first()
            .map(|e| e.position)
            .unwrap_or(GlobalPosition(0))
            .0;
        let last = recorded
            .last()
            .map(|e| e.position)
            .unwrap_or(GlobalPosition(0))
            .0;
        let version = recorded.last().map(|e| e.stream_version.0).unwrap_or(0);
        let epoch = self.shared.log.epoch().map_err(codec::core_error)?;
        Ok(Response::new(AppendResponse {
            first_position: first,
            last_position: last,
            version,
            token: codec::position_token(self.shared.log.log_id(), epoch, last),
        }))
    }
}
