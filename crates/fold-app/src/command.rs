//! The write side: `Command.Execute` and `Command.Append`. State comes from
//! the derivation node, events go to the database with an expected
//! version; invariants run in between.

use std::sync::Arc;

use fold_core::StreamId;
use fold_proto::application::v1::command_server::Command as CommandSvc;
use fold_proto::application::v1::{AppendRequest, AppendResponse, ExecuteRequest, ExecuteResponse};
use fold_proto::common::v1 as common;
use fold_proto::database::v1 as db;
use fold_proto::derivation::v1::{EvolveRequest, GetStateRequest, WaitCheckpointRequest};
use fold_schema::{Aggregate, Context, DomainSchema, EventTypeId};
use fold_wasm::{CommandInput, CommandReply};
use serde_json::Value;
use tonic::{Request, Response, Status};

use crate::codec;
use crate::peers::RemoteRows;
use crate::state::Shared;

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
    pub fencing_token: Option<u64>,
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

/// One event validated and canonicalized, ready for the wire.
struct Prepared {
    id: EventTypeId,
    wire: common::NewEvent,
}

/// Resolves and validates one event to append, enforcing the stream id
/// against the owning aggregate's template.
fn prepare_event(
    domain: &DomainSchema,
    stream: &StreamId,
    type_ref: &str,
    payload: &[u8],
    metadata: &[u8],
    allowed: Option<(&Context, &Aggregate)>,
) -> Result<Prepared, Status> {
    let (ctx, name, version) = fold_schema::parse_event_ref(type_ref)
        .map_err(|e| codec::invalid(format!("event type {type_ref:?}: {e}")))?;
    if ctx == fold_schema::RESERVED_CONTEXT {
        return Err(Status::permission_denied(format!(
            "context {} is reserved for the daemon's own events; {type_ref} cannot be appended",
            fold_schema::RESERVED_CONTEXT
        )));
    }
    let ty = match version {
        Some(v) => domain.event_type(&ctx, &name, v),
        None => domain.latest_event_type(&ctx, &name),
    }
    .ok_or_else(|| Status::not_found(format!("event type {type_ref} is not in the schema")))?;

    let payload_json = codec::parse_json(payload, "payload")?;
    let payload_json = domain
        .canonicalize_event(ty, &payload_json)
        .map_err(|errs| codec::validation(errs, &format!("payload of {type_ref}")))?;
    if let Some((c, a)) = allowed {
        let latest = domain
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
        None => domain.aggregate_for_event(&ctx, &name),
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
    Ok(Prepared {
        id: ty.id.clone(),
        wire: common::NewEvent {
            r#type: ty.id.to_string(),
            payload: serde_json::to_vec(&payload_json).expect("json"),
            content_type: fold_proto::CONTENT_TYPE_JSON.into(),
            metadata: metadata.to_vec(),
        },
    })
}

fn parse_stream(s: &str) -> Result<StreamId, Status> {
    StreamId::new(s).map_err(|e| codec::invalid(format!("stream id: {e}")))
}

/// What the derivation node knows of a stream.
struct Loaded {
    version: Option<u64>,
    state: Option<Value>,
}

/// The stream's state from the derivation node, at least at `min_version`
/// when this node appended that far.
async fn load_state(
    shared: &Shared,
    stream: &StreamId,
    min_version: Option<u64>,
) -> Result<Loaded, Status> {
    let resp = shared
        .derivation
        .derive()
        .get_state(GetStateRequest {
            stream_id: stream.to_string(),
            min_version,
            wait_ms: Some(shared.state_wait.as_millis() as u32),
        })
        .await
        .map_err(|s| match s.code() {
            tonic::Code::Unavailable => Status::unavailable(format!(
                "the derivation node {}: {}",
                shared.derivation.url(),
                s.message()
            )),
            _ => s,
        })?
        .into_inner();
    let state = if resp.found && !resp.state.is_empty() {
        Some(codec::parse_json(&resp.state, "state")?)
    } else {
        None
    };
    Ok(Loaded {
        version: if resp.found { resp.version } else { None },
        state,
    })
}

/// Why `commit` did not append.
enum Committed {
    Status(Status),
    /// The idempotency key was already used by the append at `position`.
    Duplicate {
        position: u64,
    },
    /// The stream moved under the command: the database holds `actual`.
    Conflict {
        actual: Option<u64>,
    },
}

impl From<Status> for Committed {
    fn from(s: Status) -> Self {
        Committed::Status(s)
    }
}

fn header_u64(s: &Status, name: &str) -> Option<u64> {
    s.metadata()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
}

/// Checks every invariant that guards `agg`, then appends. The caller holds
/// the stream lock.
///
/// 1. The candidate state is the loaded state evolved over the new events,
///    by the derivation node.
/// 2. The aggregate's state invariants run against it.
/// 3. For each context invariant on this aggregate, the scope value is read
///    from the candidate state, a lock per (invariant, scope) is taken in a
///    fixed order, the guarding projection is caught up to the log head, and
///    the check runs over its rows on the derivation node.
/// 4. The events are appended with the expected version.
#[allow(clippy::too_many_arguments)]
async fn commit(
    shared: &Arc<Shared>,
    ctx: &Context,
    agg: &Aggregate,
    stream: &StreamId,
    key: &Value,
    version: Option<u64>,
    state: Option<Value>,
    events: Vec<Prepared>,
    idempotency_key: Option<&[u8]>,
    fencing_token: Option<u64>,
) -> Result<Vec<common::RecordedEvent>, Committed> {
    let aggregate_name = format!("{}.{}", ctx.name, agg.name);
    let first_version = version.map_or(0, |v| v + 1);
    let last_version = first_version + events.len() as u64 - 1;

    // 1. candidate state
    let evolved = shared
        .derivation
        .derive()
        .evolve(EvolveRequest {
            stream_id: stream.to_string(),
            version,
            state: state
                .as_ref()
                .map(|s| serde_json::to_vec(s).expect("json"))
                .unwrap_or_default(),
            events: events.iter().map(|e| e.wire.clone()).collect(),
        })
        .await
        .map_err(|s| Status::unavailable(format!("the derivation node: {}", s.message())))?
        .into_inner();
    let candidate: Value = codec::parse_json(&evolved.state, "candidate state")?;
    let mut pending = Vec::with_capacity(events.len());
    for (i, e) in evolved.events.iter().enumerate() {
        let payload: Value = codec::parse_json(&e.payload, "evolved payload")?;
        let metadata: Value = if e.metadata.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&e.metadata).unwrap_or(Value::Null)
        };
        pending.push(fold_wasm::PendingEvent {
            r#type: e.r#type.clone(),
            version: first_version + i as u64,
            payload,
            metadata,
        });
    }

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
        held.push(shared.locks.get(lock_key));
    }
    let mut held_guards = Vec::with_capacity(held.len());
    for lock in &held {
        held_guards.push(lock.lock().await);
    }
    let wait_ms = shared.invariant_wait.as_millis() as u32;
    for (_, scope, inv) in &guards {
        let projection = inv.projection.to_string();
        // Every committed event must be visible to the check.
        let caught_up = shared.db_head().checked_sub(1);
        shared
            .derivation
            .derive()
            .wait_checkpoint(WaitCheckpointRequest {
                projection: projection.clone(),
                min_position: caught_up,
                wait_ms: Some(wait_ms),
            })
            .await
            .map_err(|s| Status::new(s.code(), format!("the derivation node: {}", s.message())))?;
        let rows = RemoteRows {
            derivation: shared.derivation.clone(),
            projection: projection.clone(),
            min_position: caught_up,
            wait_ms,
        };
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
            tokio::task::spawn_blocking(move || guest.check(&export, &input, Arc::new(rows)))
                .await
                .map_err(|e| Status::internal(format!("check task: {e}")))?
                .map_err(codec::wasm_error)?;
        if let fold_wasm::CheckReply::Violation(v) = reply {
            return Err(rejection(&v, Some(&name)).into());
        }
    }

    // 4. append with the expected version
    let expected = match version {
        Some(v) => common::expected_version::Kind::Exact(v),
        None => common::expected_version::Kind::NoStream(true),
    };
    let appended = shared
        .db
        .log()
        .append(db::AppendRequest {
            stream_id: stream.to_string(),
            expected: Some(common::ExpectedVersion {
                kind: Some(expected),
            }),
            events: events.into_iter().map(|e| e.wire).collect(),
            fencing_token,
            idempotency_key: idempotency_key.map(<[u8]>::to_vec).unwrap_or_default(),
        })
        .await;
    match appended {
        Ok(r) => {
            let r = r.into_inner();
            shared
                .last_appended
                .lock()
                .expect("last_appended")
                .insert(stream.to_string(), r.version);
            Ok(r.events)
        }
        Err(s) if s.code() == tonic::Code::AlreadyExists => Err(Committed::Duplicate {
            position: header_u64(&s, fold_proto::FIRST_POSITION_HEADER).unwrap_or(0),
        }),
        Err(s)
            if s.code() == tonic::Code::FailedPrecondition && s.message().contains("expected") =>
        {
            Err(Committed::Conflict {
                actual: header_u64(&s, fold_proto::CONFLICT_ACTUAL_VERSION_HEADER),
            })
        }
        Err(s) => Err(Committed::Status(s)),
    }
}

/// The position token a write hands back.
fn token(shared: &Shared, position: u64) -> String {
    codec::position_token(shared.log_id, shared.epoch(), position)
}

/// The whole write path for one command: resolve, validate, lock the
/// stream, load state, run the handler, check invariants, append. A
/// version conflict (the derivation node was behind) retries once from a
/// fresh state.
pub async fn execute(shared: &Arc<Shared>, req: ExecuteParams) -> Result<ExecuteOutcome, Status> {
    let parts: Vec<&str> = req.command.split('.').collect();
    let [ctx_name, agg_name, cmd_name] = parts.as_slice() else {
        return Err(codec::invalid("command must be Context.Aggregate.Command"));
    };
    let schema = &shared.schema;
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
    if let Some(refusal) = shared.layer_refusal() {
        return Err(refusal);
    }
    if let Some(refusal) = shared.write_refusal() {
        return Err(refusal);
    }

    let lock = shared.locks.get(&stream);
    let _guard = lock.lock().await;

    // A key used before means the command's effects are in the log
    // already: answer before the handler runs, whose view of the state
    // would now reject the command.
    if let Some(key) = req.idempotency_key.as_deref() {
        let found = shared
            .db
            .log()
            .lookup_idempotency_key(db::LookupIdempotencyKeyRequest { key: key.to_vec() })
            .await
            .map_err(|s| Status::unavailable(format!("the database: {}", s.message())))?
            .into_inner();
        if found.found {
            return Ok(ExecuteOutcome::AlreadyExecuted {
                position: found.position,
            });
        }
    }

    let mut min_version = shared
        .last_appended
        .lock()
        .expect("last_appended")
        .get(&*stream)
        .copied();
    for attempt in 0..2 {
        let loaded = load_state(shared, &stream, min_version).await?;
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
            now: shared.now_rfc3339(),
            command: fold_wasm::Command {
                r#type: req.command.clone(),
                payload: payload.clone(),
            },
        };
        let guest = shared.guest(&cmd.handler.module);
        let export = cmd
            .handler
            .export_or(&format!("handle_{}", cmd.name))
            .to_string();
        let reply = tokio::task::spawn_blocking(move || guest.handle(&export, &input))
            .await
            .map_err(|e| Status::internal(format!("handler task: {e}")))?
            .map_err(codec::wasm_error)?;
        let emitted = match reply {
            CommandReply::Rejected(r) => return Err(rejection(&r, None)),
            CommandReply::Events(events) => events,
        };
        if emitted.is_empty() {
            let last_position = shared.db_head().saturating_sub(1);
            return Ok(ExecuteOutcome::Done(ExecuteResponse {
                events: vec![],
                first_position: 0,
                last_position,
                version: loaded.version,
                token: token(shared, last_position),
            }));
        }
        let mut prepared = Vec::with_capacity(emitted.len());
        for e in &emitted {
            let payload = serde_json::to_vec(&e.payload).expect("json");
            let metadata = if e.metadata.is_null() {
                req.metadata.clone()
            } else {
                serde_json::to_vec(&e.metadata).expect("json")
            };
            prepared.push(prepare_event(
                &shared.schema,
                &stream,
                &e.r#type,
                &payload,
                &metadata,
                Some((ctx, agg)),
            )?);
        }
        match commit(
            shared,
            ctx,
            agg,
            &stream,
            &key,
            loaded.version,
            loaded.state,
            prepared,
            req.idempotency_key.as_deref(),
            req.fencing_token,
        )
        .await
        {
            Ok(recorded) => {
                let first = recorded.first().map(|e| e.position).unwrap_or(0);
                let last = recorded.last().map(|e| e.position).unwrap_or(0);
                let version = recorded.last().map(|e| e.version);
                return Ok(ExecuteOutcome::Done(ExecuteResponse {
                    events: recorded,
                    first_position: first,
                    last_position: last,
                    version,
                    token: token(shared, last),
                }));
            }
            Err(Committed::Duplicate { position }) => {
                return Ok(ExecuteOutcome::AlreadyExecuted { position });
            }
            Err(Committed::Conflict { actual }) if attempt == 0 => {
                tracing::debug!(%stream, ?actual, "stale state; retrying from the database's version");
                min_version = actual;
            }
            Err(Committed::Conflict { actual }) => {
                return Err(Status::failed_precondition(format!(
                    "stream {stream} moved under the command twice (now at version {}); retry",
                    actual
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "none".into())
                )));
            }
            Err(Committed::Status(s)) => return Err(s),
        }
    }
    unreachable!("two attempts return or retry")
}

#[tonic::async_trait]
impl CommandSvc for Service {
    async fn execute(
        &self,
        req: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let req = req.into_inner();
        match execute(
            &self.shared,
            ExecuteParams {
                command: req.command,
                stream_id: req.stream_id,
                payload: req.payload,
                metadata: req.metadata,
                idempotency_key: None,
                fencing_token: req.fencing_token,
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
        let shared = &self.shared;
        if let Some(refusal) = shared.layer_refusal() {
            return Err(refusal);
        }
        if let Some(refusal) = shared.write_refusal() {
            return Err(refusal);
        }
        let stream = parse_stream(&req.stream_id)?;
        if req.events.is_empty() {
            return Err(codec::invalid("at least one event is required"));
        }
        let mut prepared = Vec::with_capacity(req.events.len());
        for e in &req.events {
            prepared.push(prepare_event(
                &shared.schema,
                &stream,
                &e.r#type,
                &e.payload,
                &e.metadata,
                None,
            )?);
        }
        let lock = shared.locks.get(&stream);
        let _guard = lock.lock().await;
        let owner = prepared
            .first()
            .and_then(|e| shared.schema.aggregate_for_event(&e.id.context, &e.id.name));
        let recorded = match owner {
            None => {
                // Nothing to evolve or check: the database's own rules apply.
                shared
                    .db
                    .log()
                    .append(db::AppendRequest {
                        stream_id: stream.to_string(),
                        expected: req.expected,
                        events: prepared.into_iter().map(|e| e.wire).collect(),
                        fencing_token: req.fencing_token,
                        idempotency_key: vec![],
                    })
                    .await?
                    .into_inner()
                    .events
            }
            Some((ctx, agg)) => {
                let loaded = load_state(shared, &stream, None).await?;
                let key = agg.stream.matches(&stream).ok_or_else(|| {
                    codec::invalid(format!(
                        "stream {stream} does not match aggregate {}.{} ({})",
                        ctx.name, agg.name, agg.stream
                    ))
                })?;
                // The caller's expectation is checked here, against the
                // loaded version, so a stale client fails before any check.
                let ok = match req.expected.and_then(|e| e.kind) {
                    None | Some(common::expected_version::Kind::Any(_)) => true,
                    Some(common::expected_version::Kind::NoStream(_)) => loaded.version.is_none(),
                    Some(common::expected_version::Kind::StreamExists(_)) => {
                        loaded.version.is_some()
                    }
                    Some(common::expected_version::Kind::Exact(v)) => loaded.version == Some(v),
                };
                if !ok {
                    return Err(Status::failed_precondition(format!(
                        "stream {stream}: expected {:?}, actual version {}",
                        req.expected.and_then(|e| e.kind),
                        loaded
                            .version
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "none".into())
                    )));
                }
                commit(
                    shared,
                    ctx,
                    agg,
                    &stream,
                    &key,
                    loaded.version,
                    loaded.state,
                    prepared,
                    None,
                    req.fencing_token,
                )
                .await
                .map_err(|e| match e {
                    Committed::Status(s) => s,
                    Committed::Duplicate { .. } => unreachable!("no idempotency key"),
                    Committed::Conflict { actual } => Status::failed_precondition(format!(
                        "stream {stream}: expected {:?}, actual version {}",
                        req.expected.and_then(|e| e.kind),
                        actual
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "none".into())
                    )),
                })?
            }
        };
        let first = recorded.first().map(|e| e.position).unwrap_or(0);
        let last = recorded.last().map(|e| e.position).unwrap_or(0);
        let version = recorded.last().map(|e| e.version).unwrap_or(0);
        Ok(Response::new(AppendResponse {
            first_position: first,
            last_position: last,
            version,
            token: token(shared, last),
        }))
    }
}
