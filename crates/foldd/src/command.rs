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
    locks: StreamLocks,
}

impl Service {
    pub fn new(shared: Arc<Shared>) -> Self {
        Service {
            shared,
            locks: StreamLocks::default(),
        }
    }
}

fn load_error(e: LoadError) -> Status {
    match e {
        LoadError::NoAggregate(s) => {
            Status::not_found(format!("stream {s} does not belong to any aggregate"))
        }
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
    let ty = match version {
        Some(v) => shared.schema.event_type(&ctx, &name, v),
        None => shared.schema.latest_event_type(&ctx, &name),
    }
    .ok_or_else(|| Status::not_found(format!("event type {type_ref} is not in the schema")))?;

    let payload_json = codec::parse_json(payload, "payload")?;
    shared
        .schema
        .validate_event(ty, &payload_json)
        .map_err(|errs| codec::validation(errs, &format!("payload of {type_ref}")))?;
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

async fn append_and_read(
    shared: &Arc<Shared>,
    stream: StreamId,
    expected: ExpectedVersion,
    events: Vec<NewEvent>,
) -> Result<Vec<RecordedEvent>, Status> {
    let shared2 = shared.clone();
    let stream2 = stream.clone();
    let recorded =
        tokio::task::spawn_blocking(move || -> Result<Vec<RecordedEvent>, fold_core::Error> {
            let n = events.len();
            let r = shared2.log.append(&stream2, expected, events)?;
            let recorded = shared2.log.read_all(r.first, n)?;
            aggregate::advance(&shared2, &stream2, &recorded);
            Ok(recorded)
        })
        .await
        .map_err(|e| Status::internal(format!("append task: {e}")))?
        .map_err(codec::core_error)?;
    Ok(recorded)
}

#[tonic::async_trait]
impl CommandSvc for Service {
    async fn execute(
        &self,
        req: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let req = req.into_inner();
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
        let cmd = agg.commands.get(*cmd_name).ok_or_else(|| {
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
        schema
            .validate_command(agg, cmd, &payload)
            .map_err(|errs| codec::validation(errs, &format!("command {}", req.command)))?;
        if !req.metadata.is_empty() && !codec::parse_json(&req.metadata, "metadata")?.is_object() {
            return Err(codec::invalid("metadata must be a JSON object"));
        }

        let lock = self.locks.get(&stream);
        let _guard = lock.lock().await;

        let shared = self.shared.clone();
        let stream_b = stream.clone();
        let loaded = tokio::task::spawn_blocking(move || aggregate::load(&shared, &stream_b))
            .await
            .map_err(|e| Status::internal(format!("load task: {e}")))?
            .map_err(load_error)?;

        let input = CommandInput {
            abi: fold_wasm::ABI_VERSION,
            aggregate: format!("{ctx_name}.{agg_name}"),
            stream: stream.to_string(),
            key,
            version: loaded.version,
            state: loaded.state,
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
            return Ok(Response::new(ExecuteResponse {
                events: vec![],
                first_position: 0,
                last_position: head_before.saturating_sub(1),
                version: loaded.version,
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
                &self.shared,
                &stream,
                &e.r#type,
                &payload,
                &metadata,
                Some((ctx, agg)),
            )?);
        }
        let expected = match loaded.version {
            Some(v) => ExpectedVersion::Exact(StreamVersion(v)),
            None => ExpectedVersion::NoStream,
        };
        let recorded = append_and_read(&self.shared, stream, expected, new_events).await?;
        let first = recorded.first().map(|e| e.position.0).unwrap_or(0);
        let last = recorded.last().map(|e| e.position.0).unwrap_or(0);
        let version = recorded.last().map(|e| e.stream_version.0);
        Ok(Response::new(ExecuteResponse {
            events: recorded.iter().map(codec::event_to_wire).collect(),
            first_position: first,
            last_position: last,
            version,
        }))
    }

    async fn append(
        &self,
        req: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let req = req.into_inner();
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
        let lock = self.locks.get(&stream);
        let _guard = lock.lock().await;
        let recorded = append_and_read(&self.shared, stream, expected, events).await?;
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
        Ok(Response::new(AppendResponse {
            first_position: first,
            last_position: last,
            version,
        }))
    }
}
