//! `Log.Append`: validation against the domain, the stream rule, the system
//! token for `Fold.*` events, idempotency keys.

use std::sync::Arc;

use fold_core::{EventType, ExpectedVersion, NewEvent, RecordedEvent, StreamId, StreamVersion};
use fold_proto::common::v1::expected_version;
use fold_proto::database::v1::{AppendRequest, AppendResponse};
use fold_schema::{DomainSchema, RESERVED_CONTEXT, TIMER_FIRED_EVENT, TimerFired};
use tonic::Status;

use crate::codec;
use crate::state::Shared;

/// Resolves and validates one event to append, enforcing the stream id
/// against the owning aggregate's template. `system` says whether the
/// request carried the system token (for `Fold.*` events).
pub fn prepare_event(
    domain: &DomainSchema,
    stream: &StreamId,
    type_ref: &str,
    payload: &[u8],
    metadata: &[u8],
    system: Result<(), Status>,
) -> Result<NewEvent, Status> {
    let (ctx, name, version) = fold_schema::parse_event_ref(type_ref)
        .map_err(|e| codec::invalid(format!("event type {type_ref:?}: {e}")))?;
    if !metadata.is_empty() {
        let m = codec::parse_json(metadata, "metadata")?;
        if !m.is_object() {
            return Err(codec::invalid("metadata must be a JSON object"));
        }
    }
    if ctx == RESERVED_CONTEXT {
        system?;
        return prepare_system_event(&name, version, payload, metadata);
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

    if let Some((_, agg)) = domain.aggregate_for_event(&ctx, &name) {
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

/// `Fold.TimerFired@v1`, the one system event: its payload is validated by
/// shape, not by the domain (which may not declare the `Fold` context).
fn prepare_system_event(
    name: &str,
    version: Option<u16>,
    payload: &[u8],
    metadata: &[u8],
) -> Result<NewEvent, Status> {
    if name != TIMER_FIRED_EVENT || version.is_some_and(|v| v != 1) {
        return Err(Status::not_found(format!(
            "{RESERVED_CONTEXT}.{name}{} is not a system event; the only one is {}",
            version.map(|v| format!("@v{v}")).unwrap_or_default(),
            TimerFired::event_type()
        )));
    }
    let fired: TimerFired = serde_json::from_slice(payload).map_err(|e| {
        codec::invalid(format!(
            "payload of {} is not a TimerFired record: {e}",
            TimerFired::event_type()
        ))
    })?;
    Ok(NewEvent {
        id: None,
        event_type: EventType {
            context: RESERVED_CONTEXT.to_string(),
            name: TIMER_FIRED_EVENT.to_string(),
            version: 1,
        },
        payload: serde_json::to_vec(&fired).expect("json").into(),
        metadata: metadata.to_vec().into(),
    })
}

pub fn expected_version(e: Option<fold_proto::common::v1::ExpectedVersion>) -> ExpectedVersion {
    match e.and_then(|e| e.kind) {
        None | Some(expected_version::Kind::Any(_)) => ExpectedVersion::Any,
        Some(expected_version::Kind::NoStream(_)) => ExpectedVersion::NoStream,
        Some(expected_version::Kind::StreamExists(_)) => ExpectedVersion::StreamExists,
        Some(expected_version::Kind::Exact(v)) => ExpectedVersion::Exact(StreamVersion(v)),
    }
}

/// The whole append: fencing token, role, validation, the log.
pub async fn append(
    shared: &Arc<Shared>,
    req: AppendRequest,
    system: Result<(), Status>,
) -> Result<AppendResponse, Status> {
    shared.check_fencing_token(req.fencing_token)?;
    if let Some(refusal) = shared.write_refusal() {
        return Err(refusal);
    }
    let stream =
        StreamId::new(&req.stream_id).map_err(|e| codec::invalid(format!("stream id: {e}")))?;
    if req.events.is_empty() {
        return Err(codec::invalid("at least one event is required"));
    }
    let expected = expected_version(req.expected);
    let mut events = Vec::with_capacity(req.events.len());
    for e in &req.events {
        events.push(prepare_event(
            &shared.domain,
            &stream,
            &e.r#type,
            &e.payload,
            &e.metadata,
            system.clone(),
        )?);
    }
    let key = (!req.idempotency_key.is_empty()).then_some(req.idempotency_key);
    let shared2 = shared.clone();
    let recorded: Vec<RecordedEvent> =
        tokio::task::spawn_blocking(move || -> Result<Vec<RecordedEvent>, fold_core::Error> {
            let n = events.len();
            let r = match &key {
                Some(k) => shared2
                    .log
                    .append_idempotent(&stream, expected, events, k)?,
                None => shared2.log.append(&stream, expected, events)?,
            };
            shared2.log.read_all(r.first, n)
        })
        .await
        .map_err(|e| Status::internal(format!("append task: {e}")))?
        .map_err(codec::core_error)?;
    let first = recorded.first().map(|e| e.position.0).unwrap_or(0);
    let last = recorded.last().map(|e| e.position.0).unwrap_or(0);
    let version = recorded.last().map(|e| e.stream_version.0).unwrap_or(0);
    Ok(AppendResponse {
        events: recorded.iter().map(codec::event_to_wire).collect(),
        first_position: first,
        last_position: last,
        version,
        token: codec::token(shared, last),
    })
}
