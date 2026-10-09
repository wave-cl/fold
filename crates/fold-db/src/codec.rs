//! Conversions between core types and the wire, and error → gRPC status.

use fold_core::RecordedEvent;
use fold_proto::common::v1 as common;
use tonic::{Code, Status};

pub fn event_to_wire(e: &RecordedEvent) -> common::RecordedEvent {
    common::RecordedEvent {
        id: e.id.0.to_string(),
        stream_id: e.stream_id.to_string(),
        version: e.stream_version.0,
        position: e.position.0,
        r#type: e.event_type.to_string(),
        payload: e.payload.to_vec(),
        content_type: fold_proto::CONTENT_TYPE_JSON.into(),
        metadata: e.metadata.to_vec(),
        recorded_at_unix_nanos: e.recorded_at,
    }
}

/// A log error as a status. A wrong expected version carries the stream's
/// actual version in the `fold-conflict-actual-version` header; a duplicate
/// idempotency key is `ALREADY_EXISTS` with the first position in
/// `fold-first-position`.
pub fn core_error(e: fold_core::Error) -> Status {
    use fold_core::Error as E;
    match &e {
        E::WrongExpectedVersion {
            stream,
            expected,
            actual,
        } => {
            let mut status = Status::failed_precondition(format!(
                "stream {stream}: expected {expected:?}, actual version {}",
                actual
                    .map(|v| v.0.to_string())
                    .unwrap_or_else(|| "none".into())
            ));
            if let Some(v) = actual
                && let Ok(value) = v.0.to_string().parse()
            {
                status
                    .metadata_mut()
                    .insert(fold_proto::CONFLICT_ACTUAL_VERSION_HEADER, value);
            }
            status
        }
        E::DuplicateKey { position } => {
            let mut status = Status::already_exists(format!(
                "idempotency key was used before; its events start at position {position}"
            ));
            if let Ok(value) = position.0.to_string().parse() {
                status
                    .metadata_mut()
                    .insert(fold_proto::FIRST_POSITION_HEADER, value);
            }
            status
        }
        E::InvalidStreamId(_)
        | E::InvalidEventType(_)
        | E::InvalidKey(_)
        | E::EmptyBatch
        | E::RecordTooLarge { .. }
        | E::PositionOutOfRange { .. } => Status::invalid_argument(e.to_string()),
        E::NotFound { .. } => Status::not_found(e.to_string()),
        _ => {
            tracing::error!(error = %e, "log failure");
            Status::internal(e.to_string())
        }
    }
}

pub fn invalid(msg: impl Into<String>) -> Status {
    Status::new(Code::InvalidArgument, msg.into())
}

pub fn parse_json(bytes: &[u8], what: &str) -> Result<serde_json::Value, Status> {
    if bytes.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_slice(bytes).map_err(|e| invalid(format!("{what} is not valid JSON: {e}")))
}

pub fn validation(errors: Vec<fold_schema::ValidationError>, what: &str) -> Status {
    let lines: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
    invalid(format!(
        "{what} does not match the schema: {}",
        lines.join("; ")
    ))
}

/// The position token for `position` of this log at the current epoch.
pub fn token(shared: &crate::Shared, position: u64) -> String {
    fold_proto::token::position_token(shared.log.log_id(), shared.gate.epoch(), position)
}

/// Lowercase hex.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
