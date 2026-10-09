//! Conversions between core types and the wire, and error → gRPC status.

use fold_core::{EventType, RecordedEvent};
use fold_proto::v1;
use tonic::{Code, Status};

pub fn event_to_wire(e: &RecordedEvent) -> v1::RecordedEvent {
    v1::RecordedEvent {
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

/// `Context.Event@vN` for the guest.
pub fn type_string(t: &EventType) -> String {
    format!("{}.{}@v{}", t.context, t.name, t.version)
}

/// A position token: `fold1:<log_id>:<epoch>:<position>`. What a write
/// returns and what a read on any member hands back for read-your-writes.
pub fn position_token(log_id: uuid::Uuid, epoch: u64, position: u64) -> String {
    format!("fold1:{log_id}:{epoch}:{position}")
}

/// Parses a position token, checking it is of `log_id`. Returns the
/// position.
pub fn parse_position_token(token: &str, log_id: uuid::Uuid) -> Result<u64, Status> {
    let parts: Vec<&str> = token.split(':').collect();
    let [tag, id, _epoch, position] = parts.as_slice() else {
        return Err(invalid(
            "token must be fold1:<log_id>:<epoch>:<position>, as a write returned it",
        ));
    };
    if *tag != "fold1" {
        return Err(invalid(format!("unknown token format {tag:?}")));
    }
    let id: uuid::Uuid = id
        .parse()
        .map_err(|_| invalid("token: the log id is not a uuid"))?;
    if id != log_id {
        return Err(invalid(format!(
            "token is of log {id}; this daemon serves log {log_id}"
        )));
    }
    position
        .parse()
        .map_err(|_| invalid("token: the position is not a number"))
}

pub fn core_error(e: fold_core::Error) -> Status {
    use fold_core::Error as E;
    match &e {
        E::WrongExpectedVersion {
            stream,
            expected,
            actual,
        } => Status::failed_precondition(format!(
            "stream {stream}: expected {expected:?}, actual version {}",
            actual
                .map(|v| v.0.to_string())
                .unwrap_or_else(|| "none".into())
        )),
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

pub fn wasm_error(e: fold_wasm::WasmError) -> Status {
    tracing::error!(error = %e, "wasm failure");
    Status::internal(e.to_string())
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
