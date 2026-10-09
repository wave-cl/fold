//! A recorded event as a guest sees it.

use fold_core::RecordedEvent;
use fold_schema::DomainSchema;
use fold_wasm::Event;
use serde_json::Value;

use crate::guests::GuestSource;
use crate::upcast::{self, UpcastError};

#[derive(Debug, thiserror::Error)]
pub enum EventError {
    #[error("event {position} payload is not JSON: {source}")]
    Payload {
        position: u64,
        #[source]
        source: serde_json::Error,
    },
    #[error("event {position}: {source}")]
    Upcast {
        position: u64,
        #[source]
        source: UpcastError,
    },
}

/// A recorded event at its family's latest version, with defaults filled
/// in, shaped for a guest.
pub fn to_guest_event(
    schema: &DomainSchema,
    guests: &dyn GuestSource,
    ev: &RecordedEvent,
) -> Result<Event, EventError> {
    let payload: Value =
        serde_json::from_slice(&ev.payload).map_err(|source| EventError::Payload {
            position: ev.position.0,
            source,
        })?;
    let (id, payload) =
        upcast::to_latest(schema, guests, &upcast::type_id(&ev.event_type), payload).map_err(
            |source| EventError::Upcast {
                position: ev.position.0,
                source,
            },
        )?;
    let metadata: Value = if ev.metadata.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&ev.metadata).unwrap_or(Value::Null)
    };
    Ok(Event {
        stream: ev.stream_id.to_string(),
        r#type: upcast::type_string(&id),
        version: ev.stream_version.0,
        position: ev.position.0,
        payload,
        metadata,
    })
}
