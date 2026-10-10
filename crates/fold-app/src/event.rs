//! A recorded event as the application sees it: at its family's latest
//! version (upcast by the derivation node when it is not), with defaults
//! filled in.

use fold_core::RecordedEvent;
use fold_proto::derivation::v1::UpcastRequest;
use serde_json::Value;

use crate::state::Shared;
use crate::types::Event;

#[derive(Debug, thiserror::Error)]
pub enum EventError {
    #[error("event {position} payload is not JSON: {source}")]
    Payload {
        position: u64,
        #[source]
        source: serde_json::Error,
    },
    #[error("event {position}: {reason}")]
    Upcast { position: u64, reason: String },
}

/// `Context.Event@vN`.
pub fn type_string(t: &fold_core::EventType) -> String {
    format!("{}.{}@v{}", t.context, t.name, t.version)
}

pub async fn to_event(shared: &Shared, ev: &RecordedEvent) -> Result<Event, EventError> {
    let domain = shared.domain();
    let mut payload: Value =
        serde_json::from_slice(&ev.payload).map_err(|source| EventError::Payload {
            position: ev.position.0,
            source,
        })?;
    let mut ty = type_string(&ev.event_type);
    let family = domain.event_family(&ev.event_type.context, &ev.event_type.name);
    match family {
        // The database's own events are not in the schema and never change.
        None if ev.event_type.context == fold_schema::RESERVED_CONTEXT => {}
        None => {
            return Err(EventError::Upcast {
                position: ev.position.0,
                reason: format!("event type {ty} is not in the schema"),
            });
        }
        Some(fam) if fam.latest().id.version == ev.event_type.version => {
            domain.apply_defaults(&fam.latest().fields, &mut payload);
        }
        Some(_) => {
            // An older version: the derivation node holds the upcasters.
            let resp = shared
                .derivation
                .derive()
                .upcast(UpcastRequest {
                    events: vec![fold_host::codec::event_to_common(ev)],
                })
                .await
                .map_err(|e| EventError::Upcast {
                    position: ev.position.0,
                    reason: format!("derivation node: {e}"),
                })?
                .into_inner();
            let Some(up) = resp.events.into_iter().next() else {
                return Err(EventError::Upcast {
                    position: ev.position.0,
                    reason: "the derivation node returned nothing".into(),
                });
            };
            payload =
                serde_json::from_slice(&up.payload).map_err(|source| EventError::Payload {
                    position: ev.position.0,
                    source,
                })?;
            ty = up.r#type;
        }
    }
    let metadata: Value = if ev.metadata.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&ev.metadata).unwrap_or(Value::Null)
    };
    Ok(Event {
        stream: ev.stream_id.to_string(),
        r#type: ty,
        version: ev.stream_version.0,
        position: ev.position.0,
        payload,
        metadata,
    })
}
