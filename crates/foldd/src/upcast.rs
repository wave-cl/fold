//! Bringing a stored event up to the latest version of its family before
//! a guest sees it. Consumers (projections, processes, aggregate replay and
//! the candidate state on commit) always receive the latest version; the
//! log, raw reads, replication and backups keep what was recorded.

use fold_schema::{EventTypeId, Upcast, UpcastHow};
use serde_json::Value;

use crate::codec;
use crate::state::Shared;

#[derive(Debug, thiserror::Error)]
pub enum UpcastError {
    #[error("event type {0} is not in the schema")]
    Unknown(String),
    #[error("upcast {from} -> {to} produced an invalid payload: {reasons}")]
    Invalid {
        from: String,
        to: String,
        reasons: String,
    },
    #[error("upcast {from} -> {to}: {source}")]
    Wasm {
        from: String,
        to: String,
        #[source]
        source: fold_wasm::WasmError,
    },
}

/// The payload of `id` as its family's latest version, with defaults filled
/// in; the returned id is the latest version's. An event already at the
/// latest version only gets its defaults.
pub fn to_latest(
    shared: &Shared,
    id: &EventTypeId,
    payload: Value,
) -> Result<(EventTypeId, Value), UpcastError> {
    if id.context == fold_schema::RESERVED_CONTEXT {
        // The daemon's own events are not in the schema and never change.
        return Ok((id.clone(), payload));
    }
    let family = shared
        .schema
        .event_family(&id.context, &id.name)
        .ok_or_else(|| UpcastError::Unknown(id.to_string()))?;
    let newer = family
        .newer_than(id.version)
        .ok_or_else(|| UpcastError::Unknown(id.to_string()))?;
    let mut payload = payload;
    let mut current = id.clone();
    for target in newer {
        let from = current.to_string();
        let to = target.id.to_string();
        let up = target.upcast.as_ref().filter(|u| u.from == current.version);
        payload = match up.map(|u| &u.how) {
            // No (or an implicit) upcast: fields carry over by name.
            None => fold_schema::upcast::apply_declarative(
                &Default::default(),
                &target.fields,
                &payload,
            ),
            Some(UpcastHow::Declarative(d)) => {
                fold_schema::upcast::apply_declarative(d, &target.fields, &payload)
            }
            Some(UpcastHow::Wasm(w)) => {
                let export = w
                    .export
                    .clone()
                    .unwrap_or_else(|| Upcast::default_export(&family.name, target.id.version));
                let input = fold_wasm::UpcastInput {
                    abi: fold_wasm::ABI_VERSION,
                    event: fold_wasm::UpcastEvent {
                        r#type: from.clone(),
                        from_version: current.version,
                        to_version: target.id.version,
                        payload,
                    },
                };
                shared
                    .guest(&w.module)
                    .upcast(&export, &input)
                    .map_err(|source| UpcastError::Wasm {
                        from: from.clone(),
                        to: to.clone(),
                        source,
                    })?
            }
        };
        payload = shared
            .schema
            .canonicalize_event(target, &payload)
            .map_err(|errs| UpcastError::Invalid {
                from,
                to,
                reasons: errs
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; "),
            })?;
        current = target.id.clone();
    }
    if current.version == id.version {
        // Already the latest: records stored before a default was declared
        // get it now; nothing else is touched.
        shared
            .schema
            .apply_defaults(&family.latest().fields, &mut payload);
    }
    Ok((current, payload))
}

/// The schema's id of a recorded event's type.
pub fn type_id(t: &fold_core::EventType) -> EventTypeId {
    EventTypeId {
        context: t.context.clone(),
        name: t.name.clone(),
        version: t.version,
    }
}

/// `Context.Event@vN` of a schema id.
pub fn type_string(id: &EventTypeId) -> String {
    codec::type_string(&fold_core::EventType {
        context: id.context.clone(),
        name: id.name.clone(),
        version: id.version,
    })
}
