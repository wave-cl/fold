//! The internal `Derive` API: what the application service asks of this
//! node to run commands and process managers.

use std::sync::Arc;
use std::time::Duration;

use fold_core::StreamId;
use fold_proto::common::v1 as common;
use fold_proto::derivation::v1::derive_server::Derive as DeriveSvc;
use fold_proto::derivation::v1::{
    EvolveRequest, EvolveResponse, GetRowRequest, GetRowResponse, GetStateRequest,
    GetStateResponse, UpcastRequest, UpcastResponse, WaitCheckpointRequest, WaitCheckpointResponse,
};
use serde_json::Value;
use tonic::{Request, Response, Status};

use crate::aggregate;
use crate::codec;
use crate::projection;
use crate::state::Shared;

pub struct Service {
    shared: Arc<Shared>,
}

impl Service {
    pub fn new(shared: Arc<Shared>) -> Self {
        Service { shared }
    }
}

fn wait_of(wait_ms: Option<u32>) -> Duration {
    Duration::from_millis(u64::from(
        wait_ms
            .unwrap_or(crate::query::DEFAULT_WAIT_MS)
            .min(crate::query::MAX_WAIT_MS),
    ))
}

#[tonic::async_trait]
impl DeriveSvc for Service {
    async fn get_state(
        &self,
        req: Request<GetStateRequest>,
    ) -> Result<Response<GetStateResponse>, Status> {
        let req = req.into_inner();
        let stream =
            StreamId::new(&req.stream_id).map_err(|e| codec::invalid(format!("stream id: {e}")))?;
        let deadline = tokio::time::Instant::now() + wait_of(req.wait_ms);
        // State is read from the database's stream directly, so it is as
        // current as the database; a version not there yet is waited for.
        let loaded = loop {
            let shared = self.shared.clone();
            let target = stream.clone();
            let loaded = tokio::task::spawn_blocking(move || aggregate::load(&shared, &target))
                .await
                .map_err(|e| Status::internal(format!("load task: {e}")))??;
            match req.min_version {
                Some(min) if loaded.version.is_none_or(|v| v < min) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(Status::unavailable(format!(
                            "stream {stream} is at version {} but {min} was asked for",
                            loaded
                                .version
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "none".into())
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                _ => break loaded,
            }
        };
        let aggregate = format!("{}.{}", loaded.context, loaded.aggregate);
        Ok(Response::new(match (loaded.version, loaded.state) {
            (Some(v), Some(state)) => GetStateResponse {
                found: true,
                aggregate,
                version: Some(v),
                state: serde_json::to_vec(&state).expect("json"),
                content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                at_position: self.shared.db_head(),
            },
            _ => GetStateResponse {
                found: false,
                aggregate,
                at_position: self.shared.db_head(),
                ..Default::default()
            },
        }))
    }

    async fn evolve(
        &self,
        req: Request<EvolveRequest>,
    ) -> Result<Response<EvolveResponse>, Status> {
        let req = req.into_inner();
        let stream =
            StreamId::new(&req.stream_id).map_err(|e| codec::invalid(format!("stream id: {e}")))?;
        let shared = self.shared.clone();
        let state: Option<Value> = if req.state.is_empty() {
            None
        } else {
            Some(codec::parse_json(&req.state, "state")?)
        };
        let version = req.version;
        let events = req.events;
        let (state, version, upcast) = tokio::task::spawn_blocking(move || {
            let (ctx, agg, key) = aggregate::resolve(&shared, &stream)?;
            let domain = &shared.schema.domain;
            let head = shared.db_head();
            let first_version = version.map_or(0, |v| v + 1);
            let mut guest_events = Vec::with_capacity(events.len());
            let mut upcast_events = Vec::with_capacity(events.len());
            for (i, e) in events.iter().enumerate() {
                let (c, n, v) = fold_schema::parse_event_ref(&e.r#type)
                    .map_err(|err| codec::invalid(format!("event type {:?}: {err}", e.r#type)))?;
                let ty = match v {
                    Some(v) => domain.event_type(&c, &n, v),
                    None => domain.latest_event_type(&c, &n),
                }
                .ok_or_else(|| {
                    Status::not_found(format!("event type {} is not in the schema", e.r#type))
                })?;
                let payload = codec::parse_json(&e.payload, "payload")?;
                let (id, payload) =
                    crate::upcast::to_latest(domain, shared.guests(), &ty.id, payload)
                        .map_err(|err| Status::internal(err.to_string()))?;
                let metadata: Value = if e.metadata.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&e.metadata).unwrap_or(Value::Null)
                };
                let ty_string = crate::upcast::type_string(&id);
                guest_events.push(fold_wasm::Event {
                    stream: stream.to_string(),
                    r#type: ty_string.clone(),
                    version: first_version + i as u64,
                    position: head + i as u64,
                    payload: payload.clone(),
                    metadata,
                });
                upcast_events.push(common::NewEvent {
                    r#type: ty_string,
                    payload: serde_json::to_vec(&payload).expect("json"),
                    content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                    metadata: e.metadata.clone(),
                });
            }
            let state = aggregate::evolve_pending(
                &shared,
                ctx,
                agg,
                &stream,
                &key,
                version,
                state,
                &guest_events,
            )?;
            let last_version = first_version + events.len() as u64 - 1;
            Ok::<_, Status>((state, last_version, upcast_events))
        })
        .await
        .map_err(|e| Status::internal(format!("evolve task: {e}")))??;
        Ok(Response::new(EvolveResponse {
            state: serde_json::to_vec(&state).expect("json"),
            content_type: fold_proto::CONTENT_TYPE_JSON.into(),
            version,
            events: upcast,
        }))
    }

    async fn get_row(
        &self,
        req: Request<GetRowRequest>,
    ) -> Result<Response<GetRowResponse>, Status> {
        let req = req.into_inner();
        let (ctx, name) = req
            .projection
            .split_once('.')
            .ok_or_else(|| codec::invalid("projection must be Context.Projection"))?;
        let p = self.shared.schema.projection(ctx, name).ok_or_else(|| {
            Status::not_found(format!(
                "projection {} is not in the schema",
                req.projection
            ))
        })?;
        let table = p.tables.get(&req.table).ok_or_else(|| {
            Status::not_found(format!(
                "projection {} has no table {}",
                req.projection, req.table
            ))
        })?;
        let key_json = codec::parse_json(&req.key, "key")?;
        let key_bytes = crate::keys::encode(&self.shared.schema, table, &key_json)
            .map_err(|e| codec::invalid(format!("key: {e}")))?;
        let checkpoint = projection::wait_for_checkpoint(
            &self.shared,
            &req.projection,
            req.min_position,
            wait_of(req.wait_ms),
        )
        .await?;
        let models = self.shared.store.clone();
        let projection = req.projection.clone();
        let table_name = req.table.clone();
        let stored = tokio::task::spawn_blocking(move || {
            models.snapshot()?.get(&projection, &table_name, &key_bytes)
        })
        .await
        .map_err(|e| Status::internal(format!("read task: {e}")))?
        .map_err(codec::store_error)?;
        let (found, row) = match stored {
            None => (false, None),
            Some(bytes) => (true, Some(crate::query::row_of(table, &bytes)?)),
        };
        Ok(Response::new(GetRowResponse {
            found,
            row,
            checkpoint,
        }))
    }

    async fn wait_checkpoint(
        &self,
        req: Request<WaitCheckpointRequest>,
    ) -> Result<Response<WaitCheckpointResponse>, Status> {
        let req = req.into_inner();
        let checkpoint = projection::wait_for_checkpoint(
            &self.shared,
            &req.projection,
            req.min_position,
            wait_of(req.wait_ms),
        )
        .await?;
        Ok(Response::new(WaitCheckpointResponse { checkpoint }))
    }

    async fn upcast(
        &self,
        req: Request<UpcastRequest>,
    ) -> Result<Response<UpcastResponse>, Status> {
        let req = req.into_inner();
        let shared = self.shared.clone();
        let events = tokio::task::spawn_blocking(move || {
            let domain = &shared.schema.domain;
            let mut out = Vec::with_capacity(req.events.len());
            for e in &req.events {
                let (c, n, v) = fold_schema::parse_event_ref(&e.r#type)
                    .map_err(|err| codec::invalid(format!("event type {:?}: {err}", e.r#type)))?;
                let Some(v) = v else {
                    return Err(codec::invalid(format!(
                        "recorded event type {:?} has no version",
                        e.r#type
                    )));
                };
                let id = fold_schema::EventTypeId {
                    context: c,
                    name: n,
                    version: v,
                };
                let payload = codec::parse_json(&e.payload, "payload")?;
                let (latest, payload) =
                    crate::upcast::to_latest(domain, shared.guests(), &id, payload)
                        .map_err(|err| Status::internal(err.to_string()))?;
                out.push(common::RecordedEvent {
                    r#type: crate::upcast::type_string(&latest),
                    payload: serde_json::to_vec(&payload).expect("json"),
                    ..e.clone()
                });
            }
            Ok::<_, Status>(out)
        })
        .await
        .map_err(|e| Status::internal(format!("upcast task: {e}")))??;
        Ok(Response::new(UpcastResponse { events }))
    }
}
