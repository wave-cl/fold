//! The read side: `Query.Get` and `Query.Scan`, over read models only.
//!
//! This module holds no handle to the log or the aggregate cache: a query
//! can only ever see what a projection has written.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use fold_core::ReadModelStore;
use fold_proto::v1::query_server::Query as QuerySvc;
use fold_proto::v1::{GetRequest, GetResponse, ProjectionRow, ScanRequest};
use fold_schema::{Projection, Schema, Table};
use futures::Stream;
use tokio::sync::watch;
use tonic::{Request, Response, Status};

use crate::codec;
use crate::keys;
use crate::projection::{State, Status as ProjStatus, columns_of};
use crate::state::Shared;

pub const DEFAULT_WAIT_MS: u32 = 5_000;
pub const MAX_WAIT_MS: u32 = 30_000;
pub const DEFAULT_SCAN_LIMIT: u32 = 100;

pub struct Service {
    schema: Arc<Schema>,
    models: ReadModelStore,
    statuses: HashMap<String, watch::Receiver<ProjStatus>>,
}

impl Service {
    pub fn new(shared: Arc<Shared>) -> Self {
        Service {
            schema: shared.schema.clone(),
            models: shared.log.read_models(),
            statuses: shared.statuses.clone(),
        }
    }

    fn resolve(&self, projection: &str, table: &str) -> Result<(&Projection, &Table), Status> {
        let (ctx, name) = projection
            .split_once('.')
            .ok_or_else(|| codec::invalid("projection must be Context.Projection"))?;
        let p = self.schema.projection(ctx, name).ok_or_else(|| {
            Status::not_found(format!("projection {projection} is not in the schema"))
        })?;
        let t = p.tables.get(table).ok_or_else(|| {
            Status::not_found(format!("projection {projection} has no table {table}"))
        })?;
        Ok((p, t))
    }

    /// Waits until `projection` has applied `min_position`, or fails.
    async fn wait_for(
        &self,
        projection: &str,
        min_position: Option<u64>,
        wait_ms: Option<u32>,
    ) -> Result<Option<u64>, Status> {
        let rx = self
            .statuses
            .get(projection)
            .ok_or_else(|| Status::not_found(format!("projection {projection} is not running")))?;
        let check = |s: &ProjStatus| -> Result<bool, Status> {
            if s.state == State::Failed {
                return Err(Status::failed_precondition(format!(
                    "projection {projection} has failed: {}",
                    s.error.clone().unwrap_or_default()
                )));
            }
            Ok(match min_position {
                None => true,
                Some(p) => s.checkpoint.is_some_and(|c| c >= p),
            })
        };
        let mut rx = rx.clone();
        if check(&rx.borrow())? {
            return Ok(rx.borrow().checkpoint);
        }
        let wait = Duration::from_millis(u64::from(
            wait_ms.unwrap_or(DEFAULT_WAIT_MS).min(MAX_WAIT_MS),
        ));
        let result = tokio::time::timeout(wait, async {
            loop {
                if rx.changed().await.is_err() {
                    return Err(Status::unavailable(format!(
                        "projection {projection} stopped"
                    )));
                }
                let s = rx.borrow().clone();
                if check(&s)? {
                    return Ok(s.checkpoint);
                }
            }
        })
        .await;
        match result {
            Ok(r) => r,
            Err(_) => {
                let s = rx.borrow();
                Err(Status::unavailable(format!(
                    "projection {projection} has applied up to {} but {} was asked for",
                    s.checkpoint
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "nothing".into()),
                    min_position.unwrap_or(0)
                )))
            }
        }
    }
}

fn row_of(table: &Table, stored: &[u8]) -> Result<ProjectionRow, Status> {
    let full: serde_json::Value = serde_json::from_slice(stored)
        .map_err(|e| Status::internal(format!("stored row is not JSON: {e}")))?;
    let mut key = serde_json::Map::new();
    if let Some(obj) = full.as_object() {
        for k in &table.keys {
            if let Some(v) = obj.get(&k.name) {
                key.insert(k.name.clone(), v.clone());
            }
        }
    }
    let cols = columns_of(table, full);
    Ok(ProjectionRow {
        key: serde_json::to_vec(&serde_json::Value::Object(key)).expect("json"),
        row: serde_json::to_vec(&cols).expect("json"),
        content_type: fold_proto::CONTENT_TYPE_JSON.into(),
    })
}

#[tonic::async_trait]
impl QuerySvc for Service {
    async fn get(&self, req: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = req.into_inner();
        let (_, table) = self.resolve(&req.projection, &req.table)?;
        let key_json = codec::parse_json(&req.key, "key")?;
        let key_bytes = keys::encode(&self.schema, table, &key_json)
            .map_err(|e| codec::invalid(format!("key: {e}")))?;
        let checkpoint = self
            .wait_for(&req.projection, req.min_position, req.wait_ms)
            .await?;

        let models = self.models.clone();
        let projection = req.projection.clone();
        let table_name = req.table.clone();
        let stored = tokio::task::spawn_blocking(move || {
            models.snapshot()?.get(&projection, &table_name, &key_bytes)
        })
        .await
        .map_err(|e| Status::internal(format!("read task: {e}")))?
        .map_err(codec::core_error)?;
        let (found, row) = match stored {
            None => (false, None),
            Some(bytes) => (true, Some(row_of(table, &bytes)?)),
        };
        Ok(Response::new(GetResponse {
            found,
            row,
            checkpoint,
        }))
    }

    type ScanStream = Pin<Box<dyn Stream<Item = Result<ProjectionRow, Status>> + Send>>;

    async fn scan(&self, req: Request<ScanRequest>) -> Result<Response<Self::ScanStream>, Status> {
        let req = req.into_inner();
        let (_, table) = self.resolve(&req.projection, &req.table)?;
        let prefix_json = codec::parse_json(&req.key_prefix, "key_prefix")?;
        let prefix = keys::encode_prefix(&self.schema, table, &prefix_json)
            .map_err(|e| codec::invalid(format!("key_prefix: {e}")))?;
        self.wait_for(&req.projection, req.min_position, req.wait_ms)
            .await?;
        let limit = if req.limit == 0 {
            DEFAULT_SCAN_LIMIT
        } else {
            req.limit
        } as usize;

        let models = self.models.clone();
        let projection = req.projection.clone();
        let table_name = req.table.clone();
        let rows = tokio::task::spawn_blocking(move || {
            models
                .snapshot()?
                .scan(&projection, &table_name, &prefix, limit)
        })
        .await
        .map_err(|e| Status::internal(format!("scan task: {e}")))?
        .map_err(codec::core_error)?;
        let table = table.clone();
        let items: Vec<Result<ProjectionRow, Status>> =
            rows.into_iter().map(|(_, v)| row_of(&table, &v)).collect();
        Ok(Response::new(Box::pin(futures::stream::iter(items))))
    }
}

#[cfg(test)]
mod boundary {
    use super::Service;

    /// Destructuring every field is a compile-time assertion: adding a
    /// `Log` or aggregate-cache field to the read side breaks this test.
    #[test]
    fn the_read_side_holds_only_read_models_schema_and_status() {
        fn fields(s: &Service) {
            let Service {
                schema: _,
                models: _,
                statuses: _,
            } = s;
        }
        let _ = fields;
    }
}
