//! The DeriveAdmin service: projections, snapshots, rebuilds, the node's
//! schema and health.

use std::sync::Arc;
use std::time::Instant;

use fold_proto::common::v1::{
    DeleteSnapshotRequest, DeleteSnapshotResponse, GetSchemaRequest, GetSchemaResponse,
    ListSnapshotsRequest, ListSnapshotsResponse, RebuildRequest, RebuildResponse, RunnerState,
    SnapshotInfo, SnapshotRequest,
};
use fold_proto::derivation::v1::derive_admin_server::DeriveAdmin as AdminSvc;
use fold_proto::derivation::v1::{
    HealthRequest, HealthResponse, ListProjectionsRequest, ListProjectionsResponse,
    ProjectionStatus,
};
use tonic::{Request, Response, Status};

use crate::projection::{Control, State};
use crate::snapshot::{RebuildError, SnapshotError};
use crate::state::Shared;

pub struct Service {
    shared: Arc<Shared>,
    started: Instant,
}

impl Service {
    pub fn new(shared: Arc<Shared>, started: Instant) -> Self {
        Service { shared, started }
    }
}

pub fn wire_state(s: State) -> RunnerState {
    match s {
        State::Starting => RunnerState::Starting,
        State::CatchingUp => RunnerState::CatchingUp,
        State::Live => RunnerState::Live,
        State::Failed => RunnerState::Failed,
        State::Stopped => RunnerState::Stopped,
        State::Rebuilding => RunnerState::Rebuilding,
    }
}

/// What a snapshot or rebuild request names.
enum Target {
    /// A projection, driven through its runner.
    Runner(tokio::sync::mpsc::Sender<Control>, String),
    /// An aggregate: its instance snapshots, handled directly.
    Aggregate {
        ctx: String,
        name: String,
        hash: String,
    },
}

impl Service {
    fn target(&self, name: &str) -> Result<Target, Status> {
        let (ctx, short) = name
            .split_once('.')
            .ok_or_else(|| Status::invalid_argument("name must be Context.Name"))?;
        if let Some(control) = self.shared.projection_controls.get(name) {
            let p = self
                .shared
                .schema
                .projection(ctx, short)
                .expect("listed projection exists");
            let hash = crate::snapshot::hex(&self.shared.guest(&p.fold.module).hash());
            return Ok(Target::Runner(control.clone(), hash));
        }
        if let Some(st) = self.shared.schema.state_of(ctx, short) {
            let hash = crate::snapshot::hex(&self.shared.guest(&st.evolve.module).hash());
            return Ok(Target::Aggregate {
                ctx: ctx.to_string(),
                name: short.to_string(),
                hash,
            });
        }
        Err(Status::not_found(format!(
            "{name} is not a projection or an aggregate with a state in the schema"
        )))
    }

    fn target_hash(&self, name: &str) -> Result<String, Status> {
        Ok(match self.target(name)? {
            Target::Runner(_, h) | Target::Aggregate { hash: h, .. } => h,
        })
    }
}

pub fn info(m: &crate::snapshot::SnapshotMeta, module_hash: &str) -> SnapshotInfo {
    SnapshotInfo {
        id: m.id.clone(),
        name: m.projection.clone(),
        checkpoint: m.checkpoint,
        rows: m.rows,
        bytes: m.bytes,
        created_at_unix_nanos: m.created_at_unix_nanos,
        module_matches: m.module_hash == module_hash,
    }
}

pub fn snapshot_status(e: SnapshotError) -> Status {
    match &e {
        SnapshotError::NotFound { .. } => Status::not_found(e.to_string()),
        SnapshotError::Empty | SnapshotError::Checksum { .. } | SnapshotError::Format { .. } => {
            Status::failed_precondition(e.to_string())
        }
        _ => Status::internal(e.to_string()),
    }
}

pub fn rebuild_status(e: RebuildError) -> Status {
    match e {
        RebuildError::ModuleMismatch { .. } => Status::failed_precondition(e.to_string()),
        RebuildError::Snapshot(s) => snapshot_status(s),
        RebuildError::Store(s) => crate::codec::store_error(s),
        RebuildError::Core(c) => crate::codec::core_error(c),
    }
}

#[tonic::async_trait]
impl AdminSvc for Service {
    async fn get_schema(
        &self,
        _: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        Ok(Response::new(GetSchemaResponse {
            source: self.shared.schema_source.clone(),
            path: self.shared.schema_path.display().to_string(),
            layer: fold_schema::Layer::Derivation.to_string(),
            sha256: self.shared.schema_sha256.clone(),
        }))
    }

    async fn list_projections(
        &self,
        _: Request<ListProjectionsRequest>,
    ) -> Result<Response<ListProjectionsResponse>, Status> {
        let head = self.shared.db_head();
        let mut projections: Vec<ProjectionStatus> = self
            .shared
            .statuses
            .iter()
            .map(|(name, rx)| {
                let s = rx.borrow();
                ProjectionStatus {
                    name: name.clone(),
                    state: wire_state(s.state) as i32,
                    checkpoint: s.checkpoint,
                    head,
                    error: s.error.clone().unwrap_or_default(),
                    tables: s.tables.clone(),
                }
            })
            .collect();
        projections.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Response::new(ListProjectionsResponse { projections }))
    }

    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        let head = self.shared.head.borrow().clone();
        let tail_position = self
            .shared
            .statuses
            .values()
            .map(|rx| rx.borrow().checkpoint.map_or(0, |c| c + 1))
            .min()
            .unwrap_or(head.position);
        Ok(Response::new(HealthResponse {
            status: "ok".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            uptime_secs: self.started.elapsed().as_secs(),
            log_id: self.shared.log_id.to_string(),
            database: self.shared.db.url().to_string(),
            database_connected: head.connected,
            database_role: self.shared.gate.role().unwrap_or_default(),
            database_head: head.position,
            tail_position,
            lag: head.position.saturating_sub(tail_position),
            database_error: head.error.clone().unwrap_or_default(),
            last_reset: self
                .shared
                .last_reset
                .lock()
                .expect("last_reset")
                .clone()
                .unwrap_or_default(),
            last_schema_change: self.shared.last_schema_change.clone().unwrap_or_default(),
            generation: head.generation,
        }))
    }

    async fn snapshot(
        &self,
        req: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotInfo>, Status> {
        let req = req.into_inner();
        match self.target(&req.name)? {
            Target::Runner(control, hash) => {
                let (reply, rx) = tokio::sync::oneshot::channel();
                control
                    .send(Control::Snapshot { reply })
                    .await
                    .map_err(|_| Status::unavailable("projection runner has stopped"))?;
                let meta = rx
                    .await
                    .map_err(|_| Status::unavailable("projection runner dropped the request"))?
                    .map_err(snapshot_status)?;
                Ok(Response::new(info(&meta, &hash)))
            }
            Target::Aggregate { ctx, name, hash } => {
                let shared = self.shared.clone();
                let meta = tokio::task::spawn_blocking(move || {
                    crate::aggregate::snapshot_all(&shared, &ctx, &name)
                })
                .await
                .map_err(|e| Status::internal(format!("snapshot task: {e}")))?
                .map_err(snapshot_status)?;
                Ok(Response::new(info(&meta, &hash)))
            }
        }
    }

    async fn list_snapshots(
        &self,
        req: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let req = req.into_inner();
        let hash = self.target_hash(&req.name)?;
        let dir = self.shared.derived_dir.clone();
        let metas = tokio::task::spawn_blocking(move || crate::snapshot::list(&dir, &req.name))
            .await
            .map_err(|e| Status::internal(format!("list task: {e}")))?
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ListSnapshotsResponse {
            snapshots: metas.iter().map(|m| info(m, &hash)).collect(),
        }))
    }

    async fn delete_snapshot(
        &self,
        req: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let req = req.into_inner();
        self.target(&req.name)?;
        let dir = self.shared.derived_dir.clone();
        tokio::task::spawn_blocking(move || crate::snapshot::delete(&dir, &req.name, &req.id))
            .await
            .map_err(|e| Status::internal(format!("delete task: {e}")))?
            .map_err(|e| match e {
                SnapshotError::NotFound { .. } => Status::not_found(e.to_string()),
                other => Status::internal(other.to_string()),
            })?;
        Ok(Response::new(DeleteSnapshotResponse {}))
    }

    async fn rebuild(
        &self,
        req: Request<RebuildRequest>,
    ) -> Result<Response<RebuildResponse>, Status> {
        let req = req.into_inner();
        let snapshot = (!req.snapshot_id.is_empty()).then_some(req.snapshot_id);
        match self.target(&req.name)? {
            Target::Runner(control, _) => {
                let (reply, rx) = tokio::sync::oneshot::channel();
                control
                    .send(Control::Rebuild {
                        snapshot,
                        force: req.force,
                        reply,
                    })
                    .await
                    .map_err(|_| Status::unavailable("projection runner has stopped"))?;
                let restarted_from = rx
                    .await
                    .map_err(|_| Status::unavailable("projection runner dropped the request"))?
                    .map_err(rebuild_status)?;
                Ok(Response::new(RebuildResponse { restarted_from }))
            }
            Target::Aggregate { ctx, name, .. } => {
                let shared = self.shared.clone();
                let force = req.force;
                let (restarted_from, warmed) = tokio::task::spawn_blocking(move || {
                    crate::aggregate::rebuild(&shared, &ctx, &name, snapshot, force)
                })
                .await
                .map_err(|e| Status::internal(format!("rebuild task: {e}")))?
                .map_err(rebuild_status)?;
                tracing::info!(aggregate = %req.name, warmed, "aggregate rebuilt");
                Ok(Response::new(RebuildResponse { restarted_from }))
            }
        }
    }
}
