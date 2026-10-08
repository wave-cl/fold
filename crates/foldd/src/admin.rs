//! The Admin service: schema, projection status, health.

use std::sync::Arc;
use std::time::Instant;

use fold_proto::v1::admin_server::Admin as AdminSvc;
use fold_proto::v1::projection_status::State as WireState;
use fold_proto::v1::{
    DeleteSnapshotRequest, DeleteSnapshotResponse, GetSchemaRequest, GetSchemaResponse,
    HealthRequest, HealthResponse, ListProcessesRequest, ListProcessesResponse,
    ListProjectionsRequest, ListProjectionsResponse, ListSnapshotsRequest, ListSnapshotsResponse,
    ProcessStatus, ProjectionStatus, RebuildProjectionRequest, RebuildProjectionResponse,
    SnapshotInfo, SnapshotProjectionRequest,
};
use tonic::{Request, Response, Status};

use crate::projection::{ApplyError, Control, State};
use crate::snapshot::SnapshotError;
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

fn wire_state(s: State) -> WireState {
    match s {
        State::Starting => WireState::Starting,
        State::CatchingUp => WireState::CatchingUp,
        State::Live => WireState::Live,
        State::Failed => WireState::Failed,
        State::Stopped => WireState::Stopped,
        State::Rebuilding => WireState::Rebuilding,
    }
}

impl Service {
    /// The projection's control channel and its fold module's hash.
    fn projection(
        &self,
        name: &str,
    ) -> Result<(tokio::sync::mpsc::Sender<Control>, String), Status> {
        let control = self
            .shared
            .projection_controls
            .get(name)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("projection {name} is not in the schema")))?;
        let (ctx, proj) = name.split_once('.').expect("listed names are qualified");
        let p = self
            .shared
            .schema
            .projection(ctx, proj)
            .expect("listed projection exists");
        let hash = crate::snapshot::hex(&self.shared.guest(&p.fold.module).hash());
        Ok((control, hash))
    }
}

fn info(m: &crate::snapshot::SnapshotMeta, module_hash: &str) -> SnapshotInfo {
    SnapshotInfo {
        id: m.id.clone(),
        projection: m.projection.clone(),
        checkpoint: m.checkpoint,
        rows: m.rows,
        bytes: m.bytes,
        created_at_unix_nanos: m.created_at_unix_nanos,
        module_matches: m.module_hash == module_hash,
    }
}

fn apply_status(e: ApplyError) -> Status {
    match &e {
        ApplyError::ModuleMismatch { .. } => Status::failed_precondition(e.to_string()),
        ApplyError::Snapshot(SnapshotError::NotFound { .. }) => Status::not_found(e.to_string()),
        ApplyError::Snapshot(
            SnapshotError::Empty | SnapshotError::Checksum { .. } | SnapshotError::Format { .. },
        ) => Status::failed_precondition(e.to_string()),
        _ => Status::internal(e.to_string()),
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
        }))
    }

    async fn list_projections(
        &self,
        _: Request<ListProjectionsRequest>,
    ) -> Result<Response<ListProjectionsResponse>, Status> {
        let head = self.shared.log.head().0;
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

    async fn list_processes(
        &self,
        _: Request<ListProcessesRequest>,
    ) -> Result<Response<ListProcessesResponse>, Status> {
        let head = self.shared.log.head().0;
        let mut processes: Vec<ProcessStatus> = self
            .shared
            .processes
            .iter()
            .map(|(name, rx)| {
                let s = rx.borrow();
                ProcessStatus {
                    name: name.clone(),
                    state: wire_state(s.state) as i32,
                    checkpoint: s.checkpoint,
                    head,
                    error: s.error.clone().unwrap_or_default(),
                    pending_commands: s.pending,
                    dispatched: s.dispatched,
                    rejected: s.rejected,
                }
            })
            .collect();
        processes.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Response::new(ListProcessesResponse { processes }))
    }

    async fn snapshot_projection(
        &self,
        req: Request<SnapshotProjectionRequest>,
    ) -> Result<Response<SnapshotInfo>, Status> {
        let req = req.into_inner();
        let (control, hash) = self.projection(&req.projection)?;
        let (reply, rx) = tokio::sync::oneshot::channel();
        control
            .send(Control::Snapshot { reply })
            .await
            .map_err(|_| Status::unavailable("projection runner has stopped"))?;
        let meta = rx
            .await
            .map_err(|_| Status::unavailable("projection runner dropped the request"))?
            .map_err(apply_status)?;
        Ok(Response::new(info(&meta, &hash)))
    }

    async fn list_snapshots(
        &self,
        req: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let req = req.into_inner();
        let (_, hash) = self.projection(&req.projection)?;
        let log_dir = self.shared.log.path().to_path_buf();
        let metas =
            tokio::task::spawn_blocking(move || crate::snapshot::list(&log_dir, &req.projection))
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
        self.projection(&req.projection)?;
        let log_dir = self.shared.log.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            crate::snapshot::delete(&log_dir, &req.projection, &req.id)
        })
        .await
        .map_err(|e| Status::internal(format!("delete task: {e}")))?
        .map_err(|e| match e {
            SnapshotError::NotFound { .. } => Status::not_found(e.to_string()),
            other => Status::internal(other.to_string()),
        })?;
        Ok(Response::new(DeleteSnapshotResponse {}))
    }

    async fn rebuild_projection(
        &self,
        req: Request<RebuildProjectionRequest>,
    ) -> Result<Response<RebuildProjectionResponse>, Status> {
        let req = req.into_inner();
        let (control, _) = self.projection(&req.projection)?;
        let (reply, rx) = tokio::sync::oneshot::channel();
        control
            .send(Control::Rebuild {
                snapshot: (!req.snapshot_id.is_empty()).then_some(req.snapshot_id),
                force: req.force,
                reply,
            })
            .await
            .map_err(|_| Status::unavailable("projection runner has stopped"))?;
        let restarted_from = rx
            .await
            .map_err(|_| Status::unavailable("projection runner dropped the request"))?
            .map_err(apply_status)?;
        Ok(Response::new(RebuildProjectionResponse { restarted_from }))
    }

    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "ok".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            uptime_secs: self.started.elapsed().as_secs(),
            head: self.shared.log.head().0,
        }))
    }
}
