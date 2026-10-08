//! The Admin service: schema, projection status, health.

use std::sync::Arc;
use std::time::Instant;

use fold_proto::v1::admin_server::Admin as AdminSvc;
use fold_proto::v1::projection_status::State as WireState;
use fold_proto::v1::{
    BackupInfo, BackupLogRequest, DeleteSnapshotRequest, DeleteSnapshotResponse, GetSchemaRequest,
    GetSchemaResponse, HealthRequest, HealthResponse, ListBackupsRequest, ListBackupsResponse,
    ListProcessesRequest, ListProcessesResponse, ListProjectionsRequest, ListProjectionsResponse,
    ListSnapshotsRequest, ListSnapshotsResponse, ProcessStatus, ProjectionStatus,
    RebuildProjectionRequest, RebuildProjectionResponse, SnapshotInfo, SnapshotProjectionRequest,
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

/// What a snapshot or rebuild request names.
enum Target {
    /// A projection or process manager, driven through its runner.
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
        if let Some(control) = self.shared.process_controls.get(name) {
            let p = self
                .shared
                .schema
                .process(ctx, short)
                .expect("listed process exists");
            let hash = crate::snapshot::hex(&self.shared.guest(&p.react.module).hash());
            return Ok(Target::Runner(control.clone(), hash));
        }
        if let Some(a) = self.shared.schema.aggregate(ctx, short) {
            let hash = crate::snapshot::hex(&self.shared.guest(&a.evolve.module).hash());
            return Ok(Target::Aggregate {
                ctx: ctx.to_string(),
                name: short.to_string(),
                hash,
            });
        }
        Err(Status::not_found(format!(
            "{name} is not a projection, process or aggregate in the schema"
        )))
    }

    fn target_hash(&self, name: &str) -> Result<String, Status> {
        Ok(match self.target(name)? {
            Target::Runner(_, h) | Target::Aggregate { hash: h, .. } => h,
        })
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

fn backup_info(meta: &fold_core::BackupMeta, path: &std::path::Path) -> BackupInfo {
    BackupInfo {
        path: path.display().to_string(),
        log_id: meta.log_id.to_string(),
        head: meta.head,
        files: meta.files,
        bytes: meta.bytes,
        created_at_unix_nanos: meta.created_at_unix_nanos,
    }
}

fn snapshot_status(e: SnapshotError) -> Status {
    match &e {
        SnapshotError::NotFound { .. } => Status::not_found(e.to_string()),
        SnapshotError::Empty | SnapshotError::Checksum { .. } | SnapshotError::Format { .. } => {
            Status::failed_precondition(e.to_string())
        }
        _ => Status::internal(e.to_string()),
    }
}

fn rebuild_status(e: RebuildError) -> Status {
    match e {
        RebuildError::ModuleMismatch { .. } => Status::failed_precondition(e.to_string()),
        RebuildError::Snapshot(s) => snapshot_status(s),
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
        match self.target(&req.projection)? {
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
        let hash = self.target_hash(&req.projection)?;
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
        self.target(&req.projection)?;
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
        let snapshot = (!req.snapshot_id.is_empty()).then_some(req.snapshot_id);
        match self.target(&req.projection)? {
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
                Ok(Response::new(RebuildProjectionResponse { restarted_from }))
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
                tracing::info!(aggregate = %req.projection, warmed, "aggregate rebuilt");
                Ok(Response::new(RebuildProjectionResponse { restarted_from }))
            }
        }
    }

    async fn backup_log(
        &self,
        req: Request<BackupLogRequest>,
    ) -> Result<Response<BackupInfo>, Status> {
        let req = req.into_inner();
        let shared = self.shared.clone();
        let path = if req.path.is_empty() {
            let stamp = jiff::Timestamp::now()
                .strftime("%Y%m%dT%H%M%SZ")
                .to_string();
            shared
                .log
                .path()
                .join("backups")
                .join(format!("{:020}-{stamp}.fbak", shared.log.head().0))
        } else {
            std::path::PathBuf::from(req.path)
        };
        let (meta, path) =
            tokio::task::spawn_blocking(move || shared.log.backup_to(&path).map(|m| (m, path)))
                .await
                .map_err(|e| Status::internal(format!("backup task: {e}")))?
                .map_err(crate::codec::core_error)?;
        tracing::info!(path = %path.display(), head = meta.head, bytes = meta.bytes, "backup written");
        Ok(Response::new(backup_info(&meta, &path)))
    }

    async fn list_backups(
        &self,
        _: Request<ListBackupsRequest>,
    ) -> Result<Response<ListBackupsResponse>, Status> {
        let dir = self.shared.log.path().join("backups");
        let backups = tokio::task::spawn_blocking(move || -> Vec<BackupInfo> {
            let mut out = Vec::new();
            let Ok(entries) = std::fs::read_dir(&dir) else {
                return out;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("fbak") {
                    continue;
                }
                match fold_core::inspect_backup(&path) {
                    Ok(meta) => out.push(backup_info(&meta, &path)),
                    Err(e) => tracing::warn!(path = %path.display(), error = %e, "skipping unreadable backup"),
                }
            }
            out.sort_by_key(|b| std::cmp::Reverse((b.head, b.created_at_unix_nanos)));
            out
        })
        .await
        .map_err(|e| Status::internal(format!("list task: {e}")))?;
        Ok(Response::new(ListBackupsResponse { backups }))
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
