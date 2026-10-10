//! The AppAdmin service: process managers, their snapshots, the node's
//! registrations and health.

use std::sync::Arc;
use std::time::Instant;

use fold_proto::application::v1::app_admin_server::AppAdmin as AdminSvc;
use fold_proto::application::v1::{
    GetProcessRequest, GetProcessResponse, HealthRequest, HealthResponse, ListProcessesRequest,
    ListProcessesResponse, ProcessStatus,
};
use fold_proto::common::v1::{
    DeleteSnapshotRequest, DeleteSnapshotResponse, GetSchemaRequest, GetSchemaResponse,
    ListSnapshotsRequest, ListSnapshotsResponse, RebuildRequest, RebuildResponse, RunnerState,
    SnapshotInfo, SnapshotRequest,
};
use tonic::{Request, Response, Status};

use crate::codec;
use crate::process::{Control, State};
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

    fn control(&self, name: &str) -> Result<(tokio::sync::mpsc::Sender<Control>, String), Status> {
        if !name.contains('.') {
            return Err(Status::invalid_argument("name must be Context.Process"));
        }
        let control = self.shared.process_controls.get(name).ok_or_else(|| {
            Status::not_found(format!(
                "{name} is not a process this application registered"
            ))
        })?;
        let hash = crate::snapshot::hex(
            &self
                .shared
                .manifest
                .process(name)
                .expect("listed process is in the manifest")
                .fingerprint(),
        );
        Ok((control.clone(), hash))
    }
}

fn wire_state(s: State) -> RunnerState {
    match s {
        State::Starting => RunnerState::Starting,
        State::CatchingUp => RunnerState::CatchingUp,
        State::Live => RunnerState::Live,
        State::Failed => RunnerState::Failed,
        State::Stopped => RunnerState::Stopped,
        State::Rebuilding => RunnerState::Rebuilding,
    }
}

fn info(m: &crate::snapshot::SnapshotMeta, module_hash: &str) -> SnapshotInfo {
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
        RebuildError::Store(s) => codec::store_error(s),
        RebuildError::Core(c) => codec::core_error(c),
    }
}

#[tonic::async_trait]
impl AdminSvc for Service {
    async fn get_schema(
        &self,
        _: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        // An application has no schema file: its registrations, as data.
        Ok(Response::new(GetSchemaResponse {
            source: self.shared.manifest_text.clone(),
            path: String::new(),
            layer: "application".to_string(),
            sha256: self.shared.manifest_sha256.clone(),
        }))
    }

    async fn list_processes(
        &self,
        _: Request<ListProcessesRequest>,
    ) -> Result<Response<ListProcessesResponse>, Status> {
        let head = self.shared.db_head();
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
                    pending_timers: s.pending_timers,
                }
            })
            .collect();
        processes.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Response::new(ListProcessesResponse { processes }))
    }

    async fn get_process(
        &self,
        req: Request<GetProcessRequest>,
    ) -> Result<Response<GetProcessResponse>, Status> {
        let req = req.into_inner();
        if !req.process.contains('.') {
            return Err(codec::invalid("process must be Context.Process"));
        }
        if !self.shared.app.processes.contains_key(&req.process) {
            return Err(Status::not_found(format!(
                "process {} is not one this application registered",
                req.process
            )));
        }
        let key = codec::parse_json(&req.key, "key")?;
        let shared = self.shared.clone();
        let name = req.process.clone();
        let state = tokio::task::spawn_blocking(move || {
            crate::process::instance_state(&shared, &name, &key)
        })
        .await
        .map_err(|e| Status::internal(format!("state task: {e}")))?
        .map_err(|e| match e {
            crate::process::ProcessError::Key(k) => codec::invalid(format!("key: {k}")),
            other => Status::internal(other.to_string()),
        })?;
        Ok(Response::new(match state {
            Some(s) => GetProcessResponse {
                found: true,
                state: serde_json::to_vec(&s).expect("json"),
                content_type: fold_proto::CONTENT_TYPE_JSON.into(),
            },
            None => GetProcessResponse::default(),
        }))
    }

    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        let head = self.shared.head.borrow().clone();
        let tail_position = self
            .shared
            .processes
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
            derivation: self.shared.derivation.url().to_string(),
            database_connected: head.connected,
            database_role: head.role.clone(),
            database_head: head.position,
            tail_position,
            layer_check: self.shared.layer.borrow().as_health(),
            last_reset: self
                .shared
                .last_reset
                .lock()
                .expect("last_reset")
                .clone()
                .unwrap_or_default(),
            last_schema_change: self.shared.last_schema_change.clone().unwrap_or_default(),
            generation: head.generation,
            invariants: "single-node".into(),
        }))
    }

    async fn snapshot(
        &self,
        req: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotInfo>, Status> {
        let req = req.into_inner();
        let (control, hash) = self.control(&req.name)?;
        let (reply, rx) = tokio::sync::oneshot::channel();
        control
            .send(Control::Snapshot { reply })
            .await
            .map_err(|_| Status::unavailable("process runner has stopped"))?;
        let meta = rx
            .await
            .map_err(|_| Status::unavailable("process runner dropped the request"))?
            .map_err(snapshot_status)?;
        Ok(Response::new(info(&meta, &hash)))
    }

    async fn list_snapshots(
        &self,
        req: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let req = req.into_inner();
        let (_, hash) = self.control(&req.name)?;
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
        self.control(&req.name)?;
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
        let (control, _) = self.control(&req.name)?;
        let snapshot = (!req.snapshot_id.is_empty()).then_some(req.snapshot_id);
        let (reply, rx) = tokio::sync::oneshot::channel();
        control
            .send(Control::Rebuild {
                snapshot,
                force: req.force,
                reply,
            })
            .await
            .map_err(|_| Status::unavailable("process runner has stopped"))?;
        let restarted_from = rx
            .await
            .map_err(|_| Status::unavailable("process runner dropped the request"))?
            .map_err(rebuild_status)?;
        Ok(Response::new(RebuildResponse { restarted_from }))
    }
}
