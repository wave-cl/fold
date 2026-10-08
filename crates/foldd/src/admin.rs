//! The Admin service: schema, projection status, health.

use std::sync::Arc;
use std::time::Instant;

use fold_proto::v1::admin_server::Admin as AdminSvc;
use fold_proto::v1::projection_status::State as WireState;
use fold_proto::v1::{
    BackupInfo, BackupLogRequest, BackupSchedule as WireSchedule, DeleteSnapshotRequest,
    DeleteSnapshotResponse, GetSchemaRequest, GetSchemaResponse, HealthRequest, HealthResponse,
    ListBackupsRequest, ListBackupsResponse, ListProcessesRequest, ListProcessesResponse,
    ListProjectionsRequest, ListProjectionsResponse, ListSnapshotsRequest, ListSnapshotsResponse,
    ProcessStatus, ProjectionStatus, RebuildProjectionRequest, RebuildProjectionResponse,
    RestoreLogRequest, RestoreLogResponse, SnapshotInfo, SnapshotProjectionRequest,
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
        incremental: meta.kind == fold_core::BackupKind::Incremental,
        base_head: meta.base_head,
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
        let (meta, path) = tokio::task::spawn_blocking(move || {
            if req.incremental {
                let to = (!req.path.is_empty()).then(|| std::path::PathBuf::from(req.path));
                crate::scheduled::take_incremental(&shared, to)
            } else if req.path.is_empty() {
                crate::scheduled::take_full(&shared)
            } else {
                let path = std::path::PathBuf::from(req.path);
                shared.log.backup_to(&path).map(|m| (m, path))
            }
        })
        .await
        .map_err(|e| Status::internal(format!("backup task: {e}")))?
        .map_err(|e| match e {
            fold_core::Error::NotFound { .. } => {
                Status::failed_precondition("no backup to increment from; take a full backup first")
            }
            other => crate::codec::core_error(other),
        })?;
        tracing::info!(path = %path.display(), head = meta.head, bytes = meta.bytes, kind = ?meta.kind, "backup written");
        Ok(Response::new(backup_info(&meta, &path)))
    }

    async fn list_backups(
        &self,
        _: Request<ListBackupsRequest>,
    ) -> Result<Response<ListBackupsResponse>, Status> {
        let shared = self.shared.clone();
        let backups = tokio::task::spawn_blocking(move || -> Vec<BackupInfo> {
            crate::scheduled::existing(&shared)
                .iter()
                .map(|e| backup_info(&e.meta, &e.path))
                .collect()
        })
        .await
        .map_err(|e| Status::internal(format!("list task: {e}")))?;
        let schedule = {
            let st = self.shared.backup_status.lock().expect("backup status");
            st.schedule.map(|sch| WireSchedule {
                every_secs: sch.every.as_secs(),
                keep: sch.keep as u64,
                incremental: sch.incremental,
                full_every: u64::from(sch.full_every),
                last_run_unix_nanos: st.last_run_unix_nanos,
                last_head: st.last_head,
                last_error: st.last_error.clone().unwrap_or_default(),
                next_run_unix_nanos: st.next_run_unix_nanos,
            })
        };
        Ok(Response::new(ListBackupsResponse { backups, schedule }))
    }

    async fn restore_log(
        &self,
        req: Request<RestoreLogRequest>,
    ) -> Result<Response<RestoreLogResponse>, Status> {
        let req = req.into_inner();
        if req.path.is_empty() {
            return Err(Status::invalid_argument("path is required"));
        }
        let path = std::path::PathBuf::from(&req.path);
        let meta = tokio::task::spawn_blocking({
            let path = path.clone();
            move || fold_core::inspect_backup(&path)
        })
        .await
        .map_err(|e| Status::internal(format!("inspect task: {e}")))?
        .map_err(|e| match e {
            fold_core::Error::Io { .. } => Status::not_found(format!("{}: {e}", path.display())),
            other => Status::failed_precondition(other.to_string()),
        })?;
        if meta.kind == fold_core::BackupKind::Incremental {
            return Err(Status::failed_precondition(
                "an incremental backup cannot be restored on its own; restore its full backup \
                 offline and apply the increments with `fold restore --apply`",
            ));
        }
        if self.shared.restore_tx.receiver_count() <= 1 {
            // Only the Shared's own receiver exists: nobody supervises this
            // daemon, so nothing would act on the request.
            return Err(Status::failed_precondition(
                "this daemon is not supervised; restore offline with `fold restore` instead",
            ));
        }
        let to = match (req.to, req.at_unix_nanos) {
            (Some(_), Some(_)) => {
                return Err(Status::invalid_argument(
                    "give a position or a time, not both",
                ));
            }
            (Some(to), None) if to > meta.head => {
                return Err(Status::invalid_argument(format!(
                    "point in time {to} is past the archive's head {}",
                    meta.head
                )));
            }
            (Some(to), None) => Some(fold_core::PointInTime::Position(fold_core::GlobalPosition(
                to,
            ))),
            (None, Some(at)) => Some(fold_core::PointInTime::Time(at)),
            (None, None) => None,
        };
        if self.shared.restore_tx.borrow().is_some() {
            return Err(Status::already_exists("a restore is already in progress"));
        }
        self.shared
            .restore_tx
            .send_replace(Some(crate::state::RestoreRequest { archive: path, to }));
        Ok(Response::new(RestoreLogResponse {
            log_id: meta.log_id.to_string(),
            // A time is resolved once the log is restored; Health reports it.
            head: req.to.unwrap_or(meta.head),
        }))
    }

    async fn promote(
        &self,
        _: Request<fold_proto::v1::PromoteRequest>,
    ) -> Result<Response<fold_proto::v1::PromoteResponse>, Status> {
        let (head, promoted_from) = crate::replica::promote(&self.shared).await?;
        Ok(Response::new(fold_proto::v1::PromoteResponse {
            head,
            promoted_from,
        }))
    }

    async fn request_vote(
        &self,
        req: Request<fold_proto::v1::VoteRequest>,
    ) -> Result<Response<fold_proto::v1::VoteResponse>, Status> {
        let req = req.into_inner();
        let shared = &self.shared;
        let voter_epoch = shared.log.epoch().map_err(crate::codec::core_error)?;
        let voted_epoch = shared.log.voted_epoch().map_err(crate::codec::core_error)?;
        let voter_head = shared.log.head().0;
        let deny = |reason: String, primary_reachable: bool| {
            Ok(Response::new(fold_proto::v1::VoteResponse {
                granted: false,
                reason,
                voter_epoch,
                voted_epoch,
                voter_head,
                primary_reachable,
            }))
        };
        if req.log_id != shared.log.log_id().to_string() {
            return deny(format!("not the same log ({})", shared.log.log_id()), false);
        }
        if shared.is_primary() {
            return deny(format!("I am a primary at epoch {voter_epoch}"), false);
        }
        if req.epoch <= voter_epoch {
            return deny(
                format!("epoch {} is not newer than mine ({voter_epoch})", req.epoch),
                false,
            );
        }
        if req.epoch <= voted_epoch {
            return deny(format!("already voted in epoch {voted_epoch}"), false);
        }
        if req.candidate_head < voter_head {
            return deny(
                format!("candidate is behind: {} < {voter_head}", req.candidate_head),
                false,
            );
        }
        // A lease this peer granted is a promise too: the primary may still
        // be serving reads on it, so nobody is elected before it ends.
        let lease_left = shared
            .lease_granted_until
            .lock()
            .expect("lease_granted_until")
            .and_then(|until| until.checked_duration_since(std::time::Instant::now()));
        if let Some(left) = lease_left {
            return deny(
                format!(
                    "the primary holds a lease for another {} ms",
                    left.as_millis()
                ),
                false,
            );
        }
        // The point of the vote: is the primary gone from here too?
        let reachable = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let ch = tonic::transport::Channel::from_shared(req.primary.clone())
                .ok()?
                .connect_timeout(std::time::Duration::from_secs(1))
                .connect()
                .await
                .ok()?;
            fold_proto::v1::admin_client::AdminClient::new(ch)
                .health(HealthRequest {})
                .await
                .ok()
        })
        .await
        .ok()
        .flatten()
        .is_some();
        if reachable {
            return deny("the primary answers from here".into(), true);
        }
        shared
            .log
            .set_voted_epoch(req.epoch)
            .map_err(crate::codec::core_error)?;
        tracing::info!(epoch = req.epoch, candidate = %req.candidate, "voted for a failover candidate");
        Ok(Response::new(fold_proto::v1::VoteResponse {
            granted: true,
            reason: String::new(),
            voter_epoch,
            voted_epoch: req.epoch,
            voter_head,
            primary_reachable: false,
        }))
    }

    async fn renew_lease(
        &self,
        req: Request<fold_proto::v1::LeaseRequest>,
    ) -> Result<Response<fold_proto::v1::LeaseResponse>, Status> {
        let req = req.into_inner();
        let shared = &self.shared;
        let peer_epoch = shared.log.epoch().map_err(crate::codec::core_error)?;
        let voted_epoch = shared.log.voted_epoch().map_err(crate::codec::core_error)?;
        let deny = |reason: String| {
            Ok(Response::new(fold_proto::v1::LeaseResponse {
                granted: false,
                reason,
                peer_epoch,
                voted_epoch,
            }))
        };
        if req.log_id != shared.log.log_id().to_string() {
            return deny(format!("not the same log ({})", shared.log.log_id()));
        }
        if req.epoch < peer_epoch {
            return deny(format!("I know a newer epoch ({peer_epoch})"));
        }
        if req.epoch < voted_epoch {
            return deny(format!("I voted in a newer epoch ({voted_epoch})"));
        }
        if shared.is_primary() && req.epoch <= peer_epoch {
            return deny(format!("I am a primary at epoch {peer_epoch} myself"));
        }
        let until = std::time::Instant::now()
            + std::time::Duration::from_millis(req.duration_ms.min(60_000));
        *shared
            .lease_granted_until
            .lock()
            .expect("lease_granted_until") = Some(until);
        Ok(Response::new(fold_proto::v1::LeaseResponse {
            granted: true,
            reason: String::new(),
            peer_epoch,
            voted_epoch,
        }))
    }

    async fn fence(
        &self,
        req: Request<fold_proto::v1::FenceRequest>,
    ) -> Result<Response<fold_proto::v1::FenceResponse>, Status> {
        let req = req.into_inner();
        let epoch = self.shared.log.epoch().map_err(crate::codec::core_error)?;
        match self.shared.role() {
            crate::state::Role::Primary if req.epoch > epoch => {
                self.shared
                    .fence(req.epoch)
                    .map_err(|e| Status::internal(format!("cannot record the fence: {e}")))?;
            }
            crate::state::Role::Primary => {
                return Err(Status::failed_precondition(format!(
                    "epoch {} is not newer than this primary's epoch {epoch}",
                    req.epoch
                )));
            }
            // Not taking writes anyway.
            crate::state::Role::Replica | crate::state::Role::Fenced => {}
        }
        Ok(Response::new(fold_proto::v1::FenceResponse {
            role: self.shared.role().as_str().into(),
            epoch,
        }))
    }

    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        let repl = self
            .shared
            .replication
            .lock()
            .expect("replication status")
            .clone();
        Ok(Response::new(HealthResponse {
            status: "ok".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            uptime_secs: self.started.elapsed().as_secs(),
            head: self.shared.log.head().0,
            log_id: self.shared.log.log_id().to_string(),
            last_restore: self.shared.restore_note.clone().unwrap_or_default(),
            role: self.shared.role().as_str().into(),
            replicating_from: self.shared.primary().unwrap_or_default().into(),
            promoted_from: self
                .shared
                .promoted_from
                .lock()
                .expect("promoted_from")
                .clone()
                .unwrap_or_default(),
            promotion: self
                .shared
                .promotion_note
                .lock()
                .expect("promotion_note")
                .clone()
                .unwrap_or_default(),
            auto_failover_secs: self.shared.auto_failover.map(|d| d.as_secs()).unwrap_or(0),
            primary_unreachable_secs: repl.unreachable_for_secs,
            epoch: self.shared.log.epoch().map_err(crate::codec::core_error)?,
            quorum_size: if self.shared.auto_failover.is_some() {
                self.shared.quorum_peers.len() as u64 + 1
            } else {
                0
            },
            last_election: self
                .shared
                .last_election
                .lock()
                .expect("last_election")
                .clone()
                .unwrap_or_default(),
            fenced_by: *self.shared.gate.fenced_by.lock().expect("fenced_by"),
            lease_secs: self.shared.gate.lease.map(|d| d.as_secs()).unwrap_or(0),
            lease_held: self.shared.is_primary() && self.shared.gate.lease_remaining().is_some(),
            lease_remaining_ms: self
                .shared
                .gate
                .lease_remaining()
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            lease_error: self
                .shared
                .gate
                .lease_error
                .lock()
                .expect("lease_error")
                .clone()
                .unwrap_or_default(),
            old_primary_fenced: self
                .shared
                .old_primary_fenced
                .load(std::sync::atomic::Ordering::Acquire),
            replica_connected: repl.connected,
            primary_head: repl.primary_head,
            replication_error: repl.last_error.clone().unwrap_or_default(),
        }))
    }
}
