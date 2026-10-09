//! The Backup service: archives of the log and the online restore.

use std::sync::Arc;

use fold_proto::database::v1::backup_server::Backup as BackupSvc;
use fold_proto::database::v1::{
    BackupInfo, BackupLogRequest, BackupSchedule as WireSchedule, ListBackupsRequest,
    ListBackupsResponse, RestoreLogRequest, RestoreLogResponse,
};
use tonic::{Request, Response, Status};

use crate::codec;
use crate::state::Shared;

pub struct Service {
    shared: Arc<Shared>,
}

impl Service {
    pub fn new(shared: Arc<Shared>) -> Self {
        Service { shared }
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

#[tonic::async_trait]
impl BackupSvc for Service {
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
            other => codec::core_error(other),
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
            // database, so nothing would act on the request.
            return Err(Status::failed_precondition(
                "this database is not supervised; restore offline with `fold restore` instead",
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
}
