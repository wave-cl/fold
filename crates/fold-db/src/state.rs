//! Everything the database's services share: the domain, the log, the role.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use anyhow::Context as _;
use fold_core::{FsyncPolicy, Log, OpenOptions};
use fold_proto::database::v1::LogStatus;
use fold_schema::DomainSchema;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::Options;
use crate::system::SystemToken;

/// What `Backup.RestoreLog` asks the supervisor for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreRequest {
    pub archive: PathBuf,
    /// Point in time to cut the restored log at.
    pub to: Option<fold_core::PointInTime>,
}

/// What this database does with writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Primary,
    /// Tailing a primary; writes refused.
    Replica,
    /// Told of a newer primary; writes refused until made a replica of it.
    Fenced,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Primary => "primary",
            Role::Replica => "replica",
            Role::Fenced => "fenced",
        }
    }
    fn from_u8(v: u8) -> Role {
        match v {
            1 => Role::Replica,
            2 => Role::Fenced,
            _ => Role::Primary,
        }
    }
}

/// Marker file in a fenced log's directory.
pub const FENCED_MARKER: &str = "fenced";

/// The role, the epoch and the leader lease: what every subscriber learns
/// through `LogStatus` and what `Cluster.Health` reports.
pub struct Gate {
    role: AtomicU8,
    epoch: AtomicU64,
    /// Role fenced: the newer epoch that fenced this database.
    pub fenced_by: std::sync::Mutex<Option<u64>>,
    /// Lease duration, when leases are on.
    pub lease: Option<std::time::Duration>,
    /// Until when a majority has confirmed this primary.
    pub lease_until: std::sync::Mutex<Option<std::time::Instant>>,
    pub lease_error: std::sync::Mutex<Option<String>>,
}

impl Gate {
    pub fn role(&self) -> Role {
        Role::from_u8(self.role.load(Ordering::Acquire))
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    fn set_epoch(&self, epoch: u64) {
        self.epoch.store(epoch, Ordering::Release);
    }

    fn set_role(&self, role: Role) {
        self.role.store(role as u8, Ordering::Release);
    }

    /// Remaining lease, if one is held right now.
    pub fn lease_remaining(&self) -> Option<std::time::Duration> {
        let until = (*self.lease_until.lock().expect("lease_until"))?;
        until.checked_duration_since(std::time::Instant::now())
    }
}

pub struct Shared {
    pub domain: Arc<DomainSchema>,
    /// The bundle stored in the log.
    pub schema_source: String,
    pub schema_sha256: String,
    pub schema_path: PathBuf,
    pub log: Log,
    pub system: SystemToken,
    pub gate: Arc<Gate>,
    /// As a peer: until when this database granted a lease to a primary; it
    /// votes for nobody else before then.
    pub lease_granted_until: std::sync::Mutex<Option<std::time::Instant>>,
    /// After a promotion: whether the old primary acknowledged the fence.
    pub old_primary_fenced: AtomicBool,
    /// After a promotion: the former primary, and how it happened.
    pub promoted_from: std::sync::Mutex<Option<String>>,
    pub promotion_note: std::sync::Mutex<Option<String>>,
    pub auto_failover: Option<std::time::Duration>,
    pub quorum_peers: Vec<String>,
    /// Outcome of the last election round, for Health.
    pub last_election: std::sync::Mutex<Option<String>>,
    pub replication: std::sync::Mutex<crate::replica::ReplicationStatus>,
    /// Stops the tail task alone (a promotion); cancelled with `cancel` too.
    pub replica_cancel: CancellationToken,
    /// `true` once the tail task has exited.
    pub replica_done: watch::Sender<bool>,
    /// The backup schedule's state, if one runs.
    pub backup_status: std::sync::Mutex<crate::scheduled::BackupStatus>,
    /// An online restore request: the archive to swap in.
    pub restore_tx: watch::Sender<Option<RestoreRequest>>,
    pub restore_rx: watch::Receiver<Option<RestoreRequest>>,
    /// Outcome of the last online restore, for Health.
    pub restore_note: Option<String>,
    /// What the start-up schema check did, when the file differed from the
    /// stored text.
    pub last_schema_change: Option<String>,
    /// The primary this database was configured to replicate.
    pub replicate_from: Option<String>,
    /// Bumped on every role or epoch change: subscribers send a status item.
    pub status_changed: watch::Sender<u64>,
    pub cancel: CancellationToken,
}

impl Shared {
    pub fn open(opts: &Options, cancel: CancellationToken) -> anyhow::Result<Self> {
        let loaded = crate::domain::load(&opts.schema)?;
        let fsync = if opts.fsync {
            FsyncPolicy::Always
        } else {
            FsyncPolicy::Never
        };
        let open = OpenOptions {
            fsync,
            ..OpenOptions::default()
        };
        std::fs::create_dir_all(&opts.data_dir)
            .with_context(|| format!("cannot create data dir {}", opts.data_dir.display()))?;
        let log = Log::open_or_create(&opts.data_dir, crate::LOG_NAME, open)
            .with_context(|| format!("cannot open log in {}", opts.data_dir.display()))?;

        let mut last_schema_change = None;
        match log.schema_source()? {
            None => log.set_schema_source(&loaded.source)?,
            Some(stored) => {
                if let Some(outcome) = crate::domain::check(
                    &log,
                    &stored,
                    &loaded.source,
                    &loaded.domain,
                    opts.force_schema,
                )? {
                    log.set_schema_source(&loaded.source)?;
                    tracing::info!(note = %outcome.note, "schema changed since the log was written");
                    last_schema_change = Some(outcome.note);
                }
            }
        }

        let (restore_tx, restore_rx) = watch::channel(None);
        // A fenced log stays fenced across restarts; becoming a replica of
        // the new primary (`replica::prepare` removes the marker) is the
        // way back.
        let fenced_by: Option<u64> = std::fs::read_to_string(log.path().join(FENCED_MARKER))
            .ok()
            .and_then(|s| s.split_whitespace().nth(3).and_then(|e| e.parse().ok()));
        let initial_epoch = log.epoch()?;
        let initial_role = if opts.replicate_from.is_some() {
            Role::Replica
        } else if fenced_by.is_some() {
            Role::Fenced
        } else {
            Role::Primary
        };
        Ok(Shared {
            domain: loaded.domain,
            schema_source: loaded.source,
            schema_sha256: loaded.sha256,
            schema_path: opts.schema.clone(),
            log,
            system: SystemToken::from_secret(opts.system_secret.as_deref()),
            gate: Arc::new(Gate {
                role: AtomicU8::new(initial_role as u8),
                epoch: AtomicU64::new(initial_epoch),
                fenced_by: std::sync::Mutex::new(fenced_by),
                lease: opts.lease,
                lease_until: std::sync::Mutex::new(None),
                lease_error: std::sync::Mutex::new(None),
            }),
            lease_granted_until: std::sync::Mutex::new(None),
            old_primary_fenced: AtomicBool::new(false),
            promoted_from: std::sync::Mutex::new(None),
            promotion_note: std::sync::Mutex::new(None),
            auto_failover: opts.auto_failover,
            quorum_peers: opts.quorum_peers.clone(),
            last_election: std::sync::Mutex::new(None),
            replication: std::sync::Mutex::new(Default::default()),
            replica_cancel: cancel.child_token(),
            replica_done: watch::channel(opts.replicate_from.is_none()).0,
            backup_status: std::sync::Mutex::new(Default::default()),
            restore_tx,
            restore_rx,
            restore_note: opts.restore_note.clone(),
            last_schema_change,
            replicate_from: opts.replicate_from.clone(),
            status_changed: watch::channel(0).0,
            cancel,
        })
    }

    pub fn role(&self) -> Role {
        self.gate.role()
    }

    fn bump_status(&self) {
        self.status_changed.send_modify(|n| *n += 1);
    }

    /// Sets the epoch in the log and in the gate's mirror.
    pub fn set_epoch(&self, epoch: u64) -> Result<(), fold_core::Error> {
        self.log.set_epoch(epoch)?;
        self.gate.set_epoch(epoch);
        self.bump_status();
        Ok(())
    }

    /// Mirrors an epoch the log already carries (a replicated chunk's).
    pub(crate) fn mirror_epoch(&self, epoch: u64) {
        if self.gate.epoch() != epoch {
            self.gate.set_epoch(epoch);
            self.bump_status();
        }
    }

    pub fn is_replica(&self) -> bool {
        self.role() == Role::Replica
    }

    pub fn is_primary(&self) -> bool {
        self.role() == Role::Primary
    }

    /// Flips the role to primary; the tail must already have stopped.
    pub(crate) fn set_primary(&self) {
        self.gate.set_role(Role::Primary);
        self.bump_status();
    }

    /// Fenced by a newer primary at `epoch`: writes are refused from now
    /// on, and across restarts (a marker in the log directory).
    pub fn fence(&self, epoch: u64) -> std::io::Result<()> {
        let marker = self.log.path().join(FENCED_MARKER);
        std::fs::write(
            &marker,
            format!(
                "fenced by epoch {epoch} at {}\n",
                jiff::Timestamp::now().strftime("%Y-%m-%dT%H:%M:%SZ")
            ),
        )?;
        *self.gate.fenced_by.lock().expect("fenced_by") = Some(epoch);
        self.gate.set_role(Role::Fenced);
        self.bump_status();
        tracing::warn!(
            epoch,
            "fenced: a newer primary exists; refusing writes from now on"
        );
        Ok(())
    }

    /// The primary this database tails right now, if it is a replica.
    pub fn primary(&self) -> Option<&str> {
        if self.is_replica() {
            self.replicate_from.as_deref()
        } else {
            None
        }
    }

    /// The status a non-primary answers writes with.
    pub fn write_refusal(&self) -> Option<tonic::Status> {
        match self.role() {
            Role::Primary => None,
            Role::Replica => Some(tonic::Status::failed_precondition(format!(
                "this database is a read-only replica of {}; send writes to the primary",
                self.replicate_from.as_deref().unwrap_or("?")
            ))),
            Role::Fenced => Some(tonic::Status::failed_precondition(format!(
                "this database was fenced: a newer primary (epoch {}) exists; send writes there",
                self.gate.fenced_by.lock().expect("fenced_by").unwrap_or(0)
            ))),
        }
    }

    /// Checks a write's fencing token against the epoch. A newer token
    /// fences this database; a stale one is refused; none at all passes.
    pub fn check_fencing_token(&self, token: Option<u64>) -> Result<(), tonic::Status> {
        let Some(token) = token else {
            return Ok(());
        };
        let epoch = self.log.epoch().map_err(crate::codec::core_error)?;
        if token > epoch {
            if self.is_primary()
                && let Err(e) = self.fence(token)
            {
                return Err(tonic::Status::internal(format!(
                    "cannot record the fence: {e}"
                )));
            }
            return Err(tonic::Status::failed_precondition(format!(
                "fencing token {token} is newer than this database's epoch {epoch}: a newer primary exists; this database now refuses writes"
            )));
        }
        if token < epoch {
            return Err(tonic::Status::failed_precondition(format!(
                "stale fencing token {token}: this primary is at epoch {epoch}; refresh it from Health"
            )));
        }
        Ok(())
    }

    /// The log as a subscriber must know it right now.
    pub fn log_status(&self) -> Result<LogStatus, fold_core::Error> {
        Ok(LogStatus {
            log_id: self.log.log_id().to_string(),
            head: self.log.head().0,
            epoch: self.gate.epoch(),
            role: self.role().as_str().to_string(),
            generation: self.log.generation()?,
            cut: self.log.cut()?.0,
            fenced_by: *self.gate.fenced_by.lock().expect("fenced_by"),
        })
    }
}
