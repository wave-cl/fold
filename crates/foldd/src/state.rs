//! Everything the services share: schema, log, compiled guests, caches.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context as _;
use fold_core::{FsyncPolicy, Log, OpenOptions};
use fold_schema::Schema;
use fold_wasm::{Engine, Guest, ModuleCache};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::Options;
use crate::aggregate::AggregateCache;
use crate::command::StreamLocks;
use crate::process::ProcStatus;
use crate::projection::{Control, Status};

/// Projection name (`Context.Projection`) → live status.
pub use fold_host::StatusBook;
/// Process name (`Context.Process`) → live status.
pub type ProcessBook = HashMap<String, watch::Receiver<ProcStatus>>;

/// What `Admin.RestoreLog` asks the supervisor for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreRequest {
    pub archive: PathBuf,
    /// Point in time to cut the restored log at.
    pub to: Option<fold_core::PointInTime>,
}

/// What this daemon does with writes.
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

/// What the read side needs to know without touching the log: the role and
/// the leader lease. Shared with the Query service, which holds nothing of
/// the write side.
pub struct ReadGate {
    role: std::sync::atomic::AtomicU8,
    /// The log's fencing epoch, mirrored for the read side's tokens.
    epoch: std::sync::atomic::AtomicU64,
    /// Role fenced: the newer epoch that fenced this daemon.
    pub fenced_by: std::sync::Mutex<Option<u64>>,
    /// Lease duration, when leases are on.
    pub lease: Option<std::time::Duration>,
    /// Until when a majority has confirmed this primary; reads are served
    /// while `now` is before it.
    pub lease_until: std::sync::Mutex<Option<std::time::Instant>>,
    pub lease_error: std::sync::Mutex<Option<String>>,
}

impl ReadGate {
    pub fn role(&self) -> Role {
        Role::from_u8(self.role.load(std::sync::atomic::Ordering::Acquire))
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn set_epoch(&self, epoch: u64) {
        self.epoch
            .store(epoch, std::sync::atomic::Ordering::Release);
    }

    fn set_role(&self, role: Role) {
        self.role
            .store(role as u8, std::sync::atomic::Ordering::Release);
    }

    /// Remaining lease, if one is held right now.
    pub fn lease_remaining(&self) -> Option<std::time::Duration> {
        let until = (*self.lease_until.lock().expect("lease_until"))?;
        until.checked_duration_since(std::time::Instant::now())
    }

    /// Why a read must not be answered here, if it must not. A replica
    /// answers (it is eventually consistent by design); a fenced daemon
    /// never does; a primary with leases on answers only under a live one.
    pub fn read_refusal(&self) -> Option<tonic::Status> {
        match self.role() {
            Role::Replica => None,
            Role::Fenced => Some(tonic::Status::failed_precondition(format!(
                "this daemon was fenced: a newer primary (epoch {}) exists; read there",
                self.fenced_by.lock().expect("fenced_by").unwrap_or(0)
            ))),
            Role::Primary => {
                let lease = self.lease?;
                if self.lease_remaining().is_some() {
                    return None;
                }
                let why = self
                    .lease_error
                    .lock()
                    .expect("lease_error")
                    .clone()
                    .unwrap_or_else(|| "no renewal yet".into());
                Some(tonic::Status::unavailable(format!(
                    "this primary holds no lease ({why}); a majority of the cluster has not confirmed it within the last {} ms, so it may be stale; retry or read from a replica",
                    lease.as_millis()
                )))
            }
        }
    }
}

pub struct Shared {
    pub schema: Arc<Schema>,
    pub schema_source: String,
    pub schema_path: PathBuf,
    pub log: Log,
    /// What this daemon derives from the log: read models, process tables,
    /// aggregate instance snapshots. Beside the log, in `derived/`.
    pub store: fold_store::DerivedStore,
    /// `<data_dir>/derived`: the store and the snapshot files.
    pub derived_dir: PathBuf,
    pub engine: Engine,
    pub modules: ModuleCache,
    /// Module path as written in the schema → linked guest.
    guests: fold_host::Guests,
    pub aggregates: AggregateCache,
    pub statuses: StatusBook,
    pub(crate) status_senders: HashMap<String, watch::Sender<Status>>,
    pub processes: ProcessBook,
    pub(crate) process_senders: HashMap<String, watch::Sender<ProcStatus>>,
    /// Operator requests to a projection runner (snapshot, rebuild).
    pub projection_controls: HashMap<String, tokio::sync::mpsc::Sender<Control>>,
    pub(crate) projection_control_receivers:
        std::sync::Mutex<HashMap<String, tokio::sync::mpsc::Receiver<Control>>>,
    /// The same for process managers.
    pub process_controls: HashMap<String, tokio::sync::mpsc::Sender<Control>>,
    pub(crate) process_control_receivers:
        std::sync::Mutex<HashMap<String, tokio::sync::mpsc::Receiver<Control>>>,
    /// Per-stream and per-invariant-scope locks for the write side.
    pub locks: StreamLocks,
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
    /// The primary this daemon was configured to replicate.
    pub replicate_from: Option<String>,
    /// The role and the leader lease, shared with the read side.
    pub gate: Arc<ReadGate>,
    /// As a peer: until when this daemon granted a lease to a primary; it
    /// votes for nobody else before then.
    pub lease_granted_until: std::sync::Mutex<Option<std::time::Instant>>,
    /// After a promotion: whether the old primary acknowledged the fence.
    pub old_primary_fenced: std::sync::atomic::AtomicBool,
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
    pub cancel: CancellationToken,
    pub limits: fold_wasm::Limits,
}

impl Shared {
    pub fn open(opts: &Options, cancel: CancellationToken) -> anyhow::Result<Self> {
        let sources = fold_schema::Sources::load(&opts.schema)
            .with_context(|| format!("cannot read schema {}", opts.schema.display()))?;
        // The stored text is the bundle: one text for every file, which
        // compiles to the same model as the files on disk.
        let schema_source = sources.bundle();
        // The composite daemon runs every layer, so its root must be an
        // application file (S061 otherwise).
        let schema = sources.compile_application().map_err(|d| {
            anyhow::anyhow!(
                "schema {} is invalid: {} error(s)\n{d}",
                opts.schema.display(),
                d.len()
            )
        })?;
        let schema_dir = opts
            .schema
            .parent()
            .map(|p| {
                if p.as_os_str().is_empty() {
                    PathBuf::from(".")
                } else {
                    p.to_path_buf()
                }
            })
            .unwrap_or_else(|| PathBuf::from("."));

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
        // Derived data lives beside the log, bound to it; a log that moved
        // backwards since the store last saw it (a truncation, a restore)
        // invalidates what was derived past the cut.
        let derived_dir = opts.data_dir.join("derived");
        let store =
            fold_store::DerivedStore::open_or_create(&derived_dir.join("derived.redb"), fsync)
                .with_context(|| {
                    format!("cannot open the derived store in {}", derived_dir.display())
                })?;
        if store.bind(log.log_id())? {
            tracing::warn!(log_id = %log.log_id(), "derived store was of another log; rebuilt from scratch");
        }
        let generation = log.generation()?;
        match store.generation()? {
            Some(g) if g == generation => {}
            Some(g) => {
                let cut = log.cut()?;
                let report = store.reset_past(cut)?;
                prune_snapshot_files(&derived_dir, cut.0);
                tracing::warn!(
                    from_generation = g,
                    to_generation = generation,
                    %cut,
                    runners_reset = ?report.runners_reset,
                    snapshots_dropped = report.snapshots_dropped,
                    "the log moved backwards; derived data past the cut dropped"
                );
                store.set_generation(generation)?;
            }
            None => store.set_generation(generation)?,
        }

        let mut last_schema_change = None;
        match log.schema_source()? {
            None => log.set_schema_source(&schema_source)?,
            Some(stored) => {
                if let Some(outcome) = crate::schema_change::check(
                    &log,
                    &stored,
                    &schema_source,
                    &schema,
                    opts.force_schema,
                )? {
                    crate::schema_change::apply(&log, &store, &outcome, &schema, &schema_source)?;
                    tracing::info!(note = %outcome.note, "schema changed since the log was written");
                    last_schema_change = Some(outcome.note);
                }
            }
        }

        let engine = Engine::new()?;
        let modules = ModuleCache::new(engine.clone());

        // Every module the schema names is compiled and linked now, so a bad
        // module fails startup rather than the first command that needs it.
        let mut want: Vec<(String, String)> = Vec::new(); // (module, export)
        for st in schema.states.values() {
            want.push((
                st.evolve.module.clone(),
                st.evolve
                    .export_or(&format!("evolve_{}", st.aggregate.name))
                    .to_string(),
            ));
        }
        for block in schema.commands.values() {
            for cmd in block.commands.values() {
                want.push((
                    cmd.handler.module.clone(),
                    cmd.handler
                        .export_or(&format!("handle_{}", cmd.name))
                        .to_string(),
                ));
            }
        }
        for proj in schema.projections() {
            want.push((
                proj.fold.module.clone(),
                proj.fold
                    .export_or(&format!("project_{}", proj.name))
                    .to_string(),
            ));
        }
        for block in schema.commands.values() {
            for inv in block.invariants.values() {
                if let fold_schema::InvariantCheck::Wasm(w) = &inv.check {
                    want.push((
                        w.module.clone(),
                        w.export_or(&format!("check_{}", inv.name)).to_string(),
                    ));
                }
            }
        }
        for inv in schema.invariants.values() {
            want.push((
                inv.check.module.clone(),
                inv.check
                    .export_or(&format!("check_{}", inv.name))
                    .to_string(),
            ));
        }
        for proc in schema.processes() {
            want.push((
                proc.react.module.clone(),
                proc.react
                    .export_or(&format!("react_{}", proc.name))
                    .to_string(),
            ));
        }
        for ctx in schema.contexts.values() {
            for fam in ctx.events.values() {
                for ty in fam.versions.values() {
                    if let Some(fold_schema::Upcast {
                        how: fold_schema::UpcastHow::Wasm(w),
                        ..
                    }) = &ty.upcast
                    {
                        want.push((
                            w.module.clone(),
                            w.export_or(&fold_schema::Upcast::default_export(
                                &fam.name,
                                ty.id.version,
                            ))
                            .to_string(),
                        ));
                    }
                }
            }
        }
        let guests = fold_host::Guests::link(&engine, &modules, &schema_dir, &want, opts.limits)?;

        let aggregates = AggregateCache::new(opts.aggregate_cache);
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

        let mut statuses = HashMap::new();
        let mut status_senders = HashMap::new();
        let mut projection_controls = HashMap::new();
        let mut control_receivers = HashMap::new();
        // Statuses start truthful: a stored checkpoint is reported before
        // the runner's first pass, not only after it.
        let stored = |name: &str| -> anyhow::Result<Option<u64>> {
            Ok(store
                .checkpoint(name)
                .with_context(|| format!("cannot read the checkpoint of {name}"))?
                .and_then(|c| c.next.0.checked_sub(1)))
        };
        for proj in schema.projections() {
            let name = format!("{}.{}", proj.context, proj.name);
            let (tx, rx) = watch::channel(Status {
                checkpoint: stored(&name)?,
                ..Status::starting(proj.tables.keys().cloned().collect())
            });
            statuses.insert(name.clone(), rx);
            status_senders.insert(name.clone(), tx);
            let (ctx_tx, ctx_rx) = tokio::sync::mpsc::channel(4);
            projection_controls.insert(name.clone(), ctx_tx);
            control_receivers.insert(name, ctx_rx);
        }

        let mut processes = HashMap::new();
        let mut process_senders = HashMap::new();
        let mut process_controls = HashMap::new();
        let mut process_control_receivers = HashMap::new();
        for proc in schema.processes() {
            let name = format!("{}.{}", proc.context, proc.name);
            let (tx, rx) = watch::channel(ProcStatus {
                checkpoint: stored(&name)?,
                ..ProcStatus::starting()
            });
            processes.insert(name.clone(), rx);
            process_senders.insert(name.clone(), tx);
            let (ctx_tx, ctx_rx) = tokio::sync::mpsc::channel(4);
            process_controls.insert(name.clone(), ctx_tx);
            process_control_receivers.insert(name, ctx_rx);
        }

        Ok(Shared {
            schema,
            schema_source,
            schema_path: opts.schema.clone(),
            log,
            store,
            derived_dir,
            engine,
            modules,
            guests,
            aggregates,
            statuses,
            status_senders,
            processes,
            process_senders,
            projection_controls,
            projection_control_receivers: std::sync::Mutex::new(control_receivers),
            process_controls,
            process_control_receivers: std::sync::Mutex::new(process_control_receivers),
            locks: StreamLocks::default(),
            backup_status: std::sync::Mutex::new(Default::default()),
            restore_tx,
            restore_rx,
            restore_note: opts.restore_note.clone(),
            last_schema_change,
            replicate_from: opts.replicate_from.clone(),
            gate: Arc::new(ReadGate {
                role: std::sync::atomic::AtomicU8::new(initial_role as u8),
                epoch: std::sync::atomic::AtomicU64::new(initial_epoch),
                fenced_by: std::sync::Mutex::new(fenced_by),
                lease: opts.lease,
                lease_until: std::sync::Mutex::new(None),
                lease_error: std::sync::Mutex::new(None),
            }),
            lease_granted_until: std::sync::Mutex::new(None),
            old_primary_fenced: std::sync::atomic::AtomicBool::new(false),
            promoted_from: std::sync::Mutex::new(None),
            promotion_note: std::sync::Mutex::new(None),
            auto_failover: opts.auto_failover,
            quorum_peers: opts.quorum_peers.clone(),
            last_election: std::sync::Mutex::new(None),
            replication: std::sync::Mutex::new(Default::default()),
            replica_cancel: cancel.child_token(),
            replica_done: watch::channel(opts.replicate_from.is_none()).0,
            cancel,
            limits: opts.limits,
        })
    }

    pub fn role(&self) -> Role {
        self.gate.role()
    }

    /// Why a read must not be answered here, if it must not.
    pub fn read_refusal(&self) -> Option<tonic::Status> {
        self.gate.read_refusal()
    }

    /// Sets the epoch in the log and in the read gate's mirror.
    pub fn set_epoch(&self, epoch: u64) -> Result<(), fold_core::Error> {
        self.log.set_epoch(epoch)?;
        self.gate.set_epoch(epoch);
        Ok(())
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
        tracing::warn!(
            epoch,
            "fenced: a newer primary exists; refusing writes from now on"
        );
        Ok(())
    }

    /// The primary this daemon tails right now, if it is a replica.
    pub fn primary(&self) -> Option<&str> {
        if self.is_replica() {
            self.replicate_from.as_deref()
        } else {
            None
        }
    }

    /// The status a non-primary's write side answers with.
    pub fn write_refusal(&self) -> Option<tonic::Status> {
        match self.role() {
            Role::Primary => None,
            Role::Replica => Some(tonic::Status::failed_precondition(format!(
                "this daemon is a read-only replica of {}; send commands to the primary",
                self.replicate_from.as_deref().unwrap_or("?")
            ))),
            Role::Fenced => Some(tonic::Status::failed_precondition(format!(
                "this daemon was fenced: a newer primary (epoch {}) exists; send commands there",
                self.gate.fenced_by.lock().expect("fenced_by").unwrap_or(0)
            ))),
        }
    }

    /// Checks a write's fencing token against the epoch. A newer token
    /// fences this daemon; a stale one is refused; none at all passes.
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
                "fencing token {token} is newer than this daemon's epoch {epoch}: a newer primary exists; this daemon now refuses writes"
            )));
        }
        if token < epoch {
            return Err(tonic::Status::failed_precondition(format!(
                "stale fencing token {token}: this primary is at epoch {epoch}; refresh it from Health"
            )));
        }
        Ok(())
    }

    /// The linked guest for a module path as written in the schema.
    pub fn guest(&self, module: &str) -> Arc<Guest> {
        fold_host::GuestSource::guest(&self.guests, module)
    }

    /// The linked guests, for code shared with the other services.
    pub fn guests(&self) -> &fold_host::Guests {
        &self.guests
    }

    /// RFC 3339 wall clock, handed to command handlers.
    pub fn now_rfc3339(&self) -> String {
        jiff::Timestamp::now().to_string()
    }
}

/// Removes snapshot files taken at a checkpoint past `cut` (they are named
/// by it), for every runner under `<derived>/snapshots/`.
fn prune_snapshot_files(derived_dir: &std::path::Path, cut: u64) {
    let Ok(dirs) = std::fs::read_dir(derived_dir.join("snapshots")) else {
        return;
    };
    for d in dirs.flatten() {
        let Ok(files) = std::fs::read_dir(d.path()) else {
            continue;
        };
        for f in files.flatten() {
            let path = f.path();
            if path.extension().and_then(|e| e.to_str()) != Some("fsnap") {
                continue;
            }
            let at = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok());
            if at.is_some_and(|at| at >= cut)
                && let Err(e) = std::fs::remove_file(&path)
            {
                tracing::warn!(path = %path.display(), error = %e, "cannot remove a stale snapshot file");
            }
        }
    }
}
