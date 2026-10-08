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
pub type StatusBook = HashMap<String, watch::Receiver<Status>>;
/// Process name (`Context.Process`) → live status.
pub type ProcessBook = HashMap<String, watch::Receiver<ProcStatus>>;

/// What `Admin.RestoreLog` asks the supervisor for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreRequest {
    pub archive: PathBuf,
    /// Point in time to cut the restored log at.
    pub to: Option<fold_core::PointInTime>,
}

pub struct Shared {
    pub schema: Arc<Schema>,
    pub schema_source: String,
    pub schema_path: PathBuf,
    pub log: Log,
    pub engine: Engine,
    pub modules: ModuleCache,
    /// Module path as written in the schema → linked guest.
    guests: HashMap<String, Arc<Guest>>,
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
    /// The primary this daemon was configured to replicate.
    pub replicate_from: Option<String>,
    /// Whether it is a replica right now: configured so and not promoted.
    replica_mode: std::sync::atomic::AtomicBool,
    /// After a promotion: the former primary.
    pub promoted_from: std::sync::Mutex<Option<String>>,
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
        let schema_source = std::fs::read_to_string(&opts.schema)
            .with_context(|| format!("cannot read schema {}", opts.schema.display()))?;
        let schema =
            Arc::new(Schema::from_file(&opts.schema).map_err(|e| {
                anyhow::anyhow!("schema {} is invalid:\n{e}", opts.schema.display())
            })?);
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

        let open = OpenOptions {
            fsync: if opts.fsync {
                FsyncPolicy::Always
            } else {
                FsyncPolicy::Never
            },
            ..OpenOptions::default()
        };
        std::fs::create_dir_all(&opts.data_dir)
            .with_context(|| format!("cannot create data dir {}", opts.data_dir.display()))?;
        let log = Log::open_or_create(&opts.data_dir, crate::LOG_NAME, open)
            .with_context(|| format!("cannot open log in {}", opts.data_dir.display()))?;
        if log.schema_source()?.is_none() {
            log.set_schema_source(&schema_source)?;
        }

        let engine = Engine::new()?;
        let modules = ModuleCache::new(engine.clone());

        // Every module the schema names is compiled and linked now, so a bad
        // module fails startup rather than the first command that needs it.
        let mut guests: HashMap<String, Arc<Guest>> = HashMap::new();
        let mut want: Vec<(String, String)> = Vec::new(); // (module, export)
        for (_, agg) in schema.aggregates() {
            want.push((
                agg.evolve.module.clone(),
                agg.evolve
                    .export_or(&format!("evolve_{}", agg.name))
                    .to_string(),
            ));
            for cmd in agg.commands.values() {
                want.push((
                    cmd.handler.module.clone(),
                    cmd.handler
                        .export_or(&format!("handle_{}", cmd.name))
                        .to_string(),
                ));
            }
        }
        for (_, proj) in schema.projections() {
            want.push((
                proj.fold.module.clone(),
                proj.fold
                    .export_or(&format!("project_{}", proj.name))
                    .to_string(),
            ));
        }
        for (_, agg) in schema.aggregates() {
            for inv in agg.invariants.values() {
                want.push((
                    inv.check.module.clone(),
                    inv.check
                        .export_or(&format!("check_{}", inv.name))
                        .to_string(),
                ));
            }
        }
        for ctx in schema.contexts.values() {
            for inv in ctx.invariants.values() {
                want.push((
                    inv.check.module.clone(),
                    inv.check
                        .export_or(&format!("check_{}", inv.name))
                        .to_string(),
                ));
            }
        }
        for (_, proc) in schema.processes() {
            want.push((
                proc.react.module.clone(),
                proc.react
                    .export_or(&format!("react_{}", proc.name))
                    .to_string(),
            ));
        }
        for (module, export) in want {
            if !guests.contains_key(&module) {
                let loaded = modules
                    .load(&schema_dir, &module)
                    .with_context(|| format!("cannot load wasm module {module}"))?;
                let guest = Guest::new(&engine, &loaded, opts.limits)
                    .with_context(|| format!("cannot link wasm module {module}"))?;
                guests.insert(module.clone(), Arc::new(guest));
            }
            let guest = &guests[&module];
            anyhow::ensure!(
                guest.has_export(&export),
                "wasm module {module} does not export `{export}` as (i32, i32) -> i64"
            );
        }

        let aggregates = AggregateCache::new(opts.aggregate_cache);
        let (restore_tx, restore_rx) = watch::channel(None);

        let mut statuses = HashMap::new();
        let mut status_senders = HashMap::new();
        let mut projection_controls = HashMap::new();
        let mut control_receivers = HashMap::new();
        // Statuses start truthful: a stored checkpoint is reported before
        // the runner's first pass, not only after it.
        let models = log.read_models();
        let stored = |name: &str| -> anyhow::Result<Option<u64>> {
            Ok(models
                .checkpoint(name)
                .with_context(|| format!("cannot read the checkpoint of {name}"))?
                .and_then(|p| p.0.checked_sub(1)))
        };
        for (ctx, proj) in schema.projections() {
            let name = format!("{}.{}", ctx.name, proj.name);
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
        for (ctx, proc) in schema.processes() {
            let name = format!("{}.{}", ctx.name, proc.name);
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
            replicate_from: opts.replicate_from.clone(),
            replica_mode: std::sync::atomic::AtomicBool::new(opts.replicate_from.is_some()),
            promoted_from: std::sync::Mutex::new(None),
            replication: std::sync::Mutex::new(Default::default()),
            replica_cancel: cancel.child_token(),
            replica_done: watch::channel(opts.replicate_from.is_none()).0,
            cancel,
            limits: opts.limits,
        })
    }

    pub fn is_replica(&self) -> bool {
        self.replica_mode.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Flips the role to primary; the tail must already have stopped.
    pub(crate) fn set_primary(&self) {
        self.replica_mode
            .store(false, std::sync::atomic::Ordering::Release);
    }

    pub fn role(&self) -> &'static str {
        if self.is_replica() {
            "replica"
        } else {
            "primary"
        }
    }

    /// The primary this daemon tails right now, if it is a replica.
    pub fn primary(&self) -> Option<&str> {
        if self.is_replica() {
            self.replicate_from.as_deref()
        } else {
            None
        }
    }

    /// The status a replica's write side answers with.
    pub fn replica_refusal(&self) -> Option<tonic::Status> {
        self.primary().map(|p| {
            tonic::Status::failed_precondition(format!(
                "this daemon is a read-only replica of {p}; send commands to the primary"
            ))
        })
    }

    /// The linked guest for a module path as written in the schema.
    pub fn guest(&self, module: &str) -> Arc<Guest> {
        self.guests
            .get(module)
            .cloned()
            .expect("every module named by the schema was linked at startup")
    }

    /// RFC 3339 wall clock, handed to command handlers.
    pub fn now_rfc3339(&self) -> String {
        jiff::Timestamp::now().to_string()
    }
}
