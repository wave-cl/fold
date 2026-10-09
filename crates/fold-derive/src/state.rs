//! Everything the node's services share: schema, store, guests, the
//! database's identity and what the tail and the role watch learn of it.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use fold_core::FsyncPolicy;
use fold_schema::DerivationSchema;
use fold_wasm::{Engine, Guest, ModuleCache};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::Options;
use crate::aggregate::AggregateCache;
use crate::db::Database;
use crate::projection::{Control, Status};
use crate::tail::Ring;

pub use fold_host::StatusBook;

/// What the tail last learned of the log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Head {
    /// The log's head (next position), as last seen.
    pub position: u64,
    pub epoch: u64,
    pub role: String,
    pub generation: u64,
    pub cut: u64,
    pub fenced_by: Option<u64>,
    /// Subscribed right now.
    pub connected: bool,
    pub error: Option<String>,
}

/// The role and the leader lease as the database reports them, polled by
/// the role watch: whether a read may be answered here at all.
pub struct ReadGate {
    inner: std::sync::Mutex<GateState>,
}

#[derive(Default)]
struct GateState {
    /// `None` until the first Health answered.
    role: Option<String>,
    epoch: u64,
    fenced_by: Option<u64>,
    lease_secs: u64,
    /// Request time plus the remaining lease the database reported.
    lease_until: Option<Instant>,
    lease_error: String,
    last_error: Option<String>,
}

impl ReadGate {
    fn new() -> Self {
        ReadGate {
            inner: std::sync::Mutex::new(GateState::default()),
        }
    }

    pub fn epoch(&self) -> u64 {
        self.inner.lock().expect("gate").epoch
    }

    pub fn role(&self) -> Option<String> {
        self.inner.lock().expect("gate").role.clone()
    }

    /// Why a read must not be answered here, if it must not. A replica's
    /// data is eventually consistent by design; a fenced database's is
    /// stale for good; a primary with leases on is current only under a
    /// live one.
    pub fn read_refusal(&self) -> Option<tonic::Status> {
        let g = self.inner.lock().expect("gate");
        match g.role.as_deref() {
            None => Some(tonic::Status::unavailable(format!(
                "the database has not answered Health yet{}",
                g.last_error
                    .as_deref()
                    .map(|e| format!(": {e}"))
                    .unwrap_or_default()
            ))),
            Some("fenced") => Some(tonic::Status::failed_precondition(format!(
                "the database was fenced: a newer primary (epoch {}) exists; read from a node of that one",
                g.fenced_by.unwrap_or(0)
            ))),
            Some("primary") if g.lease_secs > 0 => {
                if g.lease_until.is_some_and(|u| u > Instant::now()) {
                    return None;
                }
                Some(tonic::Status::unavailable(format!(
                    "the primary holds no lease ({}); a majority of the cluster has not confirmed it within the last {} s, so it may be stale; retry or read from a replica's node",
                    if g.lease_error.is_empty() {
                        "no renewal yet"
                    } else {
                        &g.lease_error
                    },
                    g.lease_secs
                )))
            }
            Some(_) => None,
        }
    }
}

pub struct Shared {
    pub schema: Arc<DerivationSchema>,
    /// The derivation bundle this node runs (what it stores and serves).
    pub schema_source: String,
    pub schema_sha256: String,
    pub schema_path: PathBuf,
    /// Read models, checkpoints and instance snapshots, bound to the log.
    pub store: fold_store::DerivedStore,
    /// Holds the store and `snapshots/`.
    pub derived_dir: PathBuf,
    pub engine: Engine,
    pub modules: ModuleCache,
    guests: fold_host::Guests,
    pub aggregates: AggregateCache,
    pub statuses: StatusBook,
    pub(crate) status_senders: HashMap<String, watch::Sender<Status>>,
    pub projection_controls: HashMap<String, tokio::sync::mpsc::Sender<Control>>,
    pub(crate) projection_control_receivers:
        std::sync::Mutex<HashMap<String, tokio::sync::mpsc::Receiver<Control>>>,
    pub db: Database,
    /// The log this node is bound to.
    pub log_id: uuid::Uuid,
    pub head: watch::Sender<Head>,
    pub gate: Arc<ReadGate>,
    pub ring: Ring,
    /// Bumped when the tail reset derived data past a cut: runners re-derive
    /// their resume points.
    pub generation_changed: watch::Sender<u64>,
    /// "reset past <cut> (generation n)" or "rebound to log <id>", for Health.
    pub last_reset: std::sync::Mutex<Option<String>>,
    pub last_schema_change: Option<String>,
    pub cancel: CancellationToken,
    pub limits: fold_wasm::Limits,
}

impl Shared {
    pub async fn open(opts: &Options, cancel: CancellationToken) -> anyhow::Result<Self> {
        let sources = fold_schema::Sources::load(&opts.schema)
            .with_context(|| format!("cannot read schema {}", opts.schema.display()))?;
        let schema_source = sources.bundle();
        let schema = sources.compile_derivation().map_err(|d| {
            anyhow::anyhow!(
                "schema {} is invalid: {} error(s)\n{d}",
                opts.schema.display(),
                d.len()
            )
        })?;
        let schema_sha256 = sha256_hex(&schema_source);
        let schema_dir = opts.wasm_dir.clone().unwrap_or_else(|| {
            opts.schema
                .parent()
                .map(|p| {
                    if p.as_os_str().is_empty() {
                        PathBuf::from(".")
                    } else {
                        p.to_path_buf()
                    }
                })
                .unwrap_or_else(|| PathBuf::from("."))
        });

        // The database: its identity binds the store; its domain must not
        // break against ours.
        let db = Database::connect_lazy(&opts.database)?;
        let asked_at = Instant::now();
        let health = db
            .health()
            .await
            .map_err(|e| anyhow::anyhow!("cannot reach the database {}: {e}", opts.database))?;
        let log_id: uuid::Uuid = health.log_id.parse().map_err(|e| {
            anyhow::anyhow!(
                "the database's log id {:?} is not a uuid: {e}",
                health.log_id
            )
        })?;
        let theirs = db
            .schema()
            .get_schema(fold_proto::common::v1::GetSchemaRequest {})
            .await
            .map_err(|e| anyhow::anyhow!("the database did not answer GetSchema: {e}"))?
            .into_inner();
        match fold_schema::Sources::from_bundle(&theirs.source).compile_domain() {
            Ok(db_domain) => {
                let diff =
                    fold_schema::diff_domain(&db_domain, &schema.domain, &fold_schema::AssumeData);
                anyhow::ensure!(
                    !diff.has_breaking(),
                    "the domain of {} breaks against the database's:\n{diff}",
                    opts.schema.display()
                );
                if !diff.is_empty() {
                    tracing::info!(summary = %diff.summary(), "the domain differs from the database's compatibly");
                }
            }
            Err(d) => {
                tracing::warn!(
                    errors = d.len(),
                    "the database's schema does not compile here; skipping the domain check"
                );
            }
        }

        let fsync = if opts.fsync {
            FsyncPolicy::Always
        } else {
            FsyncPolicy::Never
        };
        std::fs::create_dir_all(&opts.data_dir)
            .with_context(|| format!("cannot create data dir {}", opts.data_dir.display()))?;
        let derived_dir = opts.data_dir.clone();
        let store =
            fold_store::DerivedStore::open_or_create(&derived_dir.join("derived.redb"), fsync)
                .with_context(|| {
                    format!("cannot open the derived store in {}", derived_dir.display())
                })?;
        let mut last_reset = None;
        if store.bind(log_id)? {
            tracing::warn!(%log_id, "derived store was of another log; rebuilt from scratch");
            prune_snapshot_files(&derived_dir, 0);
            last_reset = Some(format!("rebound to log {log_id}"));
        }
        match store.generation()? {
            Some(g) if g == health.generation => {}
            Some(g) => {
                let report = store.reset_past(fold_core::GlobalPosition(health.cut))?;
                prune_snapshot_files(&derived_dir, health.cut);
                tracing::warn!(
                    from_generation = g,
                    to_generation = health.generation,
                    cut = health.cut,
                    runners_reset = ?report.runners_reset,
                    snapshots_dropped = report.snapshots_dropped,
                    "the log moved backwards; derived data past the cut dropped"
                );
                store.set_generation(health.generation)?;
                last_reset = Some(format!(
                    "reset past {} (generation {})",
                    health.cut, health.generation
                ));
            }
            None => store.set_generation(health.generation)?,
        }

        let last_schema_change =
            crate::schema_check::check(&store, &schema_source, &schema, opts.force_schema)?;

        let engine = Engine::new()?;
        let modules = ModuleCache::new(engine.clone());
        // Every module the derivation names is compiled and linked now: the
        // evolve, fold and wasm-upcaster exports only.
        let mut want: Vec<(String, String)> = Vec::new();
        for st in schema.states.values() {
            want.push((
                st.evolve.module.clone(),
                st.evolve
                    .export_or(&format!("evolve_{}", st.aggregate.name))
                    .to_string(),
            ));
        }
        for proj in schema.projections() {
            want.push((
                proj.fold.module.clone(),
                proj.fold
                    .export_or(&format!("project_{}", proj.name))
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

        let mut statuses = HashMap::new();
        let mut status_senders = HashMap::new();
        let mut projection_controls = HashMap::new();
        let mut control_receivers = HashMap::new();
        for proj in schema.projections() {
            let name = format!("{}.{}", proj.context, proj.name);
            let stored = store
                .checkpoint(&name)
                .with_context(|| format!("cannot read the checkpoint of {name}"))?
                .and_then(|c| c.next.0.checked_sub(1));
            let (tx, rx) = watch::channel(Status {
                checkpoint: stored,
                head: health.head,
                ..Status::starting(proj.tables.keys().cloned().collect())
            });
            statuses.insert(name.clone(), rx);
            status_senders.insert(name.clone(), tx);
            let (ctx_tx, ctx_rx) = tokio::sync::mpsc::channel(4);
            projection_controls.insert(name.clone(), ctx_tx);
            control_receivers.insert(name, ctx_rx);
        }

        // The gate starts from the Health already fetched, so a read right
        // after start is answered rather than told to wait for the watch.
        let gate = Arc::new(ReadGate::new());
        let shared = Shared {
            schema,
            schema_source,
            schema_sha256,
            schema_path: opts.schema.clone(),
            store,
            derived_dir,
            engine,
            modules,
            guests,
            aggregates: AggregateCache::new(opts.aggregate_cache),
            statuses,
            status_senders,
            projection_controls,
            projection_control_receivers: std::sync::Mutex::new(control_receivers),
            db,
            log_id,
            head: watch::channel(Head {
                position: health.head,
                epoch: health.epoch,
                role: health.role.clone(),
                generation: health.generation,
                cut: health.cut,
                fenced_by: health.fenced_by,
                connected: false,
                error: None,
            })
            .0,
            gate,
            ring: Ring::new(4096),
            generation_changed: watch::channel(0).0,
            last_reset: std::sync::Mutex::new(last_reset),
            last_schema_change,
            cancel,
            limits: opts.limits,
        };
        shared.adopt_health(asked_at, &health);
        Ok(shared)
    }

    /// The linked guest for a module path as written in the schema.
    pub fn guest(&self, module: &str) -> Arc<Guest> {
        fold_host::GuestSource::guest(&self.guests, module)
    }

    pub fn guests(&self) -> &fold_host::Guests {
        &self.guests
    }

    /// The database's head as last seen.
    pub fn db_head(&self) -> u64 {
        self.head.borrow().position
    }

    /// Why a read must not be answered here, if it must not.
    pub fn read_refusal(&self) -> Option<tonic::Status> {
        self.gate.read_refusal()
    }

    /// Records what the database's Health says, for the read gate.
    pub(crate) fn adopt_health(
        &self,
        asked_at: Instant,
        h: &fold_proto::database::v1::HealthResponse,
    ) {
        let mut g = self.gate.inner.lock().expect("gate");
        g.role = Some(h.role.clone());
        g.epoch = h.epoch;
        g.fenced_by = h.fenced_by;
        g.lease_secs = h.lease_secs;
        g.lease_until = if h.lease_held {
            Some(asked_at + Duration::from_millis(h.lease_remaining_ms))
        } else {
            None
        };
        g.lease_error = h.lease_error.clone();
        g.last_error = None;
    }

    pub(crate) fn health_failed(&self, error: String) {
        self.gate.inner.lock().expect("gate").last_error = Some(error);
    }
}

/// Polls the database's Health for the read gate: every third of the lease
/// when leases are on, else every second.
pub fn spawn_role_watch(shared: Arc<Shared>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if shared.cancel.is_cancelled() {
                return;
            }
            let asked_at = Instant::now();
            let every = match shared.db.health().await {
                Ok(h) => {
                    let every = if h.lease_secs > 0 {
                        Duration::from_millis((h.lease_secs * 1000 / 3).clamp(50, 1000))
                    } else {
                        Duration::from_secs(1)
                    };
                    shared.adopt_health(asked_at, &h);
                    every
                }
                Err(e) => {
                    shared.health_failed(e.to_string());
                    Duration::from_millis(500)
                }
            };
            tokio::select! {
                _ = shared.cancel.cancelled() => return,
                _ = tokio::time::sleep(every) => {}
            }
        }
    })
}

pub fn sha256_hex(text: &str) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(text.as_bytes());
    fold_host::snapshot::hex(&h.finalize())
}

/// Removes snapshot files taken at a checkpoint at or past `cut` (they are
/// named by it), for every runner under `<derived>/snapshots/`.
pub fn prune_snapshot_files(derived_dir: &std::path::Path, cut: u64) {
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
