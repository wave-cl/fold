//! Everything the node's services share: schema, store, guests, the peers
//! and what the tail, the role watch and the layer check learn of them.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use anyhow::Context as _;
use fold_core::FsyncPolicy;
use fold_schema::ApplicationSchema;
use fold_wasm::{Engine, Guest, ModuleCache};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::Options;
use crate::peers::{Database, Derivation};
use crate::process::ProcStatus;
use crate::tail::Ring;

pub use fold_host::runner::Control;

/// Process name (`Context.Process`) → live status.
pub type ProcessBook = HashMap<String, watch::Receiver<ProcStatus>>;

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
    pub connected: bool,
    pub error: Option<String>,
}

/// The layer check: this node's imports against what the database and the
/// derivation node run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayerCheck {
    Pending(String),
    Ok,
    Mismatch(String),
}

impl LayerCheck {
    pub fn as_health(&self) -> String {
        match self {
            LayerCheck::Pending(why) if why.is_empty() => "pending".to_string(),
            LayerCheck::Pending(why) => format!("pending: {why}"),
            LayerCheck::Ok => "ok".to_string(),
            LayerCheck::Mismatch(why) => format!("mismatch: {why}"),
        }
    }
}

/// One mutex per active stream (and per invariant scope), so load → handle
/// → append is atomic per stream without a global lock. Dead entries are
/// swept opportunistically.
#[derive(Default)]
pub struct StreamLocks {
    inner: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
}

impl StreamLocks {
    pub fn get(&self, stream: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self.inner.lock().expect("locks");
        if let Some(existing) = map.get(stream).and_then(Weak::upgrade) {
            return existing;
        }
        if map.len() > 1024 {
            map.retain(|_, w| w.strong_count() > 0);
        }
        let fresh = Arc::new(tokio::sync::Mutex::new(()));
        map.insert(stream.to_string(), Arc::downgrade(&fresh));
        fresh
    }
}

pub struct Shared {
    pub schema: Arc<ApplicationSchema>,
    pub schema_source: String,
    pub schema_sha256: String,
    pub schema_path: PathBuf,
    /// Process state, outbox, timers and checkpoints, bound to the log.
    pub store: fold_store::DerivedStore,
    pub derived_dir: PathBuf,
    pub engine: Engine,
    pub modules: ModuleCache,
    guests: fold_host::Guests,
    pub processes: ProcessBook,
    pub(crate) process_senders: HashMap<String, watch::Sender<ProcStatus>>,
    pub process_controls: HashMap<String, tokio::sync::mpsc::Sender<Control>>,
    pub(crate) process_control_receivers:
        Mutex<HashMap<String, tokio::sync::mpsc::Receiver<Control>>>,
    pub db: Database,
    pub derivation: Derivation,
    pub log_id: uuid::Uuid,
    pub head: watch::Sender<Head>,
    pub layer: watch::Sender<LayerCheck>,
    pub locks: StreamLocks,
    /// The last version this node appended per stream: what the derivation
    /// node must have reached before a command's state is read.
    pub last_appended: Mutex<HashMap<String, u64>>,
    pub system_secret: Option<String>,
    pub invariant_wait: Duration,
    pub state_wait: Duration,
    pub ring: Ring,
    pub generation_changed: watch::Sender<u64>,
    pub last_reset: Mutex<Option<String>>,
    pub last_schema_change: Option<String>,
    pub cancel: CancellationToken,
    pub limits: fold_wasm::Limits,
}

impl Shared {
    pub async fn open(opts: &Options, cancel: CancellationToken) -> anyhow::Result<Self> {
        let sources = fold_schema::Sources::load(&opts.schema)
            .with_context(|| format!("cannot read schema {}", opts.schema.display()))?;
        let schema_source = sources.bundle();
        let schema = sources.compile_application().map_err(|d| {
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

        let db = Database::connect_lazy(&opts.database)?;
        let derivation = Derivation::connect_lazy(&opts.derivation)?;
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
            tracing::warn!(%log_id, "the store was of another log; process managers restart from 0");
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
                    "the log moved backwards; process data past the cut dropped"
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
        // Handlers, wasm invariant checks, context invariant checks and
        // reactions: the application layer's exports only.
        let mut want: Vec<(String, String)> = Vec::new();
        for block in schema.commands.values() {
            for cmd in block.commands.values() {
                want.push((
                    cmd.handler.module.clone(),
                    cmd.handler
                        .export_or(&format!("handle_{}", cmd.name))
                        .to_string(),
                ));
            }
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
        let guests = fold_host::Guests::link(&engine, &modules, &schema_dir, &want, opts.limits)?;

        let mut processes = HashMap::new();
        let mut process_senders = HashMap::new();
        let mut process_controls = HashMap::new();
        let mut process_control_receivers = HashMap::new();
        for proc in schema.processes() {
            let name = format!("{}.{}", proc.context, proc.name);
            let stored = store
                .checkpoint(&name)
                .with_context(|| format!("cannot read the checkpoint of {name}"))?
                .and_then(|c| c.next.0.checked_sub(1));
            let (tx, rx) = watch::channel(ProcStatus {
                checkpoint: stored,
                head: health.head,
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
            schema_sha256,
            schema_path: opts.schema.clone(),
            store,
            derived_dir,
            engine,
            modules,
            guests,
            processes,
            process_senders,
            process_controls,
            process_control_receivers: Mutex::new(process_control_receivers),
            db,
            derivation,
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
            layer: watch::channel(LayerCheck::Pending(String::new())).0,
            locks: StreamLocks::default(),
            last_appended: Mutex::new(HashMap::new()),
            system_secret: opts.system_secret.clone(),
            invariant_wait: opts.invariant_wait,
            state_wait: opts.state_wait,
            ring: Ring::new(4096),
            generation_changed: watch::channel(0).0,
            last_reset: Mutex::new(last_reset),
            last_schema_change,
            cancel,
            limits: opts.limits,
        })
    }

    pub fn guest(&self, module: &str) -> Arc<Guest> {
        fold_host::GuestSource::guest(&self.guests, module)
    }

    pub fn guests(&self) -> &fold_host::Guests {
        &self.guests
    }

    pub fn db_head(&self) -> u64 {
        self.head.borrow().position
    }

    pub fn role(&self) -> String {
        self.head.borrow().role.clone()
    }

    pub fn epoch(&self) -> u64 {
        self.head.borrow().epoch
    }

    pub fn is_primary(&self) -> bool {
        self.head.borrow().role == "primary"
    }

    /// The status a write gets while the database is not a primary.
    pub fn write_refusal(&self) -> Option<tonic::Status> {
        let h = self.head.borrow();
        match h.role.as_str() {
            "primary" => None,
            "replica" => Some(tonic::Status::failed_precondition(format!(
                "the database {} is a read-only replica; send commands to the application node of its primary",
                self.db.url()
            ))),
            "fenced" => Some(tonic::Status::failed_precondition(format!(
                "the database {} was fenced: a newer primary (epoch {}) exists; send commands to its application node",
                self.db.url(),
                h.fenced_by.unwrap_or(0)
            ))),
            other => Some(tonic::Status::unavailable(format!(
                "the database's role is {other:?}; not taking commands"
            ))),
        }
    }

    /// The status a write gets until the layer check passes.
    pub fn layer_refusal(&self) -> Option<tonic::Status> {
        match &*self.layer.borrow() {
            LayerCheck::Ok => None,
            LayerCheck::Pending(why) => Some(tonic::Status::unavailable(format!(
                "the layer check has not passed yet{}; retry shortly",
                if why.is_empty() {
                    String::new()
                } else {
                    format!(" ({why})")
                }
            ))),
            LayerCheck::Mismatch(why) => Some(tonic::Status::failed_precondition(format!(
                "this node's schema does not match its peers': {why}"
            ))),
        }
    }

    /// RFC 3339 wall clock, handed to command handlers and reactions.
    pub fn now_rfc3339(&self) -> String {
        jiff::Timestamp::now().to_string()
    }

    /// Asks every process manager to dispatch what it held (a promotion).
    pub async fn drain_processes(&self) {
        for (name, control) in &self.process_controls {
            if control.send(Control::Drain).await.is_err() {
                tracing::warn!(process = %name, "cannot ask the process manager to drain its outbox");
            }
        }
    }
}

/// Polls the database's Health: the role and epoch for the write gate, the
/// head while the tail is down. A transition to primary drains the
/// process managers' outboxes.
pub fn spawn_role_watch(shared: Arc<Shared>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if shared.cancel.is_cancelled() {
                return;
            }
            let every = match shared.db.health().await {
                Ok(h) => {
                    let was_primary = shared.is_primary();
                    shared.head.send_if_modified(|head| {
                        let changed = head.role != h.role
                            || head.epoch != h.epoch
                            || head.fenced_by != h.fenced_by
                            || (!head.connected && head.position < h.head);
                        head.role = h.role.clone();
                        head.epoch = h.epoch;
                        head.fenced_by = h.fenced_by;
                        if !head.connected {
                            head.position = head.position.max(h.head);
                        }
                        changed
                    });
                    if !was_primary && shared.is_primary() {
                        shared.drain_processes().await;
                    }
                    Duration::from_secs(1)
                }
                Err(e) => {
                    shared.head.send_modify(|head| {
                        head.error = Some(e.to_string());
                    });
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

/// Compares this node's imports with the bundles the database and the
/// derivation node serve, until they match; rechecked now and then.
pub fn spawn_layer_check(shared: Arc<Shared>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if shared.cancel.is_cancelled() {
                return;
            }
            let outcome = layer_check_once(&shared).await;
            let every = match &outcome {
                LayerCheck::Ok => Duration::from_secs(30),
                _ => Duration::from_secs(2),
            };
            shared.layer.send_if_modified(|l| {
                if *l != outcome {
                    match &outcome {
                        LayerCheck::Ok => tracing::info!("layer check passed"),
                        LayerCheck::Mismatch(why) => tracing::error!(%why, "layer check: mismatch"),
                        LayerCheck::Pending(why) => tracing::warn!(%why, "layer check pending"),
                    }
                    *l = outcome.clone();
                    true
                } else {
                    false
                }
            });
            tokio::select! {
                _ = shared.cancel.cancelled() => return,
                _ = tokio::time::sleep(every) => {}
            }
        }
    })
}

async fn layer_check_once(shared: &Shared) -> LayerCheck {
    let db = match shared
        .db
        .schema()
        .get_schema(fold_proto::common::v1::GetSchemaRequest {})
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => return LayerCheck::Pending(format!("the database did not answer: {e}")),
    };
    let derive = match shared
        .derivation
        .admin()
        .get_schema(fold_proto::common::v1::GetSchemaRequest {})
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => return LayerCheck::Pending(format!("the derivation node did not answer: {e}")),
    };
    let ours = &shared.schema;
    match fold_schema::Sources::from_bundle(&db.source).compile_domain() {
        Ok(theirs) => {
            let diff = fold_schema::diff_domain(
                &theirs,
                &ours.derivation.domain,
                &fold_schema::AssumeData,
            );
            if diff.has_breaking() {
                return LayerCheck::Mismatch(format!(
                    "the domain this node imports breaks against the database's: {}",
                    diff.summary()
                ));
            }
        }
        Err(d) => {
            return LayerCheck::Mismatch(format!(
                "the database's schema does not compile here ({} error(s))",
                d.len()
            ));
        }
    }
    match fold_schema::Sources::from_bundle(&derive.source).compile_derivation() {
        Ok(theirs) => {
            let diff =
                fold_schema::diff_derivation(&theirs, &ours.derivation, &fold_schema::AssumeData);
            let derived_changes: Vec<&fold_schema::Change> =
                diff.changes.iter().filter(|c| !is_domain_only(c)).collect();
            if !derived_changes.is_empty() {
                let lines: Vec<String> = derived_changes
                    .iter()
                    .map(|c| format!("{}: {}", c.path, c.description))
                    .collect();
                return LayerCheck::Mismatch(format!(
                    "the derivation layer this node imports differs from the derivation node's: {}",
                    lines.join("; ")
                ));
            }
            if diff.has_breaking() {
                return LayerCheck::Mismatch(format!(
                    "the domain this node imports breaks against the derivation node's: {}",
                    diff.summary()
                ));
            }
        }
        Err(d) => {
            return LayerCheck::Mismatch(format!(
                "the derivation node's schema does not compile here ({} error(s))",
                d.len()
            ));
        }
    }
    LayerCheck::Ok
}

/// Whether a change lies in the domain layer (states and projections are
/// the derivation's; everything under a context is the domain's).
fn is_domain_only(c: &fold_schema::Change) -> bool {
    use fold_schema::ChangeKind as K;
    !matches!(
        c.kind,
        K::StateAdded
            | K::StateRemoved
            | K::AggregateStateChanged
            | K::ProjectionAdded
            | K::ProjectionRemoved
            | K::ProjectionSourcesChanged
            | K::TableAdded
            | K::TableRemoved
            | K::TableKeyChanged
            | K::ColumnAdded
            | K::ColumnRemoved
            | K::ColumnTypeChanged
    ) && !(c.kind == K::WasmChanged && (c.path.ends_with(".evolve") || c.path.ends_with(".fold")))
        && !(c.kind == K::SnapshotEveryChanged
            && c.action.layer() != fold_schema::Layer::Application)
}

pub fn sha256_hex(text: &str) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(text.as_bytes());
    fold_host::snapshot::hex(&h.finalize())
}

/// Removes snapshot files taken at a checkpoint at or past `cut`.
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
