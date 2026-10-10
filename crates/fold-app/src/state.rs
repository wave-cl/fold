//! Everything the node's services share: the application's registrations,
//! the schemas the database and the derivation node run, the store, the
//! peers, and what the tail, the role watch and the layer check learn of
//! them.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

use anyhow::Context as _;
use fold_core::FsyncPolicy;
use fold_schema::{DerivationSchema, DomainSchema, Field};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::Options;
use crate::app::{App, Manifest};
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

/// The layer check: the application's registrations against the domain
/// the database runs and the derivation layer the derivation node runs.
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

/// The schemas the peers run, as last adopted, and what the registrations
/// resolved against them.
#[derive(Clone)]
pub struct Schemas {
    pub domain: Arc<DomainSchema>,
    pub derivation: Arc<DerivationSchema>,
    /// Process name → the field (with its type) its correlation key is
    /// encoded as.
    pub process_keys: HashMap<String, Field>,
}

pub struct Shared {
    pub app: Arc<App>,
    pub manifest: Manifest,
    pub manifest_text: String,
    pub manifest_sha256: String,
    schemas: RwLock<Schemas>,
    /// Process state, outbox, timers and checkpoints, bound to the log.
    pub store: fold_store::DerivedStore,
    pub derived_dir: PathBuf,
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
    /// One past the furthest position this node appended: what a guarding
    /// projection must have reached before a context invariant runs.
    pub appended_next: std::sync::atomic::AtomicU64,
    pub system_secret: Option<String>,
    pub invariant_wait: Duration,
    pub state_wait: Duration,
    pub ring: Ring,
    pub generation_changed: watch::Sender<u64>,
    pub last_reset: Mutex<Option<String>>,
    pub last_schema_change: Option<String>,
    pub cancel: CancellationToken,
}

impl Shared {
    pub async fn open(app: App, opts: &Options, cancel: CancellationToken) -> anyhow::Result<Self> {
        let manifest = app.manifest();
        let manifest_text = manifest.to_json();
        let manifest_sha256 = sha256_hex(&manifest_text);

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

        // The schemas the peers run: the application registers against
        // them, and a registration that does not resolve refuses the start.
        let fetched = fetch_schemas(&db, &derivation)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let process_keys = check_registrations(&app, &fetched.0, &fetched.1)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let schemas = Schemas {
            domain: fetched.0,
            derivation: fetched.1,
            process_keys,
        };

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
        let last_schema_change = crate::registry_check::check(&store, &manifest, &manifest_text)?;

        let mut processes = HashMap::new();
        let mut process_senders = HashMap::new();
        let mut process_controls = HashMap::new();
        let mut process_control_receivers = HashMap::new();
        for name in app.processes.keys() {
            let stored = store
                .checkpoint(name)
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
            process_control_receivers.insert(name.clone(), ctx_rx);
        }

        Ok(Shared {
            app: Arc::new(app),
            manifest,
            manifest_text,
            manifest_sha256,
            schemas: RwLock::new(schemas),
            store,
            derived_dir,
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
            appended_next: std::sync::atomic::AtomicU64::new(0),
            system_secret: opts.system_secret.clone(),
            invariant_wait: opts.invariant_wait,
            state_wait: opts.state_wait,
            ring: Ring::new(4096),
            generation_changed: watch::channel(0).0,
            last_reset: Mutex::new(last_reset),
            last_schema_change,
            cancel,
        })
    }

    /// The domain the database runs, as last adopted.
    pub fn domain(&self) -> Arc<DomainSchema> {
        self.schemas.read().expect("schemas").domain.clone()
    }

    /// The derivation layer the derivation node runs, as last adopted.
    pub fn derivation_schema(&self) -> Arc<DerivationSchema> {
        self.schemas.read().expect("schemas").derivation.clone()
    }

    /// The field a process's correlation key is encoded as.
    pub fn process_key(&self, process: &str) -> Option<Field> {
        self.schemas
            .read()
            .expect("schemas")
            .process_keys
            .get(process)
            .cloned()
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

    /// Resolves once the layer check has passed, or the node stops. A
    /// mismatch keeps it pending: a node whose registrations do not fit
    /// its peers' schemas issues nothing.
    pub async fn layer_passed(&self) {
        let mut rx = self.layer.subscribe();
        loop {
            if matches!(*rx.borrow_and_update(), LayerCheck::Ok) {
                return;
            }
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                changed = rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
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
                "the database {} reports role {other:?}",
                self.db.url()
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
                "this application does not match its peers' schemas: {why}"
            ))),
        }
    }

    pub fn now_rfc3339(&self) -> String {
        jiff::Timestamp::now().to_string()
    }

    pub async fn drain_processes(&self) {
        for (name, control) in &self.process_controls {
            if control.send(Control::Drain).await.is_err() {
                tracing::warn!(process = %name, "cannot ask the process manager to drain its outbox");
            }
        }
    }
}

/// The database's domain and the derivation node's layer, compiled here.
async fn fetch_schemas(
    db: &Database,
    derivation: &Derivation,
) -> Result<(Arc<DomainSchema>, Arc<DerivationSchema>), String> {
    let theirs = db
        .schema()
        .get_schema(fold_proto::common::v1::GetSchemaRequest {})
        .await
        .map_err(|e| format!("the database did not answer GetSchema: {}", e.message()))?
        .into_inner();
    let domain = fold_schema::Sources::from_bundle(&theirs.source)
        .compile_domain()
        .map_err(|d| {
            format!(
                "the database's schema does not compile here ({} error(s))",
                d.len()
            )
        })?;
    let theirs = derivation
        .admin()
        .get_schema(fold_proto::common::v1::GetSchemaRequest {})
        .await
        .map_err(|e| {
            format!(
                "the derivation node did not answer GetSchema: {}",
                e.message()
            )
        })?
        .into_inner();
    let derivation = fold_schema::Sources::from_bundle(&theirs.source)
        .compile_derivation()
        .map_err(|d| {
            format!(
                "the derivation node's schema does not compile here ({} error(s))",
                d.len()
            )
        })?;
    let diff = fold_schema::diff_domain(&domain, &derivation.domain, &fold_schema::AssumeData);
    if diff.has_breaking() {
        return Err(format!(
            "the derivation node's domain breaks against the database's: {}",
            diff.summary()
        ));
    }
    Ok((domain, derivation))
}

/// Every registration against the schemas: aggregates and events must
/// exist, a process's sources must carry its key, a context invariant's
/// scope must be a field of the aggregate's state and its projection must
/// exist. Returns each process's key field.
pub fn check_registrations(
    app: &App,
    domain: &DomainSchema,
    derivation: &DerivationSchema,
) -> Result<HashMap<String, Field>, String> {
    fn split(what: &str, name: &str) -> Result<(String, String), String> {
        name.split_once('.')
            .filter(|(c, n)| !c.is_empty() && !n.is_empty() && !n.contains('.'))
            .map(|(c, n)| (c.to_string(), n.to_string()))
            .ok_or_else(|| format!("{what} {name:?} must be Context.Name"))
    }
    for agg in app.aggregates.values() {
        let (ctx, name) = split("aggregate", &agg.name)?;
        if domain.aggregate(&ctx, &name).is_none() {
            return Err(format!(
                "aggregate {} is registered but the domain does not declare it",
                agg.name
            ));
        }
    }
    for inv in app.invariants.values() {
        let (ctx, name) = split("invariant", &inv.name)?;
        if inv.check.is_none() {
            return Err(format!("invariant {}.{} has no check", ctx, name));
        }
        let (agg_ctx, agg_name) = split(&format!("invariant {}'s aggregate", inv.name), &inv.on)?;
        if domain.aggregate(&agg_ctx, &agg_name).is_none() {
            return Err(format!(
                "invariant {} guards aggregate {}, which the domain does not declare",
                inv.name, inv.on
            ));
        }
        let state = derivation.state_of(&agg_ctx, &agg_name).ok_or_else(|| {
            format!(
                "invariant {} guards aggregate {}, which has no state on the derivation node",
                inv.name, inv.on
            )
        })?;
        if !state.fields.iter().any(|f| f.name == inv.scope) {
            return Err(format!(
                "invariant {} is scoped by {:?}, which is not a field of {}'s state",
                inv.name, inv.scope, inv.on
            ));
        }
        let (proj_ctx, proj_name) = split(
            &format!("invariant {}'s projection", inv.name),
            &inv.projection,
        )?;
        if derivation.projection(&proj_ctx, &proj_name).is_none() {
            return Err(format!(
                "invariant {} reads projection {}, which the derivation node does not run",
                inv.name, inv.projection
            ));
        }
    }
    let mut keys = HashMap::new();
    for proc in app.processes.values() {
        split("process", &proc.name)?;
        if proc.react.is_none() {
            return Err(format!("process {} has no reaction", proc.name));
        }
        if proc.key.is_empty() {
            return Err(format!("process {} names no key", proc.name));
        }
        if proc.sources.is_empty() {
            return Err(format!("process {} lists no source events", proc.name));
        }
        if let Some(t) = proc.timers.iter().find(|t| t.is_empty()) {
            return Err(format!(
                "process {} declares a timer named {t:?}",
                proc.name
            ));
        }
        let mut key_field: Option<Field> = None;
        for (family, by) in &proc.sources {
            let (ctx, name) = split(&format!("process {}'s source", proc.name), family)?;
            let fam = domain.event_family(&ctx, &name).ok_or_else(|| {
                format!(
                    "process {} reacts to {family}, which the domain does not declare",
                    proc.name
                )
            })?;
            let field = fam
                .latest()
                .fields
                .iter()
                .find(|f| f.name == *by)
                .ok_or_else(|| {
                    format!(
                        "process {} correlates {family} by {by:?}, which the event does not carry",
                        proc.name
                    )
                })?;
            match &key_field {
                None => key_field = Some(field.clone()),
                Some(k) if k.ty == field.ty => {}
                Some(k) => {
                    return Err(format!(
                        "process {}: {family}.{by} is a {}, but the key is a {}",
                        proc.name, field.ty, k.ty
                    ));
                }
            }
        }
        let mut field = key_field.expect("at least one source");
        field.name = proc.key.clone();
        keys.insert(proc.name.clone(), field);
    }
    Ok(keys)
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

/// Compares the registrations with the schemas the database and the
/// derivation node serve, until they fit; rechecked now and then, and the
/// peers' schemas adopted as they change compatibly.
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
    let (domain, derivation) = match fetch_schemas(&shared.db, &shared.derivation).await {
        Ok(s) => s,
        Err(why)
            if why.starts_with("the database did not answer")
                || why.starts_with("the derivation node did not answer") =>
        {
            return LayerCheck::Pending(why);
        }
        Err(why) => return LayerCheck::Mismatch(why),
    };
    let current = shared.domain();
    let diff = fold_schema::diff_domain(&current, &domain, &fold_schema::AssumeData);
    if diff.has_breaking() {
        return LayerCheck::Mismatch(format!(
            "the database's domain breaks against the one this application started with: {}",
            diff.summary()
        ));
    }
    let process_keys = match check_registrations(&shared.app, &domain, &derivation) {
        Ok(k) => k,
        Err(why) => return LayerCheck::Mismatch(why),
    };
    if !diff.is_empty() {
        tracing::info!(summary = %diff.summary(), "adopting the database's domain, which changed compatibly");
    }
    *shared.schemas.write().expect("schemas") = Schemas {
        domain,
        derivation,
        process_keys,
    };
    LayerCheck::Ok
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
