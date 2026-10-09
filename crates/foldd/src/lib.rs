//! `foldd`: the three fold services in one process, for development, a
//! single machine, and the end-to-end suite.
//!
//! A composite hosts a database node ([`fold_db`]), a derivation node
//! ([`fold_derive`]) and an application node ([`fold_app`]) and serves all
//! their gRPC services on one address. The nodes talk to each other the
//! way deployed ones do, over gRPC: the database and the derivation node
//! each also listen on a loopback port of their own, which the nodes above
//! them use, so the code paths inside the composite are the deployment's.
//!
//! Under `--data-dir` the log is `default/` (unchanged from a standalone
//! database), the derivation node's store is [`DERIVE_DIR`] and the
//! application node's is [`APP_DIR`]. `--schema` names the application
//! file; the lower layers come from its imports. A system secret for the
//! application node's timers is generated for each start unless given.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::transport::server::Router;

pub use fold_db::scheduled;
pub use fold_db::shutdown;
pub use fold_db::{BackupSchedule, LOG_NAME, RestoreRequest, Role, swap_log};
pub use fold_host::snapshot;
pub use fold_wasm::Limits;

/// The derivation node's directory under the composite's data dir.
pub const DERIVE_DIR: &str = "derive";
/// The application node's directory under the composite's data dir.
pub const APP_DIR: &str = "app";

#[derive(Debug, Clone)]
pub struct Options {
    pub data_dir: PathBuf,
    /// The application schema; its imports give the lower layers.
    pub schema: PathBuf,
    pub listen: SocketAddr,
    pub limits: Limits,
    /// How many aggregate instances the derivation node keeps evolved in
    /// memory.
    pub aggregate_cache: usize,
    /// Durability of the log and the stores: `false` is for tests and bulk
    /// loads only.
    pub fsync: bool,
    /// Take a backup into the log's backups directory on this schedule.
    pub backup: Option<BackupSchedule>,
    /// Outcome of the last online restore, reported by the database's
    /// Health. Set by the supervisor; not something to configure.
    pub restore_note: Option<String>,
    /// Run the database as a read-only replica tailing this primary (a
    /// gRPC URL such as `http://10.0.0.1:4141`). Commands are refused;
    /// projections run on the replicated events, process managers hold
    /// their commands for a promotion.
    pub replicate_from: Option<String>,
    /// On a replica: promote automatically once the primary has been out
    /// of reach for this long without a break.
    pub auto_failover: Option<Duration>,
    /// With `auto_failover` or `lease`: the other members of the cluster.
    pub quorum_peers: Vec<String>,
    /// With `quorum_peers`: as a primary, serve reads only under a lease a
    /// majority renews for this long at a time.
    pub lease: Option<Duration>,
    /// Adopt a schema that breaks data in the log (or whose stored text no
    /// longer compiles) without refusing.
    pub force_schema: bool,
    /// The secret behind the system token the application node appends
    /// `Fold.*` events with; generated per start when absent.
    pub system_secret: Option<String>,
    /// How long a command waits for a guarding projection to catch up.
    pub invariant_wait: Duration,
    /// How long a command waits for the derivation node to reach the
    /// version the application node last appended.
    pub state_wait: Duration,
}

impl Options {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        schema: impl Into<PathBuf>,
        listen: SocketAddr,
    ) -> Self {
        Options {
            data_dir: data_dir.into(),
            schema: schema.into(),
            listen,
            limits: Limits::default(),
            aggregate_cache: 10_000,
            fsync: true,
            backup: None,
            restore_note: None,
            replicate_from: None,
            auto_failover: None,
            quorum_peers: Vec::new(),
            lease: None,
            force_schema: false,
            system_secret: None,
            invariant_wait: Duration::from_secs(5),
            state_wait: Duration::from_secs(5),
        }
    }

    fn database(&self, secret: &str) -> fold_db::Options {
        let mut o = fold_db::Options::new(&self.data_dir, &self.schema, self.listen);
        o.fsync = self.fsync;
        o.backup = self.backup;
        o.restore_note = self.restore_note.clone();
        o.replicate_from = self.replicate_from.clone();
        o.auto_failover = self.auto_failover;
        o.quorum_peers = self.quorum_peers.clone();
        o.lease = self.lease;
        o.force_schema = self.force_schema;
        o.system_secret = Some(secret.to_string());
        o
    }

    fn derivation(&self, database: &str) -> fold_derive::Options {
        let mut o = fold_derive::Options::new(
            self.data_dir.join(DERIVE_DIR),
            &self.schema,
            database,
            self.listen,
        );
        o.limits = self.limits;
        o.aggregate_cache = self.aggregate_cache;
        o.fsync = self.fsync;
        o.force_schema = self.force_schema;
        o
    }

    fn application(&self, database: &str, derivation: &str, secret: &str) -> fold_app::Options {
        let mut o = fold_app::Options::new(
            self.data_dir.join(APP_DIR),
            &self.schema,
            database,
            derivation,
            self.listen,
        );
        o.limits = self.limits;
        o.invariant_wait = self.invariant_wait;
        o.state_wait = self.state_wait;
        o.system_secret = Some(secret.to_string());
        o.fsync = self.fsync;
        o.force_schema = self.force_schema;
        o
    }
}

/// A secret nobody else knows, for the two halves of this process.
fn generated_secret() -> String {
    format!("{}{}", uuid::Uuid::now_v7(), uuid::Uuid::now_v7())
}

/// Everything a start has brought up so far, so a failed start and a
/// shutdown stop it the same way.
#[derive(Default)]
struct Pieces {
    cancel: CancellationToken,
    servers: Vec<JoinHandle<Result<(), tonic::transport::Error>>>,
    tasks: Vec<JoinHandle<()>>,
    db: Option<Arc<fold_db::Shared>>,
    derive: Option<Arc<fold_derive::Shared>>,
    app: Option<Arc<fold_app::Shared>>,
}

impl Pieces {
    /// Stops serving, lets every node's tasks finish their current batch
    /// (bounded), and flushes the stores and the log.
    async fn stop(self) -> anyhow::Result<()> {
        self.cancel.cancel();
        for server in self.servers {
            tokio::time::timeout(Duration::from_secs(10), server)
                .await
                .context("gRPC server did not stop within 10 s")?
                .context("gRPC server task panicked")?
                .context("gRPC server failed")?;
        }
        let joined = tokio::time::timeout(
            Duration::from_secs(10),
            futures::future::join_all(self.tasks),
        )
        .await;
        if joined.is_err() {
            tracing::warn!("the nodes' tasks did not stop within 10 s");
        }
        if let Some(app) = &self.app {
            app.store.flush()?;
        }
        if let Some(derive) = &self.derive {
            derive.store.flush()?;
        }
        if let Some(db) = &self.db {
            db.log.flush()?;
        }
        Ok(())
    }

    /// Serves `router` on a loopback port of its own and returns its URL.
    async fn serve_private(&mut self, router: Router) -> anyhow::Result<String> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .context("cannot bind a loopback listener")?;
        let addr = listener.local_addr()?;
        let cancel = self.cancel.clone();
        self.servers.push(tokio::spawn(async move {
            router
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    cancel.cancelled_owned(),
                )
                .await
        }));
        Ok(format!("http://{addr}"))
    }
}

/// A started composite.
pub struct Running {
    pub local_addr: SocketAddr,
    pieces: Pieces,
}

impl Running {
    /// Resolves when an online restore is requested of the database. Only
    /// a supervisor that will act on it should await this.
    pub async fn restore_requested(&self) -> RestoreRequest {
        let mut rx = self.db().restore_rx.clone();
        loop {
            if let Some(p) = rx.borrow_and_update().clone() {
                return p;
            }
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    /// Stops serving, waits for the nodes' tasks (bounded), and flushes
    /// the stores and the log.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.pieces.stop().await?;
        tracing::info!("foldd stopped");
        Ok(())
    }

    /// The database node's state, for tests that look inside.
    pub fn db(&self) -> &Arc<fold_db::Shared> {
        self.pieces
            .db
            .as_ref()
            .expect("a running composite has its database")
    }

    /// The derivation node's state.
    pub fn derive(&self) -> &Arc<fold_derive::Shared> {
        self.pieces
            .derive
            .as_ref()
            .expect("a running composite has its derivation node")
    }

    /// The application node's state.
    pub fn app(&self) -> &Arc<fold_app::Shared> {
        self.pieces
            .app
            .as_ref()
            .expect("a running composite has its application node")
    }
}

/// Runs a composite, restarting it in place when an online restore is
/// requested, until `shutdown` resolves. The listen address is pinned
/// after the first start so an ephemeral port survives restarts.
pub struct Supervisor {
    opts: Options,
    running: Option<Running>,
    pub local_addr: SocketAddr,
}

impl Supervisor {
    pub async fn start(mut opts: Options) -> anyhow::Result<Self> {
        let running = start(opts.clone()).await?;
        let local_addr = running.local_addr;
        opts.listen = local_addr;
        Ok(Supervisor {
            opts,
            running: Some(running),
            local_addr,
        })
    }

    pub async fn run(
        mut self,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> anyhow::Result<()> {
        tokio::pin!(shutdown);
        loop {
            let running = self.running.take().expect("a composite is running");
            let req = tokio::select! {
                _ = &mut shutdown => {
                    return running.shutdown().await;
                }
                req = running.restore_requested() => req,
            };
            let archive = req.archive;
            tracing::info!(archive = %archive.display(), to = ?req.to, "online restore: stopping to swap the log");
            running.shutdown().await?;
            let note = match swap_log(&self.opts.data_dir, &archive, req.to) {
                Ok(head) => {
                    tracing::info!(archive = %archive.display(), to = ?req.to, head, "online restore: log replaced");
                    match req.to {
                        Some(_) => format!("ok {} to {head}", archive.display()),
                        None => format!("ok {}", archive.display()),
                    }
                }
                Err(e) => {
                    tracing::error!(archive = %archive.display(), error = %e, "online restore failed; serving the previous log");
                    format!("failed {}: {e:#}", archive.display())
                }
            };
            self.opts.restore_note = Some(note);
            self.running = Some(start(self.opts.clone()).await?);
        }
    }
}

/// How long a start waits for the application node's layer check, which
/// compares the three nodes' bundles over loopback.
const LAYER_CHECK_WAIT: Duration = Duration::from_secs(30);

/// Starts a composite and returns once it is serving on `local_addr` and
/// the application node takes commands.
pub async fn start(opts: Options) -> anyhow::Result<Running> {
    let mut pieces = Pieces::default();
    match bring_up(&opts, &mut pieces).await {
        Ok(local_addr) => Ok(Running { local_addr, pieces }),
        Err(e) => {
            if let Err(stop) = pieces.stop().await {
                tracing::warn!(error = %stop, "stopping a half-started composite");
            }
            Err(e)
        }
    }
}

async fn bring_up(opts: &Options, pieces: &mut Pieces) -> anyhow::Result<SocketAddr> {
    let started = Instant::now();
    let listener = TcpListener::bind(opts.listen)
        .await
        .with_context(|| format!("cannot listen on {}", opts.listen))?;
    let local_addr = listener.local_addr()?;
    let secret = opts.system_secret.clone().unwrap_or_else(generated_secret);

    // The database, then the nodes above it in order: each needs the one
    // below serving while it opens.
    let db_opts = opts.database(&secret);
    let (db, tasks) = fold_db::open(&db_opts, pieces.cancel.clone()).await?;
    pieces.tasks.extend(tasks);
    pieces.db = Some(db.clone());
    let db_url = pieces
        .serve_private(fold_db::add_services(
            Server::builder().add_routes(tonic::service::Routes::default()),
            db.clone(),
            started,
        ))
        .await?;

    let derive_opts = opts.derivation(&db_url);
    let (derive, tasks) = fold_derive::open(&derive_opts, pieces.cancel.clone()).await?;
    pieces.tasks.extend(tasks);
    pieces.derive = Some(derive.clone());
    let derive_url = pieces
        .serve_private(fold_derive::add_services(
            Server::builder().add_routes(tonic::service::Routes::default()),
            derive.clone(),
            started,
        ))
        .await?;

    let app_opts = opts.application(&db_url, &derive_url, &secret);
    let (app, tasks) = fold_app::open(&app_opts, pieces.cancel.clone()).await?;
    pieces.tasks.extend(tasks);
    pieces.app = Some(app.clone());

    // Everything on the public address.
    let router = Server::builder().add_routes(tonic::service::Routes::default());
    let router = fold_db::add_services(router, db.clone(), started);
    let router = fold_derive::add_services(router, derive.clone(), started);
    let router = fold_app::add_services(router, app.clone(), started);
    let cancel = pieces.cancel.clone();
    pieces.servers.push(tokio::spawn(async move {
        router
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                cancel.cancelled_owned(),
            )
            .await
    }));

    // The application node compares its bundle with the other two's
    // before it takes commands; inside one process that is a formality,
    // but the check is the deployment's and runs the same way.
    let mut layer = app.layer.subscribe();
    let deadline = tokio::time::sleep(LAYER_CHECK_WAIT);
    tokio::pin!(deadline);
    loop {
        match &*layer.borrow_and_update() {
            fold_app::state::LayerCheck::Ok => break,
            fold_app::state::LayerCheck::Mismatch(why) => {
                anyhow::bail!("the application layer does not match the nodes below it: {why}")
            }
            fold_app::state::LayerCheck::Pending(_) => {}
        }
        tokio::select! {
            changed = layer.changed() => {
                if changed.is_err() {
                    anyhow::bail!("the application node stopped before its layer check passed");
                }
            }
            _ = &mut deadline => {
                let why = layer.borrow().as_health();
                anyhow::bail!("the application node's layer check did not pass within {LAYER_CHECK_WAIT:?}: {why}");
            }
        }
    }

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        listen = %local_addr,
        data_dir = %opts.data_dir.display(),
        schema = %opts.schema.display(),
        projections = derive.statuses.len(),
        processes = app.processes.len(),
        head = db.log.head().0,
        role = db.role().as_str(),
        "foldd listening"
    );
    Ok(local_addr)
}
