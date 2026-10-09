//! `fold-db`: the database service as a library, so the `fold-dbd` binary,
//! the composite `foldd` and the end-to-end tests start it the same way.
//!
//! The database owns the log and the cluster: it stores events validated
//! against the domain schema, serves reads and subscriptions, replicates
//! to replicas, runs failover, fencing and leases, and takes backups. It
//! hosts no wasm and derives nothing: aggregate state and projections are
//! the derivation service's, commands and process managers the
//! application service's; both talk to it over `fold.database.v1`.
//!
//! [`start`] loads the domain schema, opens the log, binds the listener
//! and serves `Log`, `Cluster`, `Backup` and `Schema`. [`Running::shutdown`]
//! stops accepting and flushes the log.

pub mod append;
mod backup_svc;
mod cluster;
pub mod codec;
pub mod domain;
pub mod lease;
mod log_svc;
pub mod replica;
pub mod scheduled;
mod schema_svc;
pub mod shutdown;
mod state;
pub mod system;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context as _;
use fold_proto::database::v1::backup_server::BackupServer;
use fold_proto::database::v1::cluster_server::ClusterServer;
use fold_proto::database::v1::log_server::LogServer;
use fold_proto::database::v1::schema_server::SchemaServer;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

pub use scheduled::BackupSchedule;
pub use state::{RestoreRequest, Role, Shared};

/// The name of the one log a database serves in this version.
pub const LOG_NAME: &str = "default";

#[derive(Debug, Clone)]
pub struct Options {
    pub data_dir: PathBuf,
    /// The domain schema (`layer domain`), or a higher layer's root whose
    /// domain is used.
    pub schema: PathBuf,
    pub listen: SocketAddr,
    /// Durability of the log: `false` is for tests and bulk loads only.
    pub fsync: bool,
    /// Take a backup into the log's backups directory on this schedule.
    pub backup: Option<BackupSchedule>,
    /// Outcome of the last online restore, reported by Health. Set by the
    /// supervisor; not something to configure.
    pub restore_note: Option<String>,
    /// Run as a read-only replica tailing this primary (a gRPC URL such as
    /// `http://10.0.0.1:4141`). Writes are refused.
    pub replicate_from: Option<String>,
    /// On a replica: promote automatically once the primary has been out of
    /// reach for this long without a break. Off by default, because a
    /// replica cut off from a primary that is still serving others would
    /// fork the log.
    pub auto_failover: Option<std::time::Duration>,
    /// With `auto_failover`: the other members of the cluster (the primary
    /// and the other replicas, as gRPC URLs). The replica promotes itself
    /// only with a majority of the cluster (peers plus itself) agreeing
    /// that the primary is gone. Empty means a quorum of one.
    pub quorum_peers: Vec<String>,
    /// With `quorum_peers`: as a primary, hold a lease a majority of the
    /// cluster renews for this long at a time; derivation nodes serve
    /// reads only while it is held, so a primary that lost the others
    /// cannot answer stale reads for longer than this.
    pub lease: Option<std::time::Duration>,
    /// Adopt a schema that breaks data in the log (or whose stored text no
    /// longer compiles) without refusing.
    pub force_schema: bool,
    /// The secret whose token lets the application service append `Fold.*`
    /// events (process timers). Without one, every such append is refused.
    pub system_secret: Option<String>,
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
            fsync: true,
            backup: None,
            restore_note: None,
            replicate_from: None,
            auto_failover: None,
            quorum_peers: Vec::new(),
            lease: None,
            force_schema: false,
            system_secret: None,
        }
    }
}

/// A started database.
pub struct Running {
    pub local_addr: SocketAddr,
    shared: Arc<Shared>,
    cancel: CancellationToken,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
    runners: Vec<JoinHandle<()>>,
}

impl Running {
    /// Resolves when an online restore is requested. Only a supervisor
    /// that will act on it should await this.
    pub async fn restore_requested(&self) -> RestoreRequest {
        let mut rx = self.shared.restore_rx.clone();
        loop {
            if let Some(p) = rx.borrow_and_update().clone() {
                return p;
            }
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    /// Stops serving and flushes the log.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.cancel.cancel();
        let server = self.server;
        tokio::time::timeout(std::time::Duration::from_secs(10), server)
            .await
            .context("gRPC server did not stop within 10 s")?
            .context("gRPC server task panicked")?
            .context("gRPC server failed")?;
        let joined = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            futures::future::join_all(self.runners),
        )
        .await;
        if joined.is_err() {
            tracing::warn!("database tasks did not stop within 10 s");
        }
        self.shared.log.flush()?;
        tracing::info!("fold-db stopped");
        Ok(())
    }

    /// The database's shared state, for tests that look inside.
    pub fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }
}

/// Runs a database, restarting it in place when an online restore is
/// requested, until `shutdown` resolves. The listen address is pinned after
/// the first start so an ephemeral port survives restarts.
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
            let running = self.running.take().expect("a database is running");
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

/// Moves `<data_dir>/<LOG_NAME>` aside and restores `archive` in its place.
/// On failure the previous log is moved back.
pub fn swap_log(
    data_dir: &std::path::Path,
    archive: &std::path::Path,
    to: Option<fold_core::PointInTime>,
) -> anyhow::Result<u64> {
    let current = data_dir.join(LOG_NAME);
    let stamp = jiff::Timestamp::now()
        .strftime("%Y%m%dT%H%M%SZ")
        .to_string();
    let aside = data_dir.join(format!("{LOG_NAME}.replaced-{stamp}"));
    // An archive inside the log being replaced (the default backups
    // directory) moves aside with it; follow it there.
    let archive = match (archive.canonicalize(), current.canonicalize()) {
        (Ok(a), Ok(c)) => match a.strip_prefix(&c) {
            Ok(rel) => aside.join(rel),
            Err(_) => a,
        },
        _ => archive.to_path_buf(),
    };
    if current.exists() {
        std::fs::rename(&current, &aside)
            .with_context(|| format!("cannot move {} aside", current.display()))?;
    }
    match fold_core::restore_backup_to(&archive, data_dir, LOG_NAME, to) {
        Ok(meta) => Ok(meta.head),
        Err(e) => {
            if aside.exists() {
                let _ = std::fs::remove_dir_all(&current);
                std::fs::rename(&aside, &current)
                    .with_context(|| format!("cannot move {} back", aside.display()))?;
            }
            Err(anyhow::Error::new(e).context("restore failed; the previous log is back in place"))
        }
    }
}

/// Checks the options against each other, before anything is contacted.
pub fn validate_options(opts: &Options) -> anyhow::Result<()> {
    if opts.replicate_from.is_none() && opts.auto_failover.is_some() {
        anyhow::bail!("auto_failover needs replicate_from: only a replica can fail over");
    }
    if !opts.quorum_peers.is_empty() && opts.auto_failover.is_none() && opts.lease.is_none() {
        anyhow::bail!(
            "quorum_peers needs auto_failover or lease: the quorum decides automatic failover and leader leases"
        );
    }
    if opts.lease.is_some() && opts.quorum_peers.is_empty() {
        anyhow::bail!("lease needs quorum_peers: a lease is granted by a majority of them");
    }
    if opts.auto_failover.is_some() && opts.quorum_peers.is_empty() {
        tracing::warn!(
            "auto_failover without quorum_peers: this replica will promote itself on its own judgement"
        );
    }
    Ok(())
}

/// The database's four services added to `router` (the composite's
/// or this crate's own), which more services can join.
pub fn add_services(
    router: tonic::transport::server::Router,
    shared: Arc<Shared>,
    started: Instant,
) -> tonic::transport::server::Router {
    router
        .add_service(LogServer::new(log_svc::Service::new(shared.clone())))
        .add_service(ClusterServer::new(cluster::Service::new(
            shared.clone(),
            started,
        )))
        .add_service(BackupServer::new(backup_svc::Service::new(shared.clone())))
        .add_service(SchemaServer::new(schema_svc::Service::new(shared)))
}

/// Opens the database's state (log, schema check, replica preparation) and
/// starts its background tasks; serving is the caller's.
pub async fn open(
    opts: &Options,
    cancel: CancellationToken,
) -> anyhow::Result<(Arc<Shared>, Vec<JoinHandle<()>>)> {
    validate_options(opts)?;
    if let Some(primary) = &opts.replicate_from {
        replica::prepare(opts, primary).await?;
    }
    let shared = Arc::new(Shared::open(opts, cancel)?);
    let mut runners = Vec::new();
    if let Some(primary) = &opts.replicate_from {
        runners.push(replica::spawn(shared.clone(), primary.clone()));
    }
    if opts.lease.is_some() {
        runners.push(lease::spawn(shared.clone()));
    }
    if let Some(schedule) = opts.backup {
        runners.push(scheduled::spawn(shared.clone(), schedule));
    }
    Ok((shared, runners))
}

/// Starts a database and returns once it is serving on `local_addr`.
pub async fn start(opts: Options) -> anyhow::Result<Running> {
    let started = Instant::now();
    let cancel = CancellationToken::new();
    let (shared, runners) = open(&opts, cancel.clone()).await?;

    let listener = TcpListener::bind(opts.listen)
        .await
        .with_context(|| format!("cannot listen on {}", opts.listen))?;
    let local_addr = listener.local_addr()?;

    let server = {
        let shared = shared.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            add_services(
                tonic::transport::Server::builder().add_routes(tonic::service::Routes::default()),
                shared,
                started,
            )
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                cancel.cancelled_owned(),
            )
            .await
        })
    };

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        listen = %local_addr,
        data_dir = %opts.data_dir.display(),
        schema = %opts.schema.display(),
        head = shared.log.head().0,
        role = shared.role().as_str(),
        "fold-db listening"
    );

    Ok(Running {
        local_addr,
        shared,
        cancel,
        server,
        runners,
    })
}
