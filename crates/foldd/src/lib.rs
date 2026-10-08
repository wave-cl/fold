//! `foldd`: the fold daemon as a library, so the binary and the end-to-end
//! tests start it the same way.
//!
//! [`start`] loads the schema, opens the log, compiles every WASM module the
//! schema names, spawns one projection runner per projection, binds the
//! listener and serves the four gRPC services. [`Running::shutdown`] stops
//! accepting, lets runners finish their current batch, and flushes the log.
//!
//! The write side ([`command`]) and the read side ([`query`]) are separate
//! modules joined only by the log and the projection runners.

mod admin;
pub mod aggregate;
pub mod codec;
pub mod command;
pub mod keys;
mod log_svc;
pub mod process;
pub mod projection;
pub mod query;
pub mod replica;
pub mod scheduled;
pub mod shutdown;
pub mod snapshot;
mod state;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context as _;
use fold_proto::v1::admin_server::AdminServer;
use fold_proto::v1::command_server::CommandServer;
use fold_proto::v1::log_server::LogServer;
use fold_proto::v1::query_server::QueryServer;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

pub use fold_wasm::Limits;
pub use scheduled::BackupSchedule;
pub use state::{RestoreRequest, Shared};

/// The name of the one log a daemon serves in this version.
pub const LOG_NAME: &str = "default";

#[derive(Debug, Clone)]
pub struct Options {
    pub data_dir: PathBuf,
    pub schema: PathBuf,
    pub listen: SocketAddr,
    pub limits: Limits,
    /// How many aggregate instances to keep evolved in memory.
    pub aggregate_cache: usize,
    /// Durability of the log: `false` is for tests and bulk loads only.
    pub fsync: bool,
    /// Take a backup into the log's backups directory on this schedule.
    pub backup: Option<BackupSchedule>,
    /// Outcome of the last online restore, reported by Health. Set by the
    /// supervisor; not something to configure.
    pub restore_note: Option<String>,
    /// Run as a read-only replica tailing this primary (a gRPC URL such as
    /// `http://10.0.0.1:4141`). Commands are refused; projections and
    /// process managers run on the replicated events.
    pub replicate_from: Option<String>,
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
        }
    }
}

/// A started daemon.
pub struct Running {
    pub local_addr: SocketAddr,
    shared: Arc<Shared>,
    cancel: CancellationToken,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
    runners: Vec<JoinHandle<()>>,
}

impl Running {
    /// Resolves when an online restore is requested.
    /// Only a supervisor that will act on it should await this.
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
}

/// Runs a daemon, restarting it in place when an online restore is
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
            let running = self.running.take().expect("a daemon is running");
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
fn swap_log(
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

impl Running {
    /// Stops serving, waits for projection runners to finish their current
    /// batch (bounded), and flushes the log.
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
            tracing::warn!("projection runners did not stop within 10 s");
        }
        self.shared.log.flush()?;
        tracing::info!("foldd stopped");
        Ok(())
    }

    /// The daemon's shared state, for tests that look inside.
    pub fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }
}

/// Starts a daemon and returns once it is serving on `local_addr`.
pub async fn start(opts: Options) -> anyhow::Result<Running> {
    let started = Instant::now();
    let cancel = CancellationToken::new();

    if let Some(primary) = &opts.replicate_from {
        replica::prepare(&opts, primary).await?;
    }
    let shared = Arc::new(Shared::open(&opts, cancel.clone())?);
    let mut runners = projection::spawn_all(shared.clone());
    runners.extend(process::spawn_all(shared.clone()));
    if let Some(primary) = &opts.replicate_from {
        runners.push(replica::spawn(shared.clone(), primary.clone()));
    }
    if let Some(schedule) = opts.backup {
        runners.push(scheduled::spawn(shared.clone(), schedule));
    }

    let listener = TcpListener::bind(opts.listen)
        .await
        .with_context(|| format!("cannot listen on {}", opts.listen))?;
    let local_addr = listener.local_addr()?;

    let server = {
        let shared = shared.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(CommandServer::new(command::Service::new(shared.clone())))
                .add_service(QueryServer::new(query::Service::new(shared.clone())))
                .add_service(LogServer::new(log_svc::Service::new(shared.clone())))
                .add_service(AdminServer::new(admin::Service::new(
                    shared.clone(),
                    started,
                )))
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
        projections = shared.statuses.len(),
        head = shared.log.head().0,
        role = %shared.role(),
        "foldd listening"
    );

    Ok(Running {
        local_addr,
        shared,
        cancel,
        server,
        runners,
    })
}
