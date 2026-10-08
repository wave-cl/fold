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
pub use state::Shared;

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

    let shared = Arc::new(Shared::open(&opts, cancel.clone())?);
    let mut runners = projection::spawn_all(shared.clone());
    runners.extend(process::spawn_all(shared.clone()));
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
