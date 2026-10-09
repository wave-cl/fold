//! `fold-app`: the application service as a library, so the `fold-appd`
//! binary, the composite `foldd` and the end-to-end tests start it the same
//! way.
//!
//! An application node runs commands (handlers, guards, state and context
//! invariants) against aggregate state the derivation service provides,
//! appends the results to the database with expected versions, and runs
//! process managers with their timers. It serves `Command` to clients and
//! `AppAdmin` to operators. It holds no read models of its own beyond the
//! process managers' tables.
//!
//! One application node is assumed for cross-stream invariants: the
//! per-scope locks that serialize them are in-process. Health says so.

pub mod admin;
pub mod command;
pub mod event;
pub mod peers;
pub mod process;
pub mod schema_check;
pub mod shutdown;
pub mod state;
pub mod tail;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context as _;
use fold_proto::application::v1::app_admin_server::AppAdminServer;
use fold_proto::application::v1::command_server::CommandServer;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

pub use fold_host::codec;
pub use fold_host::keys;
pub use fold_host::snapshot;
pub use fold_wasm::Limits;
pub use state::Shared;

#[derive(Debug, Clone)]
pub struct Options {
    /// Holds `derived.redb` (process state, outbox, timers, checkpoints)
    /// and `snapshots/`.
    pub data_dir: PathBuf,
    /// The application schema (`layer application`).
    pub schema: PathBuf,
    /// Where wasm modules resolve; the schema's directory by default.
    pub wasm_dir: Option<PathBuf>,
    /// The database (a gRPC URL).
    pub database: String,
    /// The derivation node (a gRPC URL).
    pub derivation: String,
    pub listen: SocketAddr,
    pub limits: Limits,
    /// How long a command waits for a guarding projection to catch up.
    pub invariant_wait: std::time::Duration,
    /// How long a command waits for the derivation node to reach the
    /// version this node last appended.
    pub state_wait: std::time::Duration,
    /// The secret whose token lets this node append `Fold.*` events
    /// (process timers) to the database.
    pub system_secret: Option<String>,
    /// Durability of the store: `false` is for tests.
    pub fsync: bool,
    /// Adopt a schema whose stored text no longer compiles.
    pub force_schema: bool,
}

impl Options {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        schema: impl Into<PathBuf>,
        database: impl Into<String>,
        derivation: impl Into<String>,
        listen: SocketAddr,
    ) -> Self {
        Options {
            data_dir: data_dir.into(),
            schema: schema.into(),
            wasm_dir: None,
            database: database.into(),
            derivation: derivation.into(),
            listen,
            limits: Limits::default(),
            invariant_wait: std::time::Duration::from_secs(5),
            state_wait: std::time::Duration::from_secs(5),
            system_secret: None,
            fsync: true,
            force_schema: false,
        }
    }
}

/// A started application node.
pub struct Running {
    pub local_addr: SocketAddr,
    shared: Arc<Shared>,
    cancel: CancellationToken,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
    runners: Vec<JoinHandle<()>>,
}

impl Running {
    /// Stops serving, waits for the process runners to finish their
    /// current batch (bounded), and flushes the store.
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
            tracing::warn!("process runners did not stop within 10 s");
        }
        self.shared.store.flush()?;
        tracing::info!("fold-app stopped");
        Ok(())
    }

    pub fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }
}

/// The node's two services added to `router`, which more services
/// can join.
pub fn add_services(
    router: tonic::transport::server::Router,
    shared: Arc<Shared>,
    started: Instant,
) -> tonic::transport::server::Router {
    router
        .add_service(CommandServer::new(command::Service::new(shared.clone())))
        .add_service(AppAdminServer::new(admin::Service::new(shared, started)))
}

/// Opens the node's state and starts its background tasks; serving is the
/// caller's.
pub async fn open(
    opts: &Options,
    cancel: CancellationToken,
) -> anyhow::Result<(Arc<Shared>, Vec<JoinHandle<()>>)> {
    let shared = Arc::new(Shared::open(opts, cancel).await?);
    let mut runners = vec![
        tail::spawn(shared.clone()),
        state::spawn_role_watch(shared.clone()),
        state::spawn_layer_check(shared.clone()),
    ];
    runners.extend(process::spawn_all(shared.clone()));
    Ok((shared, runners))
}

/// Starts an application node and returns once it is serving.
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
        database = %opts.database,
        derivation = %opts.derivation,
        processes = shared.processes.len(),
        "fold-app listening"
    );

    Ok(Running {
        local_addr,
        shared,
        cancel,
        server,
        runners,
    })
}
