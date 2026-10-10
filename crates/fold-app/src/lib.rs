//! `fold-app`: the application layer as a Rust library.
//!
//! An application registers its commands, state invariants, projection-
//! driven invariants and process managers ([`App`]), then runs them
//! against a database and a derivation node: [`serve`] exposes `Command`
//! and `AppAdmin` over gRPC and runs the process managers; [`open`] does
//! the same without a listener, for a host that serves the services itself
//! (the composite); [`AppHandle`] executes commands in-process over the
//! same path the gRPC service uses.
//!
//! The node learns the domain from the database and the derivation layer
//! from the derivation node, checks its registrations against them, keeps
//! checking as they change, and refuses commands until they fit. It holds
//! no read models of its own beyond the process managers' tables.
//!
//! One application node is assumed for cross-stream invariants: the
//! per-scope locks that serialize them are in-process. Health says so.

pub mod admin;
pub mod app;
pub mod command;
pub mod event;
pub mod peers;
pub mod process;
pub mod registry_check;
pub mod shutdown;
pub mod state;
pub mod tail;
pub mod types;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context as _;
use fold_proto::application::v1::app_admin_server::AppAdminServer;
use fold_proto::application::v1::command_server::CommandServer;
use fold_proto::application::v1::{AppendResponse, ExecuteResponse};
use serde::Serialize;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::Status;

pub use app::{AggregateBuilder, App, ContextInvariant, Manifest, Process};
pub use fold_host::codec;
pub use fold_host::keys;
pub use fold_host::snapshot;
pub use serde_json::{Value as Json, json};
pub use state::Shared;
pub use types::{
    CmdCtx, Emit, Event, Fail, InvCtx, IssuedCommand, PendingEvent, ProcCtx, Reaction, Rejected,
    Rows, SetTimer, Trigger,
};

#[derive(Debug, Clone)]
pub struct Options {
    /// Holds `derived.redb` (process state, outbox, timers, checkpoints)
    /// and `snapshots/`.
    pub data_dir: PathBuf,
    /// The database (a gRPC URL).
    pub database: String,
    /// The derivation node (a gRPC URL).
    pub derivation: String,
    pub listen: SocketAddr,
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
}

impl Options {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        database: impl Into<String>,
        derivation: impl Into<String>,
        listen: SocketAddr,
    ) -> Self {
        Options {
            data_dir: data_dir.into(),
            database: database.into(),
            derivation: derivation.into(),
            listen,
            invariant_wait: std::time::Duration::from_secs(5),
            state_wait: std::time::Duration::from_secs(5),
            system_secret: None,
            fsync: true,
        }
    }
}

/// The application in-process: commands and appends over the same path
/// the gRPC `Command` service uses, including the layer check, the role
/// gate, the invariants and the retry on a stale state.
#[derive(Clone)]
pub struct AppHandle {
    shared: Arc<Shared>,
}

impl AppHandle {
    pub fn new(shared: Arc<Shared>) -> Self {
        AppHandle { shared }
    }

    /// Executes `command` (`Context.Aggregate.Command`) on `stream` with
    /// `payload` as the command's JSON and `metadata` on every emitted
    /// event (`None` for none).
    pub async fn execute(
        &self,
        command: &str,
        stream: &str,
        payload: &impl Serialize,
        metadata: Option<Value>,
    ) -> Result<ExecuteResponse, Status> {
        let params = command::ExecuteParams {
            command: command.to_string(),
            stream_id: stream.to_string(),
            payload: serde_json::to_vec(payload)
                .map_err(|e| Status::invalid_argument(format!("payload: {e}")))?,
            metadata: match metadata {
                Some(m) => serde_json::to_vec(&m).expect("json"),
                None => Vec::new(),
            },
            idempotency_key: None,
            fencing_token: None,
        };
        match command::execute(&self.shared, params).await? {
            command::ExecuteOutcome::Done(r) => Ok(r),
            command::ExecuteOutcome::AlreadyExecuted { .. } => {
                unreachable!("no idempotency key was passed")
            }
        }
    }

    /// Appends `events` to `stream` under the aggregate's invariants, as
    /// `Command.Append` does.
    pub async fn append(
        &self,
        req: fold_proto::application::v1::AppendRequest,
    ) -> Result<AppendResponse, Status> {
        command::append(&self.shared, req).await
    }

    pub fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }
}

/// A served application node.
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

    /// The application in-process.
    pub fn handle(&self) -> AppHandle {
        AppHandle::new(self.shared.clone())
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
    app: App,
    opts: &Options,
    cancel: CancellationToken,
) -> anyhow::Result<(Arc<Shared>, Vec<JoinHandle<()>>)> {
    let shared = Arc::new(Shared::open(app, opts, cancel).await?);
    let mut runners = vec![
        tail::spawn(shared.clone()),
        state::spawn_role_watch(shared.clone()),
        state::spawn_layer_check(shared.clone()),
    ];
    runners.extend(process::spawn_all(shared.clone()));
    Ok((shared, runners))
}

/// Serves an application and returns once it is listening.
pub async fn serve(app: App, opts: Options) -> anyhow::Result<Running> {
    let started = Instant::now();
    let cancel = CancellationToken::new();
    let (shared, runners) = open(app, &opts, cancel.clone()).await?;

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
        database = %opts.database,
        derivation = %opts.derivation,
        aggregates = shared.app.aggregates.len(),
        processes = shared.app.processes.len(),
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
