//! `fold-derive`: the derivation service as a library, so the `fold-derived`
//! binary, the composite `foldd` and the end-to-end tests start it the same
//! way.
//!
//! A derivation node tails one database's log (`fold.database.v1`) and
//! folds it: aggregate state (evolve, replay, instance snapshots, a cache)
//! and projections (read models in its own derived store). It serves
//! `Query` and `Aggregate` to clients, `DeriveAdmin` to operators and the
//! internal `Derive` API to the application service. It appends nothing.
//!
//! Derived data that outlived its log (a truncation, a restore, a failover
//! to a shorter primary) is detected by the log's generation and by the
//! event-id fingerprints on checkpoints and snapshots, and reset; Health
//! says so.

pub mod admin;
pub mod aggregate;
pub mod db;
pub mod derive_svc;
pub mod projection;
pub mod query;
pub mod schema_check;
pub mod shutdown;
pub mod state;
pub mod tail;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context as _;
use fold_proto::derivation::v1::aggregate_server::AggregateServer;
use fold_proto::derivation::v1::derive_admin_server::DeriveAdminServer;
use fold_proto::derivation::v1::derive_server::DeriveServer;
use fold_proto::derivation::v1::query_server::QueryServer;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

pub use fold_host::codec;
pub use fold_host::keys;
pub use fold_host::snapshot;
pub use fold_host::upcast;
pub use fold_wasm::Limits;
pub use state::Shared;

#[derive(Debug, Clone)]
pub struct Options {
    /// Holds `derived.redb` and `snapshots/`.
    pub data_dir: PathBuf,
    /// The derivation schema (`layer derivation`, importing the domain).
    pub schema: PathBuf,
    /// Where wasm modules resolve; the schema's directory by default.
    pub wasm_dir: Option<PathBuf>,
    /// The database to tail (a gRPC URL such as `http://10.0.0.1:4141`).
    pub database: String,
    pub listen: SocketAddr,
    pub limits: Limits,
    /// How many aggregate instances to keep evolved in memory.
    pub aggregate_cache: usize,
    /// Durability of the derived store: `false` is for tests.
    pub fsync: bool,
    /// Adopt a schema whose stored text no longer compiles.
    pub force_schema: bool,
}

impl Options {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        schema: impl Into<PathBuf>,
        database: impl Into<String>,
        listen: SocketAddr,
    ) -> Self {
        Options {
            data_dir: data_dir.into(),
            schema: schema.into(),
            wasm_dir: None,
            database: database.into(),
            listen,
            limits: Limits::default(),
            aggregate_cache: 10_000,
            fsync: true,
            force_schema: false,
        }
    }
}

/// A started derivation node.
pub struct Running {
    pub local_addr: SocketAddr,
    shared: Arc<Shared>,
    cancel: CancellationToken,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
    runners: Vec<JoinHandle<()>>,
}

impl Running {
    /// Stops serving, waits for the runners to finish their current batch
    /// (bounded), and flushes the store.
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
            tracing::warn!("runners did not stop within 10 s");
        }
        self.shared.store.flush()?;
        tracing::info!("fold-derive stopped");
        Ok(())
    }

    /// The node's shared state, for tests that look inside.
    pub fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }
}

/// The node's four services added to `router`, which more services
/// can join.
pub fn add_services(
    router: tonic::transport::server::Router,
    shared: Arc<Shared>,
    started: Instant,
) -> tonic::transport::server::Router {
    router
        .add_service(QueryServer::new(query::Service::new(shared.clone())))
        .add_service(AggregateServer::new(aggregate::Service::new(
            shared.clone(),
        )))
        .add_service(DeriveAdminServer::new(admin::Service::new(
            shared.clone(),
            started,
        )))
        .add_service(DeriveServer::new(derive_svc::Service::new(shared)))
}

/// Opens the node's state (schema, store, the database's identity) and
/// starts its background tasks; serving is the caller's.
pub async fn open(
    opts: &Options,
    cancel: CancellationToken,
) -> anyhow::Result<(Arc<Shared>, Vec<JoinHandle<()>>)> {
    let shared = Arc::new(Shared::open(opts, cancel).await?);
    let mut runners = vec![
        tail::spawn(shared.clone()),
        state::spawn_role_watch(shared.clone()),
    ];
    runners.extend(projection::spawn_all(shared.clone()));
    Ok((shared, runners))
}

/// Starts a derivation node and returns once it is serving on `local_addr`.
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
        projections = shared.statuses.len(),
        "fold-derive listening"
    );

    Ok(Running {
        local_addr,
        shared,
        cancel,
        server,
        runners,
    })
}
