//! The `fold-derived` binary: the derivation service on its own.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "fold-derived",
    version,
    about = "The fold derivation service: aggregate state, projections, queries"
)]
struct Cli {
    /// Directory holding the derived store and snapshots. Created if missing.
    #[arg(long, env = "FOLD_DATA_DIR")]
    data_dir: PathBuf,
    /// The derivation schema (.fold), or an application root whose
    /// derivation layer is used.
    #[arg(long, env = "FOLD_SCHEMA")]
    schema: PathBuf,
    /// Where WASM modules resolve; the schema's directory by default.
    #[arg(long)]
    wasm_dir: Option<PathBuf>,
    /// The database to tail (a gRPC URL).
    #[arg(long, env = "FOLD_DATABASE", default_value = "http://127.0.0.1:4141")]
    database: String,
    /// Address to serve gRPC on.
    #[arg(long, env = "FOLD_LISTEN", default_value = "127.0.0.1:4142")]
    listen: SocketAddr,
    /// How many aggregate instances to keep evolved in memory.
    #[arg(long, default_value_t = 10_000)]
    aggregate_cache: usize,
    /// Skip fsync on the derived store. Only for tests.
    #[arg(long)]
    no_fsync: bool,
    /// Adopt a schema whose stored text no longer compiles.
    #[arg(long)]
    force_schema: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("FOLDD_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let mut opts = fold_derive::Options::new(cli.data_dir, cli.schema, cli.database, cli.listen);
    opts.wasm_dir = cli.wasm_dir;
    opts.aggregate_cache = cli.aggregate_cache;
    opts.fsync = !cli.no_fsync;
    opts.force_schema = cli.force_schema;
    let running = fold_derive::start(opts).await?;
    fold_derive::shutdown::signal().await;
    tracing::info!("signal received, stopping");
    running.shutdown().await
}
