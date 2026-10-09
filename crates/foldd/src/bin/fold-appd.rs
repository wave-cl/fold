//! The `fold-appd` binary: the application service on its own.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "fold-appd",
    version,
    about = "The fold application service: commands, invariants, process managers"
)]
struct Cli {
    /// Directory holding the process managers' store and snapshots.
    /// Created if missing.
    #[arg(long, env = "FOLD_DATA_DIR")]
    data_dir: PathBuf,
    /// The application schema (.fold).
    #[arg(long, env = "FOLD_SCHEMA")]
    schema: PathBuf,
    /// Where WASM modules resolve; the schema's directory by default.
    #[arg(long)]
    wasm_dir: Option<PathBuf>,
    /// The database (a gRPC URL).
    #[arg(long, env = "FOLD_DATABASE", default_value = "http://127.0.0.1:4141")]
    database: String,
    /// The derivation node (a gRPC URL).
    #[arg(long, env = "FOLD_DERIVATION", default_value = "http://127.0.0.1:4142")]
    derivation: String,
    /// Address to serve gRPC on.
    #[arg(long, env = "FOLD_LISTEN", default_value = "127.0.0.1:4143")]
    listen: SocketAddr,
    /// How long a command waits for a guarding projection to catch up.
    #[arg(long, value_parser = fold_db::scheduled::parse_duration, default_value = "5s")]
    invariant_wait: std::time::Duration,
    /// How long a command waits for the derivation node to reach the
    /// version this node last appended.
    #[arg(long, value_parser = fold_db::scheduled::parse_duration, default_value = "5s")]
    state_wait: std::time::Duration,
    /// The secret whose token lets this node append `Fold.*` events
    /// (process timers) to the database; the database's `--system-secret`.
    #[arg(long, env = "FOLD_SYSTEM_SECRET", hide_env_values = true)]
    system_secret: Option<String>,
    /// Skip fsync on the store. Only for tests.
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
    let mut opts = fold_app::Options::new(
        cli.data_dir,
        cli.schema,
        cli.database,
        cli.derivation,
        cli.listen,
    );
    opts.wasm_dir = cli.wasm_dir;
    opts.invariant_wait = cli.invariant_wait;
    opts.state_wait = cli.state_wait;
    opts.system_secret = cli.system_secret;
    opts.fsync = !cli.no_fsync;
    opts.force_schema = cli.force_schema;
    let running = fold_app::start(opts).await?;
    fold_app::shutdown::signal().await;
    tracing::info!("signal received, stopping");
    running.shutdown().await
}
