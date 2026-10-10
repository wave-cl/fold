//! The orders application as a process: standalone against a database and
//! a derivation node, or embedded with both in one process (`--embed`).

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "orders-app",
    version,
    about = "The fold orders example: commands, invariants and the fulfilment process"
)]
struct Cli {
    /// Host the database and the derivation node in this process too.
    #[arg(long)]
    embed: bool,
    /// Directory holding this application's store (and, with --embed, the
    /// log and the derivation node's store). Created if missing.
    #[arg(long, env = "FOLD_DATA_DIR")]
    data_dir: PathBuf,
    /// With --embed: the derivation schema (`derive.fold`), whose imports
    /// give the domain. WASM modules resolve relative to it.
    #[arg(long, env = "FOLD_SCHEMA")]
    schema: Option<PathBuf>,
    /// Without --embed: the database (a gRPC URL).
    #[arg(long, env = "FOLD_DATABASE", default_value = "http://127.0.0.1:4141")]
    database: String,
    /// Without --embed: the derivation node (a gRPC URL).
    #[arg(long, env = "FOLD_DERIVATION", default_value = "http://127.0.0.1:4142")]
    derivation: String,
    /// Address to serve gRPC on.
    #[arg(long, env = "FOLD_LISTEN")]
    listen: Option<SocketAddr>,
    /// The database's system secret, which lets this application fire
    /// process timers. With --embed one is generated.
    #[arg(long, env = "FOLD_SYSTEM_SECRET", hide_env_values = true)]
    system_secret: Option<String>,
    /// Skip fsync. Only for tests.
    #[arg(long)]
    no_fsync: bool,
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
    if cli.embed {
        let schema = cli
            .schema
            .ok_or_else(|| anyhow::anyhow!("--embed needs --schema (the derivation file)"))?;
        let mut opts = foldd::Options::new(
            cli.data_dir,
            schema,
            cli.listen
                .unwrap_or_else(|| "127.0.0.1:4141".parse().expect("static address")),
        );
        opts.fsync = !cli.no_fsync;
        opts.system_secret = cli.system_secret;
        let supervisor = foldd::Supervisor::start_with_app(opts, orders_app::app()).await?;
        supervisor
            .run(async {
                foldd::shutdown::signal().await;
                tracing::info!("signal received, stopping");
            })
            .await
    } else {
        let mut opts = fold_app::Options::new(
            cli.data_dir,
            cli.database,
            cli.derivation,
            cli.listen
                .unwrap_or_else(|| "127.0.0.1:4143".parse().expect("static address")),
        );
        opts.system_secret = cli.system_secret;
        opts.fsync = !cli.no_fsync;
        let running = fold_app::serve(orders_app::app(), opts).await?;
        fold_app::shutdown::signal().await;
        tracing::info!("signal received, stopping");
        running.shutdown().await
    }
}
