//! The `foldd` binary: flags, environment, optional TOML config, tracing,
//! and a clean stop on SIGINT/SIGTERM.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context as _;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "foldd",
    version,
    about = "The fold event-sourcing database daemon"
)]
struct Cli {
    /// Directory holding the log. Created if missing.
    #[arg(long, env = "FOLD_DATA_DIR")]
    data_dir: Option<PathBuf>,
    /// The domain schema (.fold). WASM modules resolve relative to it.
    #[arg(long, env = "FOLD_SCHEMA")]
    schema: Option<PathBuf>,
    /// Address to serve gRPC on.
    #[arg(long, env = "FOLD_LISTEN")]
    listen: Option<SocketAddr>,
    /// TOML config file; flags and environment take precedence over it.
    #[arg(short = 'c', long)]
    config: Option<PathBuf>,
    /// How many aggregate instances to keep evolved in memory.
    #[arg(long)]
    aggregate_cache: Option<usize>,
    /// Skip fsync on append. Only for tests and bulk loads.
    #[arg(long)]
    no_fsync: bool,
    /// Back up the log on this interval (e.g. 15m, 6h, 1d) into its backups dir.
    #[arg(long, value_parser = foldd::scheduled::parse_duration)]
    backup_every: Option<std::time::Duration>,
    /// Backups to keep when backing up on a schedule; 0 keeps all.
    #[arg(long)]
    backup_keep: Option<usize>,
}

#[derive(serde::Deserialize, Default, Debug)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    data_dir: Option<PathBuf>,
    schema: Option<PathBuf>,
    listen: Option<SocketAddr>,
    aggregate_cache: Option<usize>,
    #[serde(default)]
    wasm: WasmConfig,
    #[serde(default)]
    backup: BackupConfig,
}

#[derive(serde::Deserialize, Default, Debug)]
#[serde(deny_unknown_fields)]
struct BackupConfig {
    /// `every = "6h"`
    every: Option<String>,
    keep: Option<usize>,
}

#[derive(serde::Deserialize, Default, Debug)]
#[serde(deny_unknown_fields)]
struct WasmConfig {
    fuel: Option<u64>,
    memory_mb: Option<usize>,
    timeout_ms: Option<u64>,
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
    let file: FileConfig = match &cli.config {
        Some(p) => toml::from_str(
            &std::fs::read_to_string(p).with_context(|| format!("cannot read {}", p.display()))?,
        )
        .with_context(|| format!("cannot parse {}", p.display()))?,
        None => FileConfig::default(),
    };

    let data_dir = cli
        .data_dir
        .or(file.data_dir)
        .context("--data-dir (or FOLD_DATA_DIR, or data_dir in the config) is required")?;
    let schema = cli
        .schema
        .or(file.schema)
        .context("--schema (or FOLD_SCHEMA, or schema in the config) is required")?;
    let listen = cli
        .listen
        .or(file.listen)
        .unwrap_or_else(|| "127.0.0.1:4141".parse().expect("static address"));

    let mut opts = foldd::Options::new(data_dir, schema, listen);
    if let Some(n) = cli.aggregate_cache.or(file.aggregate_cache) {
        opts.aggregate_cache = n;
    }
    if let Some(f) = file.wasm.fuel {
        opts.limits.fuel = f;
    }
    if let Some(mb) = file.wasm.memory_mb {
        opts.limits.memory_bytes = mb * 1024 * 1024;
    }
    if let Some(ms) = file.wasm.timeout_ms {
        opts.limits.epoch_ticks = (ms / fold_wasm::Engine::TICK.as_millis() as u64).max(1);
    }
    opts.fsync = !cli.no_fsync;
    let every = match (cli.backup_every, file.backup.every.as_deref()) {
        (Some(d), _) => Some(d),
        (None, Some(text)) => Some(
            foldd::scheduled::parse_duration(text)
                .map_err(|e| anyhow::anyhow!("config backup.every: {e}"))?,
        ),
        (None, None) => None,
    };
    if let Some(every) = every {
        opts.backup = Some(foldd::BackupSchedule {
            every,
            keep: cli.backup_keep.or(file.backup.keep).unwrap_or(7),
        });
    }

    let running = foldd::start(opts).await?;
    foldd::shutdown::signal().await;
    tracing::info!("signal received, stopping");
    running.shutdown().await
}
