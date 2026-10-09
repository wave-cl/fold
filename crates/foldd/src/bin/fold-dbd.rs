//! The `fold-dbd` binary: the database service on its own.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "fold-dbd",
    version,
    about = "The fold database service: the log, replication, backups"
)]
struct Cli {
    /// Directory holding the log. Created if missing.
    #[arg(long, env = "FOLD_DATA_DIR")]
    data_dir: PathBuf,
    /// The domain schema (.fold), or a higher layer's root whose domain
    /// is used.
    #[arg(long, env = "FOLD_SCHEMA")]
    schema: PathBuf,
    /// Address to serve gRPC on.
    #[arg(long, env = "FOLD_LISTEN", default_value = "127.0.0.1:4141")]
    listen: SocketAddr,
    /// Skip fsync on append. Only for tests and bulk loads.
    #[arg(long)]
    no_fsync: bool,
    /// Back up the log on this interval (e.g. 15m, 6h, 1d) into its backups dir.
    #[arg(long, value_parser = fold_db::scheduled::parse_duration)]
    backup_every: Option<std::time::Duration>,
    /// Backups to keep when backing up on a schedule; 0 keeps all.
    #[arg(long, default_value_t = 7)]
    backup_keep: usize,
    /// Scheduled backups are increments since the newest backup.
    #[arg(long)]
    backup_incremental: bool,
    /// With --backup-incremental: a full backup every N backups.
    #[arg(long, default_value_t = 24)]
    backup_full_every: u32,
    /// Run as a read-only replica tailing this primary (e.g. http://10.0.0.1:4141).
    #[arg(long, env = "FOLD_REPLICATE_FROM", value_name = "URL")]
    replicate_from: Option<String>,
    /// On a replica: promote automatically after the primary has been out of
    /// reach this long (e.g. 30s). Off by default.
    #[arg(long, value_parser = fold_db::scheduled::parse_duration, value_name = "DURATION")]
    auto_failover: Option<std::time::Duration>,
    /// With --auto-failover or --lease: the other cluster members,
    /// comma-separated URLs.
    #[arg(long, value_delimiter = ',', value_name = "URL,URL,...")]
    quorum_peers: Vec<String>,
    /// With --quorum-peers: as a primary, serve reads only under a lease the
    /// majority renews for this long at a time (e.g. 5s).
    #[arg(long, value_parser = fold_db::scheduled::parse_duration, value_name = "DURATION")]
    lease: Option<std::time::Duration>,
    /// Adopt a changed domain even if it breaks data in the log.
    #[arg(long)]
    force_schema: bool,
    /// The secret whose token lets an application node append `Fold.*`
    /// events (process timers). Without one every such append is refused.
    #[arg(long, env = "FOLD_SYSTEM_SECRET", hide_env_values = true)]
    system_secret: Option<String>,
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
    let mut opts = fold_db::Options::new(cli.data_dir, cli.schema, cli.listen);
    opts.fsync = !cli.no_fsync;
    opts.force_schema = cli.force_schema;
    opts.system_secret = cli.system_secret;
    opts.replicate_from = cli.replicate_from;
    opts.auto_failover = cli.auto_failover;
    opts.quorum_peers = cli.quorum_peers;
    opts.lease = cli.lease;
    if let Some(every) = cli.backup_every {
        opts.backup = Some(fold_db::BackupSchedule {
            every,
            keep: cli.backup_keep,
            incremental: cli.backup_incremental,
            full_every: if cli.backup_incremental {
                cli.backup_full_every
            } else {
                0
            },
        });
    }
    let supervisor = fold_db::Supervisor::start(opts).await?;
    supervisor
        .run(async {
            fold_db::shutdown::signal().await;
            tracing::info!("signal received, stopping");
        })
        .await
}
