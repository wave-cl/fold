//! `fold`: the command-line client for the fold services.
//!
//! Subcommands mirror the services, so the layers are visible at the shell:
//! `exec` and `append` write through the application node, `query` and
//! `log aggregate` read the derivation node, `log read/all/tail`, `promote`,
//! `fence` and the backups talk to the database, and `health` asks all
//! three. `--addr` names the composite (every layer on one address);
//! `--db`, `--derive` and `--app` name the services of a split deployment.
//! `init`, `schema check`, `schema diff` and `schema fmt` work offline.
//!
//! Exit codes: 0 ok; 1 the server refused or a command was rejected (the code
//! and message are printed); 2 usage or connection failure.

mod client;
mod cmd;
mod output;
mod session;

use clap::{Parser, Subcommand};

use crate::output::Format;

#[derive(Parser, Debug)]
#[command(
    name = "fold",
    version,
    about = "Client for the fold event-sourcing database"
)]
pub struct Cli {
    /// Address every layer answers on (the composite `foldd`).
    #[arg(
        long,
        global = true,
        env = "FOLD_ADDR",
        default_value = "http://127.0.0.1:4141"
    )]
    pub addr: String,
    /// The database (`fold-dbd`), when it is not at --addr.
    #[arg(long, global = true, env = "FOLD_DB_ADDR", value_name = "URL")]
    pub db: Option<String>,
    /// The derivation node (`fold-derived`), when it is not at --addr.
    #[arg(long, global = true, env = "FOLD_DERIVE_ADDR", value_name = "URL")]
    pub derive: Option<String>,
    /// The application node (`fold-appd`), when it is not at --addr.
    #[arg(long, global = true, env = "FOLD_APP_ADDR", value_name = "URL")]
    pub app: Option<String>,

    /// Print one JSON object per line instead of human-readable output.
    #[arg(long, global = true)]
    pub json: bool,

    /// Session file: reads carry the latest position token seen and every
    /// read or write advances it, so this client's reads never go backwards
    /// whichever member answers.
    #[arg(long, global = true, env = "FOLD_SESSION", value_name = "FILE")]
    pub session: Option<std::path::PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Create a log directory from a schema file (offline).
    Init(cmd::init::Args),
    /// Schema tools.
    Schema {
        #[command(subcommand)]
        cmd: cmd::schema::Cmd,
    },
    /// Execute a declared command against an aggregate (application: Command.Execute).
    Exec(cmd::exec::Args),
    /// Append raw events to a stream without a handler (application: Command.Append).
    Append(cmd::append::Args),
    /// Read projections (derivation: Query).
    Query {
        #[command(subcommand)]
        cmd: cmd::query::Cmd,
    },
    /// Read events (database: Log), aggregate state and process instances.
    Log {
        #[command(subcommand)]
        cmd: cmd::log::Cmd,
    },
    /// Projection status and snapshots (derivation: DeriveAdmin).
    Projection {
        #[command(subcommand)]
        cmd: cmd::projection::Cmd,
    },
    /// Process manager status and snapshots (application: AppAdmin).
    Process {
        #[command(subcommand)]
        cmd: cmd::process::Cmd,
    },
    /// Aggregate instance snapshots (derivation: DeriveAdmin).
    Aggregate {
        #[command(subcommand)]
        cmd: cmd::aggregate::Cmd,
    },
    /// Health of every layer (Cluster.Health, DeriveAdmin.Health, AppAdmin.Health).
    Health,
    /// Failover: promote the replica database to a primary, in place (database: Cluster.Promote).
    Promote,
    /// Fencing: tell the database that a primary at EPOCH exists, so it stops taking writes (database: Cluster.Fence).
    Fence {
        /// The newer primary's epoch (its `fold health` epoch).
        epoch: u64,
    },
    /// Write a backup of the whole log on the database's host (database: Backup.BackupLog).
    Backup {
        /// Archive path on the database's host; default: the log's backups directory.
        #[arg(long)]
        to: Option<String>,
        /// Only the records since the newest backup in the backups directory.
        #[arg(long)]
        incremental: bool,
    },
    /// List backups in the log's backups directory (database: Backup.ListBackups).
    Backups,
    /// Restore a backup archive as a new log directory (offline; database stopped).
    Restore(cmd::backup::RestoreArgs),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let format = if cli.json {
        Format::Json
    } else {
        Format::Human
    };
    let code = match run(cli, format).await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("fold: {}", output::render_error(&e));
            output::exit_code(&e)
        }
    };
    std::process::exit(code);
}

async fn run(cli: Cli, format: Format) -> anyhow::Result<()> {
    let mut session = match &cli.session {
        Some(path) => Some(session::Session::load(path)?),
        None => None,
    };
    let session = &mut session;
    let addrs = client::Addrs {
        db: cli.db.unwrap_or_else(|| cli.addr.clone()),
        derive: cli.derive.unwrap_or_else(|| cli.addr.clone()),
        app: cli.app.unwrap_or_else(|| cli.addr.clone()),
    };
    let addrs = &addrs;
    match cli.command {
        Commands::Init(args) => cmd::init::run(args, format),
        Commands::Schema { cmd } => cmd::schema::run(cmd, addrs, format).await,
        Commands::Exec(args) => cmd::exec::run(args, addrs, format, session).await,
        Commands::Append(args) => cmd::append::run(args, addrs, format, session).await,
        Commands::Query { cmd } => cmd::query::run(cmd, addrs, format, session).await,
        Commands::Log { cmd } => cmd::log::run(cmd, addrs, format).await,
        Commands::Projection { cmd } => cmd::projection::run(cmd, addrs, format).await,
        Commands::Process { cmd } => cmd::process::run(cmd, addrs, format).await,
        Commands::Aggregate { cmd } => cmd::aggregate::run(cmd, addrs, format).await,
        Commands::Health => cmd::health::run(addrs, format).await,
        Commands::Promote => cmd::promote::run(addrs, format).await,
        Commands::Fence { epoch } => cmd::fence::run(epoch, addrs, format).await,
        Commands::Backup { to, incremental } => {
            cmd::backup::backup(to, incremental, addrs, format).await
        }
        Commands::Backups => cmd::backup::list(addrs, format).await,
        Commands::Restore(args) if args.live => {
            cmd::backup::restore_live(args, addrs, format).await
        }
        Commands::Restore(args) => cmd::backup::restore(args, format),
    }
}
