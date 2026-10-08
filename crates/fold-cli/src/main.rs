//! `fold`: the command-line client for `foldd`.
//!
//! Subcommands mirror the four gRPC services so command/query segregation is
//! visible at the shell: `exec` and `append` write, `query` reads projections,
//! `log` reads events and aggregate state, and `projection list` / `health` /
//! `schema show` administer. `init` and `schema check` work offline.
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
    /// Address of foldd.
    #[arg(
        long,
        global = true,
        env = "FOLD_ADDR",
        default_value = "http://127.0.0.1:4141"
    )]
    pub addr: String,

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
    /// Execute a declared command against an aggregate (Command.Execute).
    Exec(cmd::exec::Args),
    /// Append raw events to a stream without a handler (Command.Append).
    Append(cmd::append::Args),
    /// Read projections (Query).
    Query {
        #[command(subcommand)]
        cmd: cmd::query::Cmd,
    },
    /// Read events and aggregate state (Log).
    Log {
        #[command(subcommand)]
        cmd: cmd::log::Cmd,
    },
    /// Projection status (Admin).
    Projection {
        #[command(subcommand)]
        cmd: cmd::projection::Cmd,
    },
    /// Process manager status (Admin).
    Process {
        #[command(subcommand)]
        cmd: cmd::process::Cmd,
    },
    /// Aggregate instance snapshots (Admin).
    Aggregate {
        #[command(subcommand)]
        cmd: cmd::aggregate::Cmd,
    },
    /// Daemon health (Admin.Health).
    Health,
    /// Failover: promote the replica at --addr to a primary, in place (Admin.Promote).
    Promote,
    /// Fencing: tell the daemon at --addr that a primary at EPOCH exists, so it stops taking writes (Admin.Fence).
    Fence {
        /// The newer primary's epoch (its `fold health` epoch).
        epoch: u64,
    },
    /// Write a backup of the whole log on the daemon's host (Admin.BackupLog).
    Backup {
        /// Archive path on the daemon's host; default: the log's backups directory.
        #[arg(long)]
        to: Option<String>,
        /// Only the records since the newest backup in the backups directory.
        #[arg(long)]
        incremental: bool,
    },
    /// List backups in the log's backups directory (Admin.ListBackups).
    Backups,
    /// Restore a backup archive as a new log directory (offline; daemon stopped).
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
    match cli.command {
        Commands::Init(args) => cmd::init::run(args, format),
        Commands::Schema { cmd } => cmd::schema::run(cmd, &cli.addr, format).await,
        Commands::Exec(args) => cmd::exec::run(args, &cli.addr, format, session).await,
        Commands::Append(args) => cmd::append::run(args, &cli.addr, format, session).await,
        Commands::Query { cmd } => cmd::query::run(cmd, &cli.addr, format, session).await,
        Commands::Log { cmd } => cmd::log::run(cmd, &cli.addr, format).await,
        Commands::Projection { cmd } => cmd::projection::run(cmd, &cli.addr, format).await,
        Commands::Process { cmd } => cmd::process::run(cmd, &cli.addr, format).await,
        Commands::Aggregate { cmd } => cmd::aggregate::run(cmd, &cli.addr, format).await,
        Commands::Health => cmd::health::run(&cli.addr, format).await,
        Commands::Promote => cmd::promote::run(&cli.addr, format).await,
        Commands::Fence { epoch } => cmd::fence::run(epoch, &cli.addr, format).await,
        Commands::Backup { to, incremental } => {
            cmd::backup::backup(to, incremental, &cli.addr, format).await
        }
        Commands::Backups => cmd::backup::list(&cli.addr, format).await,
        Commands::Restore(args) if args.live => {
            cmd::backup::restore_live(args, &cli.addr, format).await
        }
        Commands::Restore(args) => cmd::backup::restore(args, format),
    }
}
