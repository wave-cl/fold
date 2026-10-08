use anyhow::Context as _;
use clap::Args as ClapArgs;
use fold_proto::v1::{BackupInfo, BackupLogRequest, ListBackupsRequest, RestoreLogRequest};
use serde_json::json;

use crate::client;
use crate::output::Format;

fn print_backup(format: Format, b: &BackupInfo) {
    match format {
        Format::Json => println!(
            "{}",
            json!({ "path": b.path, "log_id": b.log_id, "head": b.head, "files": b.files, "bytes": b.bytes,
                    "created_at_unix_nanos": b.created_at_unix_nanos, "incremental": b.incremental, "base_head": b.base_head })
        ),
        Format::Human => println!(
            "{}  {}head {}  {} entries  {} bytes  log {}",
            b.path,
            match b.base_head {
                Some(base) if b.incremental => format!("increment {base}.."),
                _ => "full, ".to_string(),
            },
            b.head,
            b.files,
            b.bytes,
            b.log_id
        ),
    }
}

pub async fn backup(
    to: Option<String>,
    incremental: bool,
    addr: &str,
    format: Format,
) -> anyhow::Result<()> {
    let b = client::admin(addr)
        .await?
        .backup_log(BackupLogRequest {
            path: to.unwrap_or_default(),
            incremental,
        })
        .await?
        .into_inner();
    print_backup(format, &b);
    Ok(())
}

pub async fn list(addr: &str, format: Format) -> anyhow::Result<()> {
    let resp = client::admin(addr)
        .await?
        .list_backups(ListBackupsRequest {})
        .await?
        .into_inner();
    for b in &resp.backups {
        print_backup(format, b);
    }
    if format == Format::Human && resp.backups.is_empty() {
        println!("no backups");
    }
    match (&resp.schedule, format) {
        (Some(s), Format::Json) => println!(
            "{}",
            json!({
                "schedule": {
                    "every_secs": s.every_secs, "keep": s.keep,
                    "incremental": s.incremental, "full_every": s.full_every,
                    "last_run_unix_nanos": s.last_run_unix_nanos, "last_head": s.last_head,
                    "last_error": if s.last_error.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(s.last_error.clone()) },
                    "next_run_unix_nanos": s.next_run_unix_nanos,
                }
            })
        ),
        (Some(s), Format::Human) => {
            let last = match (s.last_run_unix_nanos, s.last_head) {
                (Some(_), Some(h)) => format!("last wrote head {h}"),
                (Some(_), None) => "last run wrote nothing".to_string(),
                (None, _) => "not run yet".to_string(),
            };
            println!(
                "schedule: every {}s, {}keep {}, {}{}",
                s.every_secs,
                if s.incremental {
                    format!("incremental (full every {}), ", s.full_every)
                } else {
                    String::new()
                },
                if s.keep == 0 {
                    "all".to_string()
                } else {
                    s.keep.to_string()
                },
                last,
                if s.last_error.is_empty() {
                    String::new()
                } else {
                    format!("; last error: {}", s.last_error)
                }
            );
        }
        (None, _) => {}
    }
    Ok(())
}

#[derive(ClapArgs, Debug)]
pub struct RestoreArgs {
    /// The .fbak archive to restore.
    pub archive: std::path::PathBuf,
    /// Data directory to restore into; the log is created at <dir>/<name>.
    /// Not used with --live.
    pub dir: Option<std::path::PathBuf>,
    /// Restore into the running daemon instead (Admin.RestoreLog): it stops,
    /// moves its current log aside, restores and serves again. The archive
    /// path is read on the daemon's host.
    #[arg(long)]
    pub live: bool,
    /// Log name inside the data directory.
    #[arg(long, default_value = "default")]
    pub name: String,
    /// Only print the archive's header.
    #[arg(long)]
    pub inspect: bool,
    /// Apply an incremental archive onto the log already restored at <dir>.
    #[arg(long)]
    pub apply: bool,
    /// Point in time: keep only the events below this global position (it
    /// must be a batch boundary). Works offline, with --apply and with --live.
    #[arg(long, value_name = "POSITION", conflicts_with = "at")]
    pub to: Option<u64>,
    /// Point in time: keep every batch recorded at or before this instant
    /// (RFC 3339, e.g. 2026-10-08T14:30:00Z). Works like --to.
    #[arg(long, value_name = "TIMESTAMP", conflicts_with = "to")]
    pub at: Option<String>,
}

impl RestoreArgs {
    fn point_in_time(&self) -> anyhow::Result<Option<fold_core::PointInTime>> {
        if let Some(to) = self.to {
            return Ok(Some(fold_core::PointInTime::Position(
                fold_core::GlobalPosition(to),
            )));
        }
        match &self.at {
            Some(at) => {
                let ts: jiff::Timestamp = at
                    .parse()
                    .with_context(|| format!("--at {at:?} is not an RFC 3339 timestamp"))?;
                Ok(Some(
                    fold_core::PointInTime::Time(ts.as_nanosecond() as i64),
                ))
            }
            None => Ok(None),
        }
    }
}

pub async fn restore_live(args: RestoreArgs, addr: &str, format: Format) -> anyhow::Result<()> {
    let resp = client::admin(addr)
        .await?
        .restore_log(RestoreLogRequest {
            path: args.archive.display().to_string(),
            to: args.to,
            at_unix_nanos: match args.point_in_time()? {
                Some(fold_core::PointInTime::Time(ns)) => Some(ns),
                _ => None,
            },
        })
        .await?
        .into_inner();
    match format {
        Format::Json => println!(
            "{}",
            json!({ "accepted": true, "log_id": resp.log_id, "head": resp.head })
        ),
        Format::Human => println!(
            "accepted: the daemon is swapping in log {} at head {}; reconnect and check `fold health`",
            resp.log_id, resp.head
        ),
    }
    Ok(())
}

/// Offline: writes a fresh log directory from the archive and verifies it
/// opens. Run with the daemon stopped, into a directory that does not exist.
pub fn restore(args: RestoreArgs, format: Format) -> anyhow::Result<()> {
    if args.inspect {
        let meta = fold_core::inspect_backup(&args.archive)
            .with_context(|| format!("cannot read {}", args.archive.display()))?;
        match format {
            Format::Json => println!("{}", serde_json::to_string(&meta)?),
            Format::Human => println!(
                "log {}  {}head {}  created {}  schema {}",
                meta.log_id,
                match meta.base_head {
                    Some(b) => format!("increment {b}.."),
                    None => "full, ".to_string(),
                },
                meta.head,
                meta.created_at_unix_nanos,
                if meta.schema.is_some() {
                    "included"
                } else {
                    "absent"
                }
            ),
        }
        return Ok(());
    }
    let dir = args
        .dir
        .clone()
        .context("a data directory is required (or --live to restore into the running daemon)")?;
    if args.apply {
        let to = args.point_in_time()?;
        let meta = fold_core::apply_backup_to(&args.archive, &dir, &args.name, to)
            .with_context(|| format!("cannot apply {}", args.archive.display()))?;
        match format {
            Format::Json => println!(
                "{}",
                json!({ "dir": dir.join(&args.name), "head": meta.head, "base_head": meta.base_head, "log_id": meta.log_id })
            ),
            Format::Human => println!(
                "applied increment {}..{} onto {}",
                meta.base_head.unwrap_or(0),
                meta.head,
                dir.join(&args.name).display()
            ),
        }
        return Ok(());
    }
    let to = args.point_in_time()?;
    let meta = fold_core::restore_backup_to(&args.archive, &dir, &args.name, to)
        .with_context(|| format!("cannot restore {}", args.archive.display()))?;
    // Prove the result opens; recovery runs here exactly as foldd would run it.
    let log = fold_core::Log::open(&dir, &args.name, fold_core::OpenOptions::default())
        .context("the restored log does not open")?;
    let head = log.head().0;
    drop(log);
    match format {
        Format::Json => println!(
            "{}",
            json!({ "dir": dir.join(&args.name), "head": head, "log_id": meta.log_id, "files": meta.files })
        ),
        Format::Human => println!(
            "restored log {} into {} at head {}; start foldd with --data-dir {}",
            meta.log_id,
            dir.join(&args.name).display(),
            head,
            dir.display()
        ),
    }
    Ok(())
}
