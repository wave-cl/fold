use anyhow::Context as _;
use clap::Args as ClapArgs;
use fold_proto::v1::{BackupInfo, BackupLogRequest, ListBackupsRequest};
use serde_json::json;

use crate::client;
use crate::output::Format;

fn print_backup(format: Format, b: &BackupInfo) {
    match format {
        Format::Json => println!(
            "{}",
            json!({ "path": b.path, "log_id": b.log_id, "head": b.head, "files": b.files, "bytes": b.bytes, "created_at_unix_nanos": b.created_at_unix_nanos })
        ),
        Format::Human => println!(
            "{}  head {}  {} entries  {} bytes  log {}",
            b.path, b.head, b.files, b.bytes, b.log_id
        ),
    }
}

pub async fn backup(to: Option<String>, addr: &str, format: Format) -> anyhow::Result<()> {
    let b = client::admin(addr)
        .await?
        .backup_log(BackupLogRequest {
            path: to.unwrap_or_default(),
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
                "schedule: every {}s, keep {}, {}{}",
                s.every_secs,
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
    pub dir: std::path::PathBuf,
    /// Log name inside the data directory.
    #[arg(long, default_value = "default")]
    pub name: String,
    /// Only print the archive's header.
    #[arg(long)]
    pub inspect: bool,
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
                "log {}  head {}  created {}  schema {}",
                meta.log_id,
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
    let meta = fold_core::restore_backup(&args.archive, &args.dir, &args.name)
        .with_context(|| format!("cannot restore {}", args.archive.display()))?;
    // Prove the result opens; recovery runs here exactly as foldd would run it.
    let log = fold_core::Log::open(&args.dir, &args.name, fold_core::OpenOptions::default())
        .context("the restored log does not open")?;
    let head = log.head().0;
    drop(log);
    match format {
        Format::Json => println!(
            "{}",
            json!({ "dir": args.dir.join(&args.name), "head": head, "log_id": meta.log_id, "files": meta.files })
        ),
        Format::Human => println!(
            "restored log {} into {} at head {}; start foldd with --data-dir {}",
            meta.log_id,
            args.dir.join(&args.name).display(),
            head,
            args.dir.display()
        ),
    }
    Ok(())
}
