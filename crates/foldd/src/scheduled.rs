//! Scheduled backups: every `every`, write a backup into the log's backups
//! directory unless the head has not moved since the last one, then keep
//! only the newest `keep`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::state::Shared;

/// How often to back up and how many backups to keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackupSchedule {
    pub every: Duration,
    /// Backups retained, newest first; `0` keeps all.
    pub keep: usize,
}

/// What the schedule did last, for `ListBackups`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackupStatus {
    pub schedule: Option<BackupSchedule>,
    pub last_run_unix_nanos: Option<i64>,
    pub last_head: Option<u64>,
    pub last_error: Option<String>,
    pub next_run_unix_nanos: Option<i64>,
}

fn now_nanos() -> i64 {
    jiff::Timestamp::now().as_nanosecond() as i64
}

pub fn default_path(shared: &Shared) -> PathBuf {
    let stamp = jiff::Timestamp::now()
        .strftime("%Y%m%dT%H%M%SZ")
        .to_string();
    shared
        .log
        .path()
        .join("backups")
        .join(format!("{:020}-{stamp}.fbak", shared.log.head().0))
}

/// Newest-first list of `(head, created_at, path)` in the backups directory.
pub fn existing(shared: &Shared) -> Vec<(u64, i64, PathBuf)> {
    let dir = shared.log.path().join("backups");
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("fbak") {
            continue;
        }
        if let Ok(meta) = fold_core::inspect_backup(&path) {
            out.push((meta.head, meta.created_at_unix_nanos, path));
        }
    }
    out.sort_by_key(|(h, c, _)| std::cmp::Reverse((*h, *c)));
    out
}

/// One scheduled pass: back up if the head moved, then prune. Blocking.
pub fn run_once(
    shared: &Shared,
    schedule: BackupSchedule,
) -> Result<Option<fold_core::BackupMeta>, fold_core::Error> {
    let head = shared.log.head().0;
    let latest = existing(shared).first().map(|(h, _, _)| *h);
    // Nothing to back up when the log is empty or unchanged since the newest.
    let written = if head == 0 || latest == Some(head) {
        None
    } else {
        Some(shared.log.backup_to(&default_path(shared))?)
    };
    if schedule.keep > 0 {
        for (_, _, path) in existing(shared).into_iter().skip(schedule.keep) {
            match std::fs::remove_file(&path) {
                Ok(()) => tracing::info!(path = %path.display(), "pruned old backup"),
                Err(e) => tracing::warn!(path = %path.display(), error = %e, "cannot prune backup"),
            }
        }
    }
    Ok(written)
}

pub fn spawn(shared: Arc<Shared>, schedule: BackupSchedule) -> JoinHandle<()> {
    {
        let mut st = shared.backup_status.lock().expect("backup status");
        st.schedule = Some(schedule);
        st.next_run_unix_nanos = Some(now_nanos() + schedule.every.as_nanos() as i64);
    }
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shared.cancel.cancelled() => return,
                _ = tokio::time::sleep(schedule.every) => {}
            }
            let s2 = shared.clone();
            let result = tokio::task::spawn_blocking(move || run_once(&s2, schedule))
                .await
                .unwrap_or_else(|e| {
                    Err(fold_core::Error::Io {
                        path: PathBuf::from("<backup task>"),
                        op: "join",
                        source: std::io::Error::other(e.to_string()),
                    })
                });
            let mut st = shared.backup_status.lock().expect("backup status");
            st.last_run_unix_nanos = Some(now_nanos());
            st.next_run_unix_nanos = Some(now_nanos() + schedule.every.as_nanos() as i64);
            match result {
                Ok(Some(meta)) => {
                    tracing::info!(
                        head = meta.head,
                        bytes = meta.bytes,
                        "scheduled backup written"
                    );
                    st.last_head = Some(meta.head);
                    st.last_error = None;
                }
                Ok(None) => {
                    tracing::debug!("scheduled backup skipped: head unchanged");
                    st.last_error = None;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "scheduled backup failed");
                    st.last_error = Some(e.to_string());
                }
            }
        }
    })
}

/// Parses `90s`, `15m`, `6h`, `1d` (or a bare number of seconds).
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, unit) = match s.find(|c: char| !c.is_ascii_digit()) {
        Some(i) => s.split_at(i),
        None => (s, "s"),
    };
    let n: u64 = num.parse().map_err(|_| format!("bad duration {s:?}"))?;
    let secs = match unit.trim() {
        "s" | "sec" | "secs" => n,
        "m" | "min" | "mins" => n * 60,
        "h" | "hr" | "hours" => n * 3600,
        "d" | "day" | "days" => n * 86_400,
        other => return Err(format!("unknown duration unit {other:?} in {s:?}")),
    };
    if secs == 0 {
        return Err("a backup interval must be positive".into());
    }
    Ok(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::parse_duration;

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("90s").unwrap().as_secs(), 90);
        assert_eq!(parse_duration("15m").unwrap().as_secs(), 900);
        assert_eq!(parse_duration("6h").unwrap().as_secs(), 21_600);
        assert_eq!(parse_duration("1d").unwrap().as_secs(), 86_400);
        assert_eq!(parse_duration("42").unwrap().as_secs(), 42);
        assert!(parse_duration("0m").is_err());
        assert!(parse_duration("3 fortnights").is_err());
        assert!(parse_duration("abc").is_err());
    }
}
