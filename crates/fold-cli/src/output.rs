//! Human and JSON rendering, and the error-to-exit-code mapping.

use fold_proto::common::v1::{ProjectionRow, RecordedEvent, RunnerState, SnapshotInfo};
use fold_proto::derivation::v1::{GetAggregateResponse, ProjectionStatus};
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Human,
    Json,
}

fn json_bytes(b: &[u8]) -> Value {
    serde_json::from_slice(b)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(b).into_owned()))
}

pub fn event_json(e: &RecordedEvent) -> Value {
    json!({
        "id": e.id,
        "stream": e.stream_id,
        "version": e.version,
        "position": e.position,
        "type": e.r#type,
        "payload": json_bytes(&e.payload),
        "metadata": if e.metadata.is_empty() { Value::Null } else { json_bytes(&e.metadata) },
        "recorded_at_unix_nanos": e.recorded_at_unix_nanos,
    })
}

pub fn print_event(f: Format, e: &RecordedEvent) {
    match f {
        Format::Json => println!("{}", event_json(e)),
        Format::Human => {
            let payload = json_bytes(&e.payload);
            println!(
                "{:>8}  {}@{}  {}  {}",
                e.position,
                e.stream_id,
                e.version,
                e.r#type,
                serde_json::to_string(&payload).unwrap_or_default()
            );
        }
    }
}

pub fn row_json(r: &ProjectionRow) -> Value {
    json!({ "key": json_bytes(&r.key), "row": json_bytes(&r.row) })
}

pub fn print_row(f: Format, r: &ProjectionRow) {
    match f {
        Format::Json => println!("{}", row_json(r)),
        Format::Human => {
            println!(
                "key: {}",
                serde_json::to_string(&json_bytes(&r.key)).unwrap_or_default()
            );
            println!(
                "{}",
                serde_json::to_string_pretty(&json_bytes(&r.row)).unwrap_or_default()
            );
        }
    }
}

pub fn print_aggregate(f: Format, a: &GetAggregateResponse) {
    let state = json_bytes(&a.state);
    match f {
        Format::Json => println!(
            "{}",
            json!({
                "aggregate": a.aggregate,
                "version": a.version,
                "snapshot_version": a.snapshot_version,
                "replayed": a.replayed,
                "state": state,
            })
        ),
        Format::Human => {
            println!("aggregate: {}", a.aggregate);
            println!("version:   {}", a.version);
            match a.snapshot_version {
                Some(v) => println!("snapshot:  v{v}, {} event(s) replayed after it", a.replayed),
                None => println!("snapshot:  none, {} event(s) replayed", a.replayed),
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&state).unwrap_or_default()
            );
        }
    }
}

pub fn print_statuses(f: Format, statuses: &[ProjectionStatus]) {
    match f {
        Format::Json => {
            for s in statuses {
                println!(
                    "{}",
                    json!({
                        "name": s.name,
                        "state": state_name(s.state),
                        "checkpoint": s.checkpoint,
                        "head": s.head,
                        "error": if s.error.is_empty() { Value::Null } else { Value::String(s.error.clone()) },
                        "tables": s.tables,
                    })
                );
            }
        }
        Format::Human => {
            let w = statuses
                .iter()
                .map(|s| s.name.len())
                .max()
                .unwrap_or(4)
                .max(4);
            println!(
                "{:<w$}  {:<11}  {:>10}  {:>8}  TABLES",
                "NAME", "STATE", "CHECKPOINT", "HEAD"
            );
            for s in statuses {
                let cp = s
                    .checkpoint
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "-".into());
                println!(
                    "{:<w$}  {:<11}  {:>10}  {:>8}  {}",
                    s.name,
                    state_name(s.state),
                    cp,
                    s.head,
                    s.tables.join(", ")
                );
                if !s.error.is_empty() {
                    println!("{:<w$}  error: {}", "", s.error);
                }
            }
        }
    }
}

pub fn state_name(state: i32) -> &'static str {
    match RunnerState::try_from(state).unwrap_or(RunnerState::Unspecified) {
        RunnerState::Unspecified => "unknown",
        RunnerState::Starting => "starting",
        RunnerState::CatchingUp => "catching-up",
        RunnerState::Live => "live",
        RunnerState::Failed => "FAILED",
        RunnerState::Stopped => "stopped",
        RunnerState::Rebuilding => "rebuilding",
    }
}

/// A snapshot of a projection, an aggregate or a process, as any node
/// lists it.
pub fn print_snapshot(f: Format, s: &SnapshotInfo) {
    match f {
        Format::Json => println!(
            "{}",
            json!({
                "id": s.id, "name": s.name, "checkpoint": s.checkpoint, "rows": s.rows,
                "bytes": s.bytes, "created_at_unix_nanos": s.created_at_unix_nanos, "module_matches": s.module_matches,
            })
        ),
        Format::Human => println!(
            "{}  checkpoint {}  {} row(s)  {} bytes{}",
            s.id,
            s.checkpoint,
            s.rows,
            s.bytes,
            if s.module_matches {
                ""
            } else {
                "  (the module has changed)"
            }
        ),
    }
}

/// A `Rebuild` answer, for the three kinds of runner.
pub fn print_rebuild(f: Format, restarted_from: Option<u64>, what: &str) {
    match f {
        Format::Json => println!("{}", json!({ "restarted_from": restarted_from })),
        Format::Human => match restarted_from {
            Some(c) => println!("rebuilding {what} from the snapshot at checkpoint {c}"),
            None => println!("rebuilding {what} from scratch"),
        },
    }
}

pub fn render_error(e: &anyhow::Error) -> String {
    if let Some(status) = e.downcast_ref::<tonic::Status>() {
        format!("{:?}: {}", status.code(), status.message())
    } else {
        format!("{e:#}")
    }
}

pub fn exit_code(e: &anyhow::Error) -> i32 {
    if e.downcast_ref::<crate::client::Unreachable>().is_some() {
        return 2;
    }
    if let Some(status) = e.downcast_ref::<tonic::Status>()
        && status.code() == tonic::Code::Unavailable
        && status.message().contains("connect")
    {
        return 2;
    }
    1
}
