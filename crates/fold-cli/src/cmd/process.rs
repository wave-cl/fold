use clap::Subcommand;
use fold_proto::v1::{
    DeleteSnapshotRequest, ListProcessesRequest, ListSnapshotsRequest, RebuildProjectionRequest,
    SnapshotProjectionRequest,
};
use serde_json::{Value, json};

use crate::client;
use crate::cmd::projection::print_snapshot;
use crate::output::{Format, state_name};

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Every process manager with its state, checkpoint and outbox (Admin.ListProcesses).
    List,
    /// Snapshot a process's instances and outbox at its checkpoint.
    Snapshot {
        /// "Context.Process"
        process: String,
    },
    /// List a process's snapshots, newest first.
    Snapshots { process: String },
    /// Delete one snapshot.
    DropSnapshot { process: String, id: String },
    /// Reset a process and replay: from scratch, or from a snapshot. Commands
    /// already issued are recognised by their idempotency keys and skipped.
    Rebuild {
        process: String,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        force: bool,
    },
}

pub async fn run(cmd: Cmd, addr: &str, format: Format) -> anyhow::Result<()> {
    match cmd {
        Cmd::Snapshot { process } => {
            let s = client::admin(addr)
                .await?
                .snapshot_projection(SnapshotProjectionRequest {
                    projection: process,
                })
                .await?
                .into_inner();
            print_snapshot(format, &s);
        }
        Cmd::Snapshots { process } => {
            let resp = client::admin(addr)
                .await?
                .list_snapshots(ListSnapshotsRequest {
                    projection: process,
                })
                .await?
                .into_inner();
            for s in &resp.snapshots {
                print_snapshot(format, s);
            }
            if format == Format::Human && resp.snapshots.is_empty() {
                println!("no snapshots");
            }
        }
        Cmd::DropSnapshot { process, id } => {
            client::admin(addr)
                .await?
                .delete_snapshot(DeleteSnapshotRequest {
                    projection: process,
                    id,
                })
                .await?;
            if format == Format::Human {
                println!("deleted");
            }
        }
        Cmd::Rebuild {
            process,
            from,
            force,
        } => {
            let resp = client::admin(addr)
                .await?
                .rebuild_projection(RebuildProjectionRequest {
                    projection: process,
                    snapshot_id: from.unwrap_or_default(),
                    force,
                })
                .await?
                .into_inner();
            match format {
                Format::Json => println!("{}", json!({ "restarted_from": resp.restarted_from })),
                Format::Human => match resp.restarted_from {
                    Some(c) => println!(
                        "rebuilding from snapshot at checkpoint {c}; replaying in the background"
                    ),
                    None => println!("rebuilding from scratch; replaying in the background"),
                },
            }
        }
        Cmd::List => {
            let resp = client::admin(addr)
                .await?
                .list_processes(ListProcessesRequest {})
                .await?
                .into_inner();
            match format {
                Format::Json => {
                    for p in &resp.processes {
                        println!(
                            "{}",
                            json!({
                                "name": p.name,
                                "state": state_name(p.state),
                                "checkpoint": p.checkpoint,
                                "head": p.head,
                                "pending_commands": p.pending_commands,
                                "dispatched": p.dispatched,
                                "rejected": p.rejected,
                                "pending_timers": p.pending_timers,
                                "error": if p.error.is_empty() { Value::Null } else { Value::String(p.error.clone()) },
                            })
                        );
                    }
                }
                Format::Human => {
                    let w = resp
                        .processes
                        .iter()
                        .map(|p| p.name.len())
                        .max()
                        .unwrap_or(4)
                        .max(4);
                    println!(
                        "{:<w$}  {:<11}  {:>10}  {:>8}  {:>7}  {:>10}  {:>8}  {:>6}",
                        "NAME",
                        "STATE",
                        "CHECKPOINT",
                        "HEAD",
                        "PENDING",
                        "DISPATCHED",
                        "REJECTED",
                        "TIMERS"
                    );
                    for p in &resp.processes {
                        let cp = p
                            .checkpoint
                            .map(|c| c.to_string())
                            .unwrap_or_else(|| "-".into());
                        println!(
                            "{:<w$}  {:<11}  {:>10}  {:>8}  {:>7}  {:>10}  {:>8}  {:>6}",
                            p.name,
                            state_name(p.state),
                            cp,
                            p.head,
                            p.pending_commands,
                            p.dispatched,
                            p.rejected,
                            p.pending_timers
                        );
                        if !p.error.is_empty() {
                            println!("{:<w$}  error: {}", "", p.error);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
