use clap::Subcommand;
use fold_proto::application::v1::ListProcessesRequest;
use fold_proto::common::v1::{
    DeleteSnapshotRequest, ListSnapshotsRequest, RebuildRequest, SnapshotRequest,
};
use serde_json::{Value, json};

use crate::client::{self, Addrs};
use crate::output::{self, Format, state_name};

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Every process manager with its state, checkpoint and outbox (AppAdmin.ListProcesses).
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

pub async fn run(cmd: Cmd, addrs: &Addrs, format: Format) -> anyhow::Result<()> {
    let mut admin = client::app_admin(addrs).await?;
    match cmd {
        Cmd::Snapshot { process } => {
            let s = admin
                .snapshot(SnapshotRequest { name: process })
                .await?
                .into_inner();
            output::print_snapshot(format, &s);
        }
        Cmd::Snapshots { process } => {
            let resp = admin
                .list_snapshots(ListSnapshotsRequest { name: process })
                .await?
                .into_inner();
            for s in &resp.snapshots {
                output::print_snapshot(format, s);
            }
            if format == Format::Human && resp.snapshots.is_empty() {
                println!("no snapshots");
            }
        }
        Cmd::DropSnapshot { process, id } => {
            admin
                .delete_snapshot(DeleteSnapshotRequest { name: process, id })
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
            let resp = admin
                .rebuild(RebuildRequest {
                    name: process,
                    snapshot_id: from.unwrap_or_default(),
                    force,
                })
                .await?
                .into_inner();
            output::print_rebuild(
                format,
                resp.restarted_from,
                "the process; it replays in the background",
            );
        }
        Cmd::List => {
            let resp = admin
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
