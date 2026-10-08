use clap::Subcommand;
use fold_proto::v1::{
    DeleteSnapshotRequest, ListSnapshotsRequest, RebuildProjectionRequest,
    SnapshotProjectionRequest,
};
use serde_json::json;

use crate::client;
use crate::cmd::projection::print_snapshot;
use crate::output::Format;

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Write every instance snapshot of an aggregate to a snapshot file.
    Snapshot {
        /// "Context.Aggregate"
        aggregate: String,
    },
    /// List an aggregate's snapshot files, newest first.
    Snapshots { aggregate: String },
    /// Delete one snapshot file.
    DropSnapshot { aggregate: String, id: String },
    /// Drop every instance snapshot (and the cache), optionally restore a
    /// file, then re-derive every instance from its events with the current
    /// evolve module. Returns when every instance has been rebuilt.
    Rebuild {
        aggregate: String,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        force: bool,
    },
}

pub async fn run(cmd: Cmd, addr: &str, format: Format) -> anyhow::Result<()> {
    let mut admin = client::admin(addr).await?;
    match cmd {
        Cmd::Snapshot { aggregate } => {
            let s = admin
                .snapshot_projection(SnapshotProjectionRequest {
                    projection: aggregate,
                })
                .await?
                .into_inner();
            print_snapshot(format, &s);
        }
        Cmd::Snapshots { aggregate } => {
            let resp = admin
                .list_snapshots(ListSnapshotsRequest {
                    projection: aggregate,
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
        Cmd::DropSnapshot { aggregate, id } => {
            admin
                .delete_snapshot(DeleteSnapshotRequest {
                    projection: aggregate,
                    id,
                })
                .await?;
            if format == Format::Human {
                println!("deleted");
            }
        }
        Cmd::Rebuild {
            aggregate,
            from,
            force,
        } => {
            let resp = admin
                .rebuild_projection(RebuildProjectionRequest {
                    projection: aggregate,
                    snapshot_id: from.unwrap_or_default(),
                    force,
                })
                .await?
                .into_inner();
            match format {
                Format::Json => println!("{}", json!({ "restarted_from": resp.restarted_from })),
                Format::Human => match resp.restarted_from {
                    Some(c) => println!("rebuilt from the snapshot file taken at position {c}"),
                    None => {
                        println!("rebuilt from scratch: every instance re-derived from its events")
                    }
                },
            }
        }
    }
    Ok(())
}
