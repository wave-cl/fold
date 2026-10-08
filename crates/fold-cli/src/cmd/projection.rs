use clap::Subcommand;
use fold_proto::v1::{
    DeleteSnapshotRequest, ListProjectionsRequest, ListSnapshotsRequest, RebuildProjectionRequest,
    SnapshotInfo, SnapshotProjectionRequest,
};
use serde_json::json;

use crate::client;
use crate::output::{self, Format};

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Every projection with its state and checkpoint (Admin.ListProjections).
    List,
    /// Write a snapshot of a projection at its current checkpoint.
    Snapshot {
        /// "Context.Projection"
        projection: String,
    },
    /// List a projection's snapshots, newest first.
    Snapshots { projection: String },
    /// Delete one snapshot.
    DropSnapshot { projection: String, id: String },
    /// Reset a projection and replay: from scratch, or from a snapshot.
    Rebuild {
        projection: String,
        /// Start from this snapshot instead of position 0.
        #[arg(long)]
        from: Option<String>,
        /// Use a snapshot made by a different fold module.
        #[arg(long)]
        force: bool,
    },
}

fn print_snapshot(format: Format, s: &SnapshotInfo) {
    match format {
        Format::Json => println!(
            "{}",
            json!({
                "id": s.id, "projection": s.projection, "checkpoint": s.checkpoint, "rows": s.rows,
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
                "  (fold module has changed)"
            }
        ),
    }
}

pub async fn run(cmd: Cmd, addr: &str, format: Format) -> anyhow::Result<()> {
    let mut admin = client::admin(addr).await?;
    match cmd {
        Cmd::List => {
            let resp = admin
                .list_projections(ListProjectionsRequest {})
                .await?
                .into_inner();
            output::print_statuses(format, &resp.projections);
        }
        Cmd::Snapshot { projection } => {
            let s = admin
                .snapshot_projection(SnapshotProjectionRequest { projection })
                .await?
                .into_inner();
            print_snapshot(format, &s);
        }
        Cmd::Snapshots { projection } => {
            let resp = admin
                .list_snapshots(ListSnapshotsRequest { projection })
                .await?
                .into_inner();
            for s in &resp.snapshots {
                print_snapshot(format, s);
            }
            if format == Format::Human && resp.snapshots.is_empty() {
                println!("no snapshots");
            }
        }
        Cmd::DropSnapshot { projection, id } => {
            admin
                .delete_snapshot(DeleteSnapshotRequest { projection, id })
                .await?;
            if format == Format::Human {
                println!("deleted");
            }
        }
        Cmd::Rebuild {
            projection,
            from,
            force,
        } => {
            let resp = admin
                .rebuild_projection(RebuildProjectionRequest {
                    projection,
                    snapshot_id: from.unwrap_or_default(),
                    force,
                })
                .await?
                .into_inner();
            match format {
                Format::Json => println!("{}", json!({ "restarted_from": resp.restarted_from })),
                Format::Human => match resp.restarted_from {
                    Some(c) => println!(
                        "rebuilding from snapshot at checkpoint {c}; catching up in the background"
                    ),
                    None => println!("rebuilding from scratch; catching up in the background"),
                },
            }
        }
    }
    Ok(())
}
