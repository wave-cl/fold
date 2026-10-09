use clap::Subcommand;
use fold_proto::common::v1::{
    DeleteSnapshotRequest, ListSnapshotsRequest, RebuildRequest, SnapshotRequest,
};
use fold_proto::derivation::v1::ListProjectionsRequest;

use crate::client::{self, Addrs};
use crate::output::{self, Format};

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Every projection with its state and checkpoint (DeriveAdmin.ListProjections).
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

pub async fn run(cmd: Cmd, addrs: &Addrs, format: Format) -> anyhow::Result<()> {
    let mut admin = client::derive_admin(addrs).await?;
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
                .snapshot(SnapshotRequest { name: projection })
                .await?
                .into_inner();
            output::print_snapshot(format, &s);
        }
        Cmd::Snapshots { projection } => {
            let resp = admin
                .list_snapshots(ListSnapshotsRequest { name: projection })
                .await?
                .into_inner();
            for s in &resp.snapshots {
                output::print_snapshot(format, s);
            }
            if format == Format::Human && resp.snapshots.is_empty() {
                println!("no snapshots");
            }
        }
        Cmd::DropSnapshot { projection, id } => {
            admin
                .delete_snapshot(DeleteSnapshotRequest {
                    name: projection,
                    id,
                })
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
                .rebuild(RebuildRequest {
                    name: projection,
                    snapshot_id: from.unwrap_or_default(),
                    force,
                })
                .await?
                .into_inner();
            output::print_rebuild(
                format,
                resp.restarted_from,
                "the projection; it catches up in the background",
            );
        }
    }
    Ok(())
}
