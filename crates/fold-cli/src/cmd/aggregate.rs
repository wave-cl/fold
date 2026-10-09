use clap::Subcommand;
use fold_proto::common::v1::{
    DeleteSnapshotRequest, ListSnapshotsRequest, RebuildRequest, SnapshotRequest,
};

use crate::client::{self, Addrs};
use crate::output::{self, Format};

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

pub async fn run(cmd: Cmd, addrs: &Addrs, format: Format) -> anyhow::Result<()> {
    let mut admin = client::derive_admin(addrs).await?;
    match cmd {
        Cmd::Snapshot { aggregate } => {
            let s = admin
                .snapshot(SnapshotRequest { name: aggregate })
                .await?
                .into_inner();
            output::print_snapshot(format, &s);
        }
        Cmd::Snapshots { aggregate } => {
            let resp = admin
                .list_snapshots(ListSnapshotsRequest { name: aggregate })
                .await?
                .into_inner();
            for s in &resp.snapshots {
                output::print_snapshot(format, s);
            }
            if format == Format::Human && resp.snapshots.is_empty() {
                println!("no snapshots");
            }
        }
        Cmd::DropSnapshot { aggregate, id } => {
            admin
                .delete_snapshot(DeleteSnapshotRequest {
                    name: aggregate,
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
                .rebuild(RebuildRequest {
                    name: aggregate,
                    snapshot_id: from.unwrap_or_default(),
                    force,
                })
                .await?
                .into_inner();
            output::print_rebuild(
                format,
                resp.restarted_from,
                "the aggregate: every instance re-derived from its events",
            );
        }
    }
    Ok(())
}
