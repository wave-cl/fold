use clap::Subcommand;
use fold_proto::v1::ListProjectionsRequest;

use crate::client;
use crate::output::{self, Format};

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Every projection with its state and checkpoint (Admin.ListProjections).
    List,
}

pub async fn run(cmd: Cmd, addr: &str, format: Format) -> anyhow::Result<()> {
    match cmd {
        Cmd::List => {
            let resp = client::admin(addr)
                .await?
                .list_projections(ListProjectionsRequest {})
                .await?
                .into_inner();
            output::print_statuses(format, &resp.projections);
        }
    }
    Ok(())
}
