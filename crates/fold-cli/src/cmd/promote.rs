use fold_proto::database::v1::PromoteRequest;
use serde_json::json;

use crate::client::{self, Addrs};
use crate::output::Format;

pub async fn run(addrs: &Addrs, format: Format) -> anyhow::Result<()> {
    let r = client::cluster(addrs)
        .await?
        .promote(PromoteRequest {})
        .await?
        .into_inner();
    match format {
        Format::Json => println!(
            "{}",
            json!({ "promoted": true, "head": r.head, "promoted_from": r.promoted_from })
        ),
        Format::Human => println!(
            "promoted: the database is now a primary at head {}, no longer tailing {}",
            r.head, r.promoted_from
        ),
    }
    Ok(())
}
