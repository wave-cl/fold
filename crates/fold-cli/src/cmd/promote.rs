use fold_proto::v1::PromoteRequest;
use serde_json::json;

use crate::client;
use crate::output::Format;

pub async fn run(addr: &str, format: Format) -> anyhow::Result<()> {
    let r = client::admin(addr)
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
            "promoted: this daemon is now a primary at head {}, no longer tailing {}",
            r.head, r.promoted_from
        ),
    }
    Ok(())
}
