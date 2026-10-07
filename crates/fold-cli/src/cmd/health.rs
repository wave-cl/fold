use fold_proto::v1::HealthRequest;
use serde_json::json;

use crate::client;
use crate::output::Format;

pub async fn run(addr: &str, format: Format) -> anyhow::Result<()> {
    let h = client::admin(addr)
        .await?
        .health(HealthRequest {})
        .await?
        .into_inner();
    match format {
        Format::Json => println!(
            "{}",
            json!({ "status": h.status, "version": h.version, "uptime_secs": h.uptime_secs, "head": h.head })
        ),
        Format::Human => println!(
            "{} (foldd {}), up {}s, head at position {}",
            h.status, h.version, h.uptime_secs, h.head
        ),
    }
    Ok(())
}
