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
            json!({ "status": h.status, "version": h.version, "uptime_secs": h.uptime_secs, "head": h.head,
                    "log_id": h.log_id, "last_restore": if h.last_restore.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(h.last_restore.clone()) } })
        ),
        Format::Human => {
            println!(
                "{} (foldd {}), up {}s, head at position {}, log {}",
                h.status, h.version, h.uptime_secs, h.head, h.log_id
            );
            if !h.last_restore.is_empty() {
                println!("last restore: {}", h.last_restore);
            }
        }
    }
    Ok(())
}
