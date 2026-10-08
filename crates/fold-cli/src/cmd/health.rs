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
                    "log_id": h.log_id, "last_restore": if h.last_restore.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(h.last_restore.clone()) },
                    "role": h.role, "replicating_from": if h.replicating_from.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(h.replicating_from.clone()) },
                    "replica_connected": h.replica_connected, "primary_head": h.primary_head,
                    "replication_error": if h.replication_error.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(h.replication_error.clone()) },
                    "promoted_from": if h.promoted_from.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(h.promoted_from.clone()) } })
        ),
        Format::Human => {
            println!(
                "{} (foldd {}, {}), up {}s, head at position {}, log {}",
                h.status, h.version, h.role, h.uptime_secs, h.head, h.log_id
            );
            if !h.promoted_from.is_empty() {
                println!("promoted from {}", h.promoted_from);
            }
            if !h.replicating_from.is_empty() {
                println!(
                    "replicating from {}: {}{}{}",
                    h.replicating_from,
                    if h.replica_connected {
                        "connected"
                    } else {
                        "not connected"
                    },
                    h.primary_head
                        .map(|p| format!(", primary head {p}"))
                        .unwrap_or_default(),
                    if h.replication_error.is_empty() {
                        String::new()
                    } else {
                        format!(", last error: {}", h.replication_error)
                    }
                );
            }
            if !h.last_restore.is_empty() {
                println!("last restore: {}", h.last_restore);
            }
        }
    }
    Ok(())
}
