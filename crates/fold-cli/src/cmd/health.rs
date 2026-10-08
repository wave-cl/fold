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
                    "promoted_from": if h.promoted_from.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(h.promoted_from.clone()) },
                    "epoch": h.epoch, "fenced_by": h.fenced_by, "old_primary_fenced": h.old_primary_fenced,
                    "quorum_size": h.quorum_size, "last_election": if h.last_election.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(h.last_election.clone()) },
                    "lease_secs": h.lease_secs, "lease_held": h.lease_held, "lease_remaining_ms": h.lease_remaining_ms,
                    "lease_error": if h.lease_error.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(h.lease_error.clone()) } })
        ),
        Format::Human => {
            println!(
                "{} (foldd {}, {}, epoch {}), up {}s, head at position {}, log {}",
                h.status, h.version, h.role, h.epoch, h.uptime_secs, h.head, h.log_id
            );
            if let Some(by) = h.fenced_by {
                println!("fenced by a primary at epoch {by}: not taking writes or reads");
            }
            if h.lease_secs > 0 {
                println!(
                    "lease of {}s: {}{}",
                    h.lease_secs,
                    if h.lease_held {
                        format!("held, {} ms left", h.lease_remaining_ms)
                    } else {
                        "not held: reads refused".to_string()
                    },
                    if h.lease_error.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", h.lease_error)
                    }
                );
            }
            if !h.promoted_from.is_empty() {
                println!(
                    "old primary {}: {}",
                    h.promoted_from,
                    if h.old_primary_fenced {
                        "fenced"
                    } else {
                        "not yet fenced"
                    }
                );
            }
            if !h.promotion.is_empty() {
                println!("{}", h.promotion);
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
                if h.auto_failover_secs > 0 {
                    println!(
                        "automatic failover after {}s out of reach{}, quorum of {}{}",
                        h.auto_failover_secs,
                        h.primary_unreachable_secs
                            .map(|s| format!(" (out of reach for {s}s)"))
                            .unwrap_or_default(),
                        h.quorum_size,
                        if h.last_election.is_empty() {
                            String::new()
                        } else {
                            format!("; last election {}", h.last_election)
                        }
                    );
                }
            }
            if !h.last_restore.is_empty() {
                println!("last restore: {}", h.last_restore);
            }
        }
    }
    Ok(())
}
