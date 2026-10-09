//! `fold health`: every layer's health, each from its own admin service.
//! A layer that does not answer is reported as unreachable and makes the
//! command exit 2; the others are still printed.

use fold_proto::application::v1 as app;
use fold_proto::database::v1 as db;
use fold_proto::derivation::v1 as derive;
use serde_json::{Value, json};

use crate::client::{self, Addrs};
use crate::output::Format;

fn opt(s: &str) -> Value {
    if s.is_empty() {
        Value::Null
    } else {
        Value::String(s.to_string())
    }
}

async fn database(addrs: &Addrs) -> anyhow::Result<db::HealthResponse> {
    Ok(client::cluster(addrs)
        .await?
        .health(db::HealthRequest {})
        .await?
        .into_inner())
}

async fn derivation(addrs: &Addrs) -> anyhow::Result<derive::HealthResponse> {
    Ok(client::derive_admin(addrs)
        .await?
        .health(derive::HealthRequest {})
        .await?
        .into_inner())
}

async fn application(addrs: &Addrs) -> anyhow::Result<app::HealthResponse> {
    Ok(client::app_admin(addrs)
        .await?
        .health(app::HealthRequest {})
        .await?
        .into_inner())
}

fn database_json(h: &db::HealthResponse) -> Value {
    json!({ "status": h.status, "version": h.version, "uptime_secs": h.uptime_secs, "head": h.head,
            "log_id": h.log_id, "last_restore": opt(&h.last_restore),
            "role": h.role, "replicating_from": opt(&h.replicating_from),
            "replica_connected": h.replica_connected, "primary_head": h.primary_head,
            "replication_error": opt(&h.replication_error),
            "promoted_from": opt(&h.promoted_from),
            "epoch": h.epoch, "fenced_by": h.fenced_by, "old_primary_fenced": h.old_primary_fenced,
            "quorum_size": h.quorum_size, "last_election": opt(&h.last_election),
            "lease_secs": h.lease_secs, "lease_held": h.lease_held, "lease_remaining_ms": h.lease_remaining_ms,
            "lease_error": opt(&h.lease_error),
            "last_schema_change": opt(&h.last_schema_change),
            "generation": h.generation, "cut": h.cut })
}

fn derivation_json(h: &derive::HealthResponse) -> Value {
    json!({ "status": h.status, "version": h.version, "uptime_secs": h.uptime_secs,
            "log_id": h.log_id, "database": h.database, "database_connected": h.database_connected,
            "database_role": h.database_role, "database_head": h.database_head,
            "tail_position": h.tail_position, "lag": h.lag, "database_error": opt(&h.database_error),
            "last_reset": opt(&h.last_reset), "last_schema_change": opt(&h.last_schema_change),
            "generation": h.generation })
}

fn application_json(h: &app::HealthResponse) -> Value {
    json!({ "status": h.status, "version": h.version, "uptime_secs": h.uptime_secs,
            "log_id": h.log_id, "database": h.database, "derivation": h.derivation,
            "database_connected": h.database_connected, "database_role": h.database_role,
            "database_head": h.database_head, "tail_position": h.tail_position,
            "layer_check": h.layer_check, "last_reset": opt(&h.last_reset),
            "last_schema_change": opt(&h.last_schema_change), "generation": h.generation,
            "invariants": h.invariants })
}

fn print_database(h: &db::HealthResponse) {
    println!(
        "database: {} (fold {}, {}, epoch {}), up {}s, head at position {}, log {}",
        h.status, h.version, h.role, h.epoch, h.uptime_secs, h.head, h.log_id
    );
    if h.generation > 0 {
        println!("  generation {} (cut at {})", h.generation, h.cut);
    }
    if !h.last_schema_change.is_empty() {
        println!("  schema change at start: {}", h.last_schema_change);
    }
    if let Some(by) = h.fenced_by {
        println!("  fenced by a primary at epoch {by}: not taking writes or reads");
    }
    if h.lease_secs > 0 {
        println!(
            "  lease of {}s: {}{}",
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
            "  old primary {}: {}",
            h.promoted_from,
            if h.old_primary_fenced {
                "fenced"
            } else {
                "not yet fenced"
            }
        );
    }
    if !h.promotion.is_empty() {
        println!("  {}", h.promotion);
    }
    if !h.replicating_from.is_empty() {
        println!(
            "  replicating from {}: {}{}{}",
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
                "  automatic failover after {}s out of reach{}, quorum of {}{}",
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
        println!("  last restore: {}", h.last_restore);
    }
}

fn print_derivation(h: &derive::HealthResponse) {
    println!(
        "derivation: {} (fold {}), up {}s, tail at {} of the database's head {} ({}, lag {})",
        h.status,
        h.version,
        h.uptime_secs,
        h.tail_position,
        h.database_head,
        if h.database_connected {
            format!("connected to {} as {}", h.database, h.database_role)
        } else {
            format!("not connected to {}", h.database)
        },
        h.lag
    );
    if !h.database_error.is_empty() {
        println!("  database error: {}", h.database_error);
    }
    if !h.last_reset.is_empty() {
        println!("  last reset: {}", h.last_reset);
    }
    if !h.last_schema_change.is_empty() {
        println!("  schema change at start: {}", h.last_schema_change);
    }
}

fn print_application(h: &app::HealthResponse) {
    println!(
        "application: {} (fold {}), up {}s, layer check {}, database {} ({}), derivation {}",
        h.status,
        h.version,
        h.uptime_secs,
        h.layer_check,
        h.database,
        if h.database_connected {
            h.database_role.as_str()
        } else {
            "not connected"
        },
        h.derivation
    );
    println!(
        "  process managers' tail at {} of head {}; invariants: {}",
        h.tail_position, h.database_head, h.invariants
    );
    if !h.last_reset.is_empty() {
        println!("  last reset: {}", h.last_reset);
    }
    if !h.last_schema_change.is_empty() {
        println!("  schema change at start: {}", h.last_schema_change);
    }
}

pub async fn run(addrs: &Addrs, format: Format) -> anyhow::Result<()> {
    let (database, derivation, application) = tokio::join!(
        self::database(addrs),
        self::derivation(addrs),
        self::application(addrs)
    );
    let mut unreachable = Vec::new();
    match format {
        Format::Json => {
            let mut layer = |name: &str, r: Result<Value, &anyhow::Error>| match r {
                Ok(v) => v,
                Err(e) => {
                    unreachable.push(name.to_string());
                    json!({ "error": crate::output::render_error(e) })
                }
            };
            let out = json!({
                "database": layer("database", database.as_ref().map(database_json)),
                "derivation": layer("derivation", derivation.as_ref().map(derivation_json)),
                "application": layer("application", application.as_ref().map(application_json)),
            });
            println!("{out}");
        }
        Format::Human => {
            match &database {
                Ok(h) => print_database(h),
                Err(e) => {
                    println!("database: {}", crate::output::render_error(e));
                    unreachable.push("database".to_string());
                }
            }
            match &derivation {
                Ok(h) => print_derivation(h),
                Err(e) => {
                    println!("derivation: {}", crate::output::render_error(e));
                    unreachable.push("derivation".to_string());
                }
            }
            match &application {
                Ok(h) => print_application(h),
                Err(e) => {
                    println!("application: {}", crate::output::render_error(e));
                    unreachable.push("application".to_string());
                }
            }
        }
    }
    if unreachable.is_empty() {
        return Ok(());
    }
    // The layers that answered were printed; the first failure is the
    // error, so a connection failure keeps its type and exits 2.
    let first = [database.err(), derivation.err(), application.err()]
        .into_iter()
        .flatten()
        .next()
        .expect("a layer failed");
    Err(first.context(format!("{} did not answer", unreachable.join(", "))))
}
