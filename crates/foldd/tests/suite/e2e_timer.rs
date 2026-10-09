//! Process timers: set by a reaction, fired by the primary as an appended
//! `Fold.TimerFired` event, reacted to by everyone exactly once.

use std::time::{Duration, Instant};

use fold_proto::common::v1::expected_version::Kind;
use serde_json::{Value, json};
use tonic::Code;

use crate::common::{Daemon, line, settle, state_of, uuid};

const PROC: &str = "Orders.Fulfilment";
const TIMER_STREAM: &str = "fold-timers-Orders.Fulfilment";

async fn place(d: &Daemon, a: &str, overdue_after_ms: Option<u64>) -> u64 {
    d.exec_with_meta(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": uuid('c', 1), "lines": [line(&uuid('1', 1), 1, "1.00")] }),
        match overdue_after_ms {
            Some(ms) => json!({ "overdue_after_ms": ms }),
            None => Value::Null,
        },
    )
    .await
    .expect("place")
    .last_position
}

async fn status(d: &Daemon) -> fold_proto::application::v1::ProcessStatus {
    d.processes()
        .await
        .into_iter()
        .find(|p| p.name == PROC)
        .expect("the process is listed")
}

async fn order_status(d: &Daemon, a: &str) -> Value {
    state_of(&d.aggregate(&format!("order-{a}")).await.unwrap())["status"].clone()
}

/// Waits up to `secs` seconds for `cond`; panics with `what` and a dump
/// of `daemons` (process statuses and events) otherwise.
async fn until(secs: u64, what: &str, daemons: &[&Daemon], mut cond: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !cond().await {
        if Instant::now() >= deadline {
            let mut dump = String::new();
            for d in daemons {
                let events: Vec<String> = d
                    .all_events()
                    .await
                    .iter()
                    .map(|e| format!("{}@{}:{}", e.position, e.stream_id, e.r#type))
                    .collect();
                dump.push_str(&format!(
                    "\n{} ({}): {:?}\n  events: {events:?}",
                    d.addr,
                    d.health().await.role,
                    status(d).await
                ));
            }
            panic!("timed out waiting for: {what}{dump}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn fired_events(d: &Daemon) -> Vec<fold_proto::common::v1::RecordedEvent> {
    d.all_events()
        .await
        .into_iter()
        .filter(|e| e.r#type == "Fold.TimerFired@v1")
        .collect()
}

#[tokio::test]
async fn a_timer_fires_after_its_delay_and_the_reaction_issues_a_command() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let a = uuid('a', 1);
    let b = uuid('b', 1);
    // Two seconds: long enough that the settle below, which runs the
    // fulfilment chain for both orders, reads the timer as still pending
    // on a slow machine; the wait for the firing is bounded separately.
    place(&d, &a, Some(2_000)).await;
    place(&d, &b, None).await;
    settle(&d, &[format!("shipment-{a}"), format!("shipment-{b}")]).await;
    assert_eq!(status(&d).await.pending_timers, 1, "one order set a timer");
    assert_eq!(order_status(&d, &a).await, "Pending");

    until(10, "the overdue order is cancelled", &[&d], async || {
        order_status(&d, &a).await == "Cancelled"
    })
    .await;
    settle(&d, &[]).await;
    // The reaction's command carried the reason; the shipment followed.
    let events = d.all_events().await;
    let cancelled = events
        .iter()
        .find(|e| e.r#type == "Orders.OrderCancelled@v1" && e.stream_id == format!("order-{a}"))
        .unwrap();
    let payload: Value = serde_json::from_slice(&cancelled.payload).unwrap();
    assert_eq!(payload["reason"], "shipment overdue");
    assert_eq!(
        state_of(&d.aggregate(&format!("shipment-{a}")).await.unwrap())["stage"],
        "Cancelled"
    );
    // One fired event, on the process's own stream, naming the timer.
    let fired = fired_events(&d).await;
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].stream_id, TIMER_STREAM);
    let p: Value = serde_json::from_slice(&fired[0].payload).unwrap();
    assert_eq!(p["process"], PROC);
    assert_eq!(p["name"], "ShipmentOverdue");
    assert_eq!(p["instance"], a);
    assert_eq!(status(&d).await.pending_timers, 0);
    // The control: the order without the metadata is untouched.
    assert_eq!(order_status(&d, &b).await, "Pending");
    d.shutdown().await;
}

#[tokio::test]
async fn shipping_before_the_deadline_cancels_the_timer() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let a = uuid('a', 2);
    place(&d, &a, Some(1500)).await;
    settle(&d, &[format!("shipment-{a}")]).await;
    assert_eq!(status(&d).await.pending_timers, 1);
    d.exec(
        "Shipping.Shipment.Ship",
        &format!("shipment-{a}"),
        json!({}),
    )
    .await
    .unwrap();
    until(10, "the timer is cancelled", &[&d], async || {
        status(&d).await.pending_timers == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(2000)).await;
    assert_eq!(order_status(&d, &a).await, "Pending");
    assert!(fired_events(&d).await.is_empty(), "nothing fired");
    d.shutdown().await;
}

#[tokio::test]
async fn a_pending_timer_survives_a_restart() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    // Restarted long before it is due: the restarted runner loads it from
    // the table. (The delay is long so that a slow machine cannot fire it
    // before the restart; firing after a restart is the second half.)
    let a = uuid('a', 3);
    place(&d, &a, Some(120_000)).await;
    settle(&d, &[format!("shipment-{a}")]).await;
    d.restart().await;
    until(
        10,
        "the restarted runner lists the timer",
        &[&d],
        async || status(&d).await.pending_timers == 1,
    )
    .await;
    assert_eq!(order_status(&d, &a).await, "Pending");
    // Due while the daemon was down: it fires at start.
    let b = uuid('b', 3);
    place(&d, &b, Some(300)).await;
    settle(&d, &[format!("shipment-{b}")]).await;
    d.shutdown().await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    d.restart().await;
    until(10, "the overdue timer fires at start", &[&d], async || {
        order_status(&d, &b).await == "Cancelled"
    })
    .await;
    assert_eq!(fired_events(&d).await.len(), 1, "only b's timer fired");
    until(10, "a's timer is still pending", &[&d], async || {
        status(&d).await.pending_timers == 1
    })
    .await;
    d.shutdown().await;
}

#[tokio::test]
async fn a_timer_fired_twice_by_a_rebuild_is_harmless() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let a = uuid('a', 4);
    place(&d, &a, Some(300)).await;
    until(10, "cancelled", &[&d], async || {
        order_status(&d, &a).await == "Cancelled"
    })
    .await;
    settle(&d, &[]).await;
    let version_before = d.aggregate(&format!("order-{a}")).await.unwrap().version;
    let head_before = d.health().await.head;

    // Rebuild the process from scratch: it replays the fired event.
    d.rebuild_process(PROC, "", false).await.unwrap();
    settle(&d, &[]).await;
    assert_eq!(
        d.aggregate(&format!("order-{a}")).await.unwrap().version,
        version_before,
        "no second cancellation"
    );
    assert_eq!(d.health().await.head, head_before, "nothing appended");
    assert_eq!(fired_events(&d).await.len(), 1);
    assert_eq!(status(&d).await.pending_timers, 0);
    d.shutdown().await;
}

#[tokio::test]
async fn a_replica_reacts_to_the_primarys_timer_and_never_fires_its_own() {
    let mut primary = Daemon::start(|s| s.to_string()).await;
    let primary_addr = primary.addr.clone();
    let mut replica = Daemon::start_with(
        |s| s.to_string(),
        move |o| o.replicate_from = Some(primary_addr.clone()),
    )
    .await;
    let a = uuid('a', 5);
    place(&primary, &a, Some(800)).await;
    until(
        10,
        "the replica sees the pending timer",
        &[&primary, &replica],
        async || status(&replica).await.pending_timers == 1,
    )
    .await;
    until(
        15,
        "the primary's timer cancels the order everywhere",
        &[&primary, &replica],
        async || {
            order_status(&primary, &a).await == "Cancelled"
                && replica
                    .aggregate(&format!("order-{a}"))
                    .await
                    .map(|g| g.found && state_of(&g)["status"] == "Cancelled")
                    .unwrap_or(false)
        },
    )
    .await;
    until(
        10,
        "the replica consumed the fired timer",
        &[&primary, &replica],
        async || status(&replica).await.pending_timers == 0,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(fired_events(&primary).await.len(), 1, "fired once");
    assert_eq!(
        fired_events(&replica).await.len(),
        1,
        "replicated, not re-fired"
    );

    // A promotion: the pending timer fires on the promoted replica.
    let b = uuid('b', 5);
    place(&primary, &b, Some(2000)).await;
    until(
        10,
        "the replica sees the second timer",
        &[&primary, &replica],
        async || status(&replica).await.pending_timers == 1,
    )
    .await;
    primary.shutdown().await;
    replica
        .cluster()
        .await
        .promote(fold_proto::database::v1::PromoteRequest {})
        .await
        .expect("promoted");
    until(
        15,
        "the promoted replica fires it",
        &[&replica],
        async || order_status(&replica, &b).await == "Cancelled",
    )
    .await;
    assert_eq!(fired_events(&replica).await.len(), 2);
    // The drained outbox was already executed by the old primary, and the
    // reaction to the new cancellation runs its command to completion.
    until(
        10,
        "the promoted replica's outbox drains",
        &[&replica],
        async || {
            let p = status(&replica).await;
            assert!(p.error.is_empty(), "the process failed: {p:?}");
            p.pending_commands == 0
        },
    )
    .await;
    replica.shutdown().await;
}

#[tokio::test]
async fn fold_events_cannot_be_appended_by_clients() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let err = d
        .append(
            TIMER_STREAM,
            "Fold.TimerFired@v1",
            json!({ "process": PROC, "instance": uuid('a', 6), "name": "ShipmentOverdue", "due_at": "2026-01-01T00:00:00Z" }),
            Kind::Any(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    assert!(err.message().contains("reserved"), "{err}");
    d.shutdown().await;
}

#[tokio::test]
async fn an_undeclared_timer_fails_the_process() {
    let mut d = Daemon::start(|s| {
        assert!(s.contains("  timers ShipmentOverdue\n"));
        s.replace("  timers ShipmentOverdue\n", "")
    })
    .await;
    let a = uuid('a', 7);
    place(&d, &a, Some(300)).await;
    until(10, "the process fails", &[&d], async || {
        let p = status(&d).await;
        p.error.contains("undeclared timer `ShipmentOverdue`")
    })
    .await;
    assert_eq!(fired_events(&d).await.len(), 0);
    d.shutdown().await;
}
