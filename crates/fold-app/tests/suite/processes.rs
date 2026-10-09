//! The Fulfilment process manager on the application node: it reacts to
//! events from the database's tail, issues commands through the node's own
//! command path, fires timers through the database with the system token,
//! and survives a restart without re-issuing anything.

use std::time::Duration;

use serde_json::{Value, json};

use crate::common::{Cluster, line, state_of, until, uuid};

const PROC: &str = "Orders.Fulfilment";
const TIMER_STREAM: &str = "fold-timers-Orders.Fulfilment";

async fn place(c: &Cluster, a: &str, overdue_after_ms: Option<u64>) -> u64 {
    c.exec_with_meta(
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

async fn order_status(c: &Cluster, a: &str) -> Value {
    state_of(&c.aggregate(&format!("order-{a}")).await.unwrap())["status"].clone()
}

async fn fired_events(c: &Cluster) -> Vec<fold_proto::common::v1::RecordedEvent> {
    c.all_events()
        .await
        .into_iter()
        .filter(|e| e.r#type == "Fold.TimerFired@v1")
        .collect()
}

#[tokio::test]
async fn placing_an_order_has_a_shipment_prepared_and_cancelling_cancels_it() {
    let mut c = Cluster::start().await;
    c.ready().await;
    let cust = uuid('c', 1);
    let a = uuid('a', 1);
    c.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": cust, "lines": [line(&uuid('1', 1), 1, "9.00")] }),
    )
    .await
    .unwrap();
    c.settle(&[format!("shipment-{a}")]).await;

    // The process issued Shipping.Shipment.Prepare through this node.
    let shipment = c.aggregate(&format!("shipment-{a}")).await.unwrap();
    assert_eq!(state_of(&shipment)["stage"], "Prepared");
    assert_eq!(state_of(&shipment)["order_id"], json!(a));
    let s = c.instance(PROC, json!(a)).await.expect("instance exists");
    assert_eq!(s["shipment"], "prepared");
    assert_eq!(s["customer_id"], json!(cust));
    assert_eq!(s["cancel_refused"], false);
    let p = c.process(PROC).await;
    assert_eq!(p.dispatched, 1);
    assert_eq!(p.pending_commands, 0);

    // Cancelling the order has the process cancel the shipment, then end.
    c.exec("Orders.Order.CancelOrder", &format!("order-{a}"), json!({}))
        .await
        .unwrap();
    c.settle(&[format!("shipment-{a}")]).await;
    assert_eq!(
        state_of(&c.aggregate(&format!("shipment-{a}")).await.unwrap())["stage"],
        "Cancelled"
    );
    assert!(
        c.instance(PROC, json!(a)).await.is_none(),
        "the instance ended"
    );

    // A restart of the application node: nothing is re-issued (the
    // shipment stream keeps exactly two events) and the checkpoint holds.
    let before = c.aggregate(&format!("shipment-{a}")).await.unwrap().version;
    let cp = c.process(PROC).await.checkpoint;
    c.restart_app().await;
    c.ready().await;
    assert_eq!(c.process(PROC).await.checkpoint, cp);
    c.settle(&[]).await;
    assert_eq!(
        c.aggregate(&format!("shipment-{a}")).await.unwrap().version,
        before
    );
    c.shutdown().await;
}

#[tokio::test]
async fn a_timer_fires_through_the_database_and_the_reaction_issues_a_command() {
    let mut c = Cluster::start().await;
    c.ready().await;
    let a = uuid('a', 2);
    let b = uuid('b', 2);
    place(&c, &a, Some(400)).await;
    place(&c, &b, None).await;
    c.settle(&[format!("shipment-{a}"), format!("shipment-{b}")])
        .await;
    assert_eq!(
        c.process(PROC).await.pending_timers,
        1,
        "one order set a timer"
    );
    assert_eq!(order_status(&c, &a).await, "Pending");

    until(10, "the overdue order is cancelled", async || {
        order_status(&c, &a).await == "Cancelled"
    })
    .await;
    c.settle(&[]).await;
    let events = c.all_events().await;
    let cancelled = events
        .iter()
        .find(|e| e.r#type == "Orders.OrderCancelled@v1" && e.stream_id == format!("order-{a}"))
        .unwrap();
    let payload: Value = serde_json::from_slice(&cancelled.payload).unwrap();
    assert_eq!(payload["reason"], "shipment overdue");
    assert_eq!(
        state_of(&c.aggregate(&format!("shipment-{a}")).await.unwrap())["stage"],
        "Cancelled"
    );
    // One fired event, on the process's own stream in the database.
    let fired = fired_events(&c).await;
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].stream_id, TIMER_STREAM);
    let p: Value = serde_json::from_slice(&fired[0].payload).unwrap();
    assert_eq!(p["process"], PROC);
    assert_eq!(p["name"], "ShipmentOverdue");
    assert_eq!(c.process(PROC).await.pending_timers, 0);
    assert_eq!(
        order_status(&c, &b).await,
        "Pending",
        "the control is untouched"
    );

    // Shipping before the deadline cancels a timer.
    let d = uuid('d', 2);
    place(&c, &d, Some(1500)).await;
    c.settle(&[format!("shipment-{d}")]).await;
    assert_eq!(c.process(PROC).await.pending_timers, 1);
    c.exec(
        "Shipping.Shipment.Ship",
        &format!("shipment-{d}"),
        json!({}),
    )
    .await
    .unwrap();
    until(10, "the timer is cancelled", async || {
        c.process(PROC).await.pending_timers == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1800)).await;
    assert_eq!(order_status(&c, &d).await, "Pending");
    assert_eq!(fired_events(&c).await.len(), 1, "nothing more fired");
    c.shutdown().await;
}

#[tokio::test]
async fn a_pending_timer_survives_a_restart_and_a_rebuild_refires_nothing() {
    let mut c = Cluster::start().await;
    c.ready().await;
    let a = uuid('a', 3);
    place(&c, &a, Some(300)).await;
    until(10, "cancelled", async || {
        order_status(&c, &a).await == "Cancelled"
    })
    .await;
    c.settle(&[]).await;
    let version_before = c.aggregate(&format!("order-{a}")).await.unwrap().version;
    let head_before = c.db_head().await;

    // Rebuild the process from scratch: it replays the fired event.
    c.app_admin()
        .await
        .rebuild(fold_proto::common::v1::RebuildRequest {
            name: PROC.into(),
            snapshot_id: String::new(),
            force: false,
        })
        .await
        .unwrap();
    c.settle(&[]).await;
    assert_eq!(
        c.aggregate(&format!("order-{a}")).await.unwrap().version,
        version_before,
        "no second cancellation"
    );
    assert_eq!(c.db_head().await, head_before, "nothing appended");
    assert_eq!(fired_events(&c).await.len(), 1);

    // A long timer survives a restart; a timer due while the node was down
    // fires at start.
    let b = uuid('b', 3);
    place(&c, &b, Some(120_000)).await;
    c.settle(&[format!("shipment-{b}")]).await;
    let d = uuid('d', 3);
    place(&c, &d, Some(300)).await;
    c.settle(&[format!("shipment-{d}")]).await;
    if let Some(app) = c.app.take() {
        app.shutdown().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(600)).await;
    c.restart_app().await;
    c.ready().await;
    until(10, "the overdue timer fires at start", async || {
        order_status(&c, &d).await == "Cancelled"
    })
    .await;
    until(10, "b's timer is still pending", async || {
        c.process(PROC).await.pending_timers == 1
    })
    .await;
    assert_eq!(fired_events(&c).await.len(), 2);
    c.shutdown().await;
}

#[tokio::test]
async fn an_undeclared_timer_fails_the_process() {
    let mut c = Cluster::start_with(
        |s| {
            assert!(s.contains("  timers ShipmentOverdue\n"));
            s.replace("  timers ShipmentOverdue\n", "")
        },
        |_| {},
    )
    .await;
    c.ready().await;
    let a = uuid('a', 4);
    place(&c, &a, Some(300)).await;
    until(10, "the process fails", async || {
        c.process(PROC)
            .await
            .error
            .contains("undeclared timer `ShipmentOverdue`")
    })
    .await;
    assert_eq!(fired_events(&c).await.len(), 0);
    c.shutdown().await;
}
