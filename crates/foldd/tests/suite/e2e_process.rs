//! The Fulfilment process manager: reacts to order and shipment events,
//! issues shipment commands, learns about a refused cancellation, ends.

use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::common::{Daemon, line, state_of, uuid};

const PROC: &str = "Orders.Fulfilment";

/// Waits until the process has reacted past `position` and its outbox is
/// empty: every command it issued for that position has been executed.
async fn settled(d: &Daemon, position: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let p = d
            .processes()
            .await
            .into_iter()
            .find(|p| p.name == PROC)
            .expect("the process is listed");
        assert!(p.error.is_empty(), "process reported an error: {}", p.error);
        if p.checkpoint.is_some_and(|c| c >= position) && p.pending_commands == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "process did not settle past {position}: {p:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn instance(d: &Daemon, order: &str) -> Option<Value> {
    let r = d
        .app_admin()
        .await
        .get_process(fold_proto::application::v1::GetProcessRequest {
            process: PROC.into(),
            key: serde_json::to_vec(&json!(order)).unwrap(),
        })
        .await
        .unwrap()
        .into_inner();
    r.found.then(|| serde_json::from_slice(&r.state).unwrap())
}

#[tokio::test]
async fn placing_an_order_has_a_shipment_prepared_and_cancelling_cancels_it() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 1);
    let a = uuid('a', 1);

    let placed = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "9.00")] }),
        )
        .await
        .unwrap();
    settled(&d, placed.last_position).await;

    // The process issued Shipping.Shipment.Prepare; the shipment exists.
    let shipment = d.aggregate(&format!("shipment-{a}")).await.unwrap();
    assert!(shipment.found, "the process prepared a shipment");
    assert_eq!(state_of(&shipment)["stage"], "Prepared");
    assert_eq!(state_of(&shipment)["order_id"], json!(a));

    // ...and reacted to the ShipmentPrepared event it caused.
    let head = d.health().await.head;
    settled(&d, head - 1).await;
    let s = instance(&d, &a).await.expect("instance exists");
    assert_eq!(s["shipment"], "prepared");
    assert_eq!(s["customer_id"], json!(c));
    assert_eq!(s["cancel_refused"], false);

    // Cancelling the order has the process cancel the shipment, then end.
    let cancelled = d
        .exec("Orders.Order.CancelOrder", &format!("order-{a}"), json!({}))
        .await
        .unwrap();
    settled(&d, cancelled.last_position).await;
    let head = d.health().await.head;
    settled(&d, head - 1).await;
    assert_eq!(
        state_of(&d.aggregate(&format!("shipment-{a}")).await.unwrap())["stage"],
        "Cancelled"
    );
    assert!(
        instance(&d, &a).await.is_none(),
        "the instance ended with the shipment"
    );

    // Restart: nothing is re-issued (the shipment stream has exactly two events).
    let before = d.aggregate(&format!("shipment-{a}")).await.unwrap().version;
    d.restart().await;
    let head = d.health().await.head;
    settled(&d, head - 1).await;
    assert_eq!(
        d.aggregate(&format!("shipment-{a}")).await.unwrap().version,
        before
    );
    let p = d
        .processes()
        .await
        .into_iter()
        .find(|p| p.name == PROC)
        .unwrap();
    assert_eq!(
        p.dispatched, 0,
        "nothing to dispatch after a restart with an empty outbox"
    );
    d.shutdown().await;
}

#[tokio::test]
async fn a_refused_command_comes_back_to_the_process_as_a_trigger() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 2);
    let a = uuid('a', 2);
    let placed = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "9.00")] }),
        )
        .await
        .unwrap();
    settled(&d, placed.last_position).await;
    let head = d.health().await.head;
    settled(&d, head - 1).await;

    // The shipment leaves before the customer changes their mind.
    d.exec(
        "Shipping.Shipment.Ship",
        &format!("shipment-{a}"),
        json!({}),
    )
    .await
    .unwrap();
    let head = d.health().await.head;
    settled(&d, head - 1).await;
    assert_eq!(instance(&d, &a).await.unwrap()["shipment"], "shipped");

    let cancelled = d
        .exec("Orders.Order.CancelOrder", &format!("order-{a}"), json!({}))
        .await
        .unwrap();
    settled(&d, cancelled.last_position).await;

    // Shipment.Cancel was refused with ALREADY_SHIPPED; the process recorded it.
    let s = instance(&d, &a).await.expect("still tracked");
    assert_eq!(s["cancel_refused"], true);
    assert_eq!(s["shipment"], "shipped");
    assert_eq!(
        state_of(&d.aggregate(&format!("shipment-{a}")).await.unwrap())["stage"],
        "Shipped"
    );
    let p = d
        .processes()
        .await
        .into_iter()
        .find(|p| p.name == PROC)
        .unwrap();
    assert_eq!(p.rejected, 1);
    assert_eq!(p.dispatched, 1, "Prepare was executed; Cancel was refused");

    // Negatives on GetProcess.
    assert!(instance(&d, &uuid('a', 9)).await.is_none());
    let err = d
        .app_admin()
        .await
        .get_process(fold_proto::application::v1::GetProcessRequest {
            process: "Orders.Nope".into(),
            key: b"\"x\"".to_vec(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
    d.shutdown().await;
}

/// A process can be snapshotted and rebuilt, and a rebuild never re-issues
/// a command: outbox ids derive from positions, so the replayed commands
/// hit the log's idempotency keys and are skipped.
#[tokio::test]
async fn a_process_rebuild_replays_without_reissuing_commands() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 3);
    let a = uuid('a', 3);
    let b = uuid('b', 3);
    for id in [&a, &b] {
        let placed = d
            .exec(
                "Orders.Order.PlaceOrder",
                &format!("order-{id}"),
                json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "9.00")] }),
            )
            .await
            .unwrap();
        settled(&d, placed.last_position).await;
    }
    let head = d.health().await.head;
    settled(&d, head - 1).await;
    let before_a = instance(&d, &a).await.expect("tracked");
    let before_b = instance(&d, &b).await.expect("tracked");
    let shipment_a = d.aggregate(&format!("shipment-{a}")).await.unwrap().version;
    let shipment_b = d.aggregate(&format!("shipment-{b}")).await.unwrap().version;
    let log_head = head;

    // Snapshot the process: two instances, empty outbox.
    let snap = d.snapshot_process(PROC).await.unwrap();
    assert_eq!(snap.rows, 2, "two instances, nothing pending");
    assert!(snap.module_matches);
    let listed = d.process_snapshots(PROC).await;
    assert_eq!(listed.len(), 1);

    // Rebuild from scratch: every reaction runs again, every command is
    // recognised as already executed, the log does not grow.
    let resp = d.rebuild_process(PROC, "", false).await.unwrap();
    assert_eq!(resp.restarted_from, None);
    settled(&d, log_head - 1).await;
    let after_head = d.health().await.head;
    assert_eq!(after_head, log_head, "no command was re-issued");
    assert_eq!(
        d.aggregate(&format!("shipment-{a}")).await.unwrap().version,
        shipment_a
    );
    assert_eq!(
        d.aggregate(&format!("shipment-{b}")).await.unwrap().version,
        shipment_b
    );
    assert_eq!(instance(&d, &a).await.unwrap(), before_a);
    assert_eq!(instance(&d, &b).await.unwrap(), before_b);
    let p = d
        .processes()
        .await
        .into_iter()
        .find(|p| p.name == PROC)
        .unwrap();
    assert_eq!(p.pending_commands, 0);

    // Rebuild from the snapshot: restarts at its checkpoint, same state.
    let resp = d.rebuild_process(PROC, &snap.id, false).await.unwrap();
    assert_eq!(resp.restarted_from, Some(snap.checkpoint));
    settled(&d, log_head - 1).await;
    assert_eq!(instance(&d, &a).await.unwrap(), before_a);
    assert_eq!(instance(&d, &b).await.unwrap(), before_b);
    let after_head = d.health().await.head;
    assert_eq!(after_head, log_head);

    // Still alive afterwards: a new order is handled as before.
    let e = uuid('e', 3);
    let placed = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{e}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "9.00")] }),
        )
        .await
        .unwrap();
    settled(&d, placed.last_position).await;
    let head = d.health().await.head;
    settled(&d, head - 1).await;
    assert_eq!(instance(&d, &e).await.unwrap()["shipment"], "prepared");
    d.shutdown().await;
}

#[tokio::test]
async fn a_process_snapshots_itself_when_asked_to() {
    let mut d = Daemon::start(|s| {
        s.replace(
            "react wasm \"orders.wasm\" export \"react_fulfilment\"\n",
            "react wasm \"orders.wasm\" export \"react_fulfilment\"\n    snapshot every 2\n",
        )
    })
    .await;
    let c = uuid('c', 4);
    let mut last = 0;
    for n in 1..=3 {
        last = d
            .exec(
                "Orders.Order.PlaceOrder",
                &format!("order-{}", uuid('a', n)),
                json!({ "customer_id": c, "lines": [line(&uuid('1', n), 1, "1.00")] }),
            )
            .await
            .unwrap()
            .last_position;
    }
    settled(&d, last).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let snaps = d.process_snapshots(PROC).await;
        if !snaps.is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "no automatic process snapshot");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    d.shutdown().await;
}
