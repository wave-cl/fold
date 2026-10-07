//! The Fulfilment process manager: reacts to order and shipment events,
//! issues shipment commands, learns about a refused cancellation, ends.

use std::time::{Duration, Instant};

use fold_proto::v1::{GetProcessRequest, ListProcessesRequest};
use serde_json::{Value, json};

use crate::common::{Daemon, line, state_of, uuid};

const PROC: &str = "Orders.Fulfilment";

/// Waits until the process has reacted past `position` and its outbox is
/// empty: every command it issued for that position has been executed.
async fn settled(d: &Daemon, position: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let p = d
            .admin()
            .await
            .list_processes(ListProcessesRequest {})
            .await
            .unwrap()
            .into_inner()
            .processes
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
        .log()
        .await
        .get_process(GetProcessRequest {
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
    let head = d
        .admin()
        .await
        .health(fold_proto::v1::HealthRequest {})
        .await
        .unwrap()
        .into_inner()
        .head;
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
    let head = d
        .admin()
        .await
        .health(fold_proto::v1::HealthRequest {})
        .await
        .unwrap()
        .into_inner()
        .head;
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
    let head = d
        .admin()
        .await
        .health(fold_proto::v1::HealthRequest {})
        .await
        .unwrap()
        .into_inner()
        .head;
    settled(&d, head - 1).await;
    assert_eq!(
        d.aggregate(&format!("shipment-{a}")).await.unwrap().version,
        before
    );
    let p = d
        .admin()
        .await
        .list_processes(ListProcessesRequest {})
        .await
        .unwrap()
        .into_inner()
        .processes
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
    let head = d
        .admin()
        .await
        .health(fold_proto::v1::HealthRequest {})
        .await
        .unwrap()
        .into_inner()
        .head;
    settled(&d, head - 1).await;

    // The shipment leaves before the customer changes their mind.
    d.exec(
        "Shipping.Shipment.Ship",
        &format!("shipment-{a}"),
        json!({}),
    )
    .await
    .unwrap();
    let head = d
        .admin()
        .await
        .health(fold_proto::v1::HealthRequest {})
        .await
        .unwrap()
        .into_inner()
        .head;
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
        .admin()
        .await
        .list_processes(ListProcessesRequest {})
        .await
        .unwrap()
        .into_inner()
        .processes
        .into_iter()
        .find(|p| p.name == PROC)
        .unwrap();
    assert_eq!(p.rejected, 1);
    assert_eq!(p.dispatched, 1, "Prepare was executed; Cancel was refused");

    // Negatives on GetProcess.
    assert!(instance(&d, &uuid('a', 9)).await.is_none());
    let err = d
        .log()
        .await
        .get_process(GetProcessRequest {
            process: "Orders.Nope".into(),
            key: b"\"x\"".to_vec(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
    d.shutdown().await;
}
