//! Command → events → cross-aggregate projection → read-your-writes query.

use fold_proto::v1::expected_version::Kind;
use serde_json::json;
use tonic::Code;

use crate::common::{Daemon, line, rejection_code, uuid};

const PROJ: &str = "Orders.CustomerOrders";

#[tokio::test]
async fn commands_flow_into_the_customer_orders_read_model() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 1);
    let a = uuid('a', 1);
    let b = uuid('b', 1);

    d.exec(
        "Customers.Customer.Register",
        &format!("customer-{c}"),
        json!({ "name": "Ada" }),
    )
    .await
    .expect("register");
    d.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "15.00")] }),
    )
    .await
    .expect("place A");
    let placed_b = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{b}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 2), 1, "25.00")] }),
        )
        .await
        .expect("place B");
    assert_eq!(placed_b.events.len(), 1);
    assert_eq!(placed_b.version, Some(0));

    // Read-your-writes: no polling, the query waits for the position itself.
    let row = d
        .row(
            PROJ,
            "customer_orders",
            json!({ "customer_id": c }),
            placed_b.last_position,
        )
        .await;
    assert_eq!(row["name"], "Ada");
    assert_eq!(row["open_orders"], json!([a, b]));
    assert_eq!(row["recent_orders"], json!([a, b]));
    assert_eq!(row["spent_by_currency"], json!({ "EUR": "40.00" }));
    assert_eq!(row["order_count"], 2);

    let cancelled = d
        .exec(
            "Orders.Order.CancelOrder",
            &format!("order-{a}"),
            json!({ "reason": "changed my mind" }),
        )
        .await
        .expect("cancel A");
    let row = d
        .row(
            PROJ,
            "customer_orders",
            json!({ "customer_id": c }),
            cancelled.last_position,
        )
        .await;
    assert_eq!(
        row["open_orders"],
        json!([b]),
        "cancel removes from the set"
    );
    assert_eq!(
        row["recent_orders"],
        json!([a, b]),
        "the list is history and keeps A"
    );
    assert_eq!(row["spent_by_currency"], json!({ "EUR": "40.00" }));

    // Six more orders: the recent list keeps the last five.
    let mut last = cancelled.last_position;
    let mut ids = vec![a.clone(), b.clone()];
    for n in 1..=6u32 {
        let id = uuid('d', n);
        last = d
            .exec(
                "Orders.Order.PlaceOrder",
                &format!("order-{id}"),
                json!({ "customer_id": c, "lines": [line(&uuid('2', n), 2, "1.50")] }),
            )
            .await
            .expect("place")
            .last_position;
        ids.push(id);
    }
    let row = d
        .row(PROJ, "customer_orders", json!({ "customer_id": c }), last)
        .await;
    assert_eq!(row["recent_orders"], json!(ids[ids.len() - 5..]));
    assert_eq!(row["order_count"], 8);
    assert_eq!(row["spent_by_currency"], json!({ "EUR": "58.00" }));
    assert_eq!(row["open_orders"].as_array().unwrap().len(), 7);

    // Negatives.
    let err = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 9), 1, "1.00")] }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(rejection_code(&err).as_deref(), Some("ALREADY_PLACED"));

    let err = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{}", uuid('e', 1)),
            json!({ "customer_id": c }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("lines"), "{err}");

    let err = d
        .append(
            &format!("order-{b}"),
            "Orders.OrderCancelled",
            json!({ "order_id": b, "reason": null, "at": "2026-10-07T00:00:00Z" }),
            Kind::Exact(41),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");

    let err = d
        .append(
            &format!("order-{b}"),
            "Orders.OrderCancelled",
            json!({ "order_id": a, "reason": null, "at": "2026-10-07T00:00:00Z" }),
            Kind::Any(true),
        )
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Code::InvalidArgument,
        "stream id mismatch: {err}"
    );

    let err = d
        .get(
            PROJ,
            "customer_orders",
            json!({ "customer_id": c }),
            Some(last + 1_000),
            Some(100),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err}");

    let missing = d
        .get(
            PROJ,
            "customer_orders",
            json!({ "customer_id": uuid('f', 1) }),
            Some(last),
            None,
        )
        .await
        .unwrap();
    assert!(!missing.found);

    let err = d
        .get(PROJ, "customer_orders", json!({ "nope": 1 }), None, None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    // Restart: exactly once across restarts.
    let before = d.checkpoint(PROJ).await;
    d.restart().await;
    assert_eq!(d.checkpoint(PROJ).await, before);
    let row = d
        .row(PROJ, "customer_orders", json!({ "customer_id": c }), last)
        .await;
    assert_eq!(row["order_count"], 8, "not doubled by a replay");
    assert_eq!(row["spent_by_currency"], json!({ "EUR": "58.00" }));
    let totals = d
        .row(
            "Orders.OrderTotals",
            "order_totals",
            json!({ "order_id": a }),
            last,
        )
        .await;
    assert_eq!(totals["status"], "Cancelled");
    assert_eq!(
        totals["total"],
        json!({ "amount": "15.00", "currency": "EUR" })
    );
    d.shutdown().await;
}

#[tokio::test]
async fn an_order_may_arrive_before_its_customer() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 2);
    let a = uuid('a', 2);
    let placed = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "9.99")] }),
        )
        .await
        .unwrap();
    let row = d
        .row(
            PROJ,
            "customer_orders",
            json!({ "customer_id": c }),
            placed.last_position,
        )
        .await;
    assert_eq!(
        row["name"],
        serde_json::Value::Null,
        "row created with defaults"
    );
    assert_eq!(row["open_orders"], json!([a]));

    let reg = d
        .exec(
            "Customers.Customer.Register",
            &format!("customer-{c}"),
            json!({ "name": "Grace" }),
        )
        .await
        .unwrap();
    let row = d
        .row(
            PROJ,
            "customer_orders",
            json!({ "customer_id": c }),
            reg.last_position,
        )
        .await;
    assert_eq!(row["name"], "Grace");
    assert_eq!(row["open_orders"], json!([a]), "the earlier order is kept");
    d.shutdown().await;
}
