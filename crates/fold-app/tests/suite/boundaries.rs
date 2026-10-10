//! What the application node refuses and why: a database that is not a
//! primary, a derivation node its registrations do not fit, a node without
//! the system secret, and the Fold.* context on its own Append.

use std::time::Duration;

use fold_proto::common::v1::expected_version::Kind;
use fold_proto::database::v1::FenceRequest;
use serde_json::json;
use tonic::Code;

use crate::common::{Cluster, line, until, uuid};

#[tokio::test]
async fn commands_need_a_primary_database_and_matching_layers() {
    let mut c = Cluster::start().await;
    c.ready().await;
    let h = c.app_health().await;
    assert_eq!(
        (h.layer_check.as_str(), h.database_role.as_str()),
        ("ok", "primary")
    );
    assert_eq!(h.invariants, "single-node");
    assert!(h.database_connected);
    let a = uuid('a', 1);
    c.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": uuid('c', 1), "lines": [line(&uuid('1', 1), 1, "1.00")] }),
    )
    .await
    .unwrap();
    // Fence the database: the node learns the role and refuses commands.
    c.cluster()
        .await
        .fence(FenceRequest { epoch: 5 })
        .await
        .unwrap();
    until(10, "the fence to be seen", async || {
        c.app_health().await.database_role == "fenced"
    })
    .await;
    let err = c
        .exec(
            "Orders.Order.AddLine",
            &format!("order-{a}"),
            json!({ "line": line(&uuid('1', 2), 1, "1.00") }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("fenced"), "{err}");
    let err = c
        .append(
            &format!("customer-{}", uuid('c', 9)),
            "Customers.CustomerRegistered",
            json!({ "customer_id": uuid('c', 9), "name": "Bob" }),
            Kind::NoStream(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    c.shutdown().await;

    // A derivation node without the projection an invariant reads: the
    // registration does not resolve, so the application refuses to start
    // and says which projection it misses.
    let mut c = Cluster::start().await;
    c.ready().await;
    c.shutdown().await;
    c.rewrite("derive.fold", |s| {
        let start = s
            .find("projection Orders.CustomerOrders {")
            .expect("the projection");
        let end = start + s[start..].find("\n}\n").expect("its end") + 3;
        format!("{}{}", &s[..start], &s[end..])
    });
    c.start_db().await;
    c.start_derive().await;
    let err = c.start_app().await.expect_err("refused");
    let text = format!("{err:#}");
    assert!(
        text.contains("Orders.MaxOpenOrders") && text.contains("Orders.CustomerOrders"),
        "{text}"
    );
    // Control: the projection back, the application starts and serves.
    std::fs::copy(
        crate::common::workspace().join("examples/orders/derive.fold"),
        c.path("derive.fold"),
    )
    .unwrap();
    c.start_derive().await;
    c.start_app()
        .await
        .expect("the registrations resolve again");
    c.ready().await;
    c.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{}", uuid('a', 2)),
        json!({ "customer_id": uuid('c', 1), "lines": [line(&uuid('1', 1), 1, "1.00")] }),
    )
    .await
    .expect("the registrations fit again");
    c.shutdown().await;
}

#[tokio::test]
async fn the_reserved_context_is_refused_and_timers_need_the_secret() {
    let mut c = Cluster::start().await;
    c.ready().await;
    let err = c
        .append(
            "fold-timers-Orders.Fulfilment",
            "Fold.TimerFired@v1",
            json!({ "process": "Orders.Fulfilment", "instance": uuid('a', 6), "name": "ShipmentOverdue", "due_at": "2026-01-01T00:00:00Z" }),
            Kind::Any(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    assert!(err.message().contains("reserved"), "{err}");
    c.shutdown().await;

    // Without the secret the node runs, but a due timer fails its process
    // with a message that says what to do.
    let mut c =
        Cluster::start_with(orders_app::Options::default(), |o| o.system_secret = None).await;
    c.ready().await;
    let a = uuid('a', 7);
    c.exec_with_meta(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": uuid('c', 1), "lines": [line(&uuid('1', 1), 1, "1.00")] }),
        json!({ "overdue_after_ms": 200 }),
    )
    .await
    .unwrap();
    until(10, "the process to report the missing secret", async || {
        c.process("Orders.Fulfilment")
            .await
            .error
            .contains("system secret")
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        c.all_events()
            .await
            .iter()
            .all(|e| e.r#type != "Fold.TimerFired@v1"),
        "nothing fired"
    );
    c.shutdown().await;
}
