//! Commands through the application node: events to the database, rows and
//! state from the derivation node; guards, invariants and raw appends.

use fold_proto::common::v1::expected_version::Kind;
use serde_json::json;
use tonic::Code;

use crate::common::{Cluster, line, rejection_code, uuid, violated_invariant};

const PROJ: &str = "Orders.CustomerOrders";

#[tokio::test]
async fn commands_flow_into_the_customer_orders_read_model() {
    let mut c = Cluster::start().await;
    c.ready().await;
    let cust = uuid('c', 1);
    let a = uuid('a', 1);
    let b = uuid('b', 1);

    c.exec(
        "Customers.Customer.Register",
        &format!("customer-{cust}"),
        json!({ "name": "Ada" }),
    )
    .await
    .expect("register");
    c.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": cust, "lines": [line(&uuid('1', 1), 1, "15.00")] }),
    )
    .await
    .expect("place A");
    let placed_b = c
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{b}"),
            json!({ "customer_id": cust, "lines": [line(&uuid('1', 2), 1, "25.00")] }),
        )
        .await
        .expect("place B");
    assert_eq!(placed_b.events.len(), 1);
    assert_eq!(placed_b.version, Some(0));
    assert!(placed_b.token.starts_with("fold1:"), "{}", placed_b.token);

    // Read-your-writes on the derivation node with the position.
    let row = c
        .row(
            PROJ,
            "customer_orders",
            json!({ "customer_id": cust }),
            placed_b.last_position,
        )
        .await;
    assert_eq!(row["name"], "Ada");
    assert_eq!(row["open_orders"], json!([a, b]));
    assert_eq!(row["spent_by_currency"], json!({ "EUR": "40.00" }));
    assert_eq!(row["order_count"], 2);

    let cancelled = c
        .exec(
            "Orders.Order.CancelOrder",
            &format!("order-{a}"),
            json!({ "reason": "changed my mind" }),
        )
        .await
        .expect("cancel A");
    let row = c
        .row(
            PROJ,
            "customer_orders",
            json!({ "customer_id": cust }),
            cancelled.last_position,
        )
        .await;
    assert_eq!(row["open_orders"], json!([b]));

    // Six more orders: with one open, four more fit under MaxOpenOrders
    // (limit five, read from the derivation node's rows) and the last two
    // are refused by it.
    let mut last = cancelled.last_position;
    for n in 1..=6u32 {
        let id = uuid('d', n);
        let result = c
            .exec(
                "Orders.Order.PlaceOrder",
                &format!("order-{id}"),
                json!({ "customer_id": cust, "lines": [line(&uuid('2', n), 2, "1.50")] }),
            )
            .await;
        if n <= 4 {
            last = result.expect("fits under the limit").last_position;
        } else {
            let err = result.expect_err("over the limit");
            assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
            assert_eq!(rejection_code(&err).as_deref(), Some("MAX_OPEN_ORDERS"));
            assert_eq!(
                violated_invariant(&err).as_deref(),
                Some("Orders.MaxOpenOrders")
            );
        }
    }
    let row = c
        .row(
            PROJ,
            "customer_orders",
            json!({ "customer_id": cust }),
            last,
        )
        .await;
    assert_eq!(row["order_count"], 6);
    assert_eq!(row["open_orders"].as_array().unwrap().len(), 5);

    // The invariant guards the application's raw appends too; the
    // database's own Append is the unguarded path.
    let sixth = uuid('e', 6);
    let placed_payload = json!({
        "order_id": sixth, "customer_id": cust, "lines": [line(&uuid('2', 9), 1, "1.00")],
        "total": { "amount": "1.00", "currency": "EUR" }
    });
    let err = c
        .append(
            &format!("order-{sixth}"),
            "Orders.OrderPlaced",
            placed_payload.clone(),
            Kind::NoStream(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(
        violated_invariant(&err).as_deref(),
        Some("Orders.MaxOpenOrders")
    );
    // Cancelling one frees a slot, and the same append then succeeds.
    last = c
        .exec("Orders.Order.CancelOrder", &format!("order-{b}"), json!({}))
        .await
        .unwrap()
        .last_position;
    let appended = c
        .append(
            &format!("order-{sixth}"),
            "Orders.OrderPlaced",
            placed_payload,
            Kind::NoStream(true),
        )
        .await
        .expect("a slot is free again");
    last = last.max(appended.last_position);
    let row = c
        .row(
            PROJ,
            "customer_orders",
            json!({ "customer_id": cust }),
            last,
        )
        .await;
    assert_eq!(row["open_orders"].as_array().unwrap().len(), 5);
    assert_eq!(row["order_count"], 7);

    // Negatives.
    let err = c
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": cust, "lines": [line(&uuid('1', 9), 1, "1.00")] }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(rejection_code(&err).as_deref(), Some("ALREADY_PLACED"));
    let err = c
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{}", uuid('e', 1)),
            json!({ "customer_id": cust }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("lines"), "{err}");
    let err = c
        .append(
            &format!("order-{b}"),
            "Orders.OrderCancelled",
            json!({ "order_id": b, "reason": null, "at": "2026-10-07T00:00:00Z" }),
            Kind::Exact(41),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    let err = c
        .append(
            &format!("order-{b}"),
            "Orders.OrderCancelled",
            json!({ "order_id": a, "reason": null, "at": "2026-10-07T00:00:00Z" }),
            Kind::Any(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "stream mismatch: {err}");
    let err = c
        .exec("Orders.Order.Nope", &format!("order-{a}"), json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    let mut bad_price = line(&uuid('1', 1), 1, "-5.00");
    bad_price["price"]["currency"] = json!("eur");
    let err = c
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{}", uuid('f', 1)),
            json!({ "customer_id": cust, "lines": [bad_price] }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(
        err.message()
            .contains("$.lines[0].price: Shared.Money violates rule NonNegative"),
        "{err}"
    );

    // A restart of the application node: nothing is doubled, state is read
    // from the database afresh.
    c.restart_app().await;
    c.ready().await;
    let row = c
        .row(
            PROJ,
            "customer_orders",
            json!({ "customer_id": cust }),
            last,
        )
        .await;
    assert_eq!(row["order_count"], 7);
    let added = c
        .exec(
            "Orders.Order.AddLine",
            &format!("order-{sixth}"),
            json!({ "line": line(&uuid('3', 1), 1, "2.00") }),
        )
        .await
        .expect("a command after the restart");
    assert_eq!(added.version, Some(1));
    c.shutdown().await;
}

#[tokio::test]
async fn a_write_behind_the_nodes_back_is_seen_by_the_next_command() {
    // The database took an event without this node (another node, a raw
    // Log.Append): the command's state comes from the database's stream,
    // and the expected version is the database's.
    let mut c = Cluster::start().await;
    c.ready().await;
    let cust = uuid('c', 2);
    let a = uuid('a', 2);
    let stream = format!("order-{a}");
    c.exec(
        "Orders.Order.PlaceOrder",
        &stream,
        json!({ "customer_id": cust, "lines": [line(&uuid('1', 1), 1, "1.00")] }),
    )
    .await
    .unwrap();
    // Behind the node's back.
    c.log()
        .await
        .append(fold_proto::database::v1::AppendRequest {
            stream_id: stream.clone(),
            expected: Some(fold_proto::common::v1::ExpectedVersion {
                kind: Some(Kind::Exact(0)),
            }),
            events: vec![fold_proto::common::v1::NewEvent {
                r#type: "Orders.LineAdded".into(),
                payload: serde_json::to_vec(&json!({ "order_id": a, "line": line(&uuid('1', 2), 1, "1.00"), "total": { "amount": "2.00", "currency": "EUR" } })).unwrap(),
                content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                metadata: vec![],
            }],
            fencing_token: None,
            idempotency_key: vec![],
        })
        .await
        .unwrap();
    let added = c
        .exec(
            "Orders.Order.AddLine",
            &stream,
            json!({ "line": line(&uuid('1', 3), 1, "1.00") }),
        )
        .await
        .expect("the command sees the foreign event");
    assert_eq!(added.version, Some(2));
    let got = c.aggregate(&stream).await.unwrap();
    assert_eq!(
        crate::common::state_of(&got)["lines"]
            .as_object()
            .unwrap()
            .len(),
        3
    );
    c.shutdown().await;
}

#[tokio::test]
async fn sixteen_concurrent_commands_on_one_stream_all_succeed() {
    let mut c = Cluster::start().await;
    c.ready().await;
    let cust = uuid('c', 3);
    let a = uuid('a', 3);
    let stream = format!("order-{a}");
    c.exec(
        "Orders.Order.PlaceOrder",
        &stream,
        json!({ "customer_id": cust, "lines": [line(&uuid('1', 1), 1, "1.00")] }),
    )
    .await
    .unwrap();
    let mut tasks = Vec::new();
    for n in 2..=17u32 {
        let mut client = c.command().await;
        let stream = stream.clone();
        tasks.push(tokio::spawn(async move {
            client
                .execute(fold_proto::application::v1::ExecuteRequest {
                    command: "Orders.Order.AddLine".into(),
                    stream_id: stream,
                    payload: serde_json::to_vec(&json!({ "line": line(&uuid('1', n), 1, "1.00") }))
                        .unwrap(),
                    content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                    metadata: vec![],
                    fencing_token: None,
                })
                .await
                .map(|r| r.into_inner())
        }));
    }
    let mut versions = Vec::new();
    for t in tasks {
        let r = t.await.unwrap().expect("every command succeeds");
        versions.push(r.version.unwrap());
    }
    versions.sort();
    assert_eq!(versions, (1..=16).collect::<Vec<u64>>(), "dense versions");
    let got = c.aggregate(&stream).await.unwrap();
    assert_eq!(got.version, 16);
    assert_eq!(
        crate::common::state_of(&got)["lines"]
            .as_object()
            .unwrap()
            .len(),
        17
    );
    c.shutdown().await;
}

#[tokio::test]
async fn declarative_guards_and_invariants_run_on_the_application_node() {
    let mut c = Cluster::start_with(
        |s| {
            s.replace(
                "  LinesNotEmpty -> wasm \"orders.wasm\" export \"check_lines_not_empty\"",
                "  LinesNotEmpty -> wasm \"orders.wasm\" export \"check_lines_not_empty\",\n  MaxLines: len(lines) <= 2",
            )
            .replace(
                "  CancelOrder { reason: string? }                  -> wasm \"orders.wasm\" export \"handle_cancel_order\"",
                "  CancelOrder { reason: string? } requires { NotCancelled: state.status != Cancelled } -> wasm \"orders.wasm\" export \"handle_cancel_order\"",
            )
        },
        |_| {},
    )
    .await;
    c.ready().await;
    let cust = uuid('c', 4);
    let a = uuid('a', 4);
    let stream = format!("order-{a}");
    c.exec(
        "Orders.Order.PlaceOrder",
        &stream,
        json!({ "customer_id": cust, "lines": [line(&uuid('1', 1), 1, "1.00"), line(&uuid('1', 2), 1, "1.00")] }),
    )
    .await
    .unwrap();
    // The declarative state invariant rejects with its name.
    let err = c
        .exec(
            "Orders.Order.AddLine",
            &stream,
            json!({ "line": line(&uuid('1', 3), 1, "1.00") }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(rejection_code(&err).as_deref(), Some("MaxLines"));
    assert_eq!(
        violated_invariant(&err).as_deref(),
        Some("Orders.Order.MaxLines")
    );
    assert!(err.message().contains("len(lines) <= 2"), "{err}");
    // The guard answers before the handler, under its own name.
    c.exec("Orders.Order.CancelOrder", &stream, json!({}))
        .await
        .expect("first cancel");
    let err = c
        .exec("Orders.Order.CancelOrder", &stream, json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(rejection_code(&err).as_deref(), Some("NotCancelled"));
    assert_eq!(
        violated_invariant(&err).as_deref(),
        Some("Orders.Order.CancelOrder.NotCancelled")
    );
    c.shutdown().await;
}
