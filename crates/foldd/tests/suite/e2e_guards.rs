//! Declarative guards at runtime: `Name: expr` state invariants run with
//! the wasm ones on every commit (commands and raw appends); `requires`
//! runs after the state is loaded and before the handler, with the guard's
//! name as the rejection code.

use fold_proto::v1::expected_version::Kind;
use serde_json::json;
use tonic::Code;

use crate::common::{Daemon, line, rejection_code, state_of, uuid, violated_invariant};

const INVARIANTS: &str = "LinesNotEmpty -> wasm \"orders.wasm\" export \"check_lines_not_empty\"";
const CANCEL: &str = "CancelOrder { reason: string? }                  -> wasm \"orders.wasm\" export \"handle_cancel_order\"";
const PLACE: &str = "PlaceOrder  { customer_id: uuid, lines: [Line] } -> wasm \"orders.wasm\" export \"handle_place_order\",";
const ADD: &str = "AddLine     { line: Line }                       -> wasm \"orders.wasm\" export \"handle_add_line\",";

fn with_max_lines(s: &str) -> String {
    assert!(s.contains(INVARIANTS));
    s.replace(
        INVARIANTS,
        &format!("{INVARIANTS},\n  MaxLines: len(lines) <= 2"),
    )
}

async fn placed(d: &Daemon, a: &str, lines: usize) -> u64 {
    let lines: Vec<_> = (1..=lines)
        .map(|i| line(&uuid('1', i as u32), 1, "1.00"))
        .collect();
    d.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": uuid('c', 1), "lines": lines }),
    )
    .await
    .expect("place")
    .last_position
}

#[tokio::test]
async fn declarative_invariant_rejects_with_its_name() {
    let mut d = Daemon::start(with_max_lines).await;
    let a = uuid('a', 1);
    let stream = format!("order-{a}");
    placed(&d, &a, 2).await;
    let err = d
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
    assert!(
        err.message().contains("len(lines) <= 2"),
        "the message is the expression: {}",
        err.message()
    );
    let got = d.aggregate(&stream).await.unwrap();
    assert_eq!(got.version, 0, "nothing was appended");
    assert_eq!(state_of(&got)["lines"].as_object().unwrap().len(), 2);
    // Control: a second line fits.
    let b = uuid('b', 1);
    placed(&d, &b, 1).await;
    d.exec(
        "Orders.Order.AddLine",
        &format!("order-{b}"),
        json!({ "line": line(&uuid('1', 2), 1, "1.00") }),
    )
    .await
    .expect("two lines are allowed");
    d.shutdown().await;
}

#[tokio::test]
async fn raw_append_is_guarded_by_declarative_invariants() {
    let mut d = Daemon::start(with_max_lines).await;
    let a = uuid('a', 2);
    let stream = format!("order-{a}");
    placed(&d, &a, 2).await;
    let payload = json!({
        "order_id": a,
        "line": line(&uuid('1', 3), 1, "1.00"),
        "total": { "amount": "3.00", "currency": "EUR" },
    });
    let err = d
        .append(&stream, "Orders.LineAdded", payload.clone(), Kind::Exact(0))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(rejection_code(&err).as_deref(), Some("MaxLines"));
    assert_eq!(
        violated_invariant(&err).as_deref(),
        Some("Orders.Order.MaxLines")
    );
    assert_eq!(d.aggregate(&stream).await.unwrap().version, 0);
    // Control: the same append on an order with one line is accepted.
    let b = uuid('b', 2);
    placed(&d, &b, 1).await;
    let mut p = payload;
    p["order_id"] = json!(b);
    d.append(&format!("order-{b}"), "Orders.LineAdded", p, Kind::Exact(0))
        .await
        .expect("a second line fits");
    d.shutdown().await;
}

#[tokio::test]
async fn wasm_and_declarative_invariants_coexist() {
    let mut d = Daemon::start(with_max_lines).await;
    let a = uuid('a', 3);
    let stream = format!("order-{a}");
    placed(&d, &a, 1).await;
    // The wasm one still guards the floor...
    let err = d
        .exec(
            "Orders.Order.RemoveLine",
            &stream,
            json!({ "line_id": uuid('1', 1) }),
        )
        .await
        .unwrap_err();
    assert_eq!(rejection_code(&err).as_deref(), Some("EMPTY_ORDER"));
    assert_eq!(
        violated_invariant(&err).as_deref(),
        Some("Orders.Order.LinesNotEmpty")
    );
    // ...and the declarative one the ceiling.
    for i in 2..=3 {
        let r = d
            .exec(
                "Orders.Order.AddLine",
                &stream,
                json!({ "line": line(&uuid('1', i), 1, "1.00") }),
            )
            .await;
        if i <= 2 {
            r.expect("fits");
        } else {
            assert_eq!(rejection_code(&r.unwrap_err()).as_deref(), Some("MaxLines"));
        }
    }
    d.shutdown().await;
}

#[tokio::test]
async fn requires_runs_before_the_handler() {
    // The handler would reject a second cancel with NOT_PENDING; the guard
    // answers first, under its own name.
    let mut d = Daemon::start(|s: &str| {
        s.replace(
            CANCEL,
            "CancelOrder { reason: string? } requires { NotCancelled: state.status != Cancelled } -> wasm \"orders.wasm\" export \"handle_cancel_order\"",
        )
    })
    .await;
    let a = uuid('a', 4);
    let stream = format!("order-{a}");
    placed(&d, &a, 1).await;
    d.exec("Orders.Order.CancelOrder", &stream, json!({}))
        .await
        .expect("first cancel");
    let err = d
        .exec("Orders.Order.CancelOrder", &stream, json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(rejection_code(&err).as_deref(), Some("NotCancelled"));
    assert_eq!(
        violated_invariant(&err).as_deref(),
        Some("Orders.Order.CancelOrder.NotCancelled")
    );
    assert!(
        err.message().contains("state.status != Cancelled"),
        "{}",
        err.message()
    );
    d.shutdown().await;
}

#[tokio::test]
async fn requires_on_a_new_stream_fails_with_no_state() {
    let mut d = Daemon::start(|s: &str| {
        s.replace(
            ADD,
            "AddLine { line: Line } requires state.status == Pending -> wasm \"orders.wasm\" export \"handle_add_line\",",
        )
        .replace(
            PLACE,
            "PlaceOrder { customer_id: uuid, lines: [Line] } requires { Fresh: not state exists } -> wasm \"orders.wasm\" export \"handle_place_order\",",
        )
    })
    .await;
    let a = uuid('a', 5);
    let stream = format!("order-{a}");
    // AddLine on a stream with no state: the required operand is absent,
    // so the comparison is false and the message says why.
    let err = d
        .exec(
            "Orders.Order.AddLine",
            &stream,
            json!({ "line": line(&uuid('1', 1), 1, "1.00") }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(rejection_code(&err).as_deref(), Some("Requires"));
    assert_eq!(
        violated_invariant(&err).as_deref(),
        Some("Orders.Order.AddLine.Requires")
    );
    assert!(
        err.message()
            .contains("state.status == Pending (the stream has no state yet)"),
        "{}",
        err.message()
    );
    // `not state exists` lets the first PlaceOrder through and stops a second.
    placed(&d, &a, 1).await;
    let err = d
        .exec(
            "Orders.Order.PlaceOrder",
            &stream,
            json!({ "customer_id": uuid('c', 1), "lines": [line(&uuid('1', 1), 1, "1.00")] }),
        )
        .await
        .unwrap_err();
    assert_eq!(rejection_code(&err).as_deref(), Some("Fresh"));
    assert!(
        !err.message().contains("no state yet"),
        "the stream has state: {}",
        err.message()
    );
    // And AddLine now passes its guard.
    d.exec(
        "Orders.Order.AddLine",
        &stream,
        json!({ "line": line(&uuid('1', 2), 1, "1.00") }),
    )
    .await
    .expect("pending order takes a line");
    d.shutdown().await;
}

#[tokio::test]
async fn requires_reads_the_command_payload() {
    let mut d = Daemon::start(|s: &str| {
        s.replace(
            PLACE,
            "PlaceOrder { customer_id: uuid, lines: [Line] } requires { Small: len(command.lines) <= 2, Known: command.customer_id != \"c0000000-0000-0000-0000-000000000009\" } -> wasm \"orders.wasm\" export \"handle_place_order\",",
        )
    })
    .await;
    let a = uuid('a', 6);
    let err = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": uuid('c', 1), "lines": [line(&uuid('1', 1), 1, "1.00"), line(&uuid('1', 2), 1, "1.00"), line(&uuid('1', 3), 1, "1.00")] }),
        )
        .await
        .unwrap_err();
    assert_eq!(rejection_code(&err).as_deref(), Some("Small"));
    let err = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": uuid('c', 9), "lines": [line(&uuid('1', 1), 1, "1.00")] }),
        )
        .await
        .unwrap_err();
    assert_eq!(rejection_code(&err).as_deref(), Some("Known"));
    placed(&d, &a, 2).await;
    d.shutdown().await;
}
