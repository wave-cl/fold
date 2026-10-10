//! Invariants an application adds in Rust run with the shipped ones on
//! every commit, commands and guarded appends alike, and reject with
//! their name.

use fold_proto::common::v1::expected_version::Kind;
use serde_json::json;
use tonic::Code;

use crate::common::{Daemon, line, rejection_code, state_of, uuid, violated_invariant};

/// The orders application with a ceiling of two lines per order.
fn with_max_lines() -> fold_app::App {
    orders_app::build(orders_app::Options {
        max_lines: Some(2),
        ..orders_app::Options::default()
    })
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
async fn an_added_invariant_rejects_with_its_name() {
    let mut d = Daemon::start_app(|s| s.to_string(), with_max_lines()).await;
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
        err.message().contains("at most 2 line(s)"),
        "the message is the invariant's: {}",
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
async fn a_guarded_append_runs_the_added_invariant() {
    let mut d = Daemon::start_app(|s| s.to_string(), with_max_lines()).await;
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
async fn the_shipped_and_the_added_invariants_coexist() {
    let mut d = Daemon::start_app(|s| s.to_string(), with_max_lines()).await;
    let a = uuid('a', 3);
    let stream = format!("order-{a}");
    placed(&d, &a, 1).await;
    // The shipped one still guards the floor...
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
    // ...and the added one the ceiling.
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

/// A payload that does not fit the command's type is refused before any
/// state is read, naming the command and serde's reason (which names the
/// field when one is missing).
#[tokio::test]
async fn a_payload_that_does_not_fit_the_command_is_invalid() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let a = uuid('a', 4);
    let err = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": uuid('c', 1) }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(
        err.message().contains("Orders.Order.PlaceOrder") && err.message().contains("lines"),
        "{err}"
    );
    let err = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": "not-a-uuid", "lines": [line(&uuid('1', 1), 1, "1.00")] }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(
        err.message().contains("Orders.Order.PlaceOrder") && err.message().contains("UUID"),
        "{err}"
    );
    // A command the application did not register is NOT_FOUND.
    let err = d
        .exec("Orders.Order.Nope", &format!("order-{a}"), json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    d.shutdown().await;
}
