//! Where the layers' boundaries show inside the composite: the two append
//! paths (the application node's guarded one and the database's unguarded
//! one), the reserved context on both, and what each node's directory holds.

use fold_proto::common::v1::expected_version::Kind;
use fold_proto::common::v1::{ExpectedVersion, NewEvent};
use fold_proto::database::v1::AppendRequest;
use serde_json::{Value, json};
use tonic::Code;

use crate::common::{Daemon, line, rejection_code, settle, state_of, uuid, violated_invariant};

const INVARIANTS: &str = "LinesNotEmpty -> wasm \"orders.wasm\" export \"check_lines_not_empty\"";

/// The example with a declarative ceiling on the order's lines.
fn with_max_lines(s: &str) -> String {
    assert!(s.contains(INVARIANTS));
    s.replace(
        INVARIANTS,
        &format!("{INVARIANTS},\n  MaxLines: len(lines) <= 2"),
    )
}

/// `Log.Append` on the database: past the application node's invariants.
async fn unguarded(
    d: &Daemon,
    stream: &str,
    ty: &str,
    payload: Value,
    expected: Kind,
) -> Result<fold_proto::database::v1::AppendResponse, tonic::Status> {
    d.log()
        .await
        .append(AppendRequest {
            stream_id: stream.into(),
            expected: Some(ExpectedVersion {
                kind: Some(expected),
            }),
            events: vec![NewEvent {
                r#type: ty.into(),
                payload: serde_json::to_vec(&payload).unwrap(),
                content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                metadata: vec![],
            }],
            fencing_token: None,
            idempotency_key: vec![],
        })
        .await
        .map(|r| r.into_inner())
}

#[tokio::test]
async fn the_guarded_append_refuses_what_the_unguarded_one_lands() {
    let mut d = Daemon::start(with_max_lines).await;
    let a = uuid('a', 1);
    let stream = format!("order-{a}");
    d.exec(
        "Orders.Order.PlaceOrder",
        &stream,
        json!({ "customer_id": uuid('c', 1), "lines": [line(&uuid('1', 1), 1, "1.00"), line(&uuid('1', 2), 1, "1.00")] }),
    )
    .await
    .unwrap();
    let third = json!({
        "order_id": a,
        "line": line(&uuid('1', 3), 1, "1.00"),
        "total": { "amount": "3.00", "currency": "EUR" },
    });

    // Through the application node: the invariant refuses it.
    let err = d
        .append(&stream, "Orders.LineAdded", third.clone(), Kind::Exact(0))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(rejection_code(&err).as_deref(), Some("MaxLines"));
    assert_eq!(
        violated_invariant(&err).as_deref(),
        Some("Orders.Order.MaxLines")
    );
    assert_eq!(d.aggregate(&stream).await.unwrap().version, 0);

    // Through the database: no invariant runs, the event lands, and the
    // derivation node evolves it like any other.
    let appended = unguarded(&d, &stream, "Orders.LineAdded", third, Kind::Exact(0))
        .await
        .expect("the database checks the domain only");
    assert_eq!(appended.version, 1);
    assert_eq!(appended.events.len(), 1);
    let got = d.aggregate(&stream).await.unwrap();
    assert_eq!(got.version, 1);
    assert_eq!(state_of(&got)["lines"].as_object().unwrap().len(), 3);

    // The database still validates against the domain: an undeclared
    // event, a payload missing a field, and a stream the key does not
    // render to are refused there too.
    let err = unguarded(
        &d,
        &stream,
        "Orders.Nope",
        json!({ "order_id": a }),
        Kind::Any(true),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    let err = unguarded(
        &d,
        &stream,
        "Orders.LineAdded",
        json!({ "order_id": a }),
        Kind::Any(true),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    let err = unguarded(
        &d,
        &format!("order-{}", uuid('b', 1)),
        "Orders.LineAdded",
        json!({ "order_id": a, "line": line(&uuid('1', 4), 1, "1.00"), "total": { "amount": "4.00", "currency": "EUR" } }),
        Kind::Any(true),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("belongs to stream"), "{err}");
    d.shutdown().await;
}

#[tokio::test]
async fn the_reserved_context_is_refused_on_both_append_paths() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let fired = json!({
        "process": "Orders.Fulfilment", "instance": uuid('a', 2),
        "name": "ShipmentOverdue", "due_at": "2026-01-01T00:00:00Z",
    });
    let stream = "fold-timers-Orders.Fulfilment";
    // The application node refuses the context outright.
    let err = d
        .append(stream, "Fold.TimerFired@v1", fired.clone(), Kind::Any(true))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    assert!(err.message().contains("reserved"), "{err}");
    // The database refuses it without the system token, which only the
    // application node holds (the composite generated it for both).
    let err = unguarded(&d, stream, "Fold.TimerFired@v1", fired, Kind::Any(true))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    assert_eq!(d.head().await, 0, "nothing landed");
    d.shutdown().await;
}

#[tokio::test]
async fn each_node_keeps_its_own_store_and_the_database_only_the_log() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let a = uuid('a', 3);
    d.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": uuid('c', 3), "lines": [line(&uuid('1', 1), 1, "1.00")] }),
    )
    .await
    .unwrap();
    settle(&d, &[format!("shipment-{a}")]).await;
    let log_id = d.health().await.log_id;
    d.shutdown().await;

    let data = d.data_dir().join("data");
    let log_dir = data.join(foldd::LOG_NAME);
    assert!(log_dir.join("index.redb").is_file(), "the log's index");
    assert!(log_dir.join("segments").is_dir(), "the log's segments");
    assert!(
        !log_dir.join("derived.redb").exists(),
        "the database derives nothing"
    );
    assert!(!log_dir.join("snapshots").exists());

    let derive = fold_store::DerivedStore::open_or_create(
        &d.derive_dir().join("derived.redb"),
        fold_core::FsyncPolicy::Never,
    )
    .unwrap();
    assert_eq!(
        derive.log_id().unwrap().map(|u| u.to_string()),
        Some(log_id.clone())
    );
    assert!(
        derive.checkpoint("Orders.OrderTotals").unwrap().is_some(),
        "the derivation node runs the projections"
    );
    assert!(
        derive.checkpoint("Orders.Fulfilment").unwrap().is_none(),
        "and no process manager"
    );
    drop(derive);

    let app = fold_store::DerivedStore::open_or_create(
        &d.app_dir().join("derived.redb"),
        fold_core::FsyncPolicy::Never,
    )
    .unwrap();
    assert_eq!(app.log_id().unwrap().map(|u| u.to_string()), Some(log_id));
    assert!(
        app.checkpoint("Orders.Fulfilment").unwrap().is_some(),
        "the application node runs the process managers"
    );
    assert!(
        app.checkpoint("Orders.OrderTotals").unwrap().is_none(),
        "and no projection"
    );
}
