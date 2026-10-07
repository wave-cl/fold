//! Aggregate state: entities keyed by id, snapshot + replay, the cache.

use fold_proto::v1::expected_version::Kind;
use serde_json::json;
use tonic::Code;

use crate::common::{Daemon, line, rejection_code, state_of, uuid};

#[tokio::test]
async fn state_is_cached_snapshotted_and_replayed() {
    // Snapshot every two events so the slice exercises the snapshot path.
    let mut d = Daemon::start(|s| s.replace("snapshot every 100", "snapshot every 2")).await;
    let c = uuid('c', 1);
    let a = uuid('a', 1);
    let stream = format!("order-{a}");
    let l1 = uuid('1', 1);
    let l2 = uuid('1', 2);

    d.exec(
        "Orders.Order.PlaceOrder",
        &stream,
        json!({ "customer_id": c, "lines": [line(&l1, 2, "7.50")] }),
    )
    .await
    .unwrap();
    let got = d.aggregate(&stream).await.unwrap();
    assert!(got.found);
    assert_eq!(got.aggregate, "Orders.Order");
    assert_eq!(got.version, 0);
    assert_eq!(
        got.replayed, 0,
        "the command path left the state in the cache"
    );
    assert_eq!(got.snapshot_version, None);
    let s = state_of(&got);
    assert_eq!(s["status"], "Pending");
    assert_eq!(s["lines"].as_object().unwrap().len(), 1);
    assert_eq!(s["lines"][&l1]["qty"], 2);
    assert_eq!(s["total"]["amount"], "15.00");

    d.exec(
        "Orders.Order.AddLine",
        &stream,
        json!({ "line": line(&l2, 1, "5.00") }),
    )
    .await
    .unwrap();
    let s = state_of(&d.aggregate(&stream).await.unwrap());
    assert_eq!(s["lines"].as_object().unwrap().len(), 2);
    assert_eq!(s["total"]["amount"], "20.00");

    // Same id, higher quantity: the entity is updated, not duplicated.
    d.exec(
        "Orders.Order.AddLine",
        &stream,
        json!({ "line": line(&l2, 3, "5.00") }),
    )
    .await
    .unwrap();
    let got = d.aggregate(&stream).await.unwrap();
    let s = state_of(&got);
    assert_eq!(s["lines"].as_object().unwrap().len(), 2);
    assert_eq!(s["lines"][&l2]["qty"], 3);
    assert_eq!(s["total"]["amount"], "35.00");
    assert_eq!(got.version, 2);

    d.exec("Orders.Order.CancelOrder", &stream, json!({}))
        .await
        .unwrap();
    assert_eq!(
        state_of(&d.aggregate(&stream).await.unwrap())["status"],
        "Cancelled"
    );

    // Restart: the cache is gone, four events replay, and a snapshot is taken.
    d.restart().await;
    let got = d.aggregate(&stream).await.unwrap();
    assert_eq!(got.version, 3);
    assert_eq!(got.replayed, 4);
    assert_eq!(
        got.snapshot_version, None,
        "no snapshot existed before this load"
    );
    assert_eq!(state_of(&got)["status"], "Cancelled");
    let again = d.aggregate(&stream).await.unwrap();
    assert_eq!(again.replayed, 0, "served from the cache now");

    // A raw append past the snapshot, then another restart: snapshot + 1.
    d.append(
        &stream,
        "Orders.OrderCancelled",
        json!({ "order_id": a, "reason": "again", "at": "2026-10-07T00:00:00Z" }),
        Kind::Exact(3),
    )
    .await
    .unwrap();
    d.restart().await;
    let got = d.aggregate(&stream).await.unwrap();
    assert_eq!(got.version, 4);
    assert_eq!(
        got.snapshot_version,
        Some(3),
        "loaded from the snapshot taken at version 3"
    );
    assert_eq!(
        got.replayed, 1,
        "only the event after the snapshot was evolved"
    );
    let s = state_of(&got);
    assert_eq!(s["status"], "Cancelled");
    assert_eq!(s["lines"].as_object().unwrap().len(), 2);

    // Negatives.
    let err = d
        .exec("Orders.Order.AddLine", &stream, json!({ "line": { "sku": "x", "qty": 1, "price": { "amount": "1", "currency": "EUR" } } }))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Code::InvalidArgument,
        "a line without its id: {err}"
    );

    let err = d
        .exec(
            "Orders.Order.AddLine",
            &stream,
            json!({ "line": line(&uuid('1', 3), 1, "1.00") }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(rejection_code(&err).as_deref(), Some("NOT_PENDING"));

    let empty = d
        .aggregate(&format!("order-{}", uuid('a', 9)))
        .await
        .unwrap();
    assert!(!empty.found);

    let err = d.aggregate("nonsense-1").await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    d.shutdown().await;
}
