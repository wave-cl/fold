//! Aggregate state: entities keyed by id, snapshot + replay, the cache.

use fold_proto::v1::expected_version::Kind;
use fold_proto::v1::{ListSnapshotsRequest, RebuildProjectionRequest, SnapshotProjectionRequest};
use serde_json::json;
use tonic::Code;

use crate::common::{Daemon, line, rejection_code, state_of, uuid, violated_invariant};

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
    assert_eq!(s["total"]["amount"], "30.00");
    assert_eq!(got.version, 2);

    // Removing a line is fine while one remains...
    d.exec("Orders.Order.RemoveLine", &stream, json!({ "line_id": l1 }))
        .await
        .unwrap();
    let got = d.aggregate(&stream).await.unwrap();
    let s = state_of(&got);
    assert_eq!(s["lines"].as_object().unwrap().len(), 1);
    assert_eq!(s["total"]["amount"], "15.00");
    assert_eq!(got.version, 3);
    // ...but the handler lets the last one go, and the state invariant refuses.
    let err = d
        .exec("Orders.Order.RemoveLine", &stream, json!({ "line_id": l2 }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(rejection_code(&err).as_deref(), Some("EMPTY_ORDER"));
    assert_eq!(
        violated_invariant(&err).as_deref(),
        Some("Orders.Order.LinesNotEmpty")
    );
    let got = d.aggregate(&stream).await.unwrap();
    assert_eq!(got.version, 3, "nothing was appended");
    assert_eq!(state_of(&got)["lines"].as_object().unwrap().len(), 1);

    d.exec("Orders.Order.CancelOrder", &stream, json!({}))
        .await
        .unwrap();
    assert_eq!(
        state_of(&d.aggregate(&stream).await.unwrap())["status"],
        "Cancelled"
    );

    // Restart: the cache is gone, five events replay, and a snapshot is taken.
    d.restart().await;
    let got = d.aggregate(&stream).await.unwrap();
    assert_eq!(got.version, 4);
    assert_eq!(got.replayed, 5);
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
        Kind::Exact(4),
    )
    .await
    .unwrap();
    d.restart().await;
    let got = d.aggregate(&stream).await.unwrap();
    assert_eq!(got.version, 5);
    assert_eq!(
        got.snapshot_version,
        Some(4),
        "loaded from the snapshot taken at version 4"
    );
    assert_eq!(
        got.replayed, 1,
        "only the event after the snapshot was evolved"
    );
    let s = state_of(&got);
    assert_eq!(s["status"], "Cancelled");
    assert_eq!(s["lines"].as_object().unwrap().len(), 1);

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

/// An aggregate's instance snapshots can be exported, dropped and rebuilt
/// from scratch (every instance re-derived from its events), or restored.
#[tokio::test]
async fn aggregate_snapshots_export_and_rebuild_from_scratch() {
    let mut d = Daemon::start(|s| s.replace("snapshot every 100", "snapshot every 2")).await;
    let c = uuid('c', 5);
    let a = uuid('a', 5);
    let b = uuid('b', 5);
    for (order, lines) in [(&a, 3u32), (&b, 2u32)] {
        let stream = format!("order-{order}");
        d.exec(
            "Orders.Order.PlaceOrder",
            &stream,
            json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "1.00")] }),
        )
        .await
        .unwrap();
        for n in 2..=lines {
            d.exec(
                "Orders.Order.AddLine",
                &stream,
                json!({ "line": line(&uuid('1', n), 1, "1.00") }),
            )
            .await
            .unwrap();
        }
    }
    // Snapshots are taken on a load that replays enough: a restart forces one.
    d.restart().await;
    let got_a = d.aggregate(&format!("order-{a}")).await.unwrap();
    let got_b = d.aggregate(&format!("order-{b}")).await.unwrap();
    assert_eq!(got_a.replayed, 3);
    assert_eq!(got_b.replayed, 2);
    let state_a = state_of(&got_a);
    let state_b = state_of(&got_b);

    let snap = d
        .admin()
        .await
        .snapshot_projection(SnapshotProjectionRequest {
            projection: "Orders.Order".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(snap.rows, 2, "one instance snapshot per order");
    assert!(snap.module_matches);
    assert_eq!(
        d.admin()
            .await
            .list_snapshots(ListSnapshotsRequest {
                projection: "Orders.Order".into()
            })
            .await
            .unwrap()
            .into_inner()
            .snapshots
            .len(),
        1
    );

    // Rebuild from scratch: snapshots and cache dropped, every instance
    // re-derived and re-snapshotted before the call returns.
    let resp = d
        .admin()
        .await
        .rebuild_projection(RebuildProjectionRequest {
            projection: "Orders.Order".into(),
            snapshot_id: String::new(),
            force: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.restarted_from, None);
    let got = d.aggregate(&format!("order-{a}")).await.unwrap();
    assert_eq!(got.replayed, 0, "warmed into the cache by the rebuild");
    assert_eq!(state_of(&got), state_a);
    d.restart().await;
    let got = d.aggregate(&format!("order-{a}")).await.unwrap();
    assert_eq!(
        got.snapshot_version,
        Some(2),
        "re-snapshotted at its last version"
    );
    assert_eq!(got.replayed, 0);
    assert_eq!(state_of(&got), state_a);
    assert_eq!(
        state_of(&d.aggregate(&format!("order-{b}")).await.unwrap()),
        state_b
    );

    // Restore the exported file: the stored snapshots are back as they were.
    let resp = d
        .admin()
        .await
        .rebuild_projection(RebuildProjectionRequest {
            projection: "Orders.Order".into(),
            snapshot_id: snap.id.clone(),
            force: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.restarted_from, Some(snap.checkpoint));
    d.restart().await;
    let got = d.aggregate(&format!("order-{b}")).await.unwrap();
    assert_eq!(got.snapshot_version, Some(1));
    assert_eq!(got.replayed, 0);
    assert_eq!(state_of(&got), state_b);

    // Still a normal aggregate afterwards.
    d.exec("Orders.Order.CancelOrder", &format!("order-{b}"), json!({}))
        .await
        .unwrap();
    assert_eq!(
        state_of(&d.aggregate(&format!("order-{b}")).await.unwrap())["status"],
        "Cancelled"
    );
    d.shutdown().await;
}
