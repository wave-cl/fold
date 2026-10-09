//! A derivation node's life beside its database: a log that moved
//! backwards, a store of another log, a domain that breaks against the
//! database's, a derivation schema that changed, snapshots and rebuilds.

use fold_proto::common::v1::{DeleteSnapshotRequest, ListSnapshotsRequest, RebuildRequest, SnapshotRequest};
use serde_json::json;
use tonic::Code;

use crate::common::{Db, Derive, add_line, cancel, place, register, state_of, uuid, wait_for};

#[tokio::test]
async fn a_truncated_log_resets_derived_data_past_the_cut() {
    let mut db = Db::start().await;
    let mut d = Derive::start_with(
        &db,
        |s| s.replace("snapshot every 100", "snapshot every 2"),
        |_| {},
    )
    .await;
    let c = uuid('c', 1);
    let a = uuid('a', 1);
    let stream = format!("order-{a}");
    let placed = place(&db, &c, &a, "2.00").await;
    let head_after_place = db.head().await;
    for n in 2..=4u32 {
        add_line(&db, &a, n, u64::from(n) - 2, "1.00").await;
    }
    let full_head = db.head().await;
    d.settle(full_head).await;
    // Two loads: the first replays and snapshots, the second loads from
    // the snapshot (the control: the snapshot is used).
    assert_eq!(d.aggregate(&stream).await.unwrap().replayed, 4);
    d.restart().await;
    let got = d.aggregate(&stream).await.unwrap();
    assert!(got.snapshot_version.is_some(), "{got:?}");
    assert_eq!(state_of(&got)["lines"].as_object().unwrap().len(), 4);
    assert!(d.checkpoint("Orders.OrderTotals").await.unwrap() + 1 >= full_head);

    // Cut the database's log back to just after the placement, the way
    // `fold restore --to` does, and bring both up.
    d.shutdown().await;
    db.shutdown().await;
    let report = fold_core::truncate_log(
        &db.data_dir(),
        fold_db::LOG_NAME,
        fold_core::GlobalPosition(head_after_place),
    )
    .unwrap();
    assert_eq!(report.generation, 1);
    db.restart().await;
    d.database = db.addr.clone();
    d.restart().await;
    let h = d.health().await;
    assert!(h.last_reset.contains("reset past"), "{h:?}");
    assert_eq!(h.generation, 1);
    // The instance snapshot (version 3) is past the cut: discarded, the
    // stream replays from what is left.
    let got = d.aggregate(&stream).await.unwrap();
    assert_eq!(got.snapshot_version, None, "{got:?}");
    assert_eq!(got.version, 0);
    assert_eq!(state_of(&got)["lines"].as_object().unwrap().len(), 1);
    // Runners past the cut were reset and are back at the new head.
    d.settle(head_after_place).await;
    for p in d.projections().await {
        assert!(
            p.checkpoint.is_some_and(|cp| cp < head_after_place),
            "{p:?} looks past the cut"
        );
    }
    let row = d
        .row("Orders.OrderTotals", "order_totals", json!({ "order_id": a }), placed.last_position)
        .await;
    assert_eq!(row["total"]["amount"], "2.00");
    d.shutdown().await;
    db.shutdown().await;
}

#[tokio::test]
async fn a_live_generation_change_resets_without_a_restart() {
    // The database restores online under a running derivation node: the
    // status item's generation changes and the node resets past the cut.
    let mut db = Db::start().await;
    let mut d = Derive::start(&db).await;
    let c = uuid('c', 2);
    let a = uuid('a', 2);
    place(&db, &c, &a, "2.00").await;
    let cut = db.head().await;
    for n in 2..=3u32 {
        add_line(&db, &a, n, u64::from(n) - 2, "1.00").await;
    }
    d.settle(db.head().await).await;
    assert_eq!(d.aggregate(&format!("order-{a}")).await.unwrap().version, 2);
    // Truncate under it: the database restarts (a new generation), the
    // derivation node keeps running and reconnects.
    db.shutdown().await;
    fold_core::truncate_log(&db.data_dir(), fold_db::LOG_NAME, fold_core::GlobalPosition(cut))
        .unwrap();
    db.restart().await;
    // The node's database URL is the old one; the restart binds a new port,
    // so point the node at it through a restart of its own only if the
    // address changed.
    if db.addr != d.database {
        d.database = db.addr.clone();
        d.restart().await;
    }
    let h = wait_for(&d, "the reset", |h| h.generation == 1 && h.database_connected).await;
    assert!(h.last_reset.contains("reset past"), "{h:?}");
    d.settle(cut).await;
    let got = d.aggregate(&format!("order-{a}")).await.unwrap();
    assert_eq!(got.version, 0, "{got:?}");
    d.shutdown().await;
    db.shutdown().await;
}

#[tokio::test]
async fn a_store_of_another_log_is_rebound_and_a_breaking_domain_refused() {
    let mut db = Db::start().await;
    let mut d = Derive::start(&db).await;
    let c = uuid('c', 3);
    let a = uuid('a', 3);
    place(&db, &c, &a, "2.00").await;
    d.settle(db.head().await).await;
    assert!(d.checkpoint("Orders.OrderTotals").await.is_some());
    d.shutdown().await;
    db.shutdown().await;

    // The same derived store against a brand-new database (an operator
    // pointed it at the wrong one): reset and bound to the new log.
    let mut other = Db::start().await;
    d.database = other.addr.clone();
    d.restart().await;
    let h = d.health().await;
    assert!(h.last_reset.starts_with("rebound to log"), "{h:?}");
    assert_eq!(h.database_head, 0);
    assert!(d.projections().await.iter().all(|p| p.checkpoint.is_none()));
    d.shutdown().await;

    // A domain that breaks against the database's is refused at start.
    d.rewrite_domain(|s| {
        s.replace(
            "event OrderPlaced v1   { order_id: uuid, customer_id: uuid,",
            "event OrderPlaced v1   { order_id: uuid, channel: string, customer_id: uuid,",
        )
    });
    let err = d.try_restart().await.unwrap_err();
    let text = format!("{err:#}");
    assert!(text.contains("breaks against the database's"), "{text}");
    assert!(text.contains("Orders.OrderPlaced@v1.channel"), "{text}");
    // Control: a compatible difference starts.
    d.rewrite_domain(|s| s.replace("channel: string,", "channel: string?,"));
    d.restart().await;
    d.shutdown().await;
    other.shutdown().await;
}

#[tokio::test]
async fn a_changed_derivation_schema_rebuilds_what_it_must() {
    let mut db = Db::start().await;
    let mut d = Derive::start(&db).await;
    let c = uuid('c', 4);
    let a = uuid('a', 4);
    place(&db, &c, &a, "2.00").await;
    let cancelled = cancel(&db, &a, 0).await;
    let row = d
        .row("Orders.OrderTotals", "order_totals", json!({ "order_id": a }), cancelled.last_position)
        .await;
    assert_eq!(row["status"], "Cancelled");
    assert!(d.health().await.last_schema_change.is_empty());

    // Without OrderCancelled among its sources the rebuilt projection
    // never sees the cancellation: the row is derived afresh from history.
    d.rewrite_schema(|s| {
        s.replace(
            "  from OrderPlaced, OrderCancelled\n  fold wasm \"orders.wasm\" export \"project_order_totals\"",
            "  from OrderPlaced\n  fold wasm \"orders.wasm\" export \"project_order_totals\"",
        )
    });
    d.restart().await;
    let note = d.health().await.last_schema_change;
    assert!(note.contains("1 rebuild"), "{note}");
    let row = d
        .row("Orders.OrderTotals", "order_totals", json!({ "order_id": a }), cancelled.last_position)
        .await;
    assert_eq!(row["status"], "Pending", "rebuilt without the cancellation");
    // A textual change stores the new text and rebuilds nothing.
    let before = d.checkpoint("Orders.CustomerOrders").await;
    d.rewrite_schema(|s| format!("// a comment\n{s}"));
    d.restart().await;
    assert_eq!(
        d.health().await.last_schema_change,
        "textual change only: stored the new text"
    );
    assert_eq!(d.checkpoint("Orders.CustomerOrders").await, before);
    d.shutdown().await;
    db.shutdown().await;
}

#[tokio::test]
async fn snapshots_and_rebuilds_of_a_projection_and_an_aggregate() {
    let mut db = Db::start().await;
    let mut d = Derive::start_with(
        &db,
        |s| s.replace("snapshot every 100", "snapshot every 1"),
        |_| {},
    )
    .await;
    let c = uuid('c', 5);
    register(&db, &c).await;
    let a = uuid('a', 5);
    let placed = place(&db, &c, &a, "2.00").await;
    d.settle(db.head().await).await;
    let row_before = d
        .row("Orders.CustomerOrders", "customer_orders", json!({ "customer_id": c }), placed.last_position)
        .await;

    // A projection snapshot at its checkpoint, listed, used by a rebuild.
    let snap = d
        .admin()
        .await
        .snapshot(SnapshotRequest {
            name: "Orders.CustomerOrders".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(snap.name, "Orders.CustomerOrders");
    assert!(snap.module_matches);
    assert!(snap.rows >= 2, "{snap:?}");
    let listed = d
        .admin()
        .await
        .list_snapshots(ListSnapshotsRequest {
            name: "Orders.CustomerOrders".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .snapshots;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, snap.id);
    let r = d
        .admin()
        .await
        .rebuild(RebuildRequest {
            name: "Orders.CustomerOrders".into(),
            snapshot_id: snap.id.clone(),
            force: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.restarted_from, Some(snap.checkpoint));
    let b = uuid('b', 5);
    let placed_b = place(&db, &c, &b, "3.00").await;
    let row_after = d
        .row("Orders.CustomerOrders", "customer_orders", json!({ "customer_id": c }), placed_b.last_position)
        .await;
    assert_eq!(row_after["order_count"], 2);
    assert_eq!(row_before["order_count"], 1);
    // From scratch too.
    let r = d
        .admin()
        .await
        .rebuild(RebuildRequest {
            name: "Orders.CustomerOrders".into(),
            snapshot_id: String::new(),
            force: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.restarted_from, None);
    let row_again = d
        .row("Orders.CustomerOrders", "customer_orders", json!({ "customer_id": c }), placed_b.last_position)
        .await;
    assert_eq!(row_again, row_after);

    // An aggregate's instance snapshots, snapshotted to a file and rebuilt.
    d.aggregate(&format!("order-{a}")).await.unwrap();
    d.aggregate(&format!("order-{b}")).await.unwrap();
    let snap = d
        .admin()
        .await
        .snapshot(SnapshotRequest {
            name: "Orders.Order".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(snap.rows, 2, "both instances were snapshotted (every 1)");
    let r = d
        .admin()
        .await
        .rebuild(RebuildRequest {
            name: "Orders.Order".into(),
            snapshot_id: snap.id.clone(),
            force: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.restarted_from, Some(snap.checkpoint));
    let got = d.aggregate(&format!("order-{a}")).await.unwrap();
    assert_eq!(got.version, 0);
    d.admin()
        .await
        .delete_snapshot(DeleteSnapshotRequest {
            name: "Orders.Order".into(),
            id: snap.id.clone(),
        })
        .await
        .unwrap();
    let err = d
        .admin()
        .await
        .delete_snapshot(DeleteSnapshotRequest {
            name: "Orders.Order".into(),
            id: snap.id,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    let err = d
        .admin()
        .await
        .snapshot(SnapshotRequest {
            name: "Orders.Nope".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    d.shutdown().await;
    db.shutdown().await;
}
