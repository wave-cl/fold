//! Derived data beside the log: when the log moves backwards (a
//! truncation) the daemon drops what it derived past the cut, by the
//! generation it records and by the event-id fingerprints on checkpoints
//! and snapshots; a derived store of another log is rebuilt from scratch.

use fold_proto::v1::ListProcessesRequest;
use serde_json::json;

use crate::common::{Daemon, line, settle, state_of, uuid};

#[tokio::test]
async fn a_truncated_log_drops_derived_data_past_the_cut() {
    let mut d = Daemon::start(|s| s.replace("snapshot every 100", "snapshot every 2")).await;
    let a = uuid('a', 1);
    let stream = format!("order-{a}");
    let placed = d
        .exec(
            "Orders.Order.PlaceOrder",
            &stream,
            json!({ "customer_id": uuid('c', 1), "lines": [line(&uuid('1', 1), 1, "2.00")] }),
        )
        .await
        .unwrap();
    settle(&d, &[format!("shipment-{a}")]).await;
    let head_after_place = d.health().await.head;
    for i in 2..=4 {
        d.exec(
            "Orders.Order.AddLine",
            &stream,
            json!({ "line": line(&uuid('1', i), 1, "1.00") }),
        )
        .await
        .unwrap();
    }
    settle(&d, &[]).await;
    // Two restarts: the first load replays and snapshots the instance, the
    // second loads from that snapshot (the control: the snapshot is used).
    d.restart().await;
    assert_eq!(d.aggregate(&stream).await.unwrap().replayed, 4);
    d.restart().await;
    let got = d.aggregate(&stream).await.unwrap();
    assert!(got.snapshot_version.is_some(), "{got:?}");
    assert_eq!(state_of(&got)["lines"].as_object().unwrap().len(), 4);
    let full_head = d.health().await.head;
    assert!(d.checkpoint("Orders.OrderTotals").await.unwrap() + 1 >= full_head);

    // Cut the log back to just after the order was placed and the shipment
    // prepared, the way `fold restore --to` does.
    d.shutdown().await;
    let report = fold_core::truncate_log(
        &d.data_dir().join("data"),
        foldd::LOG_NAME,
        fold_core::GlobalPosition(head_after_place),
    )
    .unwrap();
    assert_eq!(report.generation, 1);
    d.restart().await;

    // The instance snapshot (version 3) is past the cut: it is discarded
    // and the stream replays from its remaining events.
    let got = d.aggregate(&stream).await.unwrap();
    assert_eq!(got.snapshot_version, None, "{got:?}");
    assert_eq!(got.version, 0);
    assert_eq!(state_of(&got)["lines"].as_object().unwrap().len(), 1);
    // Runners past the cut were reset and are back at the new head.
    settle(&d, &[]).await;
    let head = d.health().await.head;
    assert_eq!(head, head_after_place);
    for p in d.projections().await {
        assert!(
            p.checkpoint.is_some_and(|cp| cp < head),
            "{p:?} looks past the cut"
        );
    }
    // The totals row reflects the single line again.
    let row = d
        .row(
            "Orders.OrderTotals",
            "order_totals",
            json!({ "order_id": a }),
            placed.last_position,
        )
        .await;
    assert_eq!(row["total"]["amount"], "2.00");
    // And the log keeps working from the cut: versions are dense.
    let added = d
        .exec(
            "Orders.Order.AddLine",
            &stream,
            json!({ "line": line(&uuid('1', 9), 1, "1.00") }),
        )
        .await
        .unwrap();
    assert_eq!(added.version, Some(1));
    d.shutdown().await;
}

#[tokio::test]
async fn a_derived_store_of_another_log_is_rebuilt_from_scratch() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let a = uuid('a', 2);
    d.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": uuid('c', 1), "lines": [line(&uuid('1', 1), 1, "2.00")] }),
    )
    .await
    .unwrap();
    settle(&d, &[format!("shipment-{a}")]).await;
    d.shutdown().await;
    let store = fold_store::DerivedStore::open_or_create(
        &d.data_dir().join("data/derived/derived.redb"),
        fold_core::FsyncPolicy::Never,
    )
    .unwrap();
    let bound_to = store.log_id().unwrap().expect("bound to the log");
    assert!(store.checkpoint("Orders.OrderTotals").unwrap().is_some());
    drop(store);

    // The same derived store next to a brand-new log (an operator copied
    // the wrong directory): it is reset and bound to the new log.
    let other = d.data_dir().join("data2");
    std::fs::create_dir_all(other.join("derived")).unwrap();
    std::fs::copy(
        d.data_dir().join("data/derived/derived.redb"),
        other.join("derived/derived.redb"),
    )
    .unwrap();
    d.restart_on("data2").await;
    let h = d.health().await;
    assert_eq!(h.head, 0, "a fresh log");
    assert_ne!(h.log_id, bound_to.to_string());
    assert!(
        d.projections().await.iter().all(|p| p.checkpoint.is_none()),
        "nothing derived carried over"
    );
    assert!(
        d.admin()
            .await
            .list_processes(ListProcessesRequest {})
            .await
            .unwrap()
            .into_inner()
            .processes
            .iter()
            .all(|p| p.checkpoint.is_none())
    );
    d.shutdown().await;
    let store = fold_store::DerivedStore::open_or_create(
        &other.join("derived/derived.redb"),
        fold_core::FsyncPolicy::Never,
    )
    .unwrap();
    assert_eq!(
        store.log_id().unwrap().map(|u| u.to_string()),
        Some(h.log_id)
    );
    assert_eq!(store.checkpoint("Orders.OrderTotals").unwrap(), None);
}
