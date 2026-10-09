//! A schema that changed since the log was written: the daemon diffs the
//! file against the stored text at start, refuses a breaking change
//! (unless forced), applies the rebuilds and drops of a compatible one, and
//! reports what it did in Health.

use fold_proto::v1::GetSchemaRequest;
use serde_json::json;

use crate::common::{Daemon, line, settle, state_of, uuid};

const TOTALS: &str = "  projection OrderTotals {\n    from OrderPlaced, OrderCancelled\n    fold wasm \"orders.wasm\" export \"project_order_totals\"\n    table order_totals { key order_id: uuid, total: Shared.Money, status: Status }\n  }\n";

async fn place_and_cancel(d: &Daemon, a: &str) -> u64 {
    let stream = format!("order-{a}");
    d.exec(
        "Orders.Order.PlaceOrder",
        &stream,
        json!({ "customer_id": uuid('c', 1), "lines": [line(&uuid('1', 1), 1, "2.00")] }),
    )
    .await
    .unwrap();
    d.exec("Orders.Order.CancelOrder", &stream, json!({}))
        .await
        .unwrap()
        .last_position
}

#[tokio::test]
async fn a_compatible_change_is_accepted_and_a_new_projection_is_built_from_history() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let a = uuid('a', 1);
    let pos = place_and_cancel(&d, &a).await;
    settle(&d, &[]).await;
    assert!(d.health().await.last_schema_change.is_empty());

    // A second projection over the same fold (its tables are its own).
    d.rewrite_schema(|s| {
        assert!(s.contains(TOTALS), "the example's OrderTotals moved");
        s.replace(
            TOTALS,
            &format!(
                "{TOTALS}{}",
                TOTALS.replace("projection OrderTotals", "projection TotalsAgain")
            ),
        )
    });
    d.restart().await;
    let h = d.health().await;
    assert!(
        h.last_schema_change.contains("applied: 1 change(s)")
            && h.last_schema_change.contains("1 compatible"),
        "{}",
        h.last_schema_change
    );
    // Built from history: the cancelled order is in the new projection.
    let row = d
        .row(
            "Orders.TotalsAgain",
            "order_totals",
            json!({ "order_id": a }),
            pos,
        )
        .await;
    assert_eq!(row["status"], "Cancelled");
    assert_eq!(row["total"]["amount"], "2.00");
    // The old one was not touched (its checkpoint is still past the events).
    assert!(
        d.checkpoint("Orders.OrderTotals")
            .await
            .is_some_and(|c| c >= pos)
    );
    // The stored text is the new one.
    let stored = d
        .admin()
        .await
        .get_schema(GetSchemaRequest {})
        .await
        .unwrap()
        .into_inner()
        .source;
    assert!(stored.contains("projection TotalsAgain"));
    // A restart with the same file reports nothing.
    d.restart().await;
    assert!(d.health().await.last_schema_change.is_empty());
    d.shutdown().await;
}

#[tokio::test]
async fn a_breaking_change_is_refused_and_force_schema_overrides_it() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let a = uuid('a', 2);
    place_and_cancel(&d, &a).await;
    settle(&d, &[]).await;

    // A required field on an event the log holds: breaking.
    d.rewrite_schema(|s| {
        s.replace(
            "event OrderPlaced v1   { order_id: uuid, customer_id: uuid,",
            "event OrderPlaced v1   { order_id: uuid, channel: string, customer_id: uuid,",
        )
    });
    let err = d.try_restart().await.unwrap_err();
    let text = format!("{err:#}");
    assert!(text.contains("breaks data in the log"), "{text}");
    assert!(
        text.contains("[breaking] Orders.OrderPlaced@v1.channel"),
        "{text}"
    );
    assert!(text.contains("--force-schema"), "{text}");

    // Control: removing an event family the log never saw is compatible
    // (LineRemoved: its command and its place in the events list go too).
    d.rewrite_schema(|s| {
        s.replace("channel: string, ", "")
            .replace(
                "  event LineRemoved v1   { order_id: uuid, line_id: uuid, total: Shared.Money }\n",
                "",
            )
            .replace("events OrderPlaced, LineAdded, LineRemoved, OrderCancelled", "events OrderPlaced, LineAdded, OrderCancelled")
            .replace(
                "      RemoveLine  { line_id: uuid }                    -> wasm \"orders.wasm\" export \"handle_remove_line\",\n",
                "",
            )
    });
    d.restart().await;
    let note = d.health().await.last_schema_change;
    assert!(note.starts_with("applied:"), "{note}");
    assert!(note.contains("0 breaking"), "{note}");

    // Forced: the breaking change is adopted and the daemon runs.
    d.rewrite_schema(|s| {
        s.replace(
            "event OrderPlaced v1   { order_id: uuid, customer_id: uuid,",
            "event OrderPlaced v1   { order_id: uuid, channel: string, customer_id: uuid,",
        )
    });
    d.configure = std::sync::Arc::new(|o| o.force_schema = true);
    d.restart().await;
    let note = d.health().await.last_schema_change;
    assert!(note.starts_with("forced over a breaking change"), "{note}");
    assert!(
        d.aggregate(&format!("order-{a}")).await.unwrap().found,
        "the old stream still loads (guests tolerate the missing field)"
    );
    d.shutdown().await;
}

#[tokio::test]
async fn a_changed_projection_source_rebuilds_it_automatically() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let a = uuid('a', 3);
    let pos = place_and_cancel(&d, &a).await;
    let row = d
        .row(
            "Orders.OrderTotals",
            "order_totals",
            json!({ "order_id": a }),
            pos,
        )
        .await;
    assert_eq!(row["status"], "Cancelled");

    // Without OrderCancelled among its sources the rebuilt projection
    // never sees the cancellation: the row is derived afresh from history.
    d.rewrite_schema(|s| {
        s.replace(
            "    from OrderPlaced, OrderCancelled\n    fold wasm \"orders.wasm\" export \"project_order_totals\"",
            "    from OrderPlaced\n    fold wasm \"orders.wasm\" export \"project_order_totals\"",
        )
    });
    d.restart().await;
    let note = d.health().await.last_schema_change;
    assert!(note.contains("1 rebuild"), "{note}");
    let row = d
        .row(
            "Orders.OrderTotals",
            "order_totals",
            json!({ "order_id": a }),
            pos,
        )
        .await;
    assert_eq!(row["status"], "Pending", "rebuilt without the cancellation");
    d.shutdown().await;
}

#[tokio::test]
async fn a_textual_change_stores_the_new_text_without_rebuilding() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let a = uuid('a', 4);
    let pos = place_and_cancel(&d, &a).await;
    settle(&d, &[]).await;
    let before = d.checkpoint("Orders.OrderTotals").await;
    assert!(before.is_some_and(|c| c >= pos));

    d.rewrite_schema(|s| format!("// a comment only\n{s}"));
    d.restart().await;
    let h = d.health().await;
    assert_eq!(
        h.last_schema_change,
        "textual change only: stored the new text"
    );
    assert_eq!(
        d.checkpoint("Orders.OrderTotals").await,
        before,
        "nothing was reset"
    );
    let stored = d
        .admin()
        .await
        .get_schema(GetSchemaRequest {})
        .await
        .unwrap()
        .into_inner()
        .source;
    assert!(stored.starts_with("// a comment only\n"));
    d.shutdown().await;
}

#[tokio::test]
async fn an_aggregate_state_change_drops_its_instance_snapshots() {
    let mut d = Daemon::start(|s| s.replace("snapshot every 100", "snapshot every 2")).await;
    let a = uuid('a', 5);
    let stream = format!("order-{a}");
    d.exec(
        "Orders.Order.PlaceOrder",
        &stream,
        json!({ "customer_id": uuid('c', 1), "lines": [line(&uuid('1', 1), 1, "2.00")] }),
    )
    .await
    .unwrap();
    for i in 2..=4 {
        d.exec(
            "Orders.Order.AddLine",
            &stream,
            json!({ "line": line(&uuid('1', i), 1, "1.00") }),
        )
        .await
        .unwrap();
    }
    // A restart's first load replays and takes an instance snapshot; the
    // next restart loads from it.
    d.restart().await;
    assert_eq!(d.aggregate(&stream).await.unwrap().replayed, 4);
    d.restart().await;
    let got = d.aggregate(&stream).await.unwrap();
    assert!(got.snapshot_version.is_some(), "{got:?}");
    assert_eq!(got.replayed, 0, "{got:?}");

    // The state shape changes: snapshots are stale and dropped.
    d.rewrite_schema(|s| {
        s.replace(
            "state { customer_id: uuid, status: Status, lines: map<uuid, Line>, total: Shared.Money }",
            "state { customer_id: uuid, status: Status, lines: map<uuid, Line>, total: Shared.Money, note: string? }",
        )
    });
    d.restart().await;
    let note = d.health().await.last_schema_change;
    assert!(note.contains("1 rebuild"), "{note}");
    let got = d.aggregate(&stream).await.unwrap();
    assert_eq!(got.snapshot_version, None, "{got:?}");
    assert_eq!(got.replayed, 4, "every event replayed");
    assert_eq!(state_of(&got)["lines"].as_object().unwrap().len(), 4);
    d.shutdown().await;
}

#[tokio::test]
async fn a_removed_process_drops_its_tables() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let a = uuid('a', 6);
    d.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": uuid('c', 1), "lines": [line(&uuid('1', 1), 1, "2.00")] }),
    )
    .await
    .unwrap();
    settle(&d, &[format!("shipment-{a}")]).await;
    assert_eq!(d.processes().await.len(), 1);
    d.shutdown().await;
    {
        let store = derived_store(&d);
        assert!(store.checkpoint("Orders.Fulfilment").unwrap().is_some());
        assert!(
            !store
                .snapshot()
                .unwrap()
                .scan("Orders.Fulfilment", "state", &[], 10)
                .unwrap()
                .is_empty()
        );
    }

    d.rewrite_schema(|s| {
        let start = s
            .find("  /// A process manager:")
            .expect("the process's docs");
        let end = start + s[start..].find("\n  }\n").expect("its end") + 5;
        format!("{}{}", &s[..start], &s[end..])
    });
    d.restart().await;
    let note = d.health().await.last_schema_change;
    assert!(note.contains("1 compatible"), "{note}");
    assert!(d.processes().await.is_empty());
    d.shutdown().await;
    let store = derived_store(&d);
    assert_eq!(store.checkpoint("Orders.Fulfilment").unwrap(), None);
    assert!(
        store
            .snapshot()
            .unwrap()
            .scan("Orders.Fulfilment", "state", &[], 10)
            .unwrap()
            .is_empty()
    );
}

/// The daemon's derived store, opened after it shut down.
fn derived_store(d: &Daemon) -> fold_store::DerivedStore {
    fold_store::DerivedStore::open_or_create(
        &d.data_dir().join("data/derived/derived.redb"),
        fold_core::FsyncPolicy::Never,
    )
    .unwrap()
}
