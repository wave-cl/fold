//! Event upcasting on the daemon's paths: a stored `@v1` reaches guests as
//! the latest version, declaratively or through a wasm upcaster, through a
//! chain, on replay after a restart; the log keeps what was recorded;
//! defaults fill old records; a handler may not emit an old version.

use fold_proto::v1::expected_version::Kind;
use serde_json::{Value, json};
use tonic::Code;

use crate::common::{Daemon, copy_orders_guest, line, settle, state_of, uuid, workspace};

const V1: &str = "  event OrderCancelled v1 { order_id: uuid, reason: string?, at: timestamp }\n";

/// The example schema with OrderCancelled v2 (`note`, defaulted so the
/// example handler, which emits the v1 shape, still writes a valid v2)
/// after the given upcast clause, the state and the totals table carrying
/// `note` too.
fn with_v2(upcast: &str) -> impl Fn(&str) -> String + '_ {
    move |s: &str| {
        assert!(
            s.contains(V1),
            "the example schema's OrderCancelled v1 line moved"
        );
        s.replace(
            V1,
            &format!(
                "{V1}  event OrderCancelled v2 {{ order_id: uuid, reason: string?, at: timestamp, note: string = \"fresh\" }} {upcast}\n"
            ),
        )
        .replace(
            "state { customer_id: uuid, status: Status, lines: map<uuid, Line>, total: Shared.Money }",
            "state { customer_id: uuid, status: Status, lines: map<uuid, Line>, total: Shared.Money, note: string? }",
        )
        .replace(
            "table order_totals { key order_id: uuid, total: Shared.Money, status: Status }",
            "table order_totals { key order_id: uuid, total: Shared.Money, status: Status, note: string? }",
        )
    }
}

async fn place(d: &Daemon, a: &str, c: &str) -> u64 {
    d.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "5.00")] }),
    )
    .await
    .expect("place")
    .last_position
}

fn cancelled_v1(a: &str) -> Value {
    json!({ "order_id": a, "reason": "late", "at": "2026-01-01T00:00:00Z" })
}

fn payload_of(e: &fold_proto::v1::RecordedEvent) -> Value {
    serde_json::from_slice(&e.payload).unwrap()
}

#[tokio::test]
async fn declarative_upcast_reaches_aggregate_projection_and_replay() {
    let mut d = Daemon::start(with_v2("upcast from v1 { set note: \"legacy\" }")).await;
    let c = uuid('c', 1);
    let a = uuid('a', 1);
    let stream = format!("order-{a}");
    place(&d, &a, &c).await;

    // A raw append of the old version is allowed (the migration path).
    let appended = d
        .append(
            &stream,
            "Orders.OrderCancelled@v1",
            cancelled_v1(&a),
            Kind::Exact(0),
        )
        .await
        .expect("an old version may be appended");
    settle(&d, &[]).await;

    // The aggregate evolved from the v2 shape (the candidate state on commit).
    let s = state_of(&d.aggregate(&stream).await.unwrap());
    assert_eq!(s["status"], "Cancelled");
    assert_eq!(s["note"], "legacy");

    // The projection saw v2 too.
    let row = d
        .row(
            "Orders.OrderTotals",
            "order_totals",
            json!({ "order_id": a }),
            appended.last_position,
        )
        .await;
    assert_eq!(row["status"], "Cancelled");
    assert_eq!(row["note"], "legacy");

    // The log keeps what was recorded: `@v1`, no note.
    let events = d.all_events().await;
    let stored = events
        .iter()
        .find(|e| e.r#type.starts_with("Orders.OrderCancelled"))
        .unwrap();
    assert_eq!(stored.r#type, "Orders.OrderCancelled@v1");
    assert!(payload_of(stored).get("note").is_none());

    // A restart replays from the log and derives the same state.
    d.restart().await;
    let got = d.aggregate(&stream).await.unwrap();
    assert!(got.replayed > 0, "the state was replayed, not cached");
    let s = state_of(&got);
    assert_eq!(s["status"], "Cancelled");
    assert_eq!(s["note"], "legacy");

    // A handler emits the latest version: the command path writes `@v2`.
    let b = uuid('b', 1);
    place(&d, &b, &c).await;
    let cancelled = d
        .exec(
            "Orders.Order.CancelOrder",
            &format!("order-{b}"),
            json!({ "reason": "x" }),
        )
        .await
        .expect("cancel");
    assert!(
        cancelled.events[0].r#type.ends_with("@v2"),
        "{:?}",
        cancelled.events[0].r#type
    );
    // The handler emitted no note; the default filled it, not the upcast.
    assert_eq!(payload_of(&cancelled.events[0])["note"], "fresh");
    d.shutdown().await;
}

#[tokio::test]
async fn wasm_upcast_variant() {
    let mut d = Daemon::start(with_v2(
        "upcast from v1 wasm \"orders.wasm\" export \"upcast_order_cancelled_v2\"",
    ))
    .await;
    let c = uuid('c', 2);
    let a = uuid('a', 2);
    let stream = format!("order-{a}");
    place(&d, &a, &c).await;
    let appended = d
        .append(
            &stream,
            "Orders.OrderCancelled@v1",
            cancelled_v1(&a),
            Kind::Exact(0),
        )
        .await
        .unwrap();
    settle(&d, &[]).await;
    let s = state_of(&d.aggregate(&stream).await.unwrap());
    assert_eq!(s["note"], "wasm:late", "the guest's upcaster ran");
    let row = d
        .row(
            "Orders.OrderTotals",
            "order_totals",
            json!({ "order_id": a }),
            appended.last_position,
        )
        .await;
    assert_eq!(row["note"], "wasm:late");
    d.shutdown().await;
}

#[tokio::test]
async fn a_chain_of_two_applies_in_order() {
    // v1 -> v2 (set note) -> v3 (rename note as memo, add by).
    let mut d = Daemon::start(|s: &str| {
        with_v2("upcast from v1 { set note: \"legacy\" }")(s)
            .replace(
                "note: string = \"fresh\" } upcast from v1 { set note: \"legacy\" }\n",
                "note: string = \"fresh\" } upcast from v1 { set note: \"legacy\" }\n  event OrderCancelled v3 { order_id: uuid, reason: string?, at: timestamp, note: string = \"fresh\", by: string } upcast from v2 { set by: \"ops\" }\n",
            )
            .replace(
                "total: Shared.Money, note: string? }\n    evolve",
                "total: Shared.Money, note: string?, by: string? }\n    evolve",
            )
    })
    .await;
    let c = uuid('c', 3);
    let a = uuid('a', 3);
    let stream = format!("order-{a}");
    place(&d, &a, &c).await;
    d.append(
        &stream,
        "Orders.OrderCancelled@v1",
        cancelled_v1(&a),
        Kind::Exact(0),
    )
    .await
    .unwrap();
    let s = state_of(&d.aggregate(&stream).await.unwrap());
    assert_eq!(s["note"], "legacy", "v1 -> v2");
    assert_eq!(s["by"], "ops", "v2 -> v3");
    // Appending the middle version works too and goes through v3 only.
    let b = uuid('b', 3);
    place(&d, &b, &c).await;
    d.append(
        &format!("order-{b}"),
        "Orders.OrderCancelled@v2",
        json!({ "order_id": b, "reason": "late", "at": "2026-01-01T00:00:00Z", "note": "mine" }),
        Kind::Exact(0),
    )
    .await
    .unwrap();
    let s = state_of(&d.aggregate(&format!("order-{b}")).await.unwrap());
    assert_eq!(s["note"], "mine");
    assert_eq!(s["by"], "ops");
    d.shutdown().await;
}

#[tokio::test]
async fn defaults_fill_old_records_and_canonical_writes() {
    // A defaulted field on a stored event: absent on the wire, filled in
    // the log (canonical write) and in what guests see.
    let mut d = Daemon::start(|s: &str| {
        s.replace(
            "event LineRemoved v1   { order_id: uuid, line_id: uuid, total: Shared.Money }",
            "event LineRemoved v1   { order_id: uuid, line_id: uuid, total: Shared.Money, why: string = \"unspecified\" }",
        )
    })
    .await;
    let c = uuid('c', 4);
    let a = uuid('a', 4);
    let stream = format!("order-{a}");
    place(&d, &a, &c).await;
    // The handler emits LineRemoved without `why`; the stored payload has it.
    d.exec(
        "Orders.Order.AddLine",
        &stream,
        json!({ "line": line(&uuid('1', 2), 1, "1.00") }),
    )
    .await
    .unwrap();
    d.exec(
        "Orders.Order.RemoveLine",
        &stream,
        json!({ "line_id": uuid('1', 2) }),
    )
    .await
    .unwrap();
    let events = d.all_events().await;
    let removed = events
        .iter()
        .find(|e| e.r#type == "Orders.LineRemoved@v1")
        .unwrap();
    assert_eq!(payload_of(removed)["why"], "unspecified");
    // A raw append with `why: null` is also filled in.
    let appended = d
        .append(
            &stream,
            "Orders.LineRemoved",
            json!({ "order_id": a, "line_id": uuid('1', 9), "total": { "amount": "5.00", "currency": "EUR" }, "why": null }),
            Kind::Exact(2),
        )
        .await
        .unwrap();
    let events = d.all_events().await;
    let last = events
        .iter()
        .find(|e| e.position == appended.last_position)
        .unwrap();
    assert_eq!(payload_of(last)["why"], "unspecified");
    d.shutdown().await;
}

#[tokio::test]
async fn append_without_upcast_for_an_unknown_version_is_refused() {
    let mut d = Daemon::start(with_v2("upcast from v1 { set note: \"legacy\" }")).await;
    let a = uuid('a', 5);
    let err = d
        .append(
            &format!("order-{a}"),
            "Orders.OrderCancelled@v7",
            cancelled_v1(&a),
            Kind::Any(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    d.shutdown().await;
}

#[tokio::test]
async fn startup_refuses_a_missing_upcaster_export() {
    let dir = tempfile::tempdir().unwrap();
    let schema_src =
        std::fs::read_to_string(workspace().join("examples/orders/schema.fold")).unwrap();
    std::fs::write(
        dir.path().join("schema.fold"),
        with_v2("upcast from v1 wasm \"orders.wasm\" export \"no_such_upcaster\"")(&schema_src),
    )
    .unwrap();
    copy_orders_guest(&dir.path().join("orders.wasm"));
    let mut opts = foldd::Options::new(
        dir.path().join("data"),
        dir.path().join("schema.fold"),
        "127.0.0.1:0".parse().unwrap(),
    );
    opts.fsync = false;
    let err = match foldd::start(opts).await {
        Ok(r) => {
            r.shutdown().await.unwrap();
            panic!("the daemon started without the upcaster export");
        }
        Err(e) => format!("{e:#}"),
    };
    assert!(err.contains("no_such_upcaster"), "{err}");
}
