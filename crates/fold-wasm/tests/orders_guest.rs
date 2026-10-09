//! Drives the real Orders example guest (built to wasm32 by cargo) through
//! all three roles: a command handler emits an event, evolve folds it into
//! state, and a projection step turns it into read-model mutations.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use fold_wasm::{
    CommandInput, CommandReply, Engine, Event, EvolveInput, Guest, Limits, ModuleCache, Op,
    ProcCtx, ProcessInput, ProjectionInput, RowReader, Trigger, WasmError,
};
use serde_json::{Value, json};

/// Builds `orders-guest` for wasm32 into its own target dir (so it never
/// contends with the outer cargo's lock) and copies the module into a temp
/// dir, under a file lock shared with the daemon's suite: cargo rewrites the
/// artifact while another binary's test may be copying it.
fn build_orders_guest() -> (tempfile::TempDir, PathBuf) {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let target_dir = workspace.join("target/guest");
    std::fs::create_dir_all(&target_dir).unwrap();
    let lock = std::fs::File::create(target_dir.join(".guest.lock")).unwrap();
    lock.lock().expect("guest build lock");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let status = Command::new(cargo)
        .current_dir(&workspace)
        .args([
            "build",
            "-p",
            "orders-guest",
            "--target",
            "wasm32-unknown-unknown",
            "--release",
            "--target-dir",
        ])
        .arg(&target_dir)
        .status()
        .expect("cargo runs");
    assert!(status.success(), "building orders-guest for wasm32 failed");
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("orders.wasm");
    std::fs::copy(
        target_dir.join("wasm32-unknown-unknown/release/orders_guest.wasm"),
        &dest,
    )
    .unwrap();
    lock.unlock().expect("guest build unlock");
    (dir, dest)
}

fn load() -> Guest {
    let engine = Engine::new().unwrap();
    let cache = ModuleCache::new(engine.clone());
    let (_dir, path) = build_orders_guest();
    let module = cache
        .load(
            path.parent().unwrap(),
            path.file_name().unwrap().to_str().unwrap(),
        )
        .expect("compiles");
    Guest::new(&engine, &module, Limits::default()).expect("links")
}

struct NoRows;
impl RowReader for NoRows {
    fn get_row(&self, _: &str, _: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }
}

struct Owner(Value);
impl RowReader for Owner {
    fn get_row(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        assert_eq!(table, "order_owner");
        let key: Value = serde_json::from_slice(key).unwrap();
        assert_eq!(
            key,
            json!({"order_id": "a0000000-0000-0000-0000-000000000001"})
        );
        Ok(Some(serde_json::to_vec(&self.0).unwrap()))
    }
}

const ORDER: &str = "a0000000-0000-0000-0000-000000000001";
const CUSTOMER: &str = "c0000000-0000-0000-0000-000000000001";
const LINE: &str = "11111111-0000-0000-0000-000000000001";

fn line(id: &str, qty: u64, amount: &str) -> Value {
    json!({ "line_id": id, "sku": "SKU-1", "qty": qty, "price": { "amount": amount, "currency": "EUR" } })
}

fn cmd_input(state: Option<Value>, name: &str, payload: Value) -> CommandInput {
    CommandInput {
        abi: 1,
        aggregate: "Orders.Order".into(),
        stream: format!("order-{ORDER}"),
        key: json!(ORDER),
        version: state.as_ref().map(|_| 0),
        state,
        now: "2026-10-07T12:00:00Z".into(),
        command: fold_wasm::Command {
            r#type: format!("Orders.Order.{name}"),
            payload,
        },
    }
}

fn recorded(ty: &str, version: u64, payload: Value) -> Event {
    Event {
        stream: format!("order-{ORDER}"),
        r#type: format!("{ty}@v1"),
        version,
        position: version,
        payload,
        metadata: json!({}),
    }
}

#[test]
fn place_order_emits_evolves_and_projects() {
    let g = load();

    // 1. The handler emits OrderPlaced with the total it computed.
    let reply = g
        .handle(
            "handle_place_order",
            &cmd_input(
                None,
                "PlaceOrder",
                json!({ "customer_id": CUSTOMER, "lines": [line(LINE, 2, "7.50")] }),
            ),
        )
        .expect("handler runs");
    let CommandReply::Events(events) = reply else {
        panic!("expected events, got {reply:?}");
    };
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].r#type, "Orders.OrderPlaced");
    assert_eq!(events[0].payload["order_id"], json!(ORDER));
    assert_eq!(
        events[0].payload["total"],
        json!({ "amount": "15.00", "currency": "EUR" })
    );

    // 2. Evolve folds it into state keyed by line id.
    let placed = recorded("Orders.OrderPlaced", 0, events[0].payload.clone());
    let state = g
        .evolve(
            "evolve_order",
            &EvolveInput {
                abi: 1,
                aggregate: "Orders.Order".into(),
                stream: format!("order-{ORDER}"),
                key: json!(ORDER),
                version: None,
                state: None,
                event: placed.clone(),
            },
        )
        .expect("evolves");
    assert_eq!(state["status"], "Pending");
    assert_eq!(state["lines"][LINE]["qty"], 2);
    assert_eq!(state["total"]["amount"], "15.00");

    // 3. The projection turns it into column ops without reading any row.
    let muts = g
        .apply(
            "project_customer_orders",
            &ProjectionInput {
                abi: 1,
                projection: "Orders.CustomerOrders".into(),
                event: placed,
            },
            Arc::new(NoRows),
        )
        .expect("projects");
    let ops: Vec<&str> = muts
        .iter()
        .map(|m| match &m.op {
            Op::SetAdd { .. } => "set_add",
            Op::ListPush { .. } => "list_push",
            Op::ListTruncate { .. } => "list_truncate",
            Op::Add { .. } => "add",
            Op::Upsert { .. } => "upsert",
            _ => "other",
        })
        .collect();
    assert_eq!(
        ops,
        [
            "set_add",
            "list_push",
            "list_truncate",
            "add",
            "add",
            "upsert"
        ]
    );
    assert!(muts.iter().take(5).all(|m| m.table == "customer_orders"));
    assert_eq!(muts[5].table, "order_owner");
}

#[test]
fn a_second_place_order_is_rejected_by_the_handler() {
    let g = load();
    let reply = g
        .handle(
            "handle_place_order",
            &cmd_input(
                Some(json!({ "status": "Pending" })),
                "PlaceOrder",
                json!({ "customer_id": CUSTOMER, "lines": [line(LINE, 1, "1.00")] }),
            ),
        )
        .expect("handler runs");
    match reply {
        CommandReply::Rejected(r) => assert_eq!(r.code, "ALREADY_PLACED"),
        other => panic!("expected a rejection, got {other:?}"),
    }
}

#[test]
fn adding_a_line_with_the_same_id_replaces_it() {
    let g = load();
    let mut state = json!({
        "customer_id": CUSTOMER, "status": "Pending",
        "lines": { LINE: line(LINE, 2, "7.50") },
        "total": { "amount": "15.00", "currency": "EUR" }
    });
    for qty in [3u64, 4] {
        let reply = g
            .handle(
                "handle_add_line",
                &cmd_input(
                    Some(state.clone()),
                    "AddLine",
                    json!({ "line": line(LINE, qty, "7.50") }),
                ),
            )
            .unwrap();
        let CommandReply::Events(events) = reply else {
            panic!()
        };
        state = g
            .evolve(
                "evolve_order",
                &EvolveInput {
                    abi: 1,
                    aggregate: "Orders.Order".into(),
                    stream: format!("order-{ORDER}"),
                    key: json!(ORDER),
                    version: Some(0),
                    state: Some(state),
                    event: recorded("Orders.LineAdded", 1, events[0].payload.clone()),
                },
            )
            .unwrap();
    }
    assert_eq!(
        state["lines"].as_object().unwrap().len(),
        1,
        "same id, same entity"
    );
    assert_eq!(state["lines"][LINE]["qty"], 4);
    // Replacing a line by id swaps its contribution: 4 x 7.50, not a running sum.
    assert_eq!(state["total"]["amount"], "30.00");
}

#[test]
fn cancelling_looks_up_the_owner_and_removes_the_open_order() {
    let g = load();
    let cancelled = recorded(
        "Orders.OrderCancelled",
        1,
        json!({ "order_id": ORDER, "reason": null, "at": "2026-10-07T12:00:00Z" }),
    );
    let muts = g
        .apply(
            "project_customer_orders",
            &ProjectionInput {
                abi: 1,
                projection: "Orders.CustomerOrders".into(),
                event: cancelled.clone(),
            },
            Arc::new(Owner(json!({ "customer_id": CUSTOMER }))),
        )
        .unwrap();
    assert_eq!(muts.len(), 1);
    assert_eq!(muts[0].key, json!({ "customer_id": CUSTOMER }));
    assert!(
        matches!(&muts[0].op, Op::SetRemove { column, value } if column == "open_orders" && *value == json!(ORDER))
    );

    // Negative control: with no owner row the fold refuses rather than guessing.
    let err = g
        .apply(
            "project_customer_orders",
            &ProjectionInput {
                abi: 1,
                projection: "Orders.CustomerOrders".into(),
                event: cancelled,
            },
            Arc::new(NoRows),
        )
        .unwrap_err();
    assert!(matches!(err, WasmError::GuestError(_)), "{err}");
}

#[test]
fn mixed_currencies_are_rejected_with_a_code() {
    let g = load();
    let mut other = line("22222222-0000-0000-0000-000000000002", 1, "1.00");
    other["price"]["currency"] = json!("USD");
    let reply = g
        .handle(
            "handle_place_order",
            &cmd_input(
                None,
                "PlaceOrder",
                json!({ "customer_id": CUSTOMER, "lines": [line(LINE, 1, "1.00"), other] }),
            ),
        )
        .unwrap();
    match reply {
        CommandReply::Rejected(r) => assert_eq!(r.code, "MIXED_CURRENCY"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn upcast_order_cancelled_v2_adds_a_note() {
    let g = load();
    let out = g
        .upcast(
            "upcast_order_cancelled_v2",
            &fold_wasm::UpcastInput {
                abi: 1,
                event: fold_wasm::UpcastEvent {
                    r#type: "Orders.OrderCancelled@v1".into(),
                    from_version: 1,
                    to_version: 2,
                    payload: json!({ "order_id": ORDER, "reason": "late", "at": "2026-10-07T12:00:00Z" }),
                },
            },
        )
        .expect("upcaster runs");
    assert_eq!(out["note"], "wasm:late");
    assert_eq!(out["reason"], "late");
}

#[test]
fn fulfilment_sets_cancels_and_reacts_to_the_overdue_timer() {
    let guest = load();
    let react = |state: Option<Value>, trigger: Trigger| {
        guest
            .react(
                "react_fulfilment",
                &ProcessInput {
                    abi: 1,
                    ctx: ProcCtx {
                        process: "Orders.Fulfilment".into(),
                        key: json!(ORDER),
                        now: "2026-10-07T12:00:00Z".into(),
                    },
                    state,
                    trigger,
                },
            )
            .unwrap()
    };
    // Placed with the metadata: a timer is set.
    let mut placed = recorded(
        "Orders.OrderPlaced",
        0,
        json!({ "order_id": ORDER, "customer_id": "c", "lines": [], "total": { "amount": "0", "currency": "EUR" } }),
    );
    placed.metadata = json!({ "overdue_after_ms": 5000 });
    let r = react(None, Trigger::Event(placed.clone()));
    assert_eq!(r.timers.len(), 1);
    assert_eq!(r.timers[0].name, "ShipmentOverdue");
    assert_eq!(r.timers[0].after_ms, Some(5000));
    assert!(r.cancel_timers.is_empty());
    // Without the metadata: none (the control).
    placed.metadata = json!({});
    assert!(react(None, Trigger::Event(placed)).timers.is_empty());
    // Shipped: the timer is cancelled.
    let state = json!({ "customer_id": "c", "shipment": "prepared", "cancel_refused": false });
    let shipped = recorded(
        "Shipping.ShipmentShipped",
        1,
        json!({ "shipment_id": ORDER, "order_id": ORDER }),
    );
    let r = react(Some(state.clone()), Trigger::Event(shipped));
    assert_eq!(r.cancel_timers, ["ShipmentOverdue"]);
    // The timer fires on an unshipped order: cancel it.
    let fired = Trigger::Timer {
        name: "ShipmentOverdue".into(),
        due_at: "2026-10-07T12:00:05Z".into(),
        fired_at: "2026-10-07T12:00:05.01Z".into(),
    };
    let r = react(Some(state.clone()), fired.clone());
    assert_eq!(r.state.as_ref().unwrap()["shipment"], "overdue");
    assert_eq!(r.commands.len(), 1);
    assert_eq!(r.commands[0].command, "Orders.Order.CancelOrder");
    assert_eq!(r.commands[0].stream, format!("order-{ORDER}"));
    assert_eq!(r.commands[0].payload["reason"], "shipment overdue");
    // On a shipped order the timer is harmless.
    let mut shipped_state = state;
    shipped_state["shipment"] = json!("shipped");
    let r = react(Some(shipped_state), fired);
    assert!(r.commands.is_empty());
    assert_eq!(r.state.unwrap()["shipment"], "shipped");
}
