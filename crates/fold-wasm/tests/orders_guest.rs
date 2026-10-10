//! Drives the real Orders example guest (built to wasm32 by cargo) through
//! the derivation layer's roles: evolve folds events into state, a
//! projection step turns them into read-model mutations, an upcaster reads
//! an old version as a newer one.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use fold_wasm::{
    Engine, Event, EvolveInput, Guest, Limits, ModuleCache, Op, ProjectionInput, RowReader,
    WasmError,
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

fn evolve(g: &Guest, state: Option<Value>, event: Event) -> Result<Value, WasmError> {
    g.evolve(
        "evolve_order",
        &EvolveInput {
            abi: 1,
            aggregate: "Orders.Order".into(),
            stream: format!("order-{ORDER}"),
            key: json!(ORDER),
            version: state.as_ref().map(|_| event.version.saturating_sub(1)),
            state,
            event,
        },
    )
}

#[test]
fn an_order_placed_evolves_and_projects() {
    let g = load();
    // Evolve folds OrderPlaced into state keyed by line id.
    let placed = recorded(
        "Orders.OrderPlaced",
        0,
        json!({
            "order_id": ORDER, "customer_id": CUSTOMER,
            "lines": [line(LINE, 2, "7.50")],
            "total": { "amount": "15.00", "currency": "EUR" },
        }),
    );
    let state = evolve(&g, None, placed.clone()).expect("evolves");
    assert_eq!(state["status"], "Pending");
    assert_eq!(state["lines"][LINE]["qty"], 2);
    assert_eq!(state["total"]["amount"], "15.00");

    // The projection turns it into column ops without reading any row.
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
fn a_line_added_with_the_same_id_replaces_it() {
    let g = load();
    let mut state = json!({
        "customer_id": CUSTOMER, "status": "Pending",
        "lines": { LINE: line(LINE, 2, "7.50") },
        "total": { "amount": "15.00", "currency": "EUR" }
    });
    for (qty, total) in [(3u64, "22.50"), (4, "30.00")] {
        state = evolve(
            &g,
            Some(state),
            recorded(
                "Orders.LineAdded",
                1,
                json!({ "order_id": ORDER, "line": line(LINE, qty, "7.50"), "total": { "amount": total, "currency": "EUR" } }),
            ),
        )
        .unwrap();
    }
    assert_eq!(
        state["lines"].as_object().unwrap().len(),
        1,
        "same id, same entity"
    );
    assert_eq!(state["lines"][LINE]["qty"], 4);
    assert_eq!(state["total"]["amount"], "30.00");
    // An event the aggregate does not own is a guest error, not a guess.
    let err = evolve(
        &g,
        Some(state),
        recorded("Orders.Nope", 2, json!({ "order_id": ORDER })),
    )
    .unwrap_err();
    assert!(matches!(err, WasmError::GuestError(_)), "{err}");
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
