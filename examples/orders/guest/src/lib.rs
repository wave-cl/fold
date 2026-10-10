//! The Orders example guest: every entry point `examples/orders/derive.fold`
//! names. Built to `wasm32-unknown-unknown` by the end-to-end tests.
//!
//! The derivation layer's roles live here: evolve functions fold events
//! into aggregate state, projection steps maintain read models, and an
//! upcaster reads an old event version as a newer one. The commands,
//! invariants and the process manager are Rust code in
//! `examples/orders/app`.

use fold_guest::{Ctx, Event, Mutation, Row, UpcastEvent, Value, json};

fold_guest::module!();

// ---------------------------------------------------------------------------
// Customers.Customer

fold_guest::aggregate!(
    evolve_customer = |_state: Option<Value>, ev: &Event| {
        if ev.is("Customers.CustomerRegistered") {
            Ok(json!({ "name": ev.payload["name"] }))
        } else {
            Err(format!("Customer cannot evolve from {}", ev.r#type))
        }
    }
);

// ---------------------------------------------------------------------------
// Orders.Order

fold_guest::aggregate!(
    evolve_order = |state: Option<Value>, ev: &Event| {
        match ev.family() {
            "Orders.OrderPlaced" => {
                let mut lines = serde_json::Map::new();
                for line in ev.payload["lines"]
                    .as_array()
                    .ok_or("lines must be an array")?
                {
                    lines.insert(text(line, "line_id")?.to_string(), line.clone());
                }
                Ok(json!({
                    "customer_id": ev.payload["customer_id"],
                    "status": "Pending",
                    "lines": Value::Object(lines),
                    "total": ev.payload["total"],
                }))
            }
            "Orders.LineAdded" => {
                let mut state = state.ok_or("LineAdded before OrderPlaced")?;
                let line = &ev.payload["line"];
                let id = text(line, "line_id")?.to_string();
                // Same id, same entity: this replaces rather than duplicates.
                state["lines"][id] = line.clone();
                state["total"] = ev.payload["total"].clone();
                Ok(state)
            }
            "Orders.LineRemoved" => {
                let mut state = state.ok_or("LineRemoved before OrderPlaced")?;
                let id = text(&ev.payload, "line_id")?.to_string();
                if let Some(lines) = state["lines"].as_object_mut() {
                    lines.remove(&id);
                }
                state["total"] = ev.payload["total"].clone();
                Ok(state)
            }
            "Orders.OrderCancelled" => {
                let mut state = state.ok_or("OrderCancelled before OrderPlaced")?;
                state["status"] = json!("Cancelled");
                // Later versions of the event may carry a note and a `by`
                // (the end-to-end tests add them with an upcast); the base
                // schema has neither, so state stays valid without them.
                for extra in ["note", "by"] {
                    if let Some(v) = ev.payload.get(extra) {
                        state[extra] = v.clone();
                    }
                }
                Ok(state)
            }
            other => Err(format!("Order cannot evolve from {other}")),
        }
    }
);

// ---------------------------------------------------------------------------
// Shipping.Shipment

fold_guest::aggregate!(
    evolve_shipment = |state: Option<Value>, ev: &Event| {
        match ev.family() {
            "Shipping.ShipmentPrepared" => {
                Ok(json!({ "order_id": ev.payload["order_id"], "stage": "Prepared" }))
            }
            "Shipping.ShipmentShipped" => {
                let mut s = state.ok_or("shipped before prepared")?;
                s["stage"] = json!("Shipped");
                Ok(s)
            }
            "Shipping.ShipmentCancelled" => {
                let mut s = state.ok_or("cancelled before prepared")?;
                s["stage"] = json!("Cancelled");
                Ok(s)
            }
            other => Err(format!("Shipment cannot evolve from {other}")),
        }
    }
);

// ---------------------------------------------------------------------------
// Projections

fold_guest::projection!(
    project_order_totals = |_cx: &Ctx, ev: &Event| {
        let row = Row::new(
            "order_totals",
            json!({ "order_id": ev.payload["order_id"] }),
        );
        Ok(match ev.family() {
            "Orders.OrderPlaced" => {
                vec![row.upsert(json!({ "total": ev.payload["total"], "status": "Pending" }))]
            }
            "Orders.OrderCancelled" => {
                let mut muts = vec![row.clone().set("status", "Cancelled")];
                if let Some(note) = ev.payload.get("note") {
                    muts.push(row.set("note", note.clone()));
                }
                muts
            }
            _ => vec![],
        })
    }
);

// An upcaster the end-to-end tests name from a rewritten schema: v2 of
// OrderCancelled gains a `note` derived from the v1 reason.
fold_guest::upcast!(
    upcast_order_cancelled_v2 = |ev: &UpcastEvent| {
        let mut payload = ev.payload.clone();
        let reason = payload["reason"].as_str().unwrap_or("-").to_string();
        payload["note"] = json!(format!("wasm:{reason}"));
        Ok(payload)
    }
);

// One row per customer, maintained with column ops only: no row is ever
// read back except to find a cancelled order's owner.
fold_guest::projection!(
    project_customer_orders = |cx: &Ctx, ev: &Event| {
        let mut out: Vec<Mutation> = Vec::new();
        match ev.family() {
            "Customers.CustomerRegistered" => {
                let row = Row::new(
                    "customer_orders",
                    json!({ "customer_id": ev.payload["customer_id"] }),
                );
                out.push(row.set("name", ev.payload["name"].clone()));
            }
            "Orders.OrderPlaced" => {
                let order_id = ev.payload["order_id"].clone();
                let customer_id = ev.payload["customer_id"].clone();
                let row = Row::new("customer_orders", json!({ "customer_id": customer_id }));
                out.push(row.set_add("open_orders", order_id.clone()));
                out.push(row.push("recent_orders", order_id.clone()));
                out.push(row.truncate_back("recent_orders", 5));
                out.push(row.add_in(
                    "spent_by_currency",
                    ev.payload["total"]["currency"].clone(),
                    ev.payload["total"]["amount"].clone(),
                ));
                out.push(row.add("order_count", 1));
                // Remember who owns the order, for the cancellation below.
                let owner = Row::new("order_owner", json!({ "order_id": order_id }));
                out.push(owner.upsert(json!({ "customer_id": customer_id })));
            }
            "Orders.OrderCancelled" => {
                let order_id = ev.payload["order_id"].clone();
                let owner = cx
                    .get("order_owner", &json!({ "order_id": order_id }))?
                    .ok_or("cancelled an order this projection never saw placed")?;
                let row = Row::new(
                    "customer_orders",
                    json!({ "customer_id": owner["customer_id"] }),
                );
                out.push(row.set_remove("open_orders", order_id));
            }
            _ => {}
        }
        Ok(out)
    }
);

// ---------------------------------------------------------------------------
// Helpers

fn text<'a>(v: &'a Value, name: &str) -> Result<&'a str, String> {
    v.get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing string field {name}"))
}
