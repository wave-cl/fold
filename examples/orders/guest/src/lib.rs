//! The Orders example guest: every entry point `examples/orders/schema.fold`
//! names. Built to `wasm32-unknown-unknown` by the end-to-end tests.
//!
//! Three roles live side by side here so the example shows them all:
//! command handlers decide, evolve functions fold events into aggregate
//! state, and projection steps maintain read models.

use fold_guest::{CmdCtx, Command, Ctx, Emit, Event, Fail, Mutation, Rejected, Row, Value, json};
use rust_decimal::Decimal;

fold_guest::module!();

// ---------------------------------------------------------------------------
// Customers.Customer

fold_guest::command!(
    handle_register = |cx: &CmdCtx, state: Option<Value>, cmd: &Command| {
        if state.is_some() {
            return Err(Rejected::new("ALREADY_REGISTERED", "this customer already exists").into());
        }
        let name = str_field(&cmd.payload, "name")?;
        Ok(vec![Emit::event(
            "Customers.CustomerRegistered",
            json!({ "customer_id": cx.key.clone(), "name": name }),
        )])
    }
);

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

fold_guest::command!(
    handle_place_order = |cx: &CmdCtx, state: Option<Value>, cmd: &Command| {
        if state.is_some() {
            return Err(Rejected::new("ALREADY_PLACED", "this order was already placed").into());
        }
        let lines = cmd.payload["lines"]
            .as_array()
            .ok_or("lines must be an array")?;
        if lines.is_empty() {
            return Err(Rejected::new("EMPTY_ORDER", "an order needs at least one line").into());
        }
        let total = sum_lines(lines.iter(), None)?;
        Ok(vec![Emit::event(
            "Orders.OrderPlaced",
            json!({
                "order_id": cx.key.clone(),
                "customer_id": cmd.payload["customer_id"],
                "lines": lines,
                "total": total,
            }),
        )])
    }
);

fold_guest::command!(
    handle_add_line = |cx: &CmdCtx, state: Option<Value>, cmd: &Command| {
        let state = pending(state)?;
        let line = &cmd.payload["line"];
        let total = sum_lines(std::iter::once(line), Some(&state["total"]))?;
        Ok(vec![Emit::event(
            "Orders.LineAdded",
            json!({ "order_id": cx.key.clone(), "line": line, "total": total }),
        )])
    }
);

fold_guest::command!(
    handle_cancel_order = |cx: &CmdCtx, state: Option<Value>, cmd: &Command| {
        pending(state)?;
        Ok(vec![Emit::event(
            "Orders.OrderCancelled",
            json!({
                "order_id": cx.key.clone(),
                "reason": cmd.payload.get("reason").cloned().unwrap_or(Value::Null),
                "at": cx.now.clone(),
            }),
        )])
    }
);

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
            "Orders.OrderCancelled" => {
                let mut state = state.ok_or("OrderCancelled before OrderPlaced")?;
                state["status"] = json!("Cancelled");
                Ok(state)
            }
            other => Err(format!("Order cannot evolve from {other}")),
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
            "Orders.OrderCancelled" => vec![row.set("status", "Cancelled")],
            _ => vec![],
        })
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

fn str_field<'a>(v: &'a Value, name: &str) -> Result<&'a str, Fail> {
    text(v, name).map_err(Fail::Error)
}

fn text<'a>(v: &'a Value, name: &str) -> Result<&'a str, String> {
    v.get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing string field {name}"))
}

/// The state if the order is still pending, otherwise the matching rejection.
fn pending(state: Option<Value>) -> Result<Value, Fail> {
    let state = state.ok_or_else(|| Rejected::new("NOT_PLACED", "this order does not exist"))?;
    if state["status"] != "Pending" {
        return Err(Rejected::new("NOT_PENDING", format!("order is {}", state["status"])).into());
    }
    Ok(state)
}

/// Sums `qty * price.amount` over `lines`, on top of `base`, in one currency.
fn sum_lines<'a>(
    lines: impl Iterator<Item = &'a Value>,
    base: Option<&Value>,
) -> Result<Value, Fail> {
    let mut currency: Option<String> = None;
    let mut total = Decimal::ZERO;
    if let Some(base) = base {
        total = parse_decimal(&base["amount"])?;
        currency = Some(str_field(base, "currency")?.to_string());
    }
    for line in lines {
        let qty = line["qty"]
            .as_u64()
            .ok_or("qty must be a non-negative integer")?;
        let amount = parse_decimal(&line["price"]["amount"])?;
        let cur = str_field(&line["price"], "currency")?;
        match &currency {
            None => currency = Some(cur.to_string()),
            Some(c) if c != cur => {
                return Err(
                    Rejected::new("MIXED_CURRENCY", format!("{c} and {cur} in one order")).into(),
                );
            }
            Some(_) => {}
        }
        total += Decimal::from(qty) * amount;
    }
    Ok(json!({ "amount": total.to_string(), "currency": currency.unwrap_or_default() }))
}

fn parse_decimal(v: &Value) -> Result<Decimal, Fail> {
    v.as_str()
        .ok_or("decimal must be a string")?
        .parse::<Decimal>()
        .map_err(|e| Fail::Error(format!("bad decimal {v}: {e}")))
}
