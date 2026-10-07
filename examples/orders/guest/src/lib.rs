//! The Orders example guest: every entry point `examples/orders/schema.fold`
//! names. Built to `wasm32-unknown-unknown` by the end-to-end tests.
//!
//! Three roles live side by side here so the example shows them all:
//! command handlers decide, evolve functions fold events into aggregate
//! state, and projection steps maintain read models.

use fold_guest::{
    CmdCtx, Command, Ctx, Emit, Event, Fail, InvCtx, Mutation, PendingEvent, Rejected, Row, Value,
    json,
};
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
        // Same id, same entity: the replaced line's contribution leaves the
        // total before the new one is added.
        let base = match state["lines"].get(str_field(line, "line_id")?) {
            Some(existing) => subtract_line(&state["total"], existing)?,
            None => state["total"].clone(),
        };
        let total = sum_lines(std::iter::once(line), Some(&base))?;
        Ok(vec![Emit::event(
            "Orders.LineAdded",
            json!({ "order_id": cx.key.clone(), "line": line, "total": total }),
        )])
    }
);

fold_guest::command!(
    handle_remove_line = |cx: &CmdCtx, state: Option<Value>, cmd: &Command| {
        let state = pending(state)?;
        let id = str_field(&cmd.payload, "line_id")?;
        let line = state["lines"]
            .get(id)
            .ok_or_else(|| Rejected::new("NO_SUCH_LINE", format!("no line {id} on this order")))?;
        // The handler does not check that a line remains: the LinesNotEmpty
        // invariant does, so a forgotten rule here cannot corrupt the order.
        let total = subtract_line(&state["total"], line)?;
        Ok(vec![Emit::event(
            "Orders.LineRemoved",
            json!({ "order_id": cx.key.clone(), "line_id": id, "total": total }),
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
                Ok(state)
            }
            other => Err(format!("Order cannot evolve from {other}")),
        }
    }
);

// ---------------------------------------------------------------------------
// Invariants

// State-driven: whatever command ran, a pending order keeps at least one line.
fold_guest::invariant!(
    check_lines_not_empty = |_cx: &InvCtx, _rows: &Ctx, state: &Value, _events: &[PendingEvent]| {
        let empty = state["lines"].as_object().is_none_or(|l| l.is_empty());
        if state["status"] == "Pending" && empty {
            return Err(Rejected::new(
                "EMPTY_ORDER",
                "a pending order must keep at least one line",
            )
            .into());
        }
        Ok(())
    }
);

/// The most open orders one customer may have at a time.
pub const MAX_OPEN_ORDERS: usize = 5;

// Projection-driven: placing an order reads the customer's row in the
// CustomerOrders projection, which the host has caught up and locked by
// customer before calling this.
fold_guest::invariant!(
    check_max_open_orders = |cx: &InvCtx, rows: &Ctx, _state: &Value, events: &[PendingEvent]| {
        if !events.iter().any(|e| e.is("Orders.OrderPlaced")) {
            return Ok(());
        }
        let customer = cx.scope.clone().ok_or("MaxOpenOrders needs a scope")?;
        let open = rows
            .get("customer_orders", &json!({ "customer_id": customer }))?
            .and_then(|row| row["open_orders"].as_array().map(Vec::len))
            .unwrap_or(0);
        if open >= MAX_OPEN_ORDERS {
            return Err(Rejected::new(
                "MAX_OPEN_ORDERS",
                format!("customer already has {open} open orders (limit {MAX_OPEN_ORDERS})"),
            )
            .into());
        }
        Ok(())
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

/// `base` minus `qty * price.amount` of one line.
fn subtract_line(base: &Value, line: &Value) -> Result<Value, Fail> {
    let qty = line["qty"]
        .as_u64()
        .ok_or("qty must be a non-negative integer")?;
    let amount = parse_decimal(&line["price"]["amount"])?;
    let total = parse_decimal(&base["amount"])? - Decimal::from(qty) * amount;
    Ok(json!({ "amount": total.to_string(), "currency": str_field(base, "currency")? }))
}

fn parse_decimal(v: &Value) -> Result<Decimal, Fail> {
    v.as_str()
        .ok_or("decimal must be a string")?
        .parse::<Decimal>()
        .map_err(|e| Fail::Error(format!("bad decimal {v}: {e}")))
}
