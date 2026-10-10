//! The orders example application: the commands of the three aggregates,
//! the rules they must respect, and the process that ties the contexts
//! together, in Rust against the domain in `examples/orders/domain.fold`
//! and the derivation layer in `examples/orders/derive.fold`.
//!
//! [`app`] is the application as shipped; [`build`] takes [`Options`] for
//! the variants the test suites run (a ceiling on an order's lines, no
//! process manager, an undeclared timer).

use std::collections::BTreeMap;

use fold_app::{
    App, CmdCtx, ContextInvariant, Emit, Fail, InvCtx, IssuedCommand, Json, PendingEvent, ProcCtx,
    Process, Reaction, Rejected, Rows, SetTimer, Trigger, json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// What the suites vary; the defaults are the application as shipped.
#[derive(Debug, Clone)]
pub struct Options {
    /// The most open orders one customer may have at a time.
    pub max_open_orders: usize,
    /// A ceiling on an order's lines, as a second state invariant.
    pub max_lines: Option<usize>,
    /// Run the Fulfilment process manager.
    pub fulfilment: bool,
    /// Declare the `ShipmentOverdue` timer the process sets (an undeclared
    /// timer fails the process: the suite checks that).
    pub declare_timer: bool,
    /// Snapshot the process every this many positions; `0` = never.
    pub fulfilment_snapshot_every: u32,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            max_open_orders: MAX_OPEN_ORDERS,
            max_lines: None,
            fulfilment: true,
            declare_timer: true,
            fulfilment_snapshot_every: 0,
        }
    }
}

/// The most open orders one customer may have at a time.
pub const MAX_OPEN_ORDERS: usize = 5;

// ---------------------------------------------------------------------------
// The records, as the domain declares them

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Money {
    /// A decimal, as the domain writes it.
    pub amount: String,
    pub currency: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Discount {
    pub percent: u64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Line {
    pub line_id: Uuid,
    pub sku: String,
    pub qty: u64,
    pub price: Money,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discount: Option<Discount>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CustomerState {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderState {
    pub customer_id: Uuid,
    /// `Pending`, `Paid` or `Cancelled`.
    pub status: String,
    pub lines: BTreeMap<Uuid, Line>,
    pub total: Money,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShipmentState {
    pub order_id: Uuid,
    /// `Prepared`, `Shipped` or `Cancelled`.
    pub stage: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FulfilmentState {
    pub customer_id: Uuid,
    /// `requested`, `prepared`, `shipped` or `overdue`.
    pub shipment: String,
    pub cancel_refused: bool,
}

// ---------------------------------------------------------------------------
// The commands

#[derive(Debug, Deserialize)]
pub struct Register {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct PlaceOrder {
    pub customer_id: Uuid,
    pub lines: Vec<Line>,
}

#[derive(Debug, Deserialize)]
pub struct AddLine {
    pub line: Line,
}

#[derive(Debug, Deserialize)]
pub struct RemoveLine {
    pub line_id: Uuid,
}

#[derive(Debug, Default, Deserialize)]
pub struct CancelOrder {
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PrepareShipment {
    pub order_id: Uuid,
    pub customer_id: Uuid,
}

#[derive(Debug, Default, Deserialize)]
pub struct NoFields {}

/// The application as shipped.
pub fn app() -> App {
    build(Options::default())
}

/// The application with `o`'s variants.
pub fn build(o: Options) -> App {
    let max_open_orders = o.max_open_orders;
    let mut app = App::new()
        .aggregate::<CustomerState>("Customers.Customer", |a| {
            a.command("Register", |cx: &CmdCtx, state: Option<CustomerState>, cmd: Register| {
                if state.is_some() {
                    return Err(
                        Rejected::new("ALREADY_REGISTERED", "this customer already exists").into(),
                    );
                }
                Ok(vec![Emit::event(
                    "Customers.CustomerRegistered",
                    json!({ "customer_id": cx.key, "name": cmd.name }),
                )])
            })
        })
        .aggregate::<OrderState>("Orders.Order", |a| {
            let a = a
                .command("PlaceOrder", |cx: &CmdCtx, state: Option<OrderState>, cmd: PlaceOrder| {
                    if state.is_some() {
                        return Err(
                            Rejected::new("ALREADY_PLACED", "this order was already placed").into(),
                        );
                    }
                    if cmd.lines.is_empty() {
                        return Err(
                            Rejected::new("EMPTY_ORDER", "an order needs at least one line").into(),
                        );
                    }
                    let total = sum_lines(cmd.lines.iter(), None)?;
                    Ok(vec![Emit::event(
                        "Orders.OrderPlaced",
                        json!({
                            "order_id": cx.key,
                            "customer_id": cmd.customer_id,
                            "lines": cmd.lines,
                            "total": total,
                        }),
                    )])
                })
                .command("AddLine", |cx: &CmdCtx, state: Option<OrderState>, cmd: AddLine| {
                    let state = pending(state)?;
                    // Same id, same entity: the replaced line's contribution
                    // leaves the total before the new one is added.
                    let base = match state.lines.get(&cmd.line.line_id) {
                        Some(existing) => subtract_line(&state.total, existing)?,
                        None => state.total.clone(),
                    };
                    let total = sum_lines(std::iter::once(&cmd.line), Some(&base))?;
                    Ok(vec![Emit::event(
                        "Orders.LineAdded",
                        json!({ "order_id": cx.key, "line": cmd.line, "total": total }),
                    )])
                })
                .command("RemoveLine", |cx: &CmdCtx, state: Option<OrderState>, cmd: RemoveLine| {
                    let state = pending(state)?;
                    let line = state.lines.get(&cmd.line_id).ok_or_else(|| {
                        Rejected::new("NO_SUCH_LINE", format!("no line {} on this order", cmd.line_id))
                    })?;
                    // The handler does not check that a line remains: the
                    // LinesNotEmpty invariant does, so a forgotten rule here
                    // cannot corrupt the order.
                    let total = subtract_line(&state.total, line)?;
                    Ok(vec![Emit::event(
                        "Orders.LineRemoved",
                        json!({ "order_id": cx.key, "line_id": cmd.line_id, "total": total }),
                    )])
                })
                .command("CancelOrder", |cx: &CmdCtx, state: Option<OrderState>, cmd: CancelOrder| {
                    pending(state)?;
                    Ok(vec![Emit::event(
                        "Orders.OrderCancelled",
                        json!({ "order_id": cx.key, "reason": cmd.reason, "at": cx.now }),
                    )])
                })
                // Whatever command ran, a pending order keeps at least one line.
                .invariant("LinesNotEmpty", |_cx: &InvCtx, state: &OrderState, _ev: &[PendingEvent]| {
                    if state.status == "Pending" && state.lines.is_empty() {
                        return Err(Rejected::new(
                            "EMPTY_ORDER",
                            "a pending order must keep at least one line",
                        ));
                    }
                    Ok(())
                });
            match o.max_lines {
                Some(max) => a.invariant("MaxLines", move |_cx: &InvCtx, state: &OrderState, _ev: &[PendingEvent]| {
                    if state.lines.len() > max {
                        return Err(Rejected::new(
                            "MaxLines",
                            format!("an order keeps at most {max} line(s), this one would have {}", state.lines.len()),
                        ));
                    }
                    Ok(())
                }),
                None => a,
            }
        })
        .aggregate::<ShipmentState>("Shipping.Shipment", |a| {
            a.command("Prepare", |cx: &CmdCtx, state: Option<ShipmentState>, cmd: PrepareShipment| {
                if state.is_some() {
                    return Err(Rejected::new("ALREADY_PREPARED", "this shipment exists").into());
                }
                Ok(vec![Emit::event(
                    "Shipping.ShipmentPrepared",
                    json!({ "shipment_id": cx.key, "order_id": cmd.order_id, "customer_id": cmd.customer_id }),
                )])
            })
            .command("Ship", |cx: &CmdCtx, state: Option<ShipmentState>, _cmd: NoFields| {
                let state = state.ok_or_else(|| Rejected::new("NOT_PREPARED", "no such shipment"))?;
                if state.stage != "Prepared" {
                    return Err(
                        Rejected::new("NOT_PREPARED", format!("shipment is {}", state.stage)).into(),
                    );
                }
                Ok(vec![Emit::event(
                    "Shipping.ShipmentShipped",
                    json!({ "shipment_id": cx.key, "order_id": state.order_id }),
                )])
            })
            .command("Cancel", |cx: &CmdCtx, state: Option<ShipmentState>, _cmd: NoFields| {
                let state = state.ok_or_else(|| Rejected::new("NOT_PREPARED", "no such shipment"))?;
                if state.stage == "Shipped" {
                    return Err(Rejected::new("ALREADY_SHIPPED", "the shipment has left").into());
                }
                Ok(vec![Emit::event(
                    "Shipping.ShipmentCancelled",
                    json!({ "shipment_id": cx.key, "order_id": state.order_id }),
                )])
            })
        })
        // A rule across the context: at most five open orders per customer,
        // read from the CustomerOrders projection, which the node catches
        // up and locks by customer before the check runs.
        .invariant(
            ContextInvariant::new("Orders.MaxOpenOrders")
                .on("Orders.Order")
                .projection("Orders.CustomerOrders")
                .scope("customer_id")
                .check::<OrderState>(move |cx: &InvCtx, rows: &dyn Rows, _state: &OrderState, events: &[PendingEvent]| {
                    if !events.iter().any(|e| e.is("Orders.OrderPlaced")) {
                        return Ok(());
                    }
                    let customer = cx.scope.clone().ok_or("MaxOpenOrders needs a scope")?;
                    let open = rows
                        .get("customer_orders", &json!({ "customer_id": customer }))?
                        .and_then(|row| row["open_orders"].as_array().map(Vec::len))
                        .unwrap_or(0);
                    if open >= max_open_orders {
                        return Err(Rejected::new(
                            "MAX_OPEN_ORDERS",
                            format!("customer already has {open} open orders (limit {max_open_orders})"),
                        )
                        .into());
                    }
                    Ok(())
                }),
        );
    if o.fulfilment {
        app = app.process(fulfilment(o.declare_timer, o.fulfilment_snapshot_every));
    }
    app
}

/// The process manager: when an order is placed, have a shipment prepared;
/// when it is cancelled, cancel the shipment; remember what the shipment
/// did, and note when a cancellation came too late. An order placed with
/// `overdue_after_ms` in its metadata is cancelled by the ShipmentOverdue
/// timer unless it ships first.
fn fulfilment(declare_timer: bool, snapshot_every: u32) -> Process {
    let p = Process::new("Orders.Fulfilment")
        .key("order_id")
        .snapshot_every(snapshot_every)
        .from("Orders.OrderPlaced")
        .from("Orders.OrderCancelled")
        .from("Shipping.ShipmentPrepared")
        .from("Shipping.ShipmentShipped")
        .from("Shipping.ShipmentCancelled");
    let p = if declare_timer {
        p.timers(["ShipmentOverdue"])
    } else {
        p
    };
    p.react::<FulfilmentState>(
        |cx: &ProcCtx, state: Option<FulfilmentState>, trigger: &Trigger| {
            let order = cx.key.as_str().unwrap_or_default().to_string();
            let shipment_stream = format!("shipment-{order}");
            match trigger {
                Trigger::Event(ev) => match ev.family() {
                    "Orders.OrderPlaced" => {
                        let customer_id = serde_json::from_value(ev.payload["customer_id"].clone())
                            .map_err(|e| format!("customer_id: {e}"))?;
                        let mut reaction = Reaction::keep(FulfilmentState {
                            customer_id,
                            shipment: "requested".into(),
                            cancel_refused: false,
                        })
                        .issue(IssuedCommand::new(
                            "Shipping.Shipment.Prepare",
                            shipment_stream,
                            json!({ "order_id": cx.key, "customer_id": ev.payload["customer_id"] }),
                        ));
                        // An order placed with `overdue_after_ms` in its metadata
                        // is cancelled by the timer unless it ships first.
                        if let Some(ms) = ev.metadata.get("overdue_after_ms").and_then(Json::as_u64)
                        {
                            reaction = reaction.set_timer(SetTimer::after("ShipmentOverdue", ms));
                        }
                        Ok(reaction)
                    }
                    "Shipping.ShipmentPrepared" => {
                        let mut s = state.ok_or("prepared before placed")?;
                        s.shipment = "prepared".into();
                        Ok(Reaction::keep(s))
                    }
                    "Shipping.ShipmentShipped" => {
                        let mut s = state.ok_or("shipped before placed")?;
                        s.shipment = "shipped".into();
                        Ok(Reaction::keep(s).cancel_timer("ShipmentOverdue"))
                    }
                    "Orders.OrderCancelled" => match state {
                        // Cancelling an order whose shipment is under way.
                        Some(s) => Ok(Reaction::keep(s).issue(IssuedCommand::new(
                            "Shipping.Shipment.Cancel",
                            shipment_stream,
                            json!({}),
                        ))),
                        None => Ok(Reaction::end()),
                    },
                    // The shipment is cancelled: nothing left to track.
                    "Shipping.ShipmentCancelled" => Ok(Reaction::end()),
                    _ => Ok(Reaction::unchanged(state)),
                },
                Trigger::Rejected { command, rejected } => {
                    let mut s = state.ok_or("rejection for an ended instance")?;
                    if command.command == "Shipping.Shipment.Cancel"
                        && rejected.code == "ALREADY_SHIPPED"
                    {
                        s.cancel_refused = true;
                    }
                    Ok(Reaction::keep(s))
                }
                // The shipment did not go out in time: cancel the order (whose
                // OrderCancelled event then has the shipment cancelled too).
                Trigger::Timer { name, .. } if name == "ShipmentOverdue" => {
                    let mut s = state.ok_or("timer for an ended instance")?;
                    if s.shipment == "shipped" {
                        return Ok(Reaction::keep(s));
                    }
                    s.shipment = "overdue".into();
                    Ok(Reaction::keep(s).issue(IssuedCommand::new(
                        "Orders.Order.CancelOrder",
                        format!("order-{order}"),
                        json!({ "reason": "shipment overdue" }),
                    )))
                }
                Trigger::Timer { name, .. } => Err(format!("unknown timer {name}")),
            }
        },
    )
}

// ---------------------------------------------------------------------------
// Helpers

/// The state if the order is still pending, otherwise the matching rejection.
fn pending(state: Option<OrderState>) -> Result<OrderState, Fail> {
    let state = state.ok_or_else(|| Rejected::new("NOT_PLACED", "this order does not exist"))?;
    if state.status != "Pending" {
        return Err(Rejected::new("NOT_PENDING", format!("order is {}", state.status)).into());
    }
    Ok(state)
}

/// Sums `qty * price.amount` over `lines`, on top of `base`, in one currency.
fn sum_lines<'a>(
    lines: impl Iterator<Item = &'a Line>,
    base: Option<&Money>,
) -> Result<Money, Fail> {
    let mut currency: Option<String> = None;
    let mut total = Decimal::ZERO;
    if let Some(base) = base {
        total = parse_decimal(&base.amount)?;
        currency = Some(base.currency.clone());
    }
    for line in lines {
        let amount = parse_decimal(&line.price.amount)?;
        match &currency {
            None => currency = Some(line.price.currency.clone()),
            Some(c) if *c != line.price.currency => {
                return Err(Rejected::new(
                    "MIXED_CURRENCY",
                    format!("{c} and {} in one order", line.price.currency),
                )
                .into());
            }
            Some(_) => {}
        }
        total += Decimal::from(line.qty) * amount;
    }
    Ok(Money {
        amount: total.to_string(),
        currency: currency.unwrap_or_default(),
    })
}

/// `base` minus `qty * price.amount` of one line.
fn subtract_line(base: &Money, line: &Line) -> Result<Money, Fail> {
    let amount = parse_decimal(&line.price.amount)?;
    let total = parse_decimal(&base.amount)? - Decimal::from(line.qty) * amount;
    Ok(Money {
        amount: total.to_string(),
        currency: base.currency.clone(),
    })
}

fn parse_decimal(s: &str) -> Result<Decimal, Fail> {
    s.parse::<Decimal>()
        .map_err(|e| Fail::Error(format!("bad decimal {s:?}: {e}")))
}
