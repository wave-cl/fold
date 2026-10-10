//! The SDK's edges: a registration the schemas do not resolve refuses the
//! start; a handler's panic and a state that does not fit the
//! application's type are `INTERNAL` and the node keeps serving; a process
//! whose registration changed starts over.

use fold_app::{CmdCtx, Emit, Fail, Json, ProcCtx, Process, Reaction, Trigger};
use serde::Deserialize;
use serde_json::json;
use tonic::Code;

use crate::common::{Cluster, line, until, uuid};

#[tokio::test]
async fn a_registration_the_domain_does_not_declare_refuses_the_start() {
    let app = orders_app::app().aggregate::<Json>("Orders.Nope", |a| {
        a.command("Do", |_cx: &CmdCtx, _s: Option<Json>, _c: Json| Ok(vec![]))
    });
    let (mut c, started) = Cluster::start_nodes_with(app).await;
    let err = started.expect_err("refused");
    assert!(format!("{err:#}").contains("Orders.Nope"), "{err:#}");
    c.shutdown().await;

    // Likewise a process reacting to an event that does not carry its key.
    let app = orders_app::app().process(
        Process::new("Orders.Watcher")
            .key("order_id")
            .from("Customers.CustomerRegistered")
            .react::<Json>(|_cx: &ProcCtx, s: Option<Json>, _t: &Trigger| {
                Ok(Reaction::unchanged(s))
            }),
    );
    let (mut c, started) = Cluster::start_nodes_with(app).await;
    let err = started.expect_err("refused");
    let text = format!("{err:#}");
    assert!(
        text.contains("Orders.Watcher") && text.contains("order_id"),
        "{text}"
    );
    c.shutdown().await;
}

#[derive(Debug, Deserialize)]
struct NotAnOrder {
    #[allow(dead_code)]
    nope: u64,
}

#[tokio::test]
async fn application_defects_are_internal_and_the_node_keeps_serving() {
    let app = orders_app::app()
        .aggregate::<Json>("Orders.Order", |a| {
            a.command(
                "Boom",
                |_cx: &CmdCtx, _s: Option<Json>, _c: Json| -> Result<Vec<Emit>, Fail> {
                    panic!("the handler blew up")
                },
            )
        })
        .aggregate::<NotAnOrder>("Orders.Order", |a| {
            a.command("Peek", |_cx: &CmdCtx, _s: Option<NotAnOrder>, _c: Json| {
                Ok(vec![])
            })
        });
    let (mut c, started) = Cluster::start_nodes_with(app).await;
    started.expect("the registrations resolve");
    c.ready().await;
    let a = uuid('a', 1);
    let stream = format!("order-{a}");
    c.exec(
        "Orders.Order.PlaceOrder",
        &stream,
        json!({ "customer_id": uuid('c', 1), "lines": [line(&uuid('1', 1), 1, "1.00")] }),
    )
    .await
    .unwrap();

    // A panic is INTERNAL, naming the handler.
    let err = c
        .exec("Orders.Order.Boom", &stream, json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal, "{err}");
    assert!(
        err.message().contains("panicked") && err.message().contains("Orders.Order.Boom"),
        "{err}"
    );
    // A state the application's type does not fit is INTERNAL too.
    let err = c
        .exec("Orders.Order.Peek", &stream, json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal, "{err}");
    assert!(
        err.message().contains("does not fit") && err.message().contains("nope"),
        "{err}"
    );
    // On an empty stream there is no state to fit: the command runs.
    c.exec(
        "Orders.Order.Peek",
        &format!("order-{}", uuid('b', 1)),
        json!({}),
    )
    .await
    .expect("no state, nothing to fit");
    // The node is unharmed.
    c.exec(
        "Orders.Order.AddLine",
        &stream,
        json!({ "line": line(&uuid('1', 2), 1, "1.00") }),
    )
    .await
    .expect("still serving");
    assert_eq!(c.aggregate(&stream).await.unwrap().version, 1);
    c.shutdown().await;
}

#[tokio::test]
async fn a_changed_process_registration_starts_over() {
    let mut c = Cluster::start().await;
    c.ready().await;
    let a = uuid('a', 2);
    c.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": uuid('c', 2), "lines": [line(&uuid('1', 1), 1, "1.00")] }),
    )
    .await
    .unwrap();
    c.settle(&[format!("shipment-{a}")]).await;
    let before = c.process("Orders.Fulfilment").await;
    assert!(before.checkpoint.is_some());
    assert!(c.app_health().await.last_schema_change.is_empty());

    // The same process with one source fewer: its tables are reset and it
    // replays from the start of the log; the commands it re-issues are
    // found already executed.
    let mut app = orders_app::app();
    let p = app
        .processes
        .shift_remove("Orders.Fulfilment")
        .expect("the process");
    let mut narrower = Process::new("Orders.Fulfilment").key("order_id");
    for (family, by) in p
        .sources
        .iter()
        .filter(|(f, _)| f != "Shipping.ShipmentShipped")
    {
        narrower = narrower.from_by(family, by);
    }
    let narrower = narrower
        .timers(p.timers.clone())
        .react::<orders_app::FulfilmentState>(
            |_cx: &ProcCtx, s: Option<orders_app::FulfilmentState>, _t: &Trigger| {
                Ok(Reaction::unchanged(s))
            },
        );
    c.app_override = Some(app.process(narrower));
    c.restart_app().await;
    c.ready().await;
    let note = c.app_health().await.last_schema_change;
    assert!(note.contains("1 process(es) reset"), "{note}");
    until(10, "the process to replay to the head", async || {
        let p = c.process("Orders.Fulfilment").await;
        p.error.is_empty()
            && p.checkpoint.is_some_and(|cp| cp + 1 >= p.head)
            && p.pending_commands == 0
    })
    .await;
    assert_eq!(
        c.db_head().await,
        before.head,
        "the replay re-issued nothing new"
    );
    // The registration as it is now is what the store remembers: another
    // restart changes nothing.
    c.restart_app().await;
    c.ready().await;
    assert!(c.app_health().await.last_schema_change.is_empty());
    c.shutdown().await;
}
