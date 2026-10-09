//! Replication: a replica started empty tails the primary, serves the same
//! read models, refuses writes, and once promoted carries on without
//! re-issuing what the primary already did.

use std::time::{Duration, Instant};

use fold_proto::v1::{HealthRequest, HealthResponse, ListProcessesRequest};
use serde_json::json;
use tonic::Code;

use crate::common::{Daemon, line, settle, state_of, uuid};

async fn health(d: &Daemon) -> HealthResponse {
    d.admin()
        .await
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner()
}

/// Waits until the replica's head reaches `head` and its runners have
/// applied up to it.
async fn caught_up(replica: &Daemon, head: u64) -> HealthResponse {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let h = health(replica).await;
        let runners_done = replica
            .projections()
            .await
            .iter()
            .all(|p| p.checkpoint.is_some_and(|cp| cp + 1 >= head))
            && replica
                .admin()
                .await
                .list_processes(ListProcessesRequest {})
                .await
                .unwrap()
                .into_inner()
                .processes
                .iter()
                .all(|p| p.checkpoint.is_some_and(|cp| cp + 1 >= head));
        if h.head >= head && runners_done {
            return h;
        }
        assert!(
            Instant::now() < deadline,
            "replica did not reach {head}: head {}, connected {}, error {:?}",
            h.head,
            h.replica_connected,
            h.replication_error
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn a_replica_tails_the_primary_and_can_be_promoted() {
    let mut primary = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 7);
    let a = uuid('a', 7);
    primary
        .exec(
            "Customers.Customer.Register",
            &format!("customer-{c}"),
            json!({ "name": "Ada" }),
        )
        .await
        .unwrap();
    let placed_a = primary
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 2, "7.50")] }),
        )
        .await
        .unwrap();
    let head1 = settle(&primary, &[format!("shipment-{a}")]).await;
    let row = primary
        .row(
            "Orders.CustomerOrders",
            "customer_orders",
            json!({ "customer_id": c }),
            placed_a.last_position,
        )
        .await;
    let order_state = state_of(&primary.aggregate(&format!("order-{a}")).await.unwrap());
    let primary_events = primary.all_events().await;
    assert_eq!(primary_events.len() as u64, head1);

    // A replica, from an empty directory, catches up with history...
    let primary_addr = primary.addr.clone();
    let mut replica = Daemon::start_with(
        |s| s.to_string(),
        move |o| {
            o.replicate_from = Some(primary_addr.clone());
        },
    )
    .await;
    let h = caught_up(&replica, head1).await;
    assert_eq!(
        (
            h.role.as_str(),
            h.replicating_from.as_str(),
            h.replica_connected
        ),
        ("replica", primary.addr.as_str(), true)
    );
    assert_eq!(h.log_id, health(&primary).await.log_id, "same log identity");
    assert_eq!(h.primary_head, Some(head1));
    assert_eq!(
        replica.all_events().await,
        primary_events,
        "the same records, byte for byte"
    );
    assert_eq!(
        state_of(&replica.aggregate(&format!("order-{a}")).await.unwrap()),
        order_state
    );
    assert_eq!(
        replica
            .row(
                "Orders.CustomerOrders",
                "customer_orders",
                json!({ "customer_id": c }),
                placed_a.last_position,
            )
            .await,
        row,
        "projections run on the replica over the replicated events"
    );

    // ...refuses writes...
    let err = replica
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{}", uuid('x', 7)),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 9), 1, "1.00")] }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("replica"), "{err}");
    let err = replica
        .append(
            &format!("customer-{}", uuid('y', 7)),
            "Customers.CustomerRegistered",
            json!({ "customer_id": uuid('y', 7), "name": "Bob" }),
            fold_proto::v1::expected_version::Kind::NoStream(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");

    // ...and its process managers hold their commands instead of issuing them.
    let procs = replica
        .admin()
        .await
        .list_processes(ListProcessesRequest {})
        .await
        .unwrap()
        .into_inner()
        .processes;
    let fulfilment = procs
        .iter()
        .find(|p| p.name == "Orders.Fulfilment")
        .expect("the process is listed");
    assert!(
        fulfilment.pending_commands > 0,
        "held for a promotion: {fulfilment:?}"
    );
    assert_eq!(
        health(&replica).await.head,
        head1,
        "nothing was appended locally"
    );

    // Live tail: new history on the primary arrives.
    let b = uuid('b', 7);
    let placed_b = primary
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{b}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 2), 1, "1.00")] }),
        )
        .await
        .unwrap();
    let head2 = settle(
        &primary,
        &[format!("shipment-{a}"), format!("shipment-{b}")],
    )
    .await;
    assert!(head2 > head1);
    caught_up(&replica, head2).await;
    assert!(
        replica
            .aggregate(&format!("order-{b}"))
            .await
            .unwrap()
            .found
    );
    assert_eq!(
        replica
            .row(
                "Orders.CustomerOrders",
                "customer_orders",
                json!({ "customer_id": c }),
                placed_b.last_position,
            )
            .await["order_count"],
        2
    );
    assert_eq!(replica.all_events().await, primary.all_events().await);

    // Promotion: the primary goes away; the replica restarts without a
    // primary and is one. Its held commands were executed by the old
    // primary, which the replicated keys prove: nothing is re-issued.
    primary.shutdown().await;
    replica.configure = std::sync::Arc::new(|_| {});
    replica.restart().await;
    let h = health(&replica).await;
    assert_eq!((h.role.as_str(), h.head), ("primary", head2));
    let settled = settle(
        &replica,
        &[format!("shipment-{a}"), format!("shipment-{b}")],
    )
    .await;
    assert_eq!(settled, head2, "the held commands were already executed");
    let procs = replica
        .admin()
        .await
        .list_processes(ListProcessesRequest {})
        .await
        .unwrap()
        .into_inner()
        .processes;
    assert!(procs.iter().all(|p| p.pending_commands == 0), "{procs:?}");
    assert_eq!(
        replica.all_events().await,
        primary_events_until(&replica, head2).await
    );

    // And it takes commands now.
    let placed_e = replica
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{}", uuid('e', 7)),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 3), 1, "3.00")] }),
        )
        .await
        .unwrap();
    assert_eq!(placed_e.first_position, head2);
    replica.shutdown().await;
}

/// The promoted daemon's own first `head` events (its history is the only
/// copy left; this just pins the length).
async fn primary_events_until(d: &Daemon, head: u64) -> Vec<fold_proto::v1::RecordedEvent> {
    d.all_events()
        .await
        .into_iter()
        .filter(|e| e.position < head)
        .collect()
}

#[tokio::test]
async fn a_replica_refuses_a_log_that_is_not_its_primarys_and_an_unreachable_primary() {
    let primary = Daemon::start(|s| s.to_string()).await;
    // A daemon with its own history cannot become this primary's replica.
    let mut other = Daemon::start(|s| s.to_string()).await;
    other
        .exec(
            "Customers.Customer.Register",
            &format!("customer-{}", uuid('c', 8)),
            json!({ "name": "Zed" }),
        )
        .await
        .unwrap();
    other.shutdown().await;
    let mut opts = foldd::Options::new(
        other.data_dir().join("data"),
        other.schema_path(),
        "127.0.0.1:0".parse().unwrap(),
    );
    opts.fsync = false;
    opts.replicate_from = Some(primary.addr.clone());
    let err = foldd::start(opts.clone()).await.err().expect("refused");
    assert!(format!("{err:#}").contains("must start empty"), "{err:#}");

    // Nobody listening there.
    opts.data_dir = other.data_dir().join("data-fresh");
    opts.replicate_from = Some("http://127.0.0.1:9".into());
    let err = foldd::start(opts).await.err().expect("refused");
    assert!(
        format!("{err:#}").contains("cannot reach the primary"),
        "{err:#}"
    );
}

/// Failover in place: the primary goes away, `Promote` turns the replica
/// into a primary without a restart, its held commands are found already
/// executed, and it takes commands. A promoted log refuses to start as a
/// replica again.
#[tokio::test]
async fn a_replica_is_promoted_in_place() {
    let mut primary = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 9);
    let a = uuid('a', 9);
    primary
        .exec(
            "Customers.Customer.Register",
            &format!("customer-{c}"),
            json!({ "name": "Ada" }),
        )
        .await
        .unwrap();
    primary
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 2, "7.50")] }),
        )
        .await
        .unwrap();
    let head1 = settle(&primary, &[format!("shipment-{a}")]).await;
    let events = primary.all_events().await;

    // A primary cannot be promoted.
    let err = primary
        .admin()
        .await
        .promote(fold_proto::v1::PromoteRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("already a primary"), "{err}");

    let primary_addr = primary.addr.clone();
    let mut replica = Daemon::start_with(
        |s| s.to_string(),
        move |o| {
            o.replicate_from = Some(primary_addr.clone());
        },
    )
    .await;
    caught_up(&replica, head1).await;
    let held = replica
        .admin()
        .await
        .list_processes(ListProcessesRequest {})
        .await
        .unwrap()
        .into_inner()
        .processes
        .iter()
        .map(|p| p.pending_commands)
        .sum::<u64>();
    assert!(held > 0, "commands are held while a replica");

    // The primary is gone; fail over.
    primary.shutdown().await;
    let r = replica
        .admin()
        .await
        .promote(fold_proto::v1::PromoteRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (r.head, r.promoted_from.as_str()),
        (head1, primary.addr.as_str())
    );
    let h = health(&replica).await;
    assert_eq!(
        (
            h.role.as_str(),
            h.replicating_from.as_str(),
            h.promoted_from.as_str(),
            h.replica_connected
        ),
        ("primary", "", primary.addr.as_str(), false)
    );
    assert!(
        replica
            .data_dir()
            .join("data")
            .join(foldd::LOG_NAME)
            .join("promoted")
            .is_file()
    );

    // The held commands were dispatched on promotion and found already
    // executed: nothing new in the log, nothing left pending.
    let settled = settle(&replica, &[format!("shipment-{a}")]).await;
    assert_eq!(settled, head1);
    assert_eq!(replica.all_events().await, events);

    // It takes commands now, and a second promotion is refused.
    let b = uuid('b', 9);
    let placed = replica
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{b}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 2), 1, "1.00")] }),
        )
        .await
        .unwrap();
    assert_eq!(placed.first_position, head1);
    // Its process managers work as a primary's: the new order is fulfilled.
    let head_b = settle(
        &replica,
        &[format!("shipment-{a}"), format!("shipment-{b}")],
    )
    .await;
    assert!(head_b > head1 + 1, "the fulfilment chain ran: {head_b}");
    let err = replica
        .admin()
        .await
        .promote(fold_proto::v1::PromoteRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");

    // Restarting with the old --replicate-from is refused: the log was
    // promoted and may be ahead of the old primary.
    replica.shutdown().await;
    let mut opts = foldd::Options::new(
        replica.data_dir().join("data"),
        replica.schema_path(),
        "127.0.0.1:0".parse().unwrap(),
    );
    opts.fsync = false;
    opts.replicate_from = Some(primary.addr.clone());
    let err = foldd::start(opts).await.err().expect("refused");
    assert!(format!("{err:#}").contains("was promoted from"), "{err:#}");

    // Without it, it is the primary it became.
    replica.configure = std::sync::Arc::new(|_| {});
    replica.restart().await;
    let h = health(&replica).await;
    assert_eq!((h.role.as_str(), h.head), ("primary", head_b));
    replica.shutdown().await;
}

/// Automatic failover: with a grace period configured, a replica whose
/// primary has been out of reach that long promotes itself; while the
/// primary answers, it never does.
#[tokio::test]
async fn a_replica_fails_over_by_itself_once_the_primary_is_gone_for_the_grace_period() {
    let mut primary = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 11);
    let a = uuid('a', 11);
    primary
        .exec(
            "Customers.Customer.Register",
            &format!("customer-{c}"),
            json!({ "name": "Ada" }),
        )
        .await
        .unwrap();
    primary
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 2, "7.50")] }),
        )
        .await
        .unwrap();
    let head1 = settle(&primary, &[format!("shipment-{a}")]).await;
    let events = primary.all_events().await;

    let grace = Duration::from_millis(600);
    let primary_addr = primary.addr.clone();
    let mut replica = Daemon::start_with(
        |s| s.to_string(),
        move |o| {
            o.replicate_from = Some(primary_addr.clone());
            o.auto_failover = Some(grace);
        },
    )
    .await;
    let h = caught_up(&replica, head1).await;
    assert_eq!(
        h.auto_failover_secs, 0,
        "600 ms rounds down to 0 s; armed all the same"
    );

    // Control: a primary that answers (its Health is probed every grace/3)
    // keeps the replica a replica well past the grace period.
    tokio::time::sleep(grace * 3).await;
    let h = health(&replica).await;
    assert_eq!(
        (
            h.role.as_str(),
            h.replica_connected,
            h.primary_unreachable_secs
        ),
        ("replica", true, None)
    );

    // The primary goes away; nobody calls Promote. It stops listening as
    // its shutdown begins, so the clock starts before the call.
    let gone_at = Instant::now();
    primary.shutdown().await;
    let deadline = Instant::now() + Duration::from_secs(15);
    let h = loop {
        let h = health(&replica).await;
        if h.role == "primary" {
            break h;
        }
        assert!(
            Instant::now() < deadline,
            "no automatic failover: connected {}, unreachable {:?}, error {:?}",
            h.replica_connected,
            h.primary_unreachable_secs,
            h.replication_error
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert!(
        gone_at.elapsed() >= grace,
        "promoted after {:?}, before the grace period",
        gone_at.elapsed()
    );
    assert_eq!(h.promoted_from, primary.addr);
    assert!(h.promotion.contains("automatically"), "{}", h.promotion);
    assert_eq!(
        (h.replicating_from.as_str(), h.replica_connected),
        ("", false)
    );
    let marker = std::fs::read_to_string(
        replica
            .data_dir()
            .join("data")
            .join(foldd::LOG_NAME)
            .join("promoted"),
    )
    .unwrap();
    assert!(marker.contains("automatically"), "{marker}");

    // Same guarantees as a requested promotion.
    let settled = settle(&replica, &[format!("shipment-{a}")]).await;
    assert_eq!(settled, head1, "held commands were already executed");
    assert_eq!(replica.all_events().await, events);
    let placed = replica
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{}", uuid('b', 11)),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 2), 1, "1.00")] }),
        )
        .await
        .unwrap();
    assert_eq!(placed.first_position, head1);
    let err = replica
        .admin()
        .await
        .promote(fold_proto::v1::PromoteRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    replica.shutdown().await;

    // The option without a primary to fail over from is a configuration error.
    let mut opts = foldd::Options::new(
        replica.data_dir().join("data-none"),
        replica.schema_path(),
        "127.0.0.1:0".parse().unwrap(),
    );
    opts.fsync = false;
    opts.auto_failover = Some(grace);
    let err = foldd::start(opts).await.err().expect("refused");
    assert!(
        format!("{err:#}").contains("needs replicate_from"),
        "{err:#}"
    );
}

#[tokio::test]
async fn a_replica_refuses_a_schema_that_breaks_against_the_primarys() {
    let mut primary = Daemon::start(|s| s.to_string()).await;
    // Breaking against the primary's: a required field on a stored event.
    let other = Daemon::start(|s| {
        s.replace(
            "event OrderPlaced v1   { order_id: uuid, customer_id: uuid,",
            "event OrderPlaced v1   { order_id: uuid, channel: string, customer_id: uuid,",
        )
    })
    .await;
    let mut opts = foldd::Options::new(
        other.data_dir().join("data-replica"),
        other.schema_path(),
        "127.0.0.1:0".parse().unwrap(),
    );
    opts.fsync = false;
    opts.replicate_from = Some(primary.addr.clone());
    let err = foldd::start(opts).await.err().expect("refused");
    let text = format!("{err:#}");
    assert!(text.contains("breaks against the primary's"), "{text}");
    assert!(text.contains("Orders.OrderPlaced@v1.channel"), "{text}");

    // Control: a compatible difference (a comment, an optional field) starts.
    let compatible = Daemon::start(|s| {
        s.replace(
            "event OrderPlaced v1   { order_id: uuid, customer_id: uuid,",
            "event OrderPlaced v1   { order_id: uuid, channel: string?, customer_id: uuid,",
        )
        .replacen(
            "// ---- file: app.fold\n",
            "// ---- file: app.fold\n// replica copy\n",
            1,
        )
    })
    .await;
    let mut opts = foldd::Options::new(
        compatible.data_dir().join("data-replica"),
        compatible.schema_path(),
        "127.0.0.1:0".parse().unwrap(),
    );
    opts.fsync = false;
    opts.replicate_from = Some(primary.addr.clone());
    let running = foldd::start(opts)
        .await
        .expect("a compatible schema replicates");
    running.shutdown().await.unwrap();
    primary.shutdown().await;
}

#[tokio::test]
async fn a_replicas_aggregate_read_is_fresh_after_new_events_arrive() {
    let mut primary = Daemon::start(|s| s.to_string()).await;
    let primary_addr = primary.addr.clone();
    let mut replica = Daemon::start_with(
        |s| s.to_string(),
        move |o| o.replicate_from = Some(primary_addr.clone()),
    )
    .await;
    let a = uuid('a', 11);
    let stream = format!("order-{a}");
    primary
        .exec(
            "Orders.Order.PlaceOrder",
            &stream,
            json!({ "customer_id": uuid('c', 11), "lines": [line(&uuid('1', 1), 1, "1.00")] }),
        )
        .await
        .unwrap();
    // Read on the replica as soon as the event is there: the state is now
    // in its cache at version 0.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let got = replica.aggregate(&stream).await.unwrap();
        if got.found {
            assert_eq!(got.version, 0);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the event never replicated"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // More events arrive by replication, not through the replica's command
    // path: the cached state must not be served as is.
    let last = primary
        .exec(
            "Orders.Order.AddLine",
            &stream,
            json!({ "line": line(&uuid('1', 2), 1, "1.00") }),
        )
        .await
        .unwrap()
        .last_position;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while replica.health().await.head <= last {
        assert!(std::time::Instant::now() < deadline, "replication stalled");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let got = replica.aggregate(&stream).await.unwrap();
    assert_eq!(got.version, 1, "the read caught up from the cache");
    assert_eq!(got.replayed, 1, "only the new event was evolved");
    assert_eq!(state_of(&got)["lines"].as_object().unwrap().len(), 2);
    // And it is cached at the new version now.
    let again = replica.aggregate(&stream).await.unwrap();
    assert_eq!(again.version, 1);
    assert_eq!(again.replayed, 0);
    replica.shutdown().await;
    primary.shutdown().await;
}
