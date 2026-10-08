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
        other.data_dir().join("schema.fold"),
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
