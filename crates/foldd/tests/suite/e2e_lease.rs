//! Leader leases: a primary answers reads only while a majority has
//! confirmed it within the lease; alone, or fenced, it refuses them, and a
//! peer that granted a lease elects nobody until it ends.

use std::time::{Duration, Instant};

use fold_proto::v1::{FenceRequest, HealthRequest, HealthResponse, VoteRequest};
use serde_json::json;
use tonic::Code;

use crate::common::{Daemon, line, settle, uuid};

async fn health(d: &Daemon) -> HealthResponse {
    d.admin()
        .await
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner()
}

async fn wait_for(d: &Daemon, what: &str, ok: impl Fn(&HealthResponse) -> bool) -> HealthResponse {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let h = health(d).await;
        if ok(&h) {
            return h;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}: {h:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// A port nobody listens on right now, for a daemon started later.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn read(d: &Daemon, c: &str) -> Result<fold_proto::v1::GetResponse, tonic::Status> {
    d.get(
        "Orders.CustomerOrders",
        "customer_orders",
        json!({ "customer_id": c }),
        None,
        None,
    )
    .await
}

#[tokio::test]
async fn reads_are_served_only_under_a_lease_from_a_majority() {
    let (pa, pb) = (free_port(), free_port());
    let peers = vec![
        format!("http://127.0.0.1:{pa}"),
        format!("http://127.0.0.1:{pb}"),
    ];
    let lease = Duration::from_millis(1500);
    let peers_for_p = peers.clone();
    let mut p = Daemon::start_with(
        |s| s.to_string(),
        move |o| {
            o.quorum_peers = peers_for_p.clone();
            o.lease = Some(lease);
        },
    )
    .await;
    let c = uuid('c', 41);
    let a_id = uuid('a', 41);
    // Writes do not need the lease (fencing guards them); reads do.
    p.exec(
        "Customers.Customer.Register",
        &format!("customer-{c}"),
        json!({ "name": "Ada" }),
    )
    .await
    .unwrap();
    p.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a_id}"),
        json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "1.00")] }),
    )
    .await
    .unwrap();
    // (Settling reads the aggregate, which the lease gates: later.)
    let h = health(&p).await;
    assert_eq!((h.lease_secs, h.lease_held), (1, false));
    let err = read(&p, &c).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err}");
    assert!(err.message().contains("holds no lease"), "{err}");
    let err = p.aggregate(&format!("order-{a_id}")).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err}");

    // The peers arrive: a majority confirms the primary, reads flow.
    let p_addr = p.addr.clone();
    let mut a = Daemon::start_with(
        |s| s.to_string(),
        move |o| {
            o.listen = format!("127.0.0.1:{pa}").parse().unwrap();
            o.replicate_from = Some(p_addr.clone());
        },
    )
    .await;
    let p_addr = p.addr.clone();
    let mut b = Daemon::start_with(
        |s| s.to_string(),
        move |o| {
            o.listen = format!("127.0.0.1:{pb}").parse().unwrap();
            o.replicate_from = Some(p_addr.clone());
        },
    )
    .await;
    let h = wait_for(&p, "a lease", |h| h.lease_held).await;
    assert!(
        h.lease_remaining_ms > 0 && h.lease_remaining_ms <= 1500,
        "{h:?}"
    );
    let head1 = settle(&p, &[format!("shipment-{a_id}")]).await;
    assert!(read(&p, &c).await.unwrap().found);
    assert!(p.aggregate(&format!("order-{a_id}")).await.unwrap().found);
    // A replica answers reads without any lease: eventual consistency is
    // its contract.
    wait_for(&a, "A catch-up", |h| h.head >= head1).await;
    assert!(read(&a, &c).await.unwrap().found);

    // One peer down: still a majority. Both down: the lease lapses and
    // reads stop, within the lease.
    b.shutdown().await;
    tokio::time::sleep(lease * 2).await;
    assert!(health(&p).await.lease_held, "P and A are 2 of 3");
    assert!(read(&p, &c).await.unwrap().found);
    a.shutdown().await;
    let lost_at = Instant::now();
    let h = wait_for(&p, "the lease to lapse", |h| !h.lease_held).await;
    assert!(
        lost_at.elapsed() <= lease + Duration::from_millis(500),
        "{:?}",
        lost_at.elapsed()
    );
    assert!(
        h.lease_error.contains("1 of 3 confirmed"),
        "{}",
        h.lease_error
    );
    let err = read(&p, &c).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err}");
    // Writes still land (fencing is what stops a stale primary writing).
    p.exec(
        "Customers.Customer.Register",
        &format!("customer-{}", uuid('d', 41)),
        json!({ "name": "Dee" }),
    )
    .await
    .unwrap();
    // A peer returns: reads resume.
    a.restart().await;
    wait_for(&p, "the lease back", |h| h.lease_held).await;
    assert!(read(&p, &c).await.unwrap().found);

    // A peer that granted a lease elects nobody until it ends. The peer
    // first catches up with the primary's last write, so its head cannot
    // move between reading the candidate's head and the vote.
    let p_head = health(&p).await.head;
    wait_for(&a, "the peer to catch up", |h| h.head >= p_head).await;
    let log_id = h.log_id.clone();
    let p_addr = p.addr.clone();
    p.shutdown().await;
    let r = a
        .admin()
        .await
        .request_vote(VoteRequest {
            epoch: 1,
            log_id: log_id.clone(),
            primary: p_addr.clone(),
            candidate_head: health(&a).await.head,
            candidate: "test".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!r.granted && r.reason.contains("holds a lease"), "{r:?}");
    tokio::time::sleep(lease).await;
    let r = a
        .admin()
        .await
        .request_vote(VoteRequest {
            epoch: 1,
            log_id,
            primary: p_addr,
            candidate_head: health(&a).await.head,
            candidate: "test".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.granted, "{r:?}");
    a.shutdown().await;
}

#[tokio::test]
async fn a_fenced_primary_refuses_reads_too() {
    let mut p = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 43);
    p.exec(
        "Customers.Customer.Register",
        &format!("customer-{c}"),
        json!({ "name": "Ada" }),
    )
    .await
    .unwrap();
    settle(&p, &[]).await;
    assert!(read(&p, &c).await.unwrap().found);
    p.admin()
        .await
        .fence(FenceRequest { epoch: 1 })
        .await
        .unwrap();
    for (what, err) in [
        ("query", read(&p, &c).await.err()),
        (
            "aggregate",
            p.aggregate(&format!("customer-{c}")).await.err(),
        ),
    ] {
        let err = err.unwrap_or_else(|| panic!("{what} answered on a fenced daemon"));
        assert_eq!(err.code(), Code::FailedPrecondition, "{what}: {err}");
        assert!(err.message().contains("fenced"), "{what}: {err}");
    }
    p.shutdown().await;

    // A lease needs peers to grant it.
    let mut opts = foldd::Options::new(
        p.data_dir().join("data-none"),
        p.data_dir().join("schema.fold"),
        "127.0.0.1:0".parse().unwrap(),
    );
    opts.fsync = false;
    opts.lease = Some(Duration::from_secs(5));
    let err = foldd::start(opts).await.err().expect("refused");
    assert!(
        format!("{err:#}").contains("lease needs quorum_peers"),
        "{err:#}"
    );
}
