//! Quorum: a replica promotes itself automatically only when a majority of
//! the cluster agrees the primary is gone; a vote is given once per epoch,
//! only to a candidate at least as far along, and never while the voter
//! still reaches the primary.

use std::time::{Duration, Instant};

use fold_proto::v1::{HealthRequest, HealthResponse, PromoteRequest, VoteRequest};
use serde_json::json;

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

async fn vote(
    voter: &Daemon,
    epoch: u64,
    log_id: &str,
    primary: &str,
    candidate_head: u64,
) -> fold_proto::v1::VoteResponse {
    voter
        .admin()
        .await
        .request_vote(VoteRequest {
            epoch,
            log_id: log_id.into(),
            primary: primary.into(),
            candidate_head,
            candidate: "test".into(),
        })
        .await
        .unwrap()
        .into_inner()
}

async fn seed(p: &Daemon, n: u32) -> u64 {
    let c = uuid('c', n);
    p.exec(
        "Customers.Customer.Register",
        &format!("customer-{c}"),
        json!({ "name": "Ada" }),
    )
    .await
    .unwrap();
    let a = uuid('a', n);
    p.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "1.00")] }),
    )
    .await
    .unwrap();
    settle(p, &[format!("shipment-{a}")]).await
}

#[tokio::test]
async fn a_replica_promotes_itself_only_with_a_majority() {
    let mut p = Daemon::start(|s| s.to_string()).await;
    let head1 = seed(&p, 31).await;
    let log_id = health(&p).await.log_id;

    // B: a plain replica that will vote. A: the candidate, needing a
    // majority of {P, B, itself}.
    let p_addr = p.addr.clone();
    let mut b = Daemon::start_with(
        |s| s.to_string(),
        move |o| {
            o.replicate_from = Some(p_addr.clone());
        },
    )
    .await;
    wait_for(&b, "B catch-up", |h| h.head >= head1).await;
    let (p_addr, b_addr) = (p.addr.clone(), b.addr.clone());
    let grace = Duration::from_millis(600);
    let mut a = Daemon::start_with(
        |s| s.to_string(),
        move |o| {
            o.replicate_from = Some(p_addr.clone());
            o.auto_failover = Some(grace);
            o.quorum_peers = vec![p_addr.clone(), b_addr.clone()];
        },
    )
    .await;
    let h = wait_for(&a, "A catch-up", |h| h.head >= head1).await;
    assert_eq!(h.quorum_size, 3);

    // The voting rules, asked directly while the primary is alive.
    let r = vote(&b, 1, &log_id, &p.addr, head1).await;
    assert!(!r.granted && r.primary_reachable, "{r:?}");
    assert!(r.reason.contains("answers from here"), "{r:?}");
    let r = vote(&p, 1, &log_id, &p.addr, head1).await;
    assert!(!r.granted && r.reason.contains("I am a primary"), "{r:?}");
    let r = vote(&b, 1, "not-this-log", &p.addr, head1).await;
    assert!(!r.granted && r.reason.contains("not the same log"), "{r:?}");
    let r = vote(&b, 0, &log_id, &p.addr, head1).await;
    assert!(!r.granted && r.reason.contains("not newer"), "{r:?}");

    // The primary goes; A holds an election, B agrees: 2 of 3.
    p.shutdown().await;
    let h = wait_for(&a, "A to win the election", |h| h.role == "primary").await;
    assert_eq!(h.epoch, 1);
    assert!(h.promotion.contains("2 of 3 votes"), "{}", h.promotion);
    assert!(
        h.last_election.starts_with("won epoch 1"),
        "{}",
        h.last_election
    );
    assert_eq!(
        health(&b).await.role,
        "replica",
        "B voted; it did not promote"
    );

    // B's vote is a promise: no second candidate gets epoch 1, a candidate
    // behind B gets nothing, and the promise survives a restart.
    let r = vote(&b, 1, &log_id, &p.addr, head1).await;
    assert!(
        !r.granted && r.reason.contains("already voted in epoch 1"),
        "{r:?}"
    );
    assert_eq!(r.voted_epoch, 1);
    let r = vote(&b, 2, &log_id, &p.addr, head1 - 1).await;
    assert!(!r.granted && r.reason.contains("behind"), "{r:?}");
    let r = vote(&b, 2, &log_id, &p.addr, head1).await;
    assert!(r.granted, "{r:?}");
    assert_eq!(r.voted_epoch, 2);
    // B follows the new primary from now on (and takes its epoch, 1); its
    // promise for epoch 2 is kept across the restart.
    let a_addr = a.addr.clone();
    b.configure = std::sync::Arc::new(move |o| {
        o.replicate_from = Some(a_addr.clone());
    });
    b.restart().await;
    wait_for(&b, "B to follow A", |h| h.epoch == 1 && h.replica_connected).await;
    let r = vote(&b, 2, &log_id, &p.addr, head1).await;
    assert!(
        !r.granted && r.reason.contains("already voted in epoch 2"),
        "{r:?}"
    );
    let r = vote(&b, 3, &log_id, &p.addr, head1).await;
    assert!(r.granted, "{r:?}");

    // The promoted candidate works as a primary.
    let c = uuid('c', 31);
    let placed = a
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{}", uuid('b', 31)),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 2), 1, "2.00")] }),
        )
        .await
        .unwrap();
    assert_eq!(placed.first_position, head1);
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn without_a_majority_a_replica_stays_a_replica_until_an_operator_decides() {
    let mut p = Daemon::start(|s| s.to_string()).await;
    let head1 = seed(&p, 33).await;
    // Peers: the primary and a member that is not there.
    let p_addr = p.addr.clone();
    let grace = Duration::from_millis(400);
    let mut a = Daemon::start_with(
        |s| s.to_string(),
        move |o| {
            o.replicate_from = Some(p_addr.clone());
            o.auto_failover = Some(grace);
            o.quorum_peers = vec![p_addr.clone(), "http://127.0.0.1:9".into()];
        },
    )
    .await;
    wait_for(&a, "A catch-up", |h| h.head >= head1).await;

    p.shutdown().await;
    let h = wait_for(&a, "an election to be held", |h| {
        !h.last_election.is_empty()
    })
    .await;
    assert!(
        h.last_election.contains("1 of 3 votes"),
        "{}",
        h.last_election
    );
    tokio::time::sleep(grace * 4).await;
    let h = health(&a).await;
    assert_eq!((h.role.as_str(), h.epoch), ("replica", 0), "{h:?}");
    assert!(h.last_election.starts_with("lost"), "{}", h.last_election);

    // The operator's call overrides the quorum.
    let r = a
        .admin()
        .await
        .promote(PromoteRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.head, head1);
    let h = health(&a).await;
    assert_eq!(h.role, "primary");
    // Past every epoch it proposed in the lost rounds, so its tokens are
    // newer than any vote it collected.
    let voted = vote(&a, 1, &h.log_id, &p.addr, head1).await.voted_epoch;
    assert!(voted >= 1, "it voted for itself at least once");
    assert_eq!(h.epoch, voted + 1, "{h:?}");
    a.shutdown().await;

    // The quorum option without automatic failover is a configuration error.
    let mut opts = foldd::Options::new(
        a.data_dir().join("data-none"),
        a.data_dir().join("schema.fold"),
        "127.0.0.1:0".parse().unwrap(),
    );
    opts.fsync = false;
    opts.replicate_from = Some("http://127.0.0.1:9".into());
    opts.quorum_peers = vec!["http://127.0.0.1:9".into()];
    let err = foldd::start(opts).await.err().expect("refused");
    assert!(
        format!("{err:#}").contains("needs auto_failover"),
        "{err:#}"
    );
}
