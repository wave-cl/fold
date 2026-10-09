//! The cluster: fencing, leases, quorum votes and automatic failover,
//! driven over `Cluster` with plain appends as the writes.

use std::time::{Duration, Instant};

use fold_proto::database::v1::{FenceRequest, PromoteRequest, VoteRequest};
use tonic::Code;

use crate::common::{DbNode, free_port, place, register, replica_of, uuid, wait_for};

#[tokio::test]
async fn a_promotion_fences_the_old_primary_which_can_rejoin_as_a_replica() {
    let mut p = DbNode::start().await;
    let c = uuid('c', 21);
    register(&p, &c).await;
    let a = uuid('a', 21);
    place(&p, &c, &a, None).await.unwrap();
    let head1 = p.head().await;
    assert_eq!(p.health().await.epoch, 0);

    let mut r = replica_of(&p).await;
    wait_for(&r, "catch-up", |h| h.head >= head1).await;
    assert_eq!(
        r.health().await.epoch,
        0,
        "a replica carries its primary's epoch"
    );

    // Promote while the old primary is alive and reachable: it is fenced.
    let promoted = r
        .cluster()
        .await
        .promote(PromoteRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(promoted.head, head1);
    let hr = wait_for(&r, "the fence to be acknowledged", |h| h.old_primary_fenced).await;
    assert_eq!((hr.role.as_str(), hr.epoch), ("primary", 1));
    let hp = p.health().await;
    assert_eq!(
        (hp.role.as_str(), hp.epoch, hp.fenced_by),
        ("fenced", 0, Some(1))
    );
    assert!(
        p.data_dir()
            .join("data")
            .join(fold_db::LOG_NAME)
            .join("fenced")
            .is_file()
    );

    // The old primary refuses writes, with or without a token.
    let err = place(&p, &c, &uuid('x', 21), None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("fenced"), "{err}");
    // Fence again: harmless. Fence the new primary with its own epoch: refused.
    let f = p
        .cluster()
        .await
        .fence(FenceRequest { epoch: 1 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!((f.role.as_str(), f.epoch), ("fenced", 0));
    let err = r
        .cluster()
        .await
        .fence(FenceRequest { epoch: 1 })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("not newer"), "{err}");

    // Fenced across a restart.
    p.restart().await;
    let hp = p.health().await;
    assert_eq!((hp.role.as_str(), hp.fenced_by), ("fenced", Some(1)));
    let err = place(&p, &c, &uuid('x', 22), None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");

    // The new primary moves on; the old one rejoins as its replica, since
    // their histories agree, and sheds the fence.
    place(&r, &c, &uuid('b', 21), Some(1)).await.unwrap();
    let head2 = r.head().await;
    let r_addr = r.addr.clone();
    p.configure = std::sync::Arc::new(move |o| {
        o.replicate_from = Some(r_addr.clone());
    });
    p.restart().await;
    let hp = wait_for(&p, "the old primary to catch up as a replica", |h| {
        h.head >= head2
    })
    .await;
    assert_eq!(
        (hp.role.as_str(), hp.epoch, hp.fenced_by),
        ("replica", 1, None)
    );
    assert!(
        !p.data_dir()
            .join("data")
            .join(fold_db::LOG_NAME)
            .join("fenced")
            .exists()
    );
    assert_eq!(p.all_events().await, r.all_events().await);
    p.shutdown().await;
    r.shutdown().await;
}

#[tokio::test]
async fn a_fencing_token_is_checked_against_the_epoch() {
    let mut p = DbNode::start().await;
    let c = uuid('c', 23);
    register(&p, &c).await;
    let head1 = p.head().await;
    let mut r = replica_of(&p).await;
    wait_for(&r, "catch-up", |h| h.head >= head1).await;

    // The primary dies; the replica is promoted (epoch 1) and never reaches
    // the old primary to fence it.
    p.shutdown().await;
    r.cluster().await.promote(PromoteRequest {}).await.unwrap();
    assert_eq!(r.health().await.epoch, 1);

    // On the new primary: the current token passes, a stale one is refused.
    place(&r, &c, &uuid('a', 23), Some(1)).await.unwrap();
    let err = place(&r, &c, &uuid('b', 23), Some(0)).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("stale fencing token"), "{err}");

    // The old primary comes back, unaware, at epoch 0 and taking writes...
    p.restart().await;
    let h = p.health().await;
    assert_eq!((h.role.as_str(), h.epoch), ("primary", 0));
    // ...until a client carrying the new primary's token reaches it: that
    // write is refused and the old primary is fenced by it, for good.
    let err = place(&p, &c, &uuid('c', 23), Some(1)).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("newer primary"), "{err}");
    let hp = p.health().await;
    assert_eq!((hp.role.as_str(), hp.fenced_by), ("fenced", Some(1)));
    let err = place(&p, &c, &uuid('d', 23), None).await.unwrap_err();
    assert!(err.message().contains("fenced"), "{err}");
    p.shutdown().await;
    r.shutdown().await;
}

#[tokio::test]
async fn a_log_that_took_writes_of_its_own_cannot_rejoin_as_a_replica() {
    let mut p = DbNode::start().await;
    let c = uuid('c', 25);
    register(&p, &c).await;
    let head1 = p.head().await;
    let mut r = replica_of(&p).await;
    wait_for(&r, "catch-up", |h| h.head >= head1).await;

    // Split brain by hand: both sides take a write while the other is down.
    p.shutdown().await;
    r.cluster().await.promote(PromoteRequest {}).await.unwrap();
    r.shutdown().await;
    p.restart().await;
    place(&p, &c, &uuid('a', 25), None).await.unwrap();
    p.shutdown().await;
    r.configure = std::sync::Arc::new(|_| {});
    r.restart().await;
    place(&r, &c, &uuid('b', 25), None).await.unwrap();

    // The old primary cannot become a replica of the new one: same head or
    // not, the histories differ past head1.
    let mut opts = p.options("data");
    opts.replicate_from = Some(r.addr.clone());
    let err = fold_db::start(opts).await.err().expect("refused");
    assert!(format!("{err:#}").contains("diverged"), "{err:#}");
    r.shutdown().await;
}

#[tokio::test]
async fn a_lease_is_held_only_with_a_majority_and_binds_the_voters() {
    let (pa, pb) = (free_port(), free_port());
    let peers = vec![
        format!("http://127.0.0.1:{pa}"),
        format!("http://127.0.0.1:{pb}"),
    ];
    let lease = Duration::from_millis(1500);
    let peers_for_p = peers.clone();
    let mut p = DbNode::start_with(
        |s| s.to_string(),
        move |o| {
            o.quorum_peers = peers_for_p.clone();
            o.lease = Some(lease);
        },
    )
    .await;
    let c = uuid('c', 41);
    // Writes do not need the lease (fencing guards them).
    register(&p, &c).await;
    let h = p.health().await;
    assert_eq!((h.lease_secs, h.lease_held), (1, false));
    assert!(
        h.lease_error.is_empty() || h.lease_error.contains("1 of 3"),
        "{}",
        h.lease_error
    );

    // The peers arrive: a majority confirms the primary.
    let p_addr = p.addr.clone();
    let mut a = DbNode::start_with(
        |s| s.to_string(),
        move |o| {
            o.listen = format!("127.0.0.1:{pa}").parse().unwrap();
            o.replicate_from = Some(p_addr.clone());
        },
    )
    .await;
    let p_addr = p.addr.clone();
    let mut b = DbNode::start_with(
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
    let head1 = p.head().await;
    wait_for(&a, "A catch-up", |h| h.head >= head1).await;

    // One peer down: still a majority. Both down: the lease lapses within
    // the lease.
    b.shutdown().await;
    tokio::time::sleep(lease * 2).await;
    assert!(p.health().await.lease_held, "P and A are 2 of 3");
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
    // Writes still land (fencing is what stops a stale primary writing).
    register(&p, &uuid('d', 41)).await;
    // A peer returns: the lease is back.
    a.restart().await;
    wait_for(&p, "the lease back", |h| h.lease_held).await;

    // A peer that granted a lease elects nobody until it ends. The peer
    // first catches up with the primary's last write, so its head cannot
    // move between reading the candidate's head and the vote.
    let p_head = p.head().await;
    wait_for(&a, "the peer to catch up", |h| h.head >= p_head).await;
    let log_id = h.log_id.clone();
    let p_addr = p.addr.clone();
    p.shutdown().await;
    let r = a
        .cluster()
        .await
        .request_vote(VoteRequest {
            epoch: 1,
            log_id: log_id.clone(),
            primary: p_addr.clone(),
            candidate_head: a.head().await,
            candidate: "test".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!r.granted && r.reason.contains("holds a lease"), "{r:?}");
    tokio::time::sleep(lease).await;
    let r = a
        .cluster()
        .await
        .request_vote(VoteRequest {
            epoch: 1,
            log_id,
            primary: p_addr,
            candidate_head: a.head().await,
            candidate: "test".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.granted, "{r:?}");
    a.shutdown().await;

    // A lease needs peers to grant it.
    let mut opts = p.options("data-none");
    opts.lease = Some(Duration::from_secs(5));
    let err = fold_db::start(opts).await.err().expect("refused");
    assert!(
        format!("{err:#}").contains("lease needs quorum_peers"),
        "{err:#}"
    );
}

async fn vote(
    voter: &DbNode,
    epoch: u64,
    log_id: &str,
    primary: &str,
    candidate_head: u64,
) -> fold_proto::database::v1::VoteResponse {
    voter
        .cluster()
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

#[tokio::test]
async fn a_replica_promotes_itself_only_with_a_majority() {
    let mut p = DbNode::start().await;
    let c = uuid('c', 31);
    register(&p, &c).await;
    place(&p, &c, &uuid('a', 31), None).await.unwrap();
    let head1 = p.head().await;
    let log_id = p.health().await.log_id;

    // B: a plain replica that will vote. A: the candidate, needing a
    // majority of {P, B, itself}.
    let p_addr = p.addr.clone();
    let mut b = DbNode::start_with(
        |s| s.to_string(),
        move |o| {
            o.replicate_from = Some(p_addr.clone());
        },
    )
    .await;
    wait_for(&b, "B catch-up", |h| h.head >= head1).await;
    let (p_addr, b_addr) = (p.addr.clone(), b.addr.clone());
    let grace = Duration::from_millis(600);
    let mut a = DbNode::start_with(
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
        b.health().await.role,
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
    let placed = place(&a, &c, &uuid('b', 31), None).await.unwrap();
    assert_eq!(placed.first_position, head1);
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn without_a_majority_a_replica_stays_a_replica_until_an_operator_decides() {
    let mut p = DbNode::start().await;
    register(&p, &uuid('c', 33)).await;
    let head1 = p.head().await;
    // Peers: the primary and a member that is not there.
    let p_addr = p.addr.clone();
    let grace = Duration::from_millis(400);
    let mut a = DbNode::start_with(
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
    let h = a.health().await;
    assert_eq!((h.role.as_str(), h.epoch), ("replica", 0), "{h:?}");
    assert!(h.last_election.starts_with("lost"), "{}", h.last_election);

    // The operator's call overrides the quorum.
    let r = a
        .cluster()
        .await
        .promote(PromoteRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.head, head1);
    let h = a.health().await;
    assert_eq!(h.role, "primary");
    // Past every epoch it proposed in the lost rounds, so its tokens are
    // newer than any vote it collected.
    let voted = vote(&a, 1, &h.log_id, &p.addr, head1).await.voted_epoch;
    assert!(voted >= 1, "it voted for itself at least once");
    assert_eq!(h.epoch, voted + 1, "{h:?}");
    a.shutdown().await;

    // The quorum option without automatic failover is a configuration error.
    let mut opts = a.options("data-none");
    opts.replicate_from = Some("http://127.0.0.1:9".into());
    opts.quorum_peers = vec!["http://127.0.0.1:9".into()];
    let err = fold_db::start(opts).await.err().expect("refused");
    assert!(
        format!("{err:#}").contains("needs auto_failover"),
        "{err:#}"
    );
}
