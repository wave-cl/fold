//! Replication: a replica started empty tails the primary byte for byte,
//! refuses writes, is promoted in place or automatically, and refuses a
//! log that is not its primary's or a schema that breaks against it.

use std::time::{Duration, Instant};

use fold_proto::database::v1::PromoteRequest;
use tonic::Code;

use crate::common::{DbNode, place, register, replica_of, uuid, wait_for};

#[tokio::test]
async fn a_replica_tails_the_primary_and_can_be_promoted_in_place() {
    let mut primary = DbNode::start().await;
    let c = uuid('c', 7);
    register(&primary, &c).await;
    place(&primary, &c, &uuid('a', 7), None).await.unwrap();
    let head1 = primary.head().await;
    let primary_events = primary.all_events().await;
    assert_eq!(primary_events.len() as u64, head1);

    // A replica, from an empty directory, catches up with history...
    let mut replica = replica_of(&primary).await;
    let h = wait_for(&replica, "catch-up", |h| h.head >= head1).await;
    assert_eq!(
        (
            h.role.as_str(),
            h.replicating_from.as_str(),
            h.replica_connected
        ),
        ("replica", primary.addr.as_str(), true)
    );
    assert_eq!(h.log_id, primary.health().await.log_id, "same log identity");
    assert_eq!(h.primary_head, Some(head1));
    assert_eq!(
        replica.all_events().await,
        primary_events,
        "the same records, byte for byte"
    );
    // ...refuses writes...
    let err = place(&replica, &c, &uuid('x', 7), None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("replica"), "{err}");
    // ...and follows live.
    place(&primary, &c, &uuid('b', 7), None).await.unwrap();
    let head2 = primary.head().await;
    wait_for(&replica, "live tail", |h| h.head >= head2).await;
    assert_eq!(replica.all_events().await, primary.all_events().await);

    // A primary cannot be promoted.
    let err = primary
        .cluster()
        .await
        .promote(PromoteRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("already a primary"), "{err}");

    // The primary is gone; fail over in place.
    primary.shutdown().await;
    let r = replica
        .cluster()
        .await
        .promote(PromoteRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (r.head, r.promoted_from.as_str()),
        (head2, primary.addr.as_str())
    );
    let h = replica.health().await;
    assert_eq!(
        (
            h.role.as_str(),
            h.replicating_from.as_str(),
            h.promoted_from.as_str(),
            h.replica_connected,
            h.epoch
        ),
        ("primary", "", primary.addr.as_str(), false, 1)
    );
    assert!(
        replica
            .data_dir()
            .join("data")
            .join(fold_db::LOG_NAME)
            .join("promoted")
            .is_file()
    );
    // It takes writes now, and a second promotion is refused.
    let placed = place(&replica, &c, &uuid('e', 7), None).await.unwrap();
    assert_eq!(placed.first_position, head2);
    let err = replica
        .cluster()
        .await
        .promote(PromoteRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");

    // Restarting with the old replicate_from is refused: the log was
    // promoted and may be ahead of the old primary.
    replica.shutdown().await;
    let mut opts = replica.options("data");
    opts.replicate_from = Some(primary.addr.clone());
    let err = fold_db::start(opts).await.err().expect("refused");
    assert!(format!("{err:#}").contains("was promoted from"), "{err:#}");

    // Without it, it is the primary it became.
    replica.configure = std::sync::Arc::new(|_| {});
    replica.restart().await;
    let h = replica.health().await;
    assert_eq!((h.role.as_str(), h.head), ("primary", head2 + 1));
    replica.shutdown().await;
}

#[tokio::test]
async fn a_replica_refuses_a_log_that_is_not_its_primarys_and_an_unreachable_primary() {
    let primary = DbNode::start().await;
    // A database with its own history cannot become this primary's replica.
    let mut other = DbNode::start().await;
    register(&other, &uuid('c', 8)).await;
    other.shutdown().await;
    let mut opts = other.options("data");
    opts.replicate_from = Some(primary.addr.clone());
    let err = fold_db::start(opts.clone()).await.err().expect("refused");
    assert!(format!("{err:#}").contains("must start empty"), "{err:#}");

    // Nobody listening there.
    opts.data_dir = other.data_dir().join("data-fresh");
    opts.replicate_from = Some("http://127.0.0.1:9".into());
    let err = fold_db::start(opts).await.err().expect("refused");
    assert!(
        format!("{err:#}").contains("cannot reach the primary"),
        "{err:#}"
    );
}

#[tokio::test]
async fn a_replica_fails_over_by_itself_once_the_primary_is_gone_for_the_grace_period() {
    let mut primary = DbNode::start().await;
    let c = uuid('c', 11);
    register(&primary, &c).await;
    let head1 = primary.head().await;
    let events = primary.all_events().await;

    let grace = Duration::from_millis(600);
    let primary_addr = primary.addr.clone();
    let mut replica = DbNode::start_with(
        |s| s.to_string(),
        move |o| {
            o.replicate_from = Some(primary_addr.clone());
            o.auto_failover = Some(grace);
        },
    )
    .await;
    let h = wait_for(&replica, "catch-up", |h| h.head >= head1).await;
    assert_eq!(
        h.auto_failover_secs, 0,
        "600 ms rounds down to 0 s; armed all the same"
    );

    // Control: a primary that answers keeps the replica a replica well
    // past the grace period.
    tokio::time::sleep(grace * 3).await;
    let h = replica.health().await;
    assert_eq!(
        (
            h.role.as_str(),
            h.replica_connected,
            h.primary_unreachable_secs
        ),
        ("replica", true, None)
    );

    // The primary goes away; nobody calls Promote.
    let gone_at = Instant::now();
    primary.shutdown().await;
    let h = wait_for(&replica, "automatic failover", |h| h.role == "primary").await;
    assert!(
        gone_at.elapsed() >= grace,
        "promoted after {:?}, before the grace period",
        gone_at.elapsed()
    );
    assert_eq!(h.promoted_from, primary.addr);
    assert!(h.promotion.contains("automatically"), "{}", h.promotion);
    let marker = std::fs::read_to_string(
        replica
            .data_dir()
            .join("data")
            .join(fold_db::LOG_NAME)
            .join("promoted"),
    )
    .unwrap();
    assert!(marker.contains("automatically"), "{marker}");
    assert_eq!(replica.all_events().await, events);
    let placed = place(&replica, &c, &uuid('b', 11), None).await.unwrap();
    assert_eq!(placed.first_position, head1);
    replica.shutdown().await;

    // The option without a primary to fail over from is a configuration error.
    let mut opts = replica.options("data-none");
    opts.auto_failover = Some(grace);
    let err = fold_db::start(opts).await.err().expect("refused");
    assert!(
        format!("{err:#}").contains("needs replicate_from"),
        "{err:#}"
    );
}

#[tokio::test]
async fn a_replica_refuses_a_domain_that_breaks_against_the_primarys() {
    let mut primary = DbNode::start().await;
    // Breaking against the primary's: a required field on a stored event.
    let other = DbNode::start_with(
        |s| {
            s.replace(
                "event OrderPlaced v1   { order_id: uuid, customer_id: uuid,",
                "event OrderPlaced v1   { order_id: uuid, channel: string, customer_id: uuid,",
            )
        },
        |_| {},
    )
    .await;
    let mut opts = other.options("data-replica");
    opts.replicate_from = Some(primary.addr.clone());
    let err = fold_db::start(opts).await.err().expect("refused");
    let text = format!("{err:#}");
    assert!(text.contains("breaks against the primary's"), "{text}");
    assert!(text.contains("Orders.OrderPlaced@v1.channel"), "{text}");

    // Control: a compatible difference (a comment, an optional field) starts.
    let compatible = DbNode::start_with(
        |s| {
            format!(
                "// replica copy\n{}",
                s.replace(
                    "event OrderPlaced v1   { order_id: uuid, customer_id: uuid,",
                    "event OrderPlaced v1   { order_id: uuid, channel: string?, customer_id: uuid,"
                )
            )
        },
        |_| {},
    )
    .await;
    let mut opts = compatible.options("data-replica");
    opts.replicate_from = Some(primary.addr.clone());
    let running = fold_db::start(opts)
        .await
        .expect("a compatible domain replicates");
    running.shutdown().await.unwrap();
    primary.shutdown().await;
}
