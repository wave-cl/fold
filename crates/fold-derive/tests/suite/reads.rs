//! Projections and aggregate state over appended events: read-your-writes
//! with the database's token, session tokens, replay, snapshots and the
//! cache, and the read gate following the database's role and lease.

use std::time::Duration;

use fold_proto::database::v1::FenceRequest;
use fold_proto::derivation::v1::ScanRequest;
use serde_json::json;
use tonic::Code;

use crate::common::{Db, Derive, add_line, cancel, place, register, state_of, uuid, wait_for};

const CO: &str = "Orders.CustomerOrders";
const TOTALS: &str = "Orders.OrderTotals";

#[tokio::test]
async fn projections_follow_the_database_and_reads_wait_for_the_token() {
    let mut db = Db::start().await;
    let mut d = Derive::start(&db).await;
    let c = uuid('c', 1);
    register(&db, &c).await;
    let a = uuid('a', 1);
    let b = uuid('b', 1);
    place(&db, &c, &a, "10.00").await;
    let placed_b = place(&db, &c, &b, "20.00").await;
    let log_id = d.health().await.log_id;
    assert!(
        placed_b.token.starts_with(&format!("fold1:{log_id}:0:")),
        "{}",
        placed_b.token
    );

    // The database's token carries read-your-writes to the derivation node.
    let got = d
        .get(
            CO,
            "customer_orders",
            json!({ "customer_id": c }),
            None,
            &placed_b.token,
            None,
        )
        .await
        .unwrap();
    assert!(got.found);
    let row: serde_json::Value = serde_json::from_slice(&got.row.unwrap().row).unwrap();
    assert_eq!(row["order_count"], 2);
    assert_eq!(row["name"], "Ada");
    assert!(got.checkpoint.unwrap() >= placed_b.last_position);
    // The session token hands the state back, at least as far along.
    assert!(got.token.starts_with("fold1:"), "{}", got.token);
    let again = d
        .get(
            CO,
            "customer_orders",
            json!({ "customer_id": c }),
            None,
            &got.token,
            None,
        )
        .await
        .unwrap();
    assert!(again.found);
    // A bare position works too, and so does a scan with the token.
    let totals = d
        .row(
            TOTALS,
            "order_totals",
            json!({ "order_id": b }),
            placed_b.last_position,
        )
        .await;
    assert_eq!(totals["total"]["amount"], "20.00");
    assert_eq!(totals["status"], "Pending");
    let resp = d
        .query()
        .await
        .scan(ScanRequest {
            projection: TOTALS.into(),
            table: "order_totals".into(),
            key_prefix: b"{}".to_vec(),
            limit: 0,
            min_position: None,
            wait_ms: None,
            token: placed_b.token.clone(),
        })
        .await
        .unwrap();
    assert!(
        resp.metadata().get(fold_proto::SESSION_HEADER).is_some(),
        "the scan hands back a session token"
    );
    let mut rows = resp.into_inner();
    let mut n = 0;
    while rows.message().await.unwrap().is_some() {
        n += 1;
    }
    assert_eq!(n, 2);

    // Live: a cancellation reaches the totals row.
    let cancelled = cancel(&db, &a, 0).await;
    let totals = d
        .row(
            TOTALS,
            "order_totals",
            json!({ "order_id": a }),
            cancelled.last_position,
        )
        .await;
    assert_eq!(totals["status"], "Cancelled");
    let h = wait_for(&d, "the tail to catch up", |h| {
        h.database_connected && h.lag == 0
    })
    .await;
    assert_eq!(h.database_head, db.head().await);

    // A position the database does not have: the wait names it.
    let ahead = format!("fold1:{log_id}:0:{}", cancelled.last_position + 1000);
    let err = d
        .get(
            CO,
            "customer_orders",
            json!({ "customer_id": c }),
            None,
            &ahead,
            Some(200),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err}");
    assert!(err.message().contains("has not reached it yet"), "{err}");
    // A token of another log is refused outright.
    let other = format!("fold1:{}:0:1", uuid::Uuid::now_v7());
    let err = d
        .get(
            CO,
            "customer_orders",
            json!({ "customer_id": c }),
            None,
            &other,
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("is of log"), "{err}");
    d.shutdown().await;
    db.shutdown().await;
}

#[tokio::test]
async fn aggregate_state_replays_snapshots_and_caches() {
    let mut db = Db::start().await;
    let mut d = Derive::start_with(
        &db,
        |s| s.replace("snapshot every 100", "snapshot every 2"),
        |_| {},
    )
    .await;
    let c = uuid('c', 2);
    let a = uuid('a', 2);
    let stream = format!("order-{a}");
    place(&db, &c, &a, "1.00").await;
    for n in 2..=4u32 {
        add_line(&db, &a, n, u64::from(n) - 2, &format!("{n}.00")).await;
    }
    let got = d.aggregate(&stream).await.unwrap();
    assert!(got.found);
    assert_eq!(
        (got.aggregate.as_str(), got.version, got.replayed),
        ("Orders.Order", 3, 4)
    );
    assert_eq!(got.snapshot_version, None);
    let s = state_of(&got);
    assert_eq!(s["lines"].as_object().unwrap().len(), 4);
    assert_eq!(s["total"]["amount"], "4.00");
    assert_eq!(s["status"], "Pending");
    // Cached now; and a new event makes the cache catch up from the log.
    let again = d.aggregate(&stream).await.unwrap();
    assert_eq!(again.replayed, 0);
    add_line(&db, &a, 5, 3, "5.00").await;
    let moved = d.aggregate(&stream).await.unwrap();
    assert_eq!((moved.version, moved.replayed), (4, 1));
    assert_eq!(state_of(&moved)["lines"].as_object().unwrap().len(), 5);
    // The first load took an instance snapshot (4 >= every 2): a restart
    // loads from it and replays only what came after.
    d.restart().await;
    let fresh = d.aggregate(&stream).await.unwrap();
    assert_eq!(fresh.snapshot_version, Some(3), "{fresh:?}");
    assert_eq!(fresh.replayed, 1);
    assert_eq!(state_of(&fresh)["lines"].as_object().unwrap().len(), 5);
    // Unknown streams.
    let none = d
        .aggregate(&format!("order-{}", uuid('e', 99)))
        .await
        .unwrap();
    assert!(!none.found);
    let err = d.aggregate("nothing-here").await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    d.shutdown().await;
    db.shutdown().await;
}

#[tokio::test]
async fn reads_follow_the_databases_role_and_lease() {
    let mut db = Db::start().await;
    let mut d = Derive::start(&db).await;
    let c = uuid('c', 3);
    let r = register(&db, &c).await;
    assert!(
        d.get(
            CO,
            "customer_orders",
            json!({ "customer_id": c }),
            Some(r.last_position),
            "",
            None
        )
        .await
        .unwrap()
        .found
    );
    // Fenced: the role watch notices within a second; reads stop.
    db.cluster()
        .await
        .fence(FenceRequest { epoch: 7 })
        .await
        .unwrap();
    wait_for(&d, "the fence to be seen", |h| h.database_role == "fenced").await;
    let err = d
        .get(
            CO,
            "customer_orders",
            json!({ "customer_id": c }),
            None,
            "",
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("fenced"), "{err}");
    let err = d.aggregate(&format!("customer-{c}")).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    d.shutdown().await;
    db.shutdown().await;

    // A primary with leases on and no peer to grant one: no lease, no reads.
    let mut db = Db::start_with(|o| {
        o.quorum_peers = vec!["http://127.0.0.1:9".into()];
        o.lease = Some(Duration::from_secs(1));
    })
    .await;
    let mut d = Derive::start(&db).await;
    wait_for(&d, "the role", |h| h.database_role == "primary").await;
    let err = d
        .get(
            CO,
            "customer_orders",
            json!({ "customer_id": c }),
            None,
            "",
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err}");
    assert!(err.message().contains("holds no lease"), "{err}");
    d.shutdown().await;
    db.shutdown().await;
}
