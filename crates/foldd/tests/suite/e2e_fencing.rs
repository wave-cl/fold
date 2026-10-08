//! Fencing: every promotion starts a new epoch; a primary that learns of a
//! newer one, from the new primary or from a client's token, stops taking
//! writes for good, and can come back only as a replica with the same
//! history.

use std::time::{Duration, Instant};

use fold_proto::v1::{FenceRequest, HealthRequest, HealthResponse, PromoteRequest};
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

async fn replica_of(primary: &Daemon) -> Daemon {
    let addr = primary.addr.clone();
    Daemon::start_with(
        |s| s.to_string(),
        move |o| {
            o.replicate_from = Some(addr.clone());
        },
    )
    .await
}

async fn place(d: &Daemon, c: &str, order: &str, token: Option<u64>) -> Result<u64, tonic::Status> {
    let mut req = fold_proto::v1::ExecuteRequest {
        command: "Orders.Order.PlaceOrder".into(),
        stream_id: format!("order-{order}"),
        payload: serde_json::to_vec(
            &json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "1.00")] }),
        )
        .unwrap(),
        content_type: fold_proto::CONTENT_TYPE_JSON.into(),
        metadata: Vec::new(),
        fencing_token: None,
    };
    req.fencing_token = token;
    d.command()
        .await
        .execute(req)
        .await
        .map(|r| r.into_inner().first_position)
}

#[tokio::test]
async fn a_promotion_fences_the_old_primary_which_can_rejoin_as_a_replica() {
    let mut p = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 21);
    p.exec(
        "Customers.Customer.Register",
        &format!("customer-{c}"),
        json!({ "name": "Ada" }),
    )
    .await
    .unwrap();
    let a = uuid('a', 21);
    place(&p, &c, &a, None).await.unwrap();
    let head1 = settle(&p, &[format!("shipment-{a}")]).await;
    assert_eq!(health(&p).await.epoch, 0);

    let mut r = replica_of(&p).await;
    wait_for(&r, "catch-up", |h| h.head >= head1).await;
    assert_eq!(
        health(&r).await.epoch,
        0,
        "a replica carries its primary's epoch"
    );

    // Promote while the old primary is alive and reachable: it is fenced.
    let promoted = r
        .admin()
        .await
        .promote(PromoteRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(promoted.head, head1);
    let hr = wait_for(&r, "the fence to be acknowledged", |h| h.old_primary_fenced).await;
    assert_eq!((hr.role.as_str(), hr.epoch), ("primary", 1));
    let hp = health(&p).await;
    assert_eq!(
        (hp.role.as_str(), hp.epoch, hp.fenced_by),
        ("fenced", 0, Some(1))
    );
    assert!(
        p.data_dir()
            .join("data")
            .join(foldd::LOG_NAME)
            .join("fenced")
            .is_file()
    );

    // The old primary refuses writes, with or without a token.
    let err = place(&p, &c, &uuid('x', 21), None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("fenced"), "{err}");
    let err = p
        .append(
            &format!("customer-{}", uuid('y', 21)),
            "Customers.CustomerRegistered",
            json!({ "customer_id": uuid('y', 21), "name": "Bob" }),
            fold_proto::v1::expected_version::Kind::NoStream(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    // Fence again: harmless. Fence the new primary with its own epoch: refused.
    let f = p
        .admin()
        .await
        .fence(FenceRequest { epoch: 1 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!((f.role.as_str(), f.epoch), ("fenced", 0));
    let err = r
        .admin()
        .await
        .fence(FenceRequest { epoch: 1 })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("not newer"), "{err}");

    // Fenced across a restart.
    p.restart().await;
    let hp = health(&p).await;
    assert_eq!((hp.role.as_str(), hp.fenced_by), ("fenced", Some(1)));
    let err = place(&p, &c, &uuid('x', 22), None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");

    // The new primary moves on; the old one rejoins as its replica, since
    // their histories agree, and sheds the fence.
    let b = uuid('b', 21);
    place(&r, &c, &b, Some(1)).await.unwrap();
    let head2 = settle(&r, &[format!("shipment-{a}"), format!("shipment-{b}")]).await;
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
            .join(foldd::LOG_NAME)
            .join("fenced")
            .exists()
    );
    assert_eq!(p.all_events().await, r.all_events().await);
    p.shutdown().await;
    r.shutdown().await;
}

#[tokio::test]
async fn a_fencing_token_is_checked_against_the_epoch() {
    let mut p = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 23);
    p.exec(
        "Customers.Customer.Register",
        &format!("customer-{c}"),
        json!({ "name": "Ada" }),
    )
    .await
    .unwrap();
    let head1 = settle(&p, &[]).await;
    let mut r = replica_of(&p).await;
    wait_for(&r, "catch-up", |h| h.head >= head1).await;

    // The primary dies; the replica is promoted (epoch 1) and never reaches
    // the old primary to fence it.
    p.shutdown().await;
    r.admin().await.promote(PromoteRequest {}).await.unwrap();
    assert_eq!(health(&r).await.epoch, 1);

    // On the new primary: the current token passes, a stale one is refused.
    place(&r, &c, &uuid('a', 23), Some(1)).await.unwrap();
    let err = place(&r, &c, &uuid('b', 23), Some(0)).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("stale fencing token"), "{err}");

    // The old primary comes back, unaware, at epoch 0 and taking writes...
    p.restart().await;
    assert_eq!(
        (health(&p).await.role.as_str(), health(&p).await.epoch),
        ("primary", 0)
    );
    // ...until a client carrying the new primary's token reaches it: that
    // write is refused and the old primary is fenced by it, for good.
    let err = place(&p, &c, &uuid('c', 23), Some(1)).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("newer primary"), "{err}");
    let hp = health(&p).await;
    assert_eq!((hp.role.as_str(), hp.fenced_by), ("fenced", Some(1)));
    let err = place(&p, &c, &uuid('d', 23), None).await.unwrap_err();
    assert!(err.message().contains("fenced"), "{err}");
    // (The new primary's fencer keeps looking for the old primary at its old
    // address; a restarted test daemon has a new one, so it never finds it
    // here. The reachable case is in the test above.)
    p.shutdown().await;
    r.shutdown().await;
}

#[tokio::test]
async fn a_log_that_took_writes_of_its_own_cannot_rejoin_as_a_replica() {
    let mut p = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 25);
    p.exec(
        "Customers.Customer.Register",
        &format!("customer-{c}"),
        json!({ "name": "Ada" }),
    )
    .await
    .unwrap();
    let head1 = settle(&p, &[]).await;
    let mut r = replica_of(&p).await;
    wait_for(&r, "catch-up", |h| h.head >= head1).await;

    // Split brain by hand: both sides take a write while the other is down.
    p.shutdown().await;
    r.admin().await.promote(PromoteRequest {}).await.unwrap();
    r.shutdown().await;
    p.restart().await;
    place(&p, &c, &uuid('a', 25), None).await.unwrap();
    settle(&p, &[format!("shipment-{}", uuid('a', 25))]).await;
    p.shutdown().await;
    // The promoted replica restarts as what it became.
    r.configure = std::sync::Arc::new(|_| {});
    r.restart().await;
    place(&r, &c, &uuid('b', 25), None).await.unwrap();
    settle(&r, &[format!("shipment-{}", uuid('b', 25))]).await;

    // The old primary cannot become a replica of the new one: same head or
    // not, the histories differ past head1.
    let mut opts = foldd::Options::new(
        p.data_dir().join("data"),
        p.data_dir().join("schema.fold"),
        "127.0.0.1:0".parse().unwrap(),
    );
    opts.fsync = false;
    opts.replicate_from = Some(r.addr.clone());
    let err = foldd::start(opts).await.err().expect("refused");
    assert!(format!("{err:#}").contains("diverged"), "{err:#}");
    r.shutdown().await;
}
