//! Restoring a backup into a running daemon: the process keeps its address,
//! the log underneath is swapped, the previous log is kept aside.

use std::time::{Duration, Instant};

use fold_proto::v1::admin_client::AdminClient;
use fold_proto::v1::{BackupLogRequest, HealthRequest, HealthResponse, RestoreLogRequest};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use tonic::transport::Channel;

use crate::common::{
    Daemon, ROOT_FILE, copy_orders_guest, example_bundle, line, settle, uuid, write_bundle,
};

async fn health(addr: &str) -> Option<HealthResponse> {
    let ch = Channel::from_shared(addr.to_string())
        .ok()?
        .connect()
        .await
        .ok()?;
    AdminClient::new(ch)
        .health(HealthRequest {})
        .await
        .ok()
        .map(|r| r.into_inner())
}

#[tokio::test]
async fn a_live_restore_swaps_the_log_under_the_same_address() {
    // A supervised daemon, as the binary runs it.
    let dir = tempfile::tempdir().unwrap();
    write_bundle(dir.path(), &example_bundle());
    copy_orders_guest(&dir.path().join("orders.wasm"));
    let mut opts = foldd::Options::new(
        dir.path().join("data"),
        dir.path().join(ROOT_FILE),
        "127.0.0.1:0".parse().unwrap(),
    );
    opts.fsync = false;
    let supervisor = foldd::Supervisor::start(opts).await.unwrap();
    let addr = format!("http://{}", supervisor.local_addr);
    let cancel = CancellationToken::new();
    let supervise = tokio::spawn({
        let cancel = cancel.clone();
        async move { supervisor.run(cancel.cancelled()).await }
    });

    // Borrow the Daemon helper's client methods over the supervisor's address.
    let mut d = Daemon::start(|s| s.to_string()).await;
    d.shutdown().await;
    d.addr = addr.clone();

    let c = uuid('c', 1);
    let a = uuid('a', 1);
    d.exec(
        "Customers.Customer.Register",
        &format!("customer-{c}"),
        json!({ "name": "Ada" }),
    )
    .await
    .unwrap();
    d.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{a}"),
        json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "1.00")] }),
    )
    .await
    .unwrap();
    // Settle first: the Fulfilment process appends a shipment event of its
    // own, and a backup taken before it would be completed after the restore
    // (correctly), which is not what this test is about.
    let settled = settle(&d, &[format!("shipment-{a}")]).await;
    let h0 = health(&addr).await.unwrap();
    assert_eq!(h0.head, settled);
    let archive = d
        .admin()
        .await
        .backup_log(BackupLogRequest {
            path: String::new(),
            incremental: false,
        })
        .await
        .unwrap()
        .into_inner();

    // Diverge: an order the backup does not know.
    let b = uuid('b', 1);
    d.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{b}"),
        json!({ "customer_id": c, "lines": [line(&uuid('1', 2), 1, "2.00")] }),
    )
    .await
    .unwrap();
    assert!(d.aggregate(&format!("order-{b}")).await.unwrap().found);
    assert!(health(&addr).await.unwrap().head > archive.head);

    // A bad path is refused before anything happens.
    let err = d
        .admin()
        .await
        .restore_log(RestoreLogRequest {
            path: dir.path().join("nope.fbak").display().to_string(),
            to: None,
            at_unix_nanos: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");

    // Ask for the restore; the daemon stops, swaps and serves again.
    let accepted = d
        .admin()
        .await
        .restore_log(RestoreLogRequest {
            path: archive.path.clone(),
            to: None,
            at_unix_nanos: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(accepted.head, archive.head);
    let deadline = Instant::now() + Duration::from_secs(20);
    let h = loop {
        if let Some(h) = health(&addr).await
            && h.last_restore.starts_with("ok ")
        {
            break h;
        }
        if supervise.is_finished() {
            let outcome = supervise.await;
            panic!("the supervisor stopped during the restore: {outcome:?}");
        }
        let last = health(&addr).await.map(|h| h.last_restore);
        assert!(
            Instant::now() < deadline,
            "the daemon did not come back restored; last health: {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(h.head, archive.head, "the archive's head");
    assert_eq!(h.log_id, h0.log_id, "same log identity, earlier state");
    assert!(
        h.last_restore.ends_with(&archive.path),
        "{}",
        h.last_restore
    );

    // The diverging order is gone; the backed-up one is there; the daemon works.
    assert!(!d.aggregate(&format!("order-{b}")).await.unwrap().found);
    assert!(d.aggregate(&format!("order-{a}")).await.unwrap().found);
    let placed = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{}", uuid('e', 1)),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 3), 1, "3.00")] }),
        )
        .await
        .unwrap();
    let row = d
        .row(
            "Orders.CustomerOrders",
            "customer_orders",
            json!({ "customer_id": c }),
            placed.last_position,
        )
        .await;
    assert_eq!(row["order_count"], 2, "one from the backup, one new");

    // The previous log was kept aside, not deleted.
    let aside: Vec<_> = std::fs::read_dir(dir.path().join("data"))
        .unwrap()
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("default.replaced-")
        })
        .collect();
    assert_eq!(aside.len(), 1);

    cancel.cancel();
    supervise.await.unwrap().unwrap();
}

#[tokio::test]
async fn an_unsupervised_daemon_refuses_a_live_restore() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let archive = d
        .admin()
        .await
        .backup_log(BackupLogRequest {
            path: String::new(),
            incremental: false,
        })
        .await
        .unwrap()
        .into_inner();
    let err = d
        .admin()
        .await
        .restore_log(RestoreLogRequest {
            path: archive.path,
            to: None,
            at_unix_nanos: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("not supervised"), "{err}");
    d.shutdown().await;
}

/// Ids of every event on `shipment-<order>`, in order.
async fn shipment_ids(d: &Daemon, order: &str) -> Vec<String> {
    let mut stream = d
        .log()
        .await
        .read_stream(fold_proto::v1::ReadStreamRequest {
            stream_id: format!("shipment-{order}"),
            from_version: 0,
            max: 0,
            backward: false,
        })
        .await
        .unwrap()
        .into_inner();
    let mut ids = Vec::new();
    while let Some(e) = stream.message().await.unwrap() {
        ids.push(e.id);
    }
    ids
}

/// A live restore may stop at a point in time: the daemon comes back at
/// that head, and the runners, reset where they had looked past it, replay
/// the process chain from there as if the cut events had never happened.
#[tokio::test]
async fn a_live_restore_can_stop_at_a_point_in_time() {
    let dir = tempfile::tempdir().unwrap();
    write_bundle(dir.path(), &example_bundle());
    copy_orders_guest(&dir.path().join("orders.wasm"));
    let mut opts = foldd::Options::new(
        dir.path().join("data"),
        dir.path().join(ROOT_FILE),
        "127.0.0.1:0".parse().unwrap(),
    );
    opts.fsync = false;
    let supervisor = foldd::Supervisor::start(opts).await.unwrap();
    let addr = format!("http://{}", supervisor.local_addr);
    let cancel = CancellationToken::new();
    let supervise = tokio::spawn({
        let cancel = cancel.clone();
        async move { supervisor.run(cancel.cancelled()).await }
    });
    let mut d = Daemon::start(|s| s.to_string()).await;
    d.shutdown().await;
    d.addr = addr.clone();

    let c = uuid('c', 2);
    let a = uuid('a', 2);
    d.exec(
        "Customers.Customer.Register",
        &format!("customer-{c}"),
        json!({ "name": "Ada" }),
    )
    .await
    .unwrap();
    let placed_a = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "1.00")] }),
        )
        .await
        .unwrap();
    // The point in time: right after the order, before the fulfilment chain.
    let to = placed_a.last_position + 1;
    let chain_head = settle(&d, &[format!("shipment-{a}")]).await;
    assert!(chain_head > to, "the process appended after the order");
    let shipment_a = d.aggregate(&format!("shipment-{a}")).await.unwrap();
    let original_ids = shipment_ids(&d, &a).await;
    assert!(!original_ids.is_empty());
    let archive = d
        .admin()
        .await
        .backup_log(BackupLogRequest {
            path: String::new(),
            incremental: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(archive.head, chain_head);

    // Diverge past the backup.
    let b = uuid('b', 2);
    d.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{b}"),
        json!({ "customer_id": c, "lines": [line(&uuid('1', 2), 1, "2.00")] }),
    )
    .await
    .unwrap();
    settle(&d, &[format!("shipment-{a}"), format!("shipment-{b}")]).await;

    // Past the archive's head: refused before anything happens.
    let err = d
        .admin()
        .await
        .restore_log(RestoreLogRequest {
            path: archive.path.clone(),
            to: Some(archive.head + 1),
            at_unix_nanos: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    let accepted = d
        .admin()
        .await
        .restore_log(RestoreLogRequest {
            path: archive.path.clone(),
            to: Some(to),
            at_unix_nanos: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        accepted.head, to,
        "the point in time, not the archive's head"
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    let h = loop {
        if let Some(h) = health(&addr).await
            && h.last_restore.starts_with("ok ")
        {
            break h;
        }
        if supervise.is_finished() {
            let outcome = supervise.await;
            panic!("the supervisor stopped during the restore: {outcome:?}");
        }
        assert!(Instant::now() < deadline, "the daemon did not come back");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        h.last_restore.ends_with(&format!(" to {to}")),
        "{}",
        h.last_restore
    );

    // The order is back, its fulfilment chain was cut and is replayed by
    // the process from the order on; the diverging order never existed.
    assert!(d.aggregate(&format!("order-{a}")).await.unwrap().found);
    assert!(!d.aggregate(&format!("order-{b}")).await.unwrap().found);
    let replayed = settle(&d, &[format!("shipment-{a}")]).await;
    assert_eq!(replayed, chain_head, "the same chain, re-run from the cut");
    let shipment = d.aggregate(&format!("shipment-{a}")).await.unwrap();
    assert_eq!(shipment.version, shipment_a.version);
    let new_ids = shipment_ids(&d, &a).await;
    assert_eq!(new_ids.len(), original_ids.len());
    assert!(
        new_ids.iter().zip(&original_ids).all(|(n, o)| n != o),
        "the chain was re-issued, not restored: {new_ids:?} vs {original_ids:?}"
    );
    let row = d
        .row(
            "Orders.CustomerOrders",
            "customer_orders",
            json!({ "customer_id": c }),
            placed_a.last_position,
        )
        .await;
    assert_eq!(row["order_count"], 1);

    cancel.cancel();
    supervise.await.unwrap().unwrap();
}

/// A live restore may stop at a time instead: every batch recorded at or
/// before it stays. One nanosecond before the order was recorded, the
/// customer is there and the order never was.
#[tokio::test]
async fn a_live_restore_can_stop_at_a_time() {
    let dir = tempfile::tempdir().unwrap();
    write_bundle(dir.path(), &example_bundle());
    copy_orders_guest(&dir.path().join("orders.wasm"));
    let mut opts = foldd::Options::new(
        dir.path().join("data"),
        dir.path().join(ROOT_FILE),
        "127.0.0.1:0".parse().unwrap(),
    );
    opts.fsync = false;
    let supervisor = foldd::Supervisor::start(opts).await.unwrap();
    let addr = format!("http://{}", supervisor.local_addr);
    let cancel = CancellationToken::new();
    let supervise = tokio::spawn({
        let cancel = cancel.clone();
        async move { supervisor.run(cancel.cancelled()).await }
    });
    let mut d = Daemon::start(|s| s.to_string()).await;
    d.shutdown().await;
    d.addr = addr.clone();

    let c = uuid('c', 4);
    let a = uuid('a', 4);
    let registered = d
        .exec(
            "Customers.Customer.Register",
            &format!("customer-{c}"),
            json!({ "name": "Ada" }),
        )
        .await
        .unwrap();
    let placed_a = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 1, "1.00")] }),
        )
        .await
        .unwrap();
    let placed_at = placed_a.events[0].recorded_at_unix_nanos;
    assert!(
        registered.events[0].recorded_at_unix_nanos < placed_at,
        "a round trip lies between the two"
    );
    settle(&d, &[format!("shipment-{a}")]).await;
    let archive = d
        .admin()
        .await
        .backup_log(BackupLogRequest {
            path: String::new(),
            incremental: false,
        })
        .await
        .unwrap()
        .into_inner();

    // A position and a time together: refused.
    let err = d
        .admin()
        .await
        .restore_log(RestoreLogRequest {
            path: archive.path.clone(),
            to: Some(1),
            at_unix_nanos: Some(placed_at),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    d.admin()
        .await
        .restore_log(RestoreLogRequest {
            path: archive.path.clone(),
            to: None,
            at_unix_nanos: Some(placed_at - 1),
        })
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let h = loop {
        if let Some(h) = health(&addr).await
            && h.last_restore.starts_with("ok ")
        {
            break h;
        }
        if supervise.is_finished() {
            let outcome = supervise.await;
            panic!("the supervisor stopped during the restore: {outcome:?}");
        }
        assert!(Instant::now() < deadline, "the daemon did not come back");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let expect = placed_a.first_position;
    assert!(
        h.last_restore.ends_with(&format!(" to {expect}")),
        "the resolved position is reported: {}",
        h.last_restore
    );
    assert_eq!(h.head, expect);
    assert!(!d.aggregate(&format!("order-{a}")).await.unwrap().found);
    assert!(!d.aggregate(&format!("shipment-{a}")).await.unwrap().found);
    assert!(d.aggregate(&format!("customer-{c}")).await.unwrap().found);

    // And a new order on the restored log works, from that position on.
    let placed_b = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{}", uuid('b', 4)),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 2), 1, "2.00")] }),
        )
        .await
        .unwrap();
    assert_eq!(placed_b.first_position, expect);

    cancel.cancel();
    supervise.await.unwrap().unwrap();
}
