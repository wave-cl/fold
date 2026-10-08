//! Restoring a backup into a running daemon: the process keeps its address,
//! the log underneath is swapped, the previous log is kept aside.

use std::time::{Duration, Instant};

use fold_proto::v1::admin_client::AdminClient;
use fold_proto::v1::{BackupLogRequest, HealthRequest, HealthResponse, RestoreLogRequest};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use tonic::transport::Channel;

use crate::common::{Daemon, copy_orders_guest, line, settle, uuid, workspace};

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
    let schema_src =
        std::fs::read_to_string(workspace().join("examples/orders/schema.fold")).unwrap();
    std::fs::write(dir.path().join("schema.fold"), &schema_src).unwrap();
    copy_orders_guest(&dir.path().join("orders.wasm"));
    let mut opts = foldd::Options::new(
        dir.path().join("data"),
        dir.path().join("schema.fold"),
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
        .restore_log(RestoreLogRequest { path: archive.path })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("not supervised"), "{err}");
    d.shutdown().await;
}
