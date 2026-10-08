//! A whole-log backup taken online, restored offline, started as a daemon:
//! events, read models, process instances and checkpoints all carry over.

use fold_proto::v1::{BackupLogRequest, ListBackupsRequest, ListProjectionsRequest};
use serde_json::json;

use crate::common::{Daemon, line, state_of, uuid};

#[tokio::test]
async fn a_backup_restores_a_working_daemon() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 1);
    let a = uuid('a', 1);
    d.exec(
        "Customers.Customer.Register",
        &format!("customer-{c}"),
        json!({ "name": "Ada" }),
    )
    .await
    .unwrap();
    let placed = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{a}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 2, "7.50")] }),
        )
        .await
        .unwrap();
    // Let the projection and the process settle so the backup holds them.
    let row = d
        .row(
            "Orders.CustomerOrders",
            "customer_orders",
            json!({ "customer_id": c }),
            placed.last_position,
        )
        .await;
    // Settle: the Fulfilment process appends a shipment event of its own, so
    // re-read the head each pass until every runner has applied up to it.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let head = loop {
        let head = d
            .admin()
            .await
            .health(fold_proto::v1::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .head;
        let ok = d
            .projections()
            .await
            .iter()
            .all(|p| p.checkpoint.is_some_and(|cp| cp + 1 >= head))
            && d.admin()
                .await
                .list_processes(fold_proto::v1::ListProcessesRequest {})
                .await
                .unwrap()
                .into_inner()
                .processes
                .iter()
                .all(|p| p.checkpoint.is_some_and(|cp| cp + 1 >= head) && p.pending_commands == 0)
            && d.aggregate(&format!("shipment-{a}")).await.unwrap().found;
        let head_after = d
            .admin()
            .await
            .health(fold_proto::v1::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .head;
        if ok && head_after == head {
            break head;
        }
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    let order_state = state_of(&d.aggregate(&format!("order-{a}")).await.unwrap());
    let shipment = d.aggregate(&format!("shipment-{a}")).await.unwrap();
    assert!(shipment.found);

    // Back up online.
    let info = d
        .admin()
        .await
        .backup_log(BackupLogRequest {
            path: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.head, head);
    assert!(std::path::Path::new(&info.path).is_file());
    let listed = d
        .admin()
        .await
        .list_backups(ListBackupsRequest {})
        .await
        .unwrap()
        .into_inner()
        .backups;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].path, info.path);

    // Restore offline into another data dir, as the CLI does, and start on it.
    d.shutdown().await;
    let restored_dir = d.data_dir().join("data2");
    fold_core::restore_backup(
        std::path::Path::new(&info.path),
        &restored_dir,
        foldd::LOG_NAME,
    )
    .unwrap();
    d.restart_on("data2").await;

    let h = d
        .admin()
        .await
        .health(fold_proto::v1::HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(h.head, head, "every event is back");
    assert_eq!(
        d.row(
            "Orders.CustomerOrders",
            "customer_orders",
            json!({ "customer_id": c }),
            placed.last_position
        )
        .await,
        row,
        "read models came with the backup"
    );
    let projections = d
        .admin()
        .await
        .list_projections(ListProjectionsRequest {})
        .await
        .unwrap()
        .into_inner()
        .projections;
    assert!(
        projections
            .iter()
            .all(|p| p.checkpoint.is_some_and(|cp| cp + 1 >= head))
    );
    assert_eq!(
        state_of(&d.aggregate(&format!("order-{a}")).await.unwrap()),
        order_state
    );
    assert_eq!(
        d.aggregate(&format!("shipment-{a}")).await.unwrap().version,
        shipment.version
    );

    // The restored daemon keeps going: a command works and the process
    // reacts without re-issuing anything (the idempotency keys came along).
    let b = uuid('b', 1);
    let placed_b = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{b}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 2), 1, "1.00")] }),
        )
        .await
        .unwrap();
    let row2 = d
        .row(
            "Orders.CustomerOrders",
            "customer_orders",
            json!({ "customer_id": c }),
            placed_b.last_position,
        )
        .await;
    assert_eq!(row2["order_count"], 2);
    d.shutdown().await;
}
