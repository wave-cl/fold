//! A whole-log backup taken online, restored offline, started as a
//! composite: the events are back, and the derivation and application
//! nodes rebuild what they derive from them without re-issuing commands.
//! The database's own backup tests are in fold-db.

use fold_proto::database::v1::{BackupLogRequest, ListBackupsRequest};
use serde_json::json;

use crate::common::{Daemon, line, settle, state_of, uuid};

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
    let head = settle(&d, &[format!("shipment-{a}")]).await;
    let order_state = state_of(&d.aggregate(&format!("order-{a}")).await.unwrap());
    let shipment = d.aggregate(&format!("shipment-{a}")).await.unwrap();
    assert!(shipment.found);

    // Back up online.
    let info = d
        .backup()
        .await
        .backup_log(BackupLogRequest {
            path: String::new(),
            incremental: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.head, head);
    assert!(std::path::Path::new(&info.path).is_file());
    let listed = d
        .backup()
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

    let h = d.health().await;
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
        "read models are rebuilt from the restored log"
    );
    let projections = d.projections().await;
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
