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

/// A schedule backs up when the head moved, skips when it did not, keeps
/// only the newest `keep`, and reports itself.
#[tokio::test]
async fn scheduled_backups_run_skip_and_prune() {
    let mut d = Daemon::start_with(
        |s| s.to_string(),
        |o| {
            o.backup = Some(foldd::BackupSchedule {
                every: std::time::Duration::from_millis(200),
                keep: 2,
            })
        },
    )
    .await;
    let c = uuid('c', 2);

    async fn backups(d: &Daemon) -> fold_proto::v1::ListBackupsResponse {
        d.admin()
            .await
            .list_backups(ListBackupsRequest {})
            .await
            .unwrap()
            .into_inner()
    }
    async fn wait_for(
        d: &Daemon,
        want: impl Fn(&fold_proto::v1::ListBackupsResponse) -> bool,
        what: &str,
    ) -> fold_proto::v1::ListBackupsResponse {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let r = backups(d).await;
            if want(&r) {
                return r;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {what}: {r:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    let r = backups(&d).await;
    let sched = r.schedule.expect("a schedule is reported");
    assert_eq!(
        (sched.every_secs, sched.keep),
        (0, 2),
        "200 ms rounds down to 0 s; keep 2"
    );
    assert!(sched.next_run_unix_nanos.is_some());

    // Head 0: the first tick finds nothing to back up and writes nothing,
    // but it does run.
    wait_for(
        &d,
        |r| {
            r.schedule
                .as_ref()
                .is_some_and(|s| s.last_run_unix_nanos.is_some())
        },
        "first tick",
    )
    .await;
    assert!(
        backups(&d).await.backups.is_empty(),
        "nothing to back up at head 0... "
    );

    // One event per round, three rounds: three backups wanted, two kept.
    let mut heads = Vec::new();
    for n in 1..=3u32 {
        let placed = d
            .exec(
                "Customers.Customer.Register",
                &format!("customer-{}", uuid('c', n + 10)),
                json!({ "name": "x" }),
            )
            .await
            .unwrap();
        let want_head = placed.last_position + 1;
        let r = wait_for(
            &d,
            |r| r.backups.first().is_some_and(|b| b.head >= want_head),
            &format!("a backup at head {want_head}"),
        )
        .await;
        heads.push(r.backups[0].head);
        let _ = c.clone();
    }
    let r = backups(&d).await;
    assert_eq!(r.backups.len(), 2, "keep 2: {:?}", r.backups);
    assert!(r.backups[0].head > r.backups[1].head, "newest first");
    assert_eq!(
        r.schedule.as_ref().unwrap().last_head,
        Some(r.backups[0].head)
    );
    assert!(r.schedule.as_ref().unwrap().last_error.is_empty());

    // No new events: several more ticks write nothing new.
    let before = r.backups.clone();
    let last_run = r.schedule.as_ref().unwrap().last_run_unix_nanos.unwrap();
    wait_for(
        &d,
        |r| {
            r.schedule
                .as_ref()
                .is_some_and(|s| s.last_run_unix_nanos.unwrap_or(0) > last_run + 500_000_000)
        },
        "three more ticks",
    )
    .await;
    let after = backups(&d).await;
    assert_eq!(after.backups, before, "an unchanged head writes no backup");
    d.shutdown().await;
}
