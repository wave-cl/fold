//! A whole-log backup taken online, restored offline, started as a daemon:
//! events, read models, process instances and checkpoints all carry over.

use fold_proto::v1::{
    BackupLogRequest, ListBackupsRequest, ListProjectionsRequest, RestoreLogRequest,
};
use serde_json::json;
use tonic::Code;

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
        .admin()
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
                incremental: false,
                full_every: 0,
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

/// An increment holds only the records since the newest backup; a full
/// backup restored offline plus the increment applied onto it is the whole
/// log, and the daemon started on it catches its runners up without
/// re-issuing the process manager's commands.
#[tokio::test]
async fn an_incremental_backup_completes_a_restored_full_one() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 3);
    let a = uuid('a', 3);
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
        json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 2, "7.50")] }),
    )
    .await
    .unwrap();
    let head1 = settle(&d, &[format!("shipment-{a}")]).await;

    // Nothing to increment from yet.
    let err = d
        .admin()
        .await
        .backup_log(BackupLogRequest {
            path: String::new(),
            incremental: true,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("full backup first"), "{err}");

    let full = d
        .admin()
        .await
        .backup_log(BackupLogRequest {
            path: String::new(),
            incremental: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (full.head, full.incremental, full.base_head),
        (head1, false, None)
    );

    // More history: an order the full backup does not know, and the
    // shipment the process issues for it.
    let b = uuid('b', 3);
    let placed_b = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{b}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 2), 1, "1.00")] }),
        )
        .await
        .unwrap();
    let head2 = settle(&d, &[format!("shipment-{a}"), format!("shipment-{b}")]).await;
    assert!(head2 > head1);
    let shipment_b = d.aggregate(&format!("shipment-{b}")).await.unwrap();
    let row = d
        .row(
            "Orders.CustomerOrders",
            "customer_orders",
            json!({ "customer_id": c }),
            placed_b.last_position,
        )
        .await;
    assert_eq!(row["order_count"], 2);

    let inc = d
        .admin()
        .await
        .backup_log(BackupLogRequest {
            path: String::new(),
            incremental: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (inc.head, inc.incremental, inc.base_head),
        (head2, true, Some(head1))
    );
    assert!(inc.bytes < full.bytes, "{} < {}", inc.bytes, full.bytes);
    assert_ne!(inc.path, full.path);
    let listed = d
        .admin()
        .await
        .list_backups(ListBackupsRequest {})
        .await
        .unwrap()
        .into_inner()
        .backups;
    assert_eq!(
        listed
            .iter()
            .map(|b| (b.head, b.incremental))
            .collect::<Vec<_>>(),
        vec![(head2, true), (head1, false)],
        "newest first, kind reported"
    );

    // An increment is not a restore point on its own.
    let err = d
        .admin()
        .await
        .restore_log(RestoreLogRequest {
            path: inc.path.clone(),
            to: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("incremental"), "{err}");

    // Offline: restore the full, apply the increment, start on the result.
    d.shutdown().await;
    let restored_dir = d.data_dir().join("data2");
    let m = fold_core::restore_backup(
        std::path::Path::new(&full.path),
        &restored_dir,
        foldd::LOG_NAME,
    )
    .unwrap();
    assert_eq!(m.head, head1);
    let err = fold_core::restore_backup(
        std::path::Path::new(&inc.path),
        &d.data_dir().join("data3"),
        foldd::LOG_NAME,
    )
    .unwrap_err();
    assert!(err.to_string().contains("--apply"), "{err}");
    let m = fold_core::apply_backup(
        std::path::Path::new(&inc.path),
        &restored_dir,
        foldd::LOG_NAME,
    )
    .unwrap();
    assert_eq!((m.head, m.base_head), (head2, Some(head1)));
    d.restart_on("data2").await;

    // Everything from both archives is there; the runners, left at the
    // full backup's checkpoints, catch up over the increment's events and
    // the process finds its commands already executed: the head stays put.
    assert_eq!(
        d.admin()
            .await
            .health(fold_proto::v1::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .head,
        head2
    );
    assert!(d.aggregate(&format!("order-{b}")).await.unwrap().found);
    let settled = settle(&d, &[format!("shipment-{a}"), format!("shipment-{b}")]).await;
    assert_eq!(settled, head2, "the process re-issued nothing");
    assert_eq!(
        d.aggregate(&format!("shipment-{b}")).await.unwrap().version,
        shipment_b.version
    );
    assert_eq!(
        d.row(
            "Orders.CustomerOrders",
            "customer_orders",
            json!({ "customer_id": c }),
            placed_b.last_position
        )
        .await,
        row,
        "the read model caught up to the same row"
    );

    // And it keeps working.
    let e = uuid('e', 3);
    let placed_e = d
        .exec(
            "Orders.Order.PlaceOrder",
            &format!("order-{e}"),
            json!({ "customer_id": c, "lines": [line(&uuid('1', 3), 1, "3.00")] }),
        )
        .await
        .unwrap();
    let row3 = d
        .row(
            "Orders.CustomerOrders",
            "customer_orders",
            json!({ "customer_id": c }),
            placed_e.last_position,
        )
        .await;
    assert_eq!(row3["order_count"], 3);
    d.shutdown().await;
}

/// An incremental schedule writes a full backup first, increments after,
/// a full again every `full_every`, and prunes a chain only once the full
/// it hangs from is gone.
#[tokio::test]
async fn an_incremental_schedule_chains_and_prunes_by_full() {
    let mut d = Daemon::start_with(
        |s| s.to_string(),
        |o| {
            o.backup = Some(foldd::BackupSchedule {
                every: std::time::Duration::from_millis(200),
                keep: 1,
                incremental: true,
                full_every: 3,
            })
        },
    )
    .await;
    async fn backups(d: &Daemon) -> Vec<fold_proto::v1::BackupInfo> {
        d.admin()
            .await
            .list_backups(ListBackupsRequest {})
            .await
            .unwrap()
            .into_inner()
            .backups
    }
    let sched = d
        .admin()
        .await
        .list_backups(ListBackupsRequest {})
        .await
        .unwrap()
        .into_inner()
        .schedule
        .unwrap();
    assert_eq!((sched.incremental, sched.full_every), (true, 3));

    // Round n: one event, then wait for a backup at the new head.
    let mut shape = Vec::new();
    for n in 1..=4u32 {
        let placed = d
            .exec(
                "Customers.Customer.Register",
                &format!("customer-{}", uuid('c', n + 20)),
                json!({ "name": "x" }),
            )
            .await
            .unwrap();
        let want = placed.last_position + 1;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let list = loop {
            let list = backups(&d).await;
            if list.first().is_some_and(|b| b.head >= want) {
                break list;
            }
            assert!(std::time::Instant::now() < deadline, "no backup at {want}");
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };
        shape.push(
            list.iter()
                .map(|b| (b.incremental, b.base_head))
                .collect::<Vec<_>>(),
        );
    }
    let h: Vec<u64> = (1..=4).collect();
    assert_eq!(
        shape,
        vec![
            vec![(false, None)],
            vec![(true, Some(h[0])), (false, None)],
            vec![(true, Some(h[1])), (true, Some(h[0])), (false, None)],
            // The third backup after a full is a full again; with keep 1 the
            // old full goes, and the increments that chained from it with it.
            vec![(false, None)],
        ]
    );
    assert_eq!(backups(&d).await[0].head, h[3]);
    d.shutdown().await;
}
