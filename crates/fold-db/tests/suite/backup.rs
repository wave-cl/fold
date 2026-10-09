//! Backups of the log: taken online, listed, scheduled, incremental, and
//! restored offline into a database that carries on.

use fold_proto::database::v1::{BackupLogRequest, ListBackupsRequest, RestoreLogRequest};
use tonic::Code;

use crate::common::{DbNode, place, register, uuid};

#[tokio::test]
async fn a_backup_restores_a_working_database() {
    let mut d = DbNode::start().await;
    let c = uuid('c', 1);
    register(&d, &c).await;
    place(&d, &c, &uuid('a', 1), None).await.unwrap();
    let head = d.head().await;
    let events = d.all_events().await;

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
        fold_db::LOG_NAME,
    )
    .unwrap();
    d.restart_on("data2").await;
    let h = d.health().await;
    assert_eq!(h.head, head, "every event is back");
    assert_eq!(d.all_events().await, events);
    // Restored logs start a new generation: derived data past the cut (none
    // here) is a derivation node's to drop.
    assert!(h.generation >= 1, "{h:?}");
    // It keeps going.
    let placed = place(&d, &c, &uuid('b', 1), None).await.unwrap();
    assert_eq!(placed.first_position, head);
    d.shutdown().await;
}

/// A schedule backs up when the head moved, skips when it did not, keeps
/// only the newest `keep`, and reports itself.
#[tokio::test]
async fn scheduled_backups_run_skip_and_prune() {
    let mut d = DbNode::start_with(
        |s| s.to_string(),
        |o| {
            o.backup = Some(fold_db::BackupSchedule {
                every: std::time::Duration::from_millis(200),
                keep: 2,
                incremental: false,
                full_every: 0,
            })
        },
    )
    .await;

    async fn backups(d: &DbNode) -> fold_proto::database::v1::ListBackupsResponse {
        d.backup()
            .await
            .list_backups(ListBackupsRequest {})
            .await
            .unwrap()
            .into_inner()
    }
    async fn wait_for(
        d: &DbNode,
        want: impl Fn(&fold_proto::database::v1::ListBackupsResponse) -> bool,
        what: &str,
    ) -> fold_proto::database::v1::ListBackupsResponse {
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
    assert_eq!((sched.every_secs, sched.keep), (0, 2));
    assert!(sched.next_run_unix_nanos.is_some());
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
    assert!(backups(&d).await.backups.is_empty(), "nothing at head 0");

    for n in 1..=3u32 {
        let placed = register(&d, &uuid('c', n + 10)).await;
        let want_head = placed.last_position + 1;
        wait_for(
            &d,
            |r| r.backups.first().is_some_and(|b| b.head >= want_head),
            &format!("a backup at head {want_head}"),
        )
        .await;
    }
    let r = backups(&d).await;
    assert_eq!(r.backups.len(), 2, "keep 2: {:?}", r.backups);
    assert!(r.backups[0].head > r.backups[1].head, "newest first");
    assert_eq!(
        r.schedule.as_ref().unwrap().last_head,
        Some(r.backups[0].head)
    );
    assert!(r.schedule.as_ref().unwrap().last_error.is_empty());

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
    assert_eq!(
        backups(&d).await.backups,
        before,
        "an unchanged head writes no backup"
    );
    d.shutdown().await;
}

#[tokio::test]
async fn an_incremental_backup_completes_a_restored_full_one() {
    let mut d = DbNode::start().await;
    let c = uuid('c', 3);
    register(&d, &c).await;
    place(&d, &c, &uuid('a', 3), None).await.unwrap();
    let head1 = d.head().await;

    let err = d
        .backup()
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
        .backup()
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

    place(&d, &c, &uuid('b', 3), None).await.unwrap();
    let head2 = d.head().await;
    let events = d.all_events().await;
    let inc = d
        .backup()
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
    let listed = d
        .backup()
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
        vec![(head2, true), (head1, false)]
    );

    // An increment is not a restore point on its own.
    let err = d
        .backup()
        .await
        .restore_log(RestoreLogRequest {
            path: inc.path.clone(),
            to: None,
            at_unix_nanos: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("incremental"), "{err}");
    // An unsupervised database refuses an online restore.
    let err = d
        .backup()
        .await
        .restore_log(RestoreLogRequest {
            path: full.path.clone(),
            to: None,
            at_unix_nanos: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("not supervised"), "{err}");

    // Offline: restore the full, apply the increment, start on the result.
    d.shutdown().await;
    let restored_dir = d.data_dir().join("data2");
    let m = fold_core::restore_backup(
        std::path::Path::new(&full.path),
        &restored_dir,
        fold_db::LOG_NAME,
    )
    .unwrap();
    assert_eq!(m.head, head1);
    let m = fold_core::apply_backup(
        std::path::Path::new(&inc.path),
        &restored_dir,
        fold_db::LOG_NAME,
    )
    .unwrap();
    assert_eq!((m.head, m.base_head), (head2, Some(head1)));
    d.restart_on("data2").await;
    assert_eq!(d.head().await, head2);
    assert_eq!(d.all_events().await, events);
    d.shutdown().await;
}

#[tokio::test]
async fn an_incremental_schedule_chains_and_prunes_by_full() {
    let mut d = DbNode::start_with(
        |s| s.to_string(),
        |o| {
            o.backup = Some(fold_db::BackupSchedule {
                every: std::time::Duration::from_millis(200),
                keep: 1,
                incremental: true,
                full_every: 3,
            })
        },
    )
    .await;
    let mut shape = Vec::new();
    for n in 1..=4u32 {
        let placed = register(&d, &uuid('c', n + 20)).await;
        let want = placed.last_position + 1;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let list = loop {
            let r = d
                .backup()
                .await
                .list_backups(ListBackupsRequest {})
                .await
                .unwrap()
                .into_inner();
            if r.schedule
                .as_ref()
                .is_some_and(|s| s.last_head.is_some_and(|h| h >= want))
            {
                break r.backups;
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
            vec![(false, None)],
        ]
    );
    d.shutdown().await;
}
