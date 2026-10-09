//! Whole-log backups: consistent at the archived head, restorable into a
//! fresh directory, and refused when damaged.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fold_core::{
    Direction, Error, ExpectedVersion, GlobalPosition, Log, OpenOptions, PointInTime, StreamVersion,
};

use crate::common::*;

fn populate(log: &Log) {
    let s = sid("order-1");
    for i in 0..30 {
        log.append(
            &s,
            ExpectedVersion::Any,
            vec![ev("Placed", &format!("n{i}"))],
        )
        .unwrap();
    }
    log.append_idempotent(
        &sid("order-2"),
        ExpectedVersion::Any,
        vec![ev("Placed", "k")],
        b"pm:x",
    )
    .unwrap();
    log.set_schema_source("context C {}").unwrap();
}

#[test]
fn a_backup_restores_to_an_identical_log() {
    let d = tmp();
    let log = create(d.path());
    populate(&log);
    let archive = d.path().join("out/backup.fbak");
    let meta = log.backup_to(&archive).unwrap();
    assert_eq!(meta.head, 31);
    assert_eq!(meta.log_id, log.log_id());
    assert_eq!(meta.schema.as_deref(), Some("context C {}"));
    assert!(meta.files >= 7, "{meta:?}");
    assert_eq!(meta.generation, 0);
    let inspected = fold_core::inspect_backup(&archive).unwrap();
    assert_eq!(inspected.head, 31);
    assert_eq!(
        inspected.files, meta.files,
        "the header carries the entry count"
    );
    assert_eq!(inspected.bytes, meta.bytes);

    let r = tmp();
    let restored_meta = fold_core::restore_backup(&archive, r.path(), "restored").unwrap();
    assert_eq!(restored_meta.files, meta.files);
    let restored = Log::open(r.path(), "restored", OpenOptions::default()).unwrap();
    assert_eq!(restored.head(), GlobalPosition(31));
    assert_eq!(restored.log_id(), log.log_id());
    let a = log.read_all(GlobalPosition(0), 100).unwrap();
    let b = restored.read_all(GlobalPosition(0), 100).unwrap();
    assert_eq!(a.len(), 31);
    assert_eq!(
        a.iter()
            .map(|e| (e.position.0, e.payload.clone()))
            .collect::<Vec<_>>(),
        b.iter()
            .map(|e| (e.position.0, e.payload.clone()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        restored
            .read_stream(&sid("order-1"), StreamVersion(0), Direction::Forward, 100)
            .unwrap()
            .len(),
        30
    );
    assert_eq!(
        restored
            .read_by_type("Orders.Placed", GlobalPosition(0), 100)
            .unwrap()
            .len(),
        31
    );
    assert_eq!(
        restored.idempotency_position(b"pm:x").unwrap(),
        Some(GlobalPosition(30))
    );
    assert_eq!(
        restored.schema_source().unwrap().as_deref(),
        Some("context C {}")
    );
    // A restored log is a log that moved backwards for anyone who derived
    // from the original: the generation says so, the cut is the head.
    assert_eq!(restored.generation().unwrap(), 1);
    assert_eq!(restored.cut().unwrap(), GlobalPosition(31));
    assert!(
        !restored.path().join("snapshots").exists(),
        "derived data is not the log's"
    );
    // The restored log keeps working.
    restored
        .append(
            &sid("order-1"),
            ExpectedVersion::Exact(StreamVersion(29)),
            vec![ev("Placed", "after")],
        )
        .unwrap();
    assert_eq!(restored.head(), GlobalPosition(32));
}

#[test]
fn restore_refuses_an_existing_log_and_a_damaged_archive() {
    let d = tmp();
    let log = create(d.path());
    populate(&log);
    let archive = d.path().join("b.fbak");
    log.backup_to(&archive).unwrap();

    let err = fold_core::restore_backup(&archive, d.path(), NAME).unwrap_err();
    assert!(matches!(err, Error::AlreadyExists { .. }), "{err}");

    let mut bytes = std::fs::read(&archive).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0x40;
    let damaged = d.path().join("damaged.fbak");
    std::fs::write(&damaged, &bytes).unwrap();
    let r = tmp();
    let err = fold_core::restore_backup(&damaged, r.path(), "x").unwrap_err();
    assert!(matches!(err, Error::Corrupt { .. }), "{err}");
    assert!(
        !r.path().join("x").exists(),
        "a failed restore leaves nothing behind"
    );

    let err = fold_core::restore_backup(&d.path().join("nope.fbak"), r.path(), "y").unwrap_err();
    assert!(matches!(err, Error::Io { .. }), "{err}");
    assert!(matches!(
        fold_core::inspect_backup(&d.path().join("LOG-not-an-archive")).unwrap_err(),
        Error::Io { .. }
    ));
}

#[test]
fn a_backup_taken_during_appends_is_consistent_at_its_head() {
    let d = tmp();
    let log = create_with(
        d.path(),
        OpenOptions::default().fsync(fold_core::FsyncPolicy::Never),
    );
    populate(&log);
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let log = log.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let s = sid("busy");
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                log.append(&s, ExpectedVersion::Any, vec![ev("Tick", &format!("{n}"))])
                    .unwrap();
                n += 1;
            }
            n
        })
    };
    let mut metas = Vec::new();
    for i in 0..5 {
        let archive = d.path().join(format!("b{i}.fbak"));
        metas.push((archive.clone(), log.backup_to(&archive).unwrap()));
    }
    stop.store(true, Ordering::Relaxed);
    let written = writer.join().unwrap();
    assert!(written > 0);

    for (i, (archive, meta)) in metas.iter().enumerate() {
        let r = tmp();
        fold_core::restore_backup(archive, r.path(), "r").unwrap();
        let restored = Log::open(r.path(), "r", OpenOptions::default()).unwrap();
        assert_eq!(restored.head(), GlobalPosition(meta.head), "backup {i}");
        // Every archived position reads back and matches the live log.
        let a = log.read_all(GlobalPosition(0), meta.head as usize).unwrap();
        let b = restored
            .read_all(GlobalPosition(0), meta.head as usize)
            .unwrap();
        assert_eq!(a.len() as u64, meta.head);
        assert_eq!(
            a.iter().map(|e| (e.position.0, e.id)).collect::<Vec<_>>(),
            b.iter().map(|e| (e.position.0, e.id)).collect::<Vec<_>>()
        );
        // Anything past the head that a segment carried is gone, and the
        // restored log appends from its head.
        assert!(
            restored
                .read_all(GlobalPosition(meta.head), 10)
                .unwrap()
                .is_empty()
        );
        restored
            .append(&sid("busy"), ExpectedVersion::Any, vec![ev("Tick", "x")])
            .unwrap();
        assert_eq!(restored.head(), GlobalPosition(meta.head + 1));
    }
}

#[test]
fn an_incremental_backup_applies_onto_the_restored_base() {
    let d = tmp();
    let log = create(d.path());
    populate(&log); // head 31
    let full = d.path().join("full.fbak");
    let full_meta = log.backup_to(&full).unwrap();
    assert_eq!(full_meta.kind, fold_core::BackupKind::Full);

    // New events after the base, across a batch and a new stream, plus an
    // idempotent append whose key must travel with the increment.
    let s = sid("order-1");
    log.append(
        &s,
        ExpectedVersion::Any,
        vec![ev("Placed", "n30"), ev("Line", "n31")],
    )
    .unwrap();
    log.append_idempotent(
        &sid("order-3"),
        ExpectedVersion::Any,
        vec![ev("Placed", "k3")],
        b"pm:later",
    )
    .unwrap();
    log.set_schema_source("context C { // v2 }").unwrap();
    let inc = d.path().join("inc.fbak");
    let inc_meta = log.backup_incremental(&inc, GlobalPosition(31)).unwrap();
    assert_eq!(inc_meta.kind, fold_core::BackupKind::Incremental);
    assert_eq!(inc_meta.base_head, Some(31));
    assert_eq!(inc_meta.head, 34);
    assert!(
        inc_meta.bytes < full_meta.bytes,
        "an increment is smaller than the base"
    );
    assert_eq!(
        fold_core::inspect_backup(&inc).unwrap().kind,
        fold_core::BackupKind::Incremental
    );

    // A full restore refuses an incremental archive, and vice versa.
    let r = tmp();
    assert!(matches!(
        fold_core::restore_backup(&inc, r.path(), "x"),
        Err(Error::Corrupt { .. })
    ));
    assert!(!r.path().join("x").exists());
    fold_core::restore_backup(&full, r.path(), "x").unwrap();
    assert!(matches!(
        fold_core::apply_backup(&full, r.path(), "x"),
        Err(Error::Corrupt { .. })
    ));

    // Apply the increment: identical log.
    let applied = fold_core::apply_backup(&inc, r.path(), "x").unwrap();
    assert_eq!(applied.head, 34);
    let restored = Log::open(r.path(), "x", OpenOptions::default()).unwrap();
    assert_eq!(restored.head(), GlobalPosition(34));
    let a = log.read_all(GlobalPosition(0), 100).unwrap();
    let b = restored.read_all(GlobalPosition(0), 100).unwrap();
    assert_eq!(
        a.iter()
            .map(|e| (
                e.position.0,
                e.id,
                e.stream_version.0,
                e.recorded_at,
                e.flags
            ))
            .collect::<Vec<_>>(),
        b.iter()
            .map(|e| (
                e.position.0,
                e.id,
                e.stream_version.0,
                e.recorded_at,
                e.flags
            ))
            .collect::<Vec<_>>(),
        "ids, versions, timestamps and batch flags survive"
    );
    assert_eq!(
        restored.stream_head(&sid("order-3")).unwrap(),
        Some(StreamVersion(0))
    );
    assert_eq!(
        restored
            .read_by_type("Orders.Line", GlobalPosition(0), 100)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        restored.idempotency_position(b"pm:later").unwrap(),
        Some(GlobalPosition(33))
    );
    assert_eq!(
        restored.idempotency_position(b"pm:x").unwrap(),
        Some(GlobalPosition(30)),
        "from the base"
    );
    assert_eq!(
        restored.schema_source().unwrap().as_deref(),
        Some("context C { // v2 }")
    );
    drop(restored);

    // Applying it twice, or onto a log at another head, is refused.
    let err = fold_core::apply_backup(&inc, r.path(), "x").unwrap_err();
    assert!(err.to_string().contains("head 31"), "{err}");
    let other = tmp();
    let other_log = create(other.path());
    for i in 0..31 {
        other_log
            .append(
                &sid("z"),
                ExpectedVersion::Any,
                vec![ev("Placed", &format!("{i}"))],
            )
            .unwrap();
    }
    drop(other_log);
    let err = fold_core::apply_backup(&inc, other.path(), NAME).unwrap_err();
    assert!(
        err.to_string().contains("is log"),
        "a different log id: {err}"
    );

    // A damaged increment is refused before the log is touched.
    let mut bytes = std::fs::read(&inc).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0x10;
    let damaged = d.path().join("inc-damaged.fbak");
    std::fs::write(&damaged, &bytes).unwrap();
    let r2 = tmp();
    fold_core::restore_backup(&full, r2.path(), "y").unwrap();
    let err = fold_core::apply_backup(&damaged, r2.path(), "y").unwrap_err();
    assert!(matches!(err, Error::Corrupt { .. }), "{err}");
    assert_eq!(
        Log::open(r2.path(), "y", OpenOptions::default())
            .unwrap()
            .head(),
        GlobalPosition(31)
    );
}

#[test]
fn increments_chain_across_a_segment_roll() {
    let d = tmp();
    let log = create_with(
        d.path(),
        OpenOptions::default()
            .segment_max_bytes(1024)
            .fsync(fold_core::FsyncPolicy::Never),
    );
    let s = sid("roll");
    for i in 0..20 {
        log.append(&s, ExpectedVersion::Any, vec![ev("Tick", &format!("{i}"))])
            .unwrap();
    }
    let full = d.path().join("full.fbak");
    log.backup_to(&full).unwrap();
    let mut increments = Vec::new();
    let mut since = 20;
    for round in 0..3 {
        for i in 0..25 {
            log.append(
                &s,
                ExpectedVersion::Any,
                vec![ev("Tick", &format!("r{round}-{i}"))],
            )
            .unwrap();
        }
        let head = log.head().0;
        let path = d.path().join(format!("inc{round}.fbak"));
        log.backup_incremental(&path, GlobalPosition(since))
            .unwrap();
        increments.push(path);
        since = head;
    }
    let r = tmp();
    fold_core::restore_backup(&full, r.path(), "c").unwrap();
    for inc in &increments {
        fold_core::apply_backup(inc, r.path(), "c").unwrap();
    }
    let restored = Log::open(
        r.path(),
        "c",
        OpenOptions::default().segment_max_bytes(1024),
    )
    .unwrap();
    assert_eq!(restored.head(), log.head());
    let a: Vec<_> = log
        .read_all(GlobalPosition(0), 1000)
        .unwrap()
        .iter()
        .map(|e| e.id)
        .collect();
    let b: Vec<_> = restored
        .read_all(GlobalPosition(0), 1000)
        .unwrap()
        .iter()
        .map(|e| e.id)
        .collect();
    assert_eq!(a, b);
    assert!(
        std::fs::read_dir(restored.path().join("segments"))
            .unwrap()
            .count()
            > 1,
        "the restored log rolled segments too"
    );
}

/// A restore may stop at a point in time; so may an increment, within its
/// own range. Either way the derived state past the cut is gone.
#[test]
fn a_restore_and_an_apply_can_stop_at_a_point_in_time() {
    let d = tmp();
    let log = create(d.path());
    populate(&log);
    let full = d.path().join("out/full.fbak");
    log.backup_to(&full).unwrap();

    // Past the archive's head: refused, and no directory left behind.
    let dir = d.path().join("pit");
    let err = fold_core::restore_backup_to(
        &full,
        &dir,
        NAME,
        Some(PointInTime::Position(GlobalPosition(32))),
    )
    .unwrap_err();
    assert!(matches!(err, Error::PositionOutOfRange { .. }), "{err}");
    assert!(!dir.exists());

    let meta = fold_core::restore_backup_to(
        &full,
        &dir,
        NAME,
        Some(PointInTime::Position(GlobalPosition(20))),
    )
    .unwrap();
    assert_eq!(meta.head, 20);
    let restored = open(&dir);
    assert_eq!(restored.head(), GlobalPosition(20));
    assert_eq!(
        restored.read_all(GlobalPosition(0), 100).unwrap(),
        log.read_all(GlobalPosition(0), 20).unwrap()
    );
    assert_eq!(restored.idempotency_position(b"pm:x").unwrap(), None);
    assert_eq!(
        restored.schema_source().unwrap().as_deref(),
        Some("context C {}")
    );
    assert_eq!(restored.cut().unwrap(), GlobalPosition(20));
    assert_eq!(restored.generation().unwrap(), 2, "restored, then cut");
    drop(restored);

    // An increment 31..34, applied onto a plain restore but cut at 32.
    let dir2 = d.path().join("pit2");
    fold_core::restore_backup(&full, &dir2, NAME).unwrap();
    for i in 0..3 {
        log.append(
            &sid("order-9"),
            ExpectedVersion::Any,
            vec![ev("Placed", &format!("z{i}"))],
        )
        .unwrap();
    }
    let inc = d.path().join("out/inc.fbak");
    log.backup_incremental(&inc, GlobalPosition(31)).unwrap();
    for outside in [30u64, 35] {
        let err = fold_core::apply_backup_to(
            &inc,
            &dir2,
            NAME,
            Some(PointInTime::Position(GlobalPosition(outside))),
        )
        .unwrap_err();
        assert!(err.to_string().contains("outside this increment"), "{err}");
    }
    assert_eq!(
        open(&dir2).head(),
        GlobalPosition(31),
        "a refused cut applied nothing"
    );
    let meta = fold_core::apply_backup_to(
        &inc,
        &dir2,
        NAME,
        Some(PointInTime::Position(GlobalPosition(32))),
    )
    .unwrap();
    assert_eq!(meta.head, 32);
    let applied = open(&dir2);
    assert_eq!(applied.head(), GlobalPosition(32));
    assert_eq!(
        applied.read_all(GlobalPosition(0), 100).unwrap(),
        log.read_all(GlobalPosition(0), 32).unwrap()
    );
    assert_eq!(applied.cut().unwrap(), GlobalPosition(32));
}

#[test]
fn a_format_1_archive_restores_the_log_and_skips_its_derived_tables() {
    // Written by the fold that kept read models, aggregate snapshots and
    // snapshot files in the log: three order-1 events, one order-2 event
    // under key `pm:x`, a checkpoint, an aggregate snapshot, a stand-in
    // snapshot file and a schema.
    let archive =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format1.fbak");
    let meta = fold_core::inspect_backup(&archive).unwrap();
    assert_eq!(meta.format, 1);
    assert_eq!(meta.head, 4);
    assert_eq!(meta.generation, 0, "absent in the header: default");
    let r = tmp();
    fold_core::restore_backup(&archive, r.path(), "legacy").unwrap();
    let log = Log::open(r.path(), "legacy", OpenOptions::default()).unwrap();
    assert_eq!(log.head(), GlobalPosition(4));
    assert_eq!(log.log_id(), meta.log_id);
    assert_eq!(
        log.read_stream(&sid("order-1"), StreamVersion(0), Direction::Forward, 10)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        log.idempotency_position(b"pm:x").unwrap(),
        Some(GlobalPosition(3))
    );
    assert_eq!(
        log.schema_source().unwrap().as_deref(),
        Some("context C {}")
    );
    assert_eq!(log.generation().unwrap(), 1);
    assert_eq!(log.cut().unwrap(), GlobalPosition(4));
    // The derived tables were skipped, the snapshot file is restored as the
    // plain file it was archived as (nothing reads it), and the log works.
    assert!(log.path().join("snapshots/C.P/x.fsnap").is_file());
    log.append(
        &sid("order-1"),
        ExpectedVersion::Exact(StreamVersion(2)),
        vec![ev("Placed", "after")],
    )
    .unwrap();
    assert_eq!(log.head(), GlobalPosition(5));
}

/// A time restores to the last batch recorded at or before it.
#[test]
fn a_restore_can_stop_at_a_time() {
    let d = tmp();
    let log = create(d.path());
    populate(&log);
    let full = d.path().join("out/full.fbak");
    log.backup_to(&full).unwrap();
    let all = log.read_all(GlobalPosition(0), 100).unwrap();
    let at = all[19].recorded_at;
    // The oracle: the first position recorded after `at` (one batch per
    // append here, so no walking back).
    let expect = all
        .iter()
        .position(|e| e.recorded_at > at)
        .map(|p| p as u64)
        .unwrap_or(all.len() as u64);
    assert!(expect >= 20);
    let dir = d.path().join("at");
    let meta =
        fold_core::restore_backup_to(&full, &dir, NAME, Some(PointInTime::Time(at))).unwrap();
    assert_eq!(meta.head, expect);
    let restored = open(&dir);
    assert_eq!(restored.head(), GlobalPosition(expect));
    assert_eq!(
        restored.read_all(GlobalPosition(0), 100).unwrap(),
        all[..expect as usize].to_vec()
    );
    // Before the first event: nothing; after the last: everything.
    let dir0 = d.path().join("at0");
    let meta = fold_core::restore_backup_to(
        &full,
        &dir0,
        NAME,
        Some(PointInTime::Time(all[0].recorded_at - 1)),
    )
    .unwrap();
    assert_eq!(meta.head, 0);
    let dir_all = d.path().join("at_all");
    let meta = fold_core::restore_backup_to(
        &full,
        &dir_all,
        NAME,
        Some(PointInTime::Time(all[30].recorded_at)),
    )
    .unwrap();
    assert_eq!(meta.head, 31);
}
