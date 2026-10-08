//! Point-in-time truncation: the events from a position on go, and with
//! them every derived fact that looked past it; the log then continues.

use bytes::Bytes;
use fold_core::{
    Direction, Error, EventType, ExpectedVersion, GlobalPosition, Log, NewEvent, OpenOptions,
    Snapshot, StreamVersion, truncate_log,
};

use crate::common::*;

fn snap(version: u64) -> Snapshot {
    Snapshot {
        version: StreamVersion(version),
        module_hash: [7u8; 32],
        state: b"{}".to_vec(),
    }
}

/// Positions 0..9: `order-1` one event per batch; 10..12: `order-2` one
/// batch of three; 13: `order-3` under key `pm:a`; 14: `order-1` under key
/// `pm:b`. Head 15. Read models `C.P` at 10 and `C.Q` at 15, snapshots of
/// every stream at its head, snapshot files named by their checkpoint.
fn populate(log: &Log) {
    for i in 0..10 {
        log.append(
            &sid("order-1"),
            ExpectedVersion::Any,
            vec![ev("Placed", &format!("n{i}"))],
        )
        .unwrap();
    }
    log.append(
        &sid("order-2"),
        ExpectedVersion::NoStream,
        vec![ev("Placed", "a"), ev("Shipped", "b"), ev("Placed", "c")],
    )
    .unwrap();
    log.append_idempotent(
        &sid("order-3"),
        ExpectedVersion::NoStream,
        vec![ev("Shipped", "d")],
        b"pm:a",
    )
    .unwrap();
    log.append_idempotent(
        &sid("order-1"),
        ExpectedVersion::Exact(StreamVersion(9)),
        vec![ev("Shipped", "e")],
        b"pm:b",
    )
    .unwrap();
    assert_eq!(log.head(), GlobalPosition(15));
    let rm = log.read_models();
    rm.commit(
        "C.P",
        GlobalPosition(10),
        vec![("t".into(), b"k1".to_vec(), b"{}".to_vec())],
        vec![],
    )
    .unwrap();
    rm.commit(
        "C.Q",
        GlobalPosition(15),
        vec![("t".into(), b"k2".to_vec(), b"{}".to_vec())],
        vec![],
    )
    .unwrap();
    let snaps = log.snapshots();
    snaps.put("C.A", &sid("order-1"), snap(10)).unwrap();
    snaps.put("C.A", &sid("order-2"), snap(2)).unwrap();
    snaps.put("C.A", &sid("order-3"), snap(0)).unwrap();
    for (name, at) in [("C.P", 10u64), ("C.Q", 15)] {
        let dir = log.path().join("snapshots").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("{at:020}.fsnap")), b"stand-in").unwrap();
        std::fs::write(dir.join(format!("{at:020}.fsnap.tmp")), b"junk").unwrap();
    }
}

#[test]
fn a_cut_at_a_boundary_drops_events_and_every_fact_derived_past_it() {
    let d = tmp();
    let log = create(d.path());
    populate(&log);
    let before = log.read_all(GlobalPosition(0), 100).unwrap();
    drop(log);

    let report = truncate_log(d.path(), NAME, GlobalPosition(13)).unwrap();
    assert_eq!(
        report,
        fold_core::Truncated {
            from: 15,
            to: 13,
            streams_cut: 1,     // order-1 loses version 10
            streams_removed: 1, // order-3 was entirely past the cut
            idempotency_keys_dropped: 2,
            checkpoints_reset: vec!["C.Q".into()],
            aggregate_snapshots_dropped: 2, // order-1 at 10 > 9, order-3
            snapshot_files_dropped: 1,
            segments_removed: 0,
        }
    );

    let log = open(d.path());
    assert_eq!(log.head(), GlobalPosition(13));
    let after = log.read_all(GlobalPosition(0), 100).unwrap();
    assert_eq!(after.len(), 13);
    assert_eq!(
        after.iter().map(|e| e.id).collect::<Vec<_>>(),
        before[..13].iter().map(|e| e.id).collect::<Vec<_>>(),
        "the kept events are the same records"
    );
    assert_eq!(
        log.stream_head(&sid("order-1")).unwrap(),
        Some(StreamVersion(9))
    );
    assert_eq!(
        log.stream_head(&sid("order-2")).unwrap(),
        Some(StreamVersion(2))
    );
    assert_eq!(log.stream_head(&sid("order-3")).unwrap(), None);
    assert_eq!(
        log.read_stream(&sid("order-1"), StreamVersion(0), Direction::Forward, 100)
            .unwrap()
            .len(),
        10
    );
    assert_eq!(
        log.read_by_type("Orders.Shipped", GlobalPosition(0), 100)
            .unwrap()
            .iter()
            .map(|e| e.position.0)
            .collect::<Vec<_>>(),
        vec![11],
        "the type index forgets the cut events"
    );
    assert_eq!(log.idempotency_position(b"pm:a").unwrap(), None);
    assert_eq!(log.idempotency_position(b"pm:b").unwrap(), None);
    let rm = log.read_models();
    assert_eq!(rm.checkpoint("C.P").unwrap(), Some(GlobalPosition(10)));
    assert!(
        rm.snapshot()
            .unwrap()
            .get("C.P", "t", b"k1")
            .unwrap()
            .is_some()
    );
    assert_eq!(
        rm.checkpoint("C.Q").unwrap(),
        None,
        "it looked past the cut"
    );
    assert!(
        rm.snapshot()
            .unwrap()
            .get("C.Q", "t", b"k2")
            .unwrap()
            .is_none()
    );
    let snaps = log.snapshots();
    assert_eq!(snaps.get("C.A", &sid("order-1")).unwrap(), None);
    assert_eq!(snaps.get("C.A", &sid("order-2")).unwrap(), Some(snap(2)));
    assert_eq!(snaps.get("C.A", &sid("order-3")).unwrap(), None);
    let files = |name: &str| -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(log.path().join("snapshots").join(name))
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    };
    assert_eq!(
        files("C.P"),
        vec![
            "00000000000000000010.fsnap",
            "00000000000000000010.fsnap.tmp"
        ]
    );
    assert_eq!(files("C.Q"), vec!["00000000000000000015.fsnap.tmp"]);

    // The log continues from the cut: versions and positions are dense.
    let r = log
        .append(
            &sid("order-1"),
            ExpectedVersion::Exact(StreamVersion(9)),
            vec![ev("Placed", "again")],
        )
        .unwrap();
    assert_eq!(
        (r.first, r.stream_version),
        (GlobalPosition(13), StreamVersion(10))
    );
    let r = log
        .append_idempotent(
            &sid("order-3"),
            ExpectedVersion::NoStream,
            vec![ev("Shipped", "again")],
            b"pm:a",
        )
        .unwrap();
    assert_eq!(
        r.first,
        GlobalPosition(14),
        "a dropped key may be used again"
    );
    // The store handles share the log's lock; let go of all of them.
    drop((rm, snaps, log));
    let log = open(d.path());
    assert_eq!(log.head(), GlobalPosition(15));
    assert_eq!(log.read_all(GlobalPosition(0), 100).unwrap().len(), 15);
}

#[test]
fn a_cut_refuses_the_inside_of_a_batch_a_position_past_the_head_and_an_open_log() {
    let d = tmp();
    let log = create(d.path());
    populate(&log);
    let before = log.read_all(GlobalPosition(0), 100).unwrap();

    let err = truncate_log(d.path(), NAME, GlobalPosition(13)).unwrap_err();
    assert!(matches!(err, Error::Locked { .. }), "{err}");
    drop(log);

    for inside in [11u64, 12] {
        let err = truncate_log(d.path(), NAME, GlobalPosition(inside)).unwrap_err();
        match err {
            Error::InsideBatch {
                position,
                batch_start,
                batch_end,
            } => assert_eq!(
                (position.0, batch_start.0, batch_end.0),
                (inside, 10, 13),
                "{position}"
            ),
            other => panic!("{other}"),
        }
        assert!(err.to_string().contains("use 10 or 13"), "{err}");
    }
    let err = truncate_log(d.path(), NAME, GlobalPosition(16)).unwrap_err();
    assert!(matches!(err, Error::PositionOutOfRange { .. }), "{err}");
    let err = truncate_log(
        d.path().join("elsewhere").as_path(),
        NAME,
        GlobalPosition(0),
    )
    .unwrap_err();
    assert!(matches!(err, Error::NotFound { .. }), "{err}");

    // At the head: a no-op that reports as one.
    let report = truncate_log(d.path(), NAME, GlobalPosition(15)).unwrap();
    assert_eq!(
        report,
        fold_core::Truncated {
            from: 15,
            to: 15,
            ..Default::default()
        }
    );

    // Nothing above touched the log.
    let log = open(d.path());
    assert_eq!(log.head(), GlobalPosition(15));
    assert_eq!(log.read_all(GlobalPosition(0), 100).unwrap(), before);
    assert_eq!(
        log.idempotency_position(b"pm:b").unwrap(),
        Some(GlobalPosition(14))
    );
    assert_eq!(
        log.read_models().checkpoint("C.Q").unwrap(),
        Some(GlobalPosition(15))
    );
    assert!(
        log.path()
            .join("snapshots/C.Q/00000000000000000015.fsnap")
            .is_file()
    );
}

#[test]
fn a_cut_to_zero_empties_the_log_and_keeps_it_usable() {
    let d = tmp();
    let log = create(d.path());
    populate(&log);
    drop(log);
    let report = truncate_log(d.path(), NAME, GlobalPosition(0)).unwrap();
    assert_eq!((report.from, report.to, report.streams_removed), (15, 0, 3));
    assert_eq!(
        report.checkpoints_reset,
        vec!["C.P".to_string(), "C.Q".to_string()]
    );
    let log = open(d.path());
    assert_eq!(log.head(), GlobalPosition(0));
    assert!(log.stream_ids().unwrap().is_empty());
    let r = log
        .append(
            &sid("order-1"),
            ExpectedVersion::NoStream,
            vec![ev("Placed", "x")],
        )
        .unwrap();
    assert_eq!(
        (r.first, r.stream_version),
        (GlobalPosition(0), StreamVersion(0))
    );
}

#[test]
fn a_cut_in_an_earlier_segment_removes_the_later_ones() {
    let d = tmp();
    let log = create_with(d.path(), OpenOptions::default().segment_max_bytes(1024));
    let big = "x".repeat(200);
    for i in 0..60 {
        log.append(
            &sid(&format!("s-{}", i % 4)),
            ExpectedVersion::Any,
            vec![NewEvent::new(
                EventType::new("Orders", "Placed", 1),
                Bytes::from(format!("{i}:{big}")),
            )],
        )
        .unwrap();
    }
    let before = log.read_all(GlobalPosition(0), 100).unwrap();
    let segments = |root: &std::path::Path| -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(root.join("segments"))
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    };
    let segs_before = segments(log.path());
    assert!(segs_before.len() > 3, "{segs_before:?}");
    let root = log.path().to_path_buf();
    drop(log);

    let report = truncate_log(d.path(), NAME, GlobalPosition(9)).unwrap();
    assert!(report.segments_removed >= 1, "{report:?}");
    assert_eq!(report.streams_cut + report.streams_removed, 4);
    let segs_after = segments(&root);
    assert!(segs_after.len() < segs_before.len());
    for name in &segs_after {
        let base: u64 = name.trim_end_matches(".seg").parse().unwrap();
        assert!(base <= 9, "{name} starts past the cut");
    }

    let log = open(d.path());
    assert_eq!(log.head(), GlobalPosition(9));
    let after = log.read_all(GlobalPosition(0), 100).unwrap();
    assert_eq!(after, before[..9].to_vec());
    for i in 0..60 {
        log.append(
            &sid(&format!("s-{}", i % 4)),
            ExpectedVersion::Any,
            vec![NewEvent::new(
                EventType::new("Orders", "Placed", 1),
                Bytes::from(format!("{i}:{big}")),
            )],
        )
        .unwrap();
    }
    assert_eq!(log.head(), GlobalPosition(69));
    drop(log);
    let log = open(d.path());
    assert_eq!(log.read_all(GlobalPosition(0), 100).unwrap().len(), 69);
}
