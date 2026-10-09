//! Point-in-time truncation: the events from a position on go, and with
//! them every index fact that looked past it; the generation moves on and
//! the log then continues.

use bytes::Bytes;
use fold_core::{
    Direction, Error, EventType, ExpectedVersion, GlobalPosition, Log, NewEvent, OpenOptions,
    PointInTime, StreamVersion, truncate_log, truncate_log_at,
};

use crate::common::*;

/// Positions 0..9: `order-1` one event per batch; 10..12: `order-2` one
/// batch of three; 13: `order-3` under key `pm:a`; 14: `order-1` under key
/// `pm:b`. Head 15.
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
}

#[test]
fn a_cut_at_a_boundary_drops_events_and_every_log_fact_past_it() {
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
            segments_removed: 0,
            generation: 1,
        }
    );

    let log = open(d.path());
    assert_eq!(log.head(), GlobalPosition(13));
    assert_eq!(log.generation().unwrap(), 1);
    assert_eq!(log.cut().unwrap(), GlobalPosition(13));
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
    drop(log);
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

    // At the head: a no-op that reports as one, and moves no generation.
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
    assert_eq!(log.generation().unwrap(), 0, "nothing moved backwards");
}

#[test]
fn a_cut_to_zero_empties_the_log_and_keeps_it_usable() {
    let d = tmp();
    let log = create(d.path());
    populate(&log);
    drop(log);
    let report = truncate_log(d.path(), NAME, GlobalPosition(0)).unwrap();
    assert_eq!((report.from, report.to, report.streams_removed), (15, 0, 3));
    assert_eq!(report.generation, 1);
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

/// A time lands on the boundary after the last batch recorded at or before
/// it: a batch carries one timestamp, so it is kept or dropped whole.
#[test]
fn a_time_resolves_to_the_last_batch_recorded_at_or_before_it() {
    let d = tmp();
    let log = create(d.path());
    for i in 0..10 {
        log.append(
            &sid("order-1"),
            ExpectedVersion::Any,
            vec![ev("Placed", &format!("n{i}"))],
        )
        .unwrap();
    }
    std::thread::sleep(std::time::Duration::from_millis(3));
    log.append(
        &sid("order-2"),
        ExpectedVersion::NoStream,
        vec![ev("Placed", "a"), ev("Shipped", "b"), ev("Placed", "c")],
    )
    .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(3));
    log.append(
        &sid("order-3"),
        ExpectedVersion::NoStream,
        vec![ev("Shipped", "d")],
    )
    .unwrap();
    let all = log.read_all(GlobalPosition(0), 100).unwrap();
    let rec = |p: usize| all[p].recorded_at;
    assert_eq!(rec(10), rec(12), "one timestamp per batch");
    assert!(rec(9) < rec(10) && rec(12) < rec(13));

    let at = |p: usize| log.position_after(rec(p)).unwrap().0;
    assert_eq!(log.position_after(rec(0) - 1).unwrap().0, 0);
    assert_eq!(at(9), 10, "the batch after it is later");
    assert_eq!(at(10), 13, "the whole batch is at this time");
    assert_eq!(at(11), 13);
    assert_eq!(at(13), 14, "at the head: everything");
    assert_eq!(log.position_after(i64::MAX).unwrap().0, 14);
    // Every timestamp against a linear oracle.
    for p in 0..all.len() {
        let t = rec(p);
        let oracle = all
            .iter()
            .position(|e| e.recorded_at > t)
            .unwrap_or(all.len()) as u64;
        assert_eq!(at(p), oracle, "position {p}");
    }
    let t9 = rec(9);
    drop(log);

    let report = truncate_log_at(d.path(), NAME, PointInTime::Time(t9)).unwrap();
    assert_eq!((report.from, report.to), (14, 10));
    assert_eq!(open(d.path()).head(), GlobalPosition(10));
    let report = truncate_log_at(d.path(), NAME, PointInTime::Position(GlobalPosition(5))).unwrap();
    assert_eq!((report.from, report.to), (10, 5));
}
