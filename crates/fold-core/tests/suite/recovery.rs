//! Crash recovery: the torn-write matrix, the fsynced-but-uncommitted tail,
//! index rebuild, index-ahead-of-data, torn rolls.

use fold_core::{Direction, Error, ExpectedVersion, GlobalPosition, OpenOptions, StreamVersion};

use crate::common::*;

const HEADER: u64 = 64;

/// 50 single-event batches with equal-width payloads, so every record has
/// the same size. Returns the record size.
fn fill_50(dir: &std::path::Path) -> u64 {
    let log = create(dir);
    for i in 0..50 {
        log.append(
            &sid("s"),
            ExpectedVersion::Any,
            vec![ev("E", &format!("{i:04}"))],
        )
        .unwrap();
    }
    drop(log);
    let len = std::fs::metadata(last_segment(dir)).unwrap().len();
    assert_eq!((len - HEADER) % 50, 0);
    (len - HEADER) / 50
}

fn expected_payloads(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{i:04}")).collect()
}

#[test]
fn torn_write_matrix_without_index() {
    // k bytes chopped off the end, with the index deleted so the rebuild
    // has only the data to go on. Each row is its own log.
    let d0 = tmp();
    let rec = fill_50(d0.path());
    let cases: Vec<(u64, usize, &str)> = vec![
        (1, 49, "one byte"),
        (3, 49, "part of the last crc"),
        (rec / 2, 49, "half a body"),
        (rec - 8, 49, "whole body, frame left"),
        (rec - 1, 49, "all but the first frame byte"),
        (rec, 49, "exactly one record"),
        (rec + 1, 48, "one record and a byte"),
        (rec + 5, 48, "one record and part of a frame"),
        (2 * rec + rec / 2, 47, "two and a half records"),
        (50 * rec, 0, "every record"),
        (50 * rec + 10, 0, "into the header"),
    ];
    for (k, survivors, what) in cases {
        let d = tmp();
        let r = fill_50(d.path());
        assert_eq!(r, rec);
        let seg = last_segment(d.path());
        chop(&seg, k);
        std::fs::remove_file(index_path(d.path())).unwrap();

        let log = open(d.path());
        assert_eq!(
            log.head(),
            GlobalPosition(survivors as u64),
            "head after chopping {what}"
        );
        let all = log.read_all(GlobalPosition(0), 100).unwrap();
        assert_eq!(
            payloads(&all),
            expected_payloads(survivors),
            "survivors after {what}"
        );
        assert_eq!(
            log.stream_head(&sid("s")).unwrap(),
            if survivors == 0 {
                None
            } else {
                Some(StreamVersion(survivors as u64 - 1))
            }
        );
        // the file is cut back to a record boundary (or a fresh header)
        let len = std::fs::metadata(&seg).unwrap().len();
        assert_eq!(
            len,
            HEADER + survivors as u64 * rec,
            "file length after {what}"
        );
        // and the log is writable again at the right position
        let r = log
            .append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "next")])
            .unwrap();
        assert_eq!(r.first.0, survivors as u64);
        let back = log.read_all(GlobalPosition(survivors as u64), 10).unwrap();
        assert_eq!(payloads(&back), ["next"]);
        drop(log);
        // and a plain reopen agrees
        let log = open(d.path());
        assert_eq!(log.head(), GlobalPosition(survivors as u64 + 1));
    }
}

#[test]
fn flipped_byte_in_last_record_without_index_truncates_it() {
    let d = tmp();
    let rec = fill_50(d.path());
    let seg = last_segment(d.path());
    let len = std::fs::metadata(&seg).unwrap().len();
    // inside the body of the last record
    flip(&seg, len - rec + 8 + 3, 0x01);
    std::fs::remove_file(index_path(d.path())).unwrap();
    let log = open(d.path());
    assert_eq!(log.head(), GlobalPosition(49));
    assert_eq!(std::fs::metadata(&seg).unwrap().len(), len - rec);
    assert_eq!(log.read_all(GlobalPosition(0), 100).unwrap().len(), 49);
}

#[test]
fn flipped_byte_in_an_earlier_record_without_index_is_a_truncation_point() {
    // Recovery stops at the first bad record; everything after it goes too.
    let d = tmp();
    let rec = fill_50(d.path());
    let seg = last_segment(d.path());
    flip(&seg, HEADER + 10 * rec + 8 + 1, 0x80);
    std::fs::remove_file(index_path(d.path())).unwrap();
    let log = open(d.path());
    assert_eq!(log.head(), GlobalPosition(10));
    assert_eq!(
        payloads(&log.read_all(GlobalPosition(0), 100).unwrap()),
        expected_payloads(10)
    );
}

#[test]
fn data_missing_below_the_committed_head_is_corrupt() {
    // With the index intact, data the index vouches for cannot just vanish.
    let d = tmp();
    let rec = fill_50(d.path());
    chop(&last_segment(d.path()), rec);
    let err = open_err(d.path());
    assert!(matches!(err, Error::Corrupt { .. }), "{err}");

    let d = tmp();
    let rec = fill_50(d.path());
    let seg = last_segment(d.path());
    let len = std::fs::metadata(&seg).unwrap().len();
    flip(&seg, len - rec + 8 + 2, 0x10);
    let err = open_err(d.path());
    assert!(matches!(err, Error::Corrupt { .. }), "{err}");

    // but a partial *frame* past the last committed record is just a torn
    // write and is dropped
    let d = tmp();
    fill_50(d.path());
    let seg = last_segment(d.path());
    let len = std::fs::metadata(&seg).unwrap().len();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&seg)
        .unwrap()
        .set_len(len + 5)
        .unwrap();
    let log = open(d.path());
    assert_eq!(log.head(), GlobalPosition(50));
    assert_eq!(std::fs::metadata(&seg).unwrap().len(), len);
}

fn open_err(dir: &std::path::Path) -> Error {
    match fold_core::Log::open(dir, NAME, OpenOptions::default()) {
        Ok(_) => panic!("open should have failed"),
        Err(e) => e,
    }
}

#[test]
fn fsynced_but_uncommitted_records_are_dropped() {
    let d = tmp();
    let log = create(d.path());
    for i in 0..10 {
        log.append(
            &sid("s"),
            ExpectedVersion::Any,
            vec![ev("E", &i.to_string())],
        )
        .unwrap();
    }
    let seg = last_segment(d.path());
    let committed_len = std::fs::metadata(&seg).unwrap().len();
    log.debug_write_without_commit(
        &sid("s"),
        vec![ev("E", "ghost-1"), ev("E", "ghost-2"), ev("E", "ghost-3")],
    )
    .unwrap();
    drop(log);
    assert!(
        std::fs::metadata(&seg).unwrap().len() > committed_len,
        "the ghosts are on disk"
    );

    let log = open(d.path());
    assert_eq!(
        log.head(),
        GlobalPosition(10),
        "nothing past META.head is acknowledged"
    );
    assert_eq!(
        std::fs::metadata(&seg).unwrap().len(),
        committed_len,
        "ghosts truncated"
    );
    let all = log.read_all(GlobalPosition(0), 100).unwrap();
    assert_eq!(all.len(), 10);
    assert!(all.iter().all(|e| !e.payload.starts_with(b"ghost")));
    assert_eq!(log.stream_head(&sid("s")).unwrap(), Some(StreamVersion(9)));

    // the next append takes position 10 and version 10, not 13
    let r = log
        .append(
            &sid("s"),
            ExpectedVersion::Exact(StreamVersion(9)),
            vec![ev("E", "real")],
        )
        .unwrap();
    assert_eq!((r.first.0, r.stream_version.0), (10, 10));
    assert_eq!(
        payloads(&log.read_all(GlobalPosition(10), 10).unwrap()),
        ["real"]
    );
}

#[test]
fn uncommitted_tail_without_index_is_kept_only_up_to_last_in_batch() {
    // Without an index, a whole batch on disk counts as acknowledged, but a
    // batch whose closing record is missing does not.
    let d = tmp();
    let log = create(d.path());
    log.append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "0")])
        .unwrap();
    log.debug_write_without_commit(&sid("s"), vec![ev("E", "1"), ev("E", "2"), ev("E", "3")])
        .unwrap();
    drop(log);
    let seg = last_segment(d.path());
    // chop into the last record of the open batch
    chop(&seg, 3);
    std::fs::remove_file(index_path(d.path())).unwrap();
    let log = open(d.path());
    assert_eq!(
        log.head(),
        GlobalPosition(1),
        "records 1 and 2 never closed a batch"
    );
    assert_eq!(
        payloads(&log.read_all(GlobalPosition(0), 10).unwrap()),
        ["0"]
    );
    assert!(std::fs::metadata(&seg).unwrap().len() > HEADER);
    drop(log);

    // and when the whole batch is there, the rebuild takes it
    let d = tmp();
    let log = create(d.path());
    log.append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "0")])
        .unwrap();
    log.debug_write_without_commit(&sid("s"), vec![ev("E", "1"), ev("E", "2")])
        .unwrap();
    drop(log);
    std::fs::remove_file(index_path(d.path())).unwrap();
    let log = open(d.path());
    assert_eq!(log.head(), GlobalPosition(3));
    assert_eq!(
        payloads(&log.read_all(GlobalPosition(0), 10).unwrap()),
        ["0", "1", "2"]
    );
    assert_eq!(log.stream_head(&sid("s")).unwrap(), Some(StreamVersion(2)));
}

#[derive(Debug, PartialEq, Eq)]
struct Everything {
    head: u64,
    all: Vec<fold_core::RecordedEvent>,
    backward: Vec<fold_core::RecordedEvent>,
    streams: Vec<(String, Option<u64>, Vec<fold_core::RecordedEvent>)>,
    types: Vec<(String, Vec<fold_core::RecordedEvent>)>,
}

fn everything(log: &fold_core::Log, streams: &[&str], types: &[&str]) -> Everything {
    Everything {
        head: log.head().0,
        all: log.read_all(GlobalPosition(0), 10_000).unwrap(),
        backward: log
            .read_all_backward(GlobalPosition(u64::MAX), 10_000)
            .unwrap(),
        streams: streams
            .iter()
            .map(|s| {
                (
                    s.to_string(),
                    log.stream_head(&sid(s)).unwrap().map(|v| v.0),
                    log.read_stream(&sid(s), StreamVersion(0), Direction::Forward, 10_000)
                        .unwrap(),
                )
            })
            .collect(),
        types: types
            .iter()
            .map(|t| {
                (
                    t.to_string(),
                    log.read_by_type(t, GlobalPosition(0), 10_000).unwrap(),
                )
            })
            .collect(),
    }
}

#[test]
fn deleting_the_index_rebuilds_identical_reads() {
    let d = tmp();
    // tiny segments so the rebuild crosses files too
    let opts = OpenOptions::default().segment_max_bytes(700);
    let log = create_with(d.path(), opts.clone());
    let streams = ["order-1", "order-2", "customer-9"];
    let types = ["Orders.Placed", "Orders.Line", "Orders.Cancelled"];
    for i in 0..120u64 {
        let s = streams[(i % 3) as usize];
        let t = ["Placed", "Line", "Line", "Cancelled"][(i % 4) as usize];
        let batch: Vec<_> = (0..=(i % 3)).map(|j| ev(t, &format!("{i}-{j}"))).collect();
        log.append(&sid(s), ExpectedVersion::Any, batch).unwrap();
    }
    let before = everything(&log, &streams, &types);
    assert!(before.head > 200);
    assert!(segments(d.path()).len() > 3);
    drop(log);

    std::fs::remove_file(index_path(d.path())).unwrap();
    let log = open_with(d.path(), opts.clone());
    assert!(index_path(d.path()).is_file());
    let after = everything(&log, &streams, &types);
    assert_eq!(after, before);

    // still appendable with the right expectations
    let v = log.stream_head(&sid("order-1")).unwrap().unwrap();
    let r = log
        .append(
            &sid("order-1"),
            ExpectedVersion::Exact(v),
            vec![ev("Placed", "post")],
        )
        .unwrap();
    assert_eq!(r.first.0, before.head);
    drop(log);
    let log = open_with(d.path(), opts);
    assert_eq!(log.head().0, before.head + 1);
}

#[test]
fn torn_roll_leaves_no_trace() {
    let d = tmp();
    let log = create(d.path());
    log.append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "0")])
        .unwrap();
    drop(log);
    // a segment whose header never finished
    let short = root(d.path())
        .join("segments")
        .join("00000000000000000001.seg");
    std::fs::write(&short, b"FOLDSEG\0...").unwrap();
    let log = open(d.path());
    assert!(!short.exists(), "short trailing segment removed");
    assert_eq!(log.head(), GlobalPosition(1));
    log.append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "1")])
        .unwrap();
    assert_eq!(
        payloads(&log.read_all(GlobalPosition(0), 10).unwrap()),
        ["0", "1"]
    );
}

#[test]
fn segment_starting_past_head_is_removed() {
    // A complete header for a segment the index never heard of (lost
    // commit under FsyncPolicy::Never, say) is unacknowledged data.
    let d = tmp();
    let opts = OpenOptions::default().segment_max_bytes(1);
    let log = create_with(d.path(), opts.clone());
    log.append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "0")])
        .unwrap();
    // rolled: segment 1 exists and is empty
    assert_eq!(segments(d.path()).len(), 2);
    log.debug_write_without_commit(&sid("s"), vec![ev("E", "ghost")])
        .unwrap();
    drop(log);
    // fake a lost index commit by restoring the index to head 1 is what we
    // have already; segment 1 (base 1 == head) holds an unacked record
    let log = open_with(d.path(), opts.clone());
    assert_eq!(log.head(), GlobalPosition(1));
    assert_eq!(segments(d.path()).len(), 2);
    assert_eq!(
        std::fs::metadata(last_segment(d.path())).unwrap().len(),
        HEADER
    );
    drop(log);

    // now a segment strictly past head
    let far = root(d.path())
        .join("segments")
        .join("00000000000000000007.seg");
    std::fs::copy(last_segment(d.path()), &far).unwrap();
    // its header says base 1, name says 7: corrupt, not silently dropped
    let err = match fold_core::Log::open(d.path(), NAME, opts.clone()) {
        Ok(_) => panic!(),
        Err(e) => e,
    };
    assert!(matches!(err, Error::Corrupt { .. }), "{err}");
    std::fs::remove_file(&far).unwrap();
    let log = open_with(d.path(), opts);
    assert_eq!(log.head(), GlobalPosition(1));
}

#[test]
fn verify_all_segments_catches_an_old_corruption() {
    let d = tmp();
    let opts = OpenOptions::default().segment_max_bytes(300);
    let log = create_with(d.path(), opts.clone());
    for i in 0..30 {
        log.append(
            &sid("s"),
            ExpectedVersion::Any,
            vec![ev("E", &format!("{i:03}"))],
        )
        .unwrap();
    }
    drop(log);
    let segs = segments(d.path());
    assert!(segs.len() >= 3);
    flip(&segs[0], HEADER + 8 + 2, 0x08);

    // default open only checks the tail; the damage shows when read
    let log = open_with(d.path(), opts.clone());
    assert!(matches!(
        log.read_all(GlobalPosition(0), 100),
        Err(Error::Corrupt { .. })
    ));
    assert!(matches!(
        log.read_stream(&sid("s"), StreamVersion(0), Direction::Forward, 1),
        Err(Error::Corrupt { .. })
    ));
    drop(log);

    let err = match fold_core::Log::open(d.path(), NAME, opts.verify_all_segments(true)) {
        Ok(_) => panic!(),
        Err(e) => e,
    };
    assert!(matches!(err, Error::Corrupt { .. }), "{err}");
}

#[test]
fn foreign_segment_is_rejected() {
    let d1 = tmp();
    let d2 = tmp();
    let l1 = create(d1.path());
    l1.append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "0")])
        .unwrap();
    drop(l1);
    let l2 = create(d2.path());
    drop(l2);
    std::fs::copy(last_segment(d1.path()), last_segment(d2.path())).unwrap();
    let err = match fold_core::Log::open(d2.path(), NAME, OpenOptions::default()) {
        Ok(_) => panic!(),
        Err(e) => e,
    };
    assert!(matches!(err, Error::Corrupt { .. }), "{err}");
    assert!(err.to_string().contains("belongs to log"));
}
