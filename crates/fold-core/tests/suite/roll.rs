//! Segment rolling with a tiny `segment_max_bytes`.

use fold_core::{Direction, ExpectedVersion, GlobalPosition, OpenOptions, StreamVersion};

use crate::common::*;

#[test]
fn tiny_segments_roll_and_reads_cross_boundaries() {
    let d = tmp();
    let opts = OpenOptions::default().segment_max_bytes(1024);
    let log = create_with(d.path(), opts.clone());
    let mut expected = Vec::new();
    let mut batch_starts = Vec::new();
    let mut i = 0u64;
    while i < 300 {
        let n = (i % 3) + 1;
        let batch: Vec<_> = (0..n).map(|j| ev("E", &format!("{}", i + j))).collect();
        batch_starts.push(i);
        for j in 0..n {
            expected.push((i + j).to_string());
        }
        let r = log
            .append(&sid(&format!("s{}", i % 5)), ExpectedVersion::Any, batch)
            .unwrap();
        assert_eq!(r.first.0, i);
        i += n;
    }
    let total = expected.len() as u64;
    assert_eq!(log.head(), GlobalPosition(total));

    let segs = segments(d.path());
    assert!(segs.len() > 10, "only {} segments", segs.len());
    // every segment but the last exceeds the limit by less than one batch;
    // a batch never spans: the record before each segment base closed one
    let bases: Vec<u64> = segs
        .iter()
        .map(|p| p.file_stem().unwrap().to_str().unwrap().parse().unwrap())
        .collect();
    assert_eq!(bases[0], 0);
    for b in &bases[1..] {
        let prev = log.read_all(GlobalPosition(b - 1), 1).unwrap();
        assert!(
            prev[0].is_last_in_batch(),
            "segment at {b} starts mid-batch"
        );
        assert!(
            batch_starts.contains(b),
            "segment base {b} is not a batch start"
        );
    }

    // full read, chunked reads, reads from arbitrary offsets
    assert_eq!(
        payloads(&log.read_all(GlobalPosition(0), 10_000).unwrap()),
        expected
    );
    let mut chunked = Vec::new();
    let mut from = 0;
    loop {
        let page = log.read_all(GlobalPosition(from), 7).unwrap();
        if page.is_empty() {
            break;
        }
        from += page.len() as u64;
        chunked.extend(payloads(&page));
    }
    assert_eq!(chunked, expected);
    for from in [1u64, 17, 99, 150, total - 1] {
        let got = log.read_all(GlobalPosition(from), 23).unwrap();
        assert_eq!(
            payloads(&got),
            expected[from as usize..(from as usize + 23).min(expected.len())]
        );
    }
    let back = log
        .read_all_backward(GlobalPosition(u64::MAX), 10_000)
        .unwrap();
    let mut rev = expected.clone();
    rev.reverse();
    assert_eq!(payloads(&back), rev);

    // per-stream reads span every segment
    let s0 = log
        .read_stream(&sid("s0"), StreamVersion(0), Direction::Forward, 10_000)
        .unwrap();
    assert!(s0.len() > 20);
    for (v, e) in s0.iter().enumerate() {
        assert_eq!(e.stream_version.0, v as u64);
    }
    drop(log);

    // reopen sees all of it and continues in the last segment
    let log = open_with(d.path(), opts);
    assert_eq!(log.head(), GlobalPosition(total));
    assert_eq!(
        payloads(&log.read_all(GlobalPosition(0), 10_000).unwrap()),
        expected
    );
    let before = segments(d.path()).len();
    let r = log
        .append(&sid("s0"), ExpectedVersion::Any, vec![ev("E", "after")])
        .unwrap();
    assert_eq!(r.first.0, total);
    assert!(segments(d.path()).len() >= before);
    assert_eq!(
        payloads(&log.read_all(GlobalPosition(total - 1), 10).unwrap()),
        [expected.last().unwrap().clone(), "after".to_string()]
    );
}

#[test]
fn a_batch_larger_than_the_segment_limit_is_still_one_segment() {
    let d = tmp();
    let log = create_with(d.path(), OpenOptions::default().segment_max_bytes(100));
    let batch: Vec<_> = (0..20).map(|i| ev("E", &format!("{i:>30}"))).collect();
    log.append(&sid("s"), ExpectedVersion::Any, batch).unwrap();
    assert_eq!(segments(d.path()).len(), 2, "rolled once, after the batch");
    assert_eq!(std::fs::metadata(last_segment(d.path())).unwrap().len(), 64);
    assert_eq!(log.read_all(GlobalPosition(0), 100).unwrap().len(), 20);
}
