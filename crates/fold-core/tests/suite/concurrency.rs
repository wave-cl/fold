//! Many writers, one stream: the writer mutex plus `ExpectedVersion` give
//! exactly one winner per version. Fsync is not what is under test here, so
//! these logs use `FsyncPolicy::Never`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Barrier};

use fold_core::{
    Direction, Error, ExpectedVersion, FsyncPolicy, GlobalPosition, OpenOptions, StreamVersion,
};

use crate::common::*;

const THREADS: usize = 8;
const ROUNDS: usize = 100;

#[test]
fn exact_version_has_exactly_one_winner_per_version() {
    let d = tmp();
    let log = create_with(d.path(), OpenOptions::default().fsync(FsyncPolicy::Never));
    let s = sid("contended");
    log.append(&s, ExpectedVersion::NoStream, vec![ev("Init", "init")])
        .unwrap();

    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let log = log.clone();
            let s = s.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let mut wins: Vec<(u64, u64)> = Vec::new(); // (version written, position)
                let mut losses = 0usize;
                for r in 0..ROUNDS {
                    let current = log.stream_head(&s).unwrap().unwrap();
                    match log.append(
                        &s,
                        ExpectedVersion::Exact(current),
                        vec![ev("Bump", &format!("t{t}-r{r}"))],
                    ) {
                        Ok(res) => {
                            assert_eq!(res.stream_version.0, current.0 + 1);
                            wins.push((res.stream_version.0, res.first.0));
                        }
                        Err(Error::WrongExpectedVersion { actual, .. }) => {
                            assert!(actual.unwrap() > current, "the loser lost to a newer head");
                            losses += 1;
                        }
                        Err(e) => panic!("{e}"),
                    }
                }
                (wins, losses)
            })
        })
        .collect();

    let mut by_version: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    let mut total_losses = 0;
    for h in handles {
        let (wins, losses) = h.join().unwrap();
        total_losses += losses;
        for (v, p) in wins {
            by_version.entry(v).or_default().push(p);
        }
    }
    let wins = by_version.values().map(Vec::len).sum::<usize>();
    assert_eq!(
        wins + total_losses,
        THREADS * ROUNDS,
        "every attempt won or lost"
    );
    assert!(
        wins >= ROUNDS,
        "at least one thread's worth of rounds succeed"
    );
    for (v, positions) in &by_version {
        assert_eq!(
            positions.len(),
            1,
            "version {v} has {} winners",
            positions.len()
        );
    }
    // dense versions 1..=wins
    let versions: Vec<u64> = by_version.keys().copied().collect();
    assert_eq!(versions, (1..=wins as u64).collect::<Vec<_>>());
    // dense positions 1..=wins, in version order
    let positions: Vec<u64> = by_version.values().map(|p| p[0]).collect();
    assert_eq!(positions, (1..=wins as u64).collect::<Vec<_>>());

    assert_eq!(log.head(), GlobalPosition(wins as u64 + 1));
    assert_eq!(
        log.stream_head(&s).unwrap(),
        Some(StreamVersion(wins as u64))
    );
    let stream = log
        .read_stream(&s, StreamVersion(0), Direction::Forward, 10_000)
        .unwrap();
    assert_eq!(stream.len(), wins + 1);
    for (i, e) in stream.iter().enumerate() {
        assert_eq!(e.stream_version.0, i as u64);
        assert_eq!(e.position.0, i as u64);
    }
    let all = log.read_all(GlobalPosition(0), 10_000).unwrap();
    assert_eq!(all.len(), wins + 1);
    let ids: BTreeSet<_> = all.iter().map(|e| e.id).collect();
    assert_eq!(ids.len(), all.len(), "ids unique");
}

#[test]
fn no_stream_from_many_threads_has_one_winner() {
    let d = tmp();
    let log = create_with(d.path(), OpenOptions::default().fsync(FsyncPolicy::Never));
    let s = sid("fresh");
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let log = log.clone();
            let s = s.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                log.append(
                    &s,
                    ExpectedVersion::NoStream,
                    vec![ev("Create", &t.to_string())],
                )
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let ok = results.iter().filter(|r| r.is_ok()).count();
    let conflicts = results
        .iter()
        .filter(|r| {
            matches!(
                r,
                Err(Error::WrongExpectedVersion {
                    actual: Some(StreamVersion(0)),
                    ..
                })
            )
        })
        .count();
    assert_eq!(ok, 1);
    assert_eq!(conflicts, THREADS - 1);
    assert_eq!(log.head(), GlobalPosition(1));
}

#[test]
fn any_from_many_threads_all_succeed() {
    let d = tmp();
    let log = create_with(d.path(), OpenOptions::default().fsync(FsyncPolicy::Never));
    let s = sid("shared");
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let log = log.clone();
            let s = s.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                (0..ROUNDS)
                    .map(|r| {
                        log.append(&s, ExpectedVersion::Any, vec![ev("E", &format!("{t}-{r}"))])
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut positions = BTreeSet::new();
    let mut versions = BTreeSet::new();
    for h in handles {
        for r in h.join().unwrap() {
            assert!(
                positions.insert(r.first.0),
                "duplicate position {}",
                r.first.0
            );
            assert!(versions.insert(r.stream_version.0), "duplicate version");
        }
    }
    let n = (THREADS * ROUNDS) as u64;
    assert_eq!(positions, (0..n).collect());
    assert_eq!(versions, (0..n).collect());
    assert_eq!(log.head(), GlobalPosition(n));
    assert_eq!(
        log.read_all(GlobalPosition(0), 10_000).unwrap().len(),
        n as usize
    );
}

#[test]
fn readers_run_alongside_the_writer() {
    // Readers must never see a torn tail: every read_all from 0 returns a
    // dense prefix of the log as of its own head snapshot.
    let d = tmp();
    let log = create_with(d.path(), OpenOptions::default().fsync(FsyncPolicy::Never));
    let s = sid("s");
    let writer = {
        let log = log.clone();
        let s = s.clone();
        std::thread::spawn(move || {
            for i in 0..300 {
                log.append(&s, ExpectedVersion::Any, vec![ev("E", &i.to_string())])
                    .unwrap();
            }
        })
    };
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let log = log.clone();
            std::thread::spawn(move || {
                let mut max_seen = 0;
                for _ in 0..200 {
                    let head = log.head().0;
                    let all = log.read_all(GlobalPosition(0), 10_000).unwrap();
                    assert!(
                        all.len() as u64 >= head,
                        "read_all sees at least its head snapshot"
                    );
                    for (i, e) in all.iter().enumerate() {
                        assert_eq!(e.position.0, i as u64);
                    }
                    let tail = log.read_all_backward(GlobalPosition(u64::MAX), 5).unwrap();
                    if let Some(first) = tail.first() {
                        assert!(first.position.0 + 1 >= head);
                    }
                    max_seen = max_seen.max(all.len());
                }
                max_seen
            })
        })
        .collect();
    writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }
    assert_eq!(log.head(), GlobalPosition(300));
}

/// Several threads scanning the same segment at once must each see every
/// record. A read path that shares a file offset between callers (a dup'd
/// handle plus `seek`) fails this with short reads.
#[test]
fn concurrent_read_all_scans_do_not_disturb_each_other() {
    let d = tmp();
    let log = create_with(d.path(), OpenOptions::default().fsync(FsyncPolicy::Never));
    let s = sid("scanned");
    for i in 0..200 {
        log.append(&s, ExpectedVersion::Any, vec![ev("Tick", &format!("n{i}"))])
            .unwrap();
    }
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let log = log.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for round in 0..ROUNDS {
                    // Different threads start at different positions so their
                    // offsets differ.
                    let from = ((t * 7 + round) % 150) as u64;
                    let got = log
                        .read_all(GlobalPosition(from), 50)
                        .unwrap_or_else(|e| panic!("thread {t} round {round} from {from}: {e}"));
                    assert_eq!(got.len(), 50, "thread {t} round {round} from {from}");
                    for (i, e) in got.iter().enumerate() {
                        assert_eq!(e.position.0, from + i as u64);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("a scanning thread panicked");
    }
}
