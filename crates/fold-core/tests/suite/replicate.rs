//! Replication chunks: batch-aligned byte ranges a replica appends as-is,
//! with the idempotency keys used inside them.

use fold_core::{Error, ExpectedVersion, GlobalPosition, Log, OpenOptions};

use crate::common::*;

fn populate(log: &Log) {
    for i in 0..10 {
        log.append(
            &sid("s-1"),
            ExpectedVersion::Any,
            vec![ev("Placed", &format!("n{i}"))],
        )
        .unwrap();
    }
    // One batch of five, longer than the test's chunk window.
    log.append(
        &sid("s-2"),
        ExpectedVersion::NoStream,
        (0..5).map(|i| ev("Placed", &format!("b{i}"))).collect(),
    )
    .unwrap();
    log.append_idempotent(
        &sid("s-3"),
        ExpectedVersion::NoStream,
        vec![ev("Shipped", "k")],
        b"pm:a",
    )
    .unwrap();
    log.append_idempotent(
        &sid("s-1"),
        ExpectedVersion::Any,
        vec![ev("Shipped", "k2")],
        b"pm:b",
    )
    .unwrap();
    assert_eq!(log.head(), GlobalPosition(17));
}

#[test]
fn chunks_end_on_batch_boundaries_and_carry_their_keys() {
    let d = tmp();
    let log = create(d.path());
    populate(&log);

    let c = log
        .replication_chunk(GlobalPosition(0), 4)
        .unwrap()
        .unwrap();
    assert_eq!((c.from.0, c.to.0), (0, 4));
    assert!(c.keys.is_empty());
    let c = log
        .replication_chunk(GlobalPosition(8), 4)
        .unwrap()
        .unwrap();
    assert_eq!(
        (c.from.0, c.to.0),
        (8, 10),
        "the window ends inside the batch; stop before it"
    );
    let c = log
        .replication_chunk(GlobalPosition(10), 3)
        .unwrap()
        .unwrap();
    assert_eq!(
        (c.from.0, c.to.0),
        (10, 15),
        "a batch longer than the window goes whole"
    );
    let c = log
        .replication_chunk(GlobalPosition(10), 100)
        .unwrap()
        .unwrap();
    assert_eq!((c.from.0, c.to.0), (10, 17));
    assert_eq!(
        c.keys,
        vec![(b"pm:a".to_vec(), 15), (b"pm:b".to_vec(), 16)],
        "keys first used in the range, by position"
    );
    let c = log
        .replication_chunk(GlobalPosition(16), 100)
        .unwrap()
        .unwrap();
    assert_eq!(c.keys, vec![(b"pm:b".to_vec(), 16)]);
    assert!(
        log.replication_chunk(GlobalPosition(17), 100)
            .unwrap()
            .is_none()
    );
    let err = log.replication_chunk(GlobalPosition(18), 100).unwrap_err();
    assert!(matches!(err, Error::PositionOutOfRange { .. }), "{err}");
}

#[test]
fn a_replica_applies_chunks_into_an_identical_log() {
    let d = tmp();
    let primary = create(d.path());
    populate(&primary);
    let all = primary.read_all(GlobalPosition(0), 100).unwrap();

    let rdir = d.path().join("replica");
    let replica =
        Log::create_with_id(&rdir, NAME, OpenOptions::default(), primary.log_id()).unwrap();
    assert_eq!(replica.log_id(), primary.log_id());
    let mut from = GlobalPosition(0);
    let mut chunks = 0;
    while let Some(c) = primary.replication_chunk(from, 4).unwrap() {
        let head = replica.apply_replication_chunk(&c).unwrap();
        assert_eq!(head, c.to);
        from = c.to;
        chunks += 1;
    }
    assert!(chunks >= 4, "{chunks}");
    assert_eq!(replica.head(), primary.head());
    assert_eq!(replica.read_all(GlobalPosition(0), 100).unwrap(), all);
    assert_eq!(
        replica.idempotency_position(b"pm:a").unwrap(),
        Some(GlobalPosition(15))
    );
    assert_eq!(
        replica.idempotency_position(b"pm:b").unwrap(),
        Some(GlobalPosition(16))
    );
    assert_eq!(
        replica.stream_head(&sid("s-1")).unwrap(),
        primary.stream_head(&sid("s-1")).unwrap()
    );

    // The same chunk again: not at the head. A chunk of another log: refused.
    let c = primary
        .replication_chunk(GlobalPosition(0), 4)
        .unwrap()
        .unwrap();
    let err = replica.apply_replication_chunk(&c).unwrap_err();
    assert!(matches!(err, Error::PositionOutOfRange { .. }), "{err}");
    let other = create(&d.path().join("other"));
    other
        .append(&sid("x"), ExpectedVersion::Any, vec![ev("Placed", "z")])
        .unwrap();
    let foreign = other
        .replication_chunk(GlobalPosition(0), 4)
        .unwrap()
        .unwrap();
    let fresh = Log::create_with_id(
        &d.path().join("fresh"),
        NAME,
        OpenOptions::default(),
        primary.log_id(),
    )
    .unwrap();
    let err = fresh.apply_replication_chunk(&foreign).unwrap_err();
    assert!(err.to_string().contains("is of log"), "{err}");
    assert_eq!(fresh.head(), GlobalPosition(0));

    // The replica keeps tailing after a reopen, and keeps its identity.
    drop(replica);
    let replica = open(&rdir);
    assert_eq!(replica.log_id(), primary.log_id());
    primary
        .append(
            &sid("s-1"),
            ExpectedVersion::Any,
            vec![ev("Placed", "later")],
        )
        .unwrap();
    let c = primary
        .replication_chunk(replica.head(), 100)
        .unwrap()
        .unwrap();
    replica.apply_replication_chunk(&c).unwrap();
    assert_eq!(replica.head(), primary.head());
}
