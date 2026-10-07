use bytes::Bytes;
use fold_core::{
    Direction, Error, EventId, EventType, ExpectedVersion, GlobalPosition, Log, NewEvent,
    OpenOptions, StreamVersion,
};
use uuid::Uuid;

use crate::common::*;

#[test]
fn append_assigns_dense_positions_and_versions() {
    let d = tmp();
    let log = create(d.path());
    assert_eq!(log.head(), GlobalPosition(0));
    let a = sid("order-a");
    let b = sid("order-b");

    let r1 = log
        .append(
            &a,
            ExpectedVersion::NoStream,
            vec![ev("Placed", "a0"), ev("Line", "a1")],
        )
        .unwrap();
    assert_eq!((r1.first.0, r1.last.0, r1.stream_version.0), (0, 1, 1));
    let r2 = log
        .append(&b, ExpectedVersion::Any, vec![ev("Placed", "b0")])
        .unwrap();
    assert_eq!((r2.first.0, r2.last.0, r2.stream_version.0), (2, 2, 0));
    let r3 = log
        .append(
            &a,
            ExpectedVersion::Exact(StreamVersion(1)),
            vec![ev("Cancelled", "a2")],
        )
        .unwrap();
    assert_eq!((r3.first.0, r3.last.0, r3.stream_version.0), (3, 3, 2));
    assert_eq!(log.head(), GlobalPosition(4));
    assert_eq!(log.stream_head(&a).unwrap(), Some(StreamVersion(2)));
    assert_eq!(log.stream_head(&b).unwrap(), Some(StreamVersion(0)));
    assert_eq!(log.stream_head(&sid("nope")).unwrap(), None);

    let all = log.read_all(GlobalPosition(0), 100).unwrap();
    assert_eq!(payloads(&all), ["a0", "a1", "b0", "a2"]);
    assert_eq!(positions(&all), [0, 1, 2, 3]);
    let versions: Vec<u64> = all.iter().map(|e| e.stream_version.0).collect();
    assert_eq!(versions, [0, 1, 0, 2]);
    // LAST_IN_BATCH only on the last record of each batch
    let last: Vec<bool> = all.iter().map(|e| e.is_last_in_batch()).collect();
    assert_eq!(last, [false, true, true, true]);
    assert!(all.iter().all(|e| e.recorded_at > 0));
    assert_eq!(all[0].event_type, EventType::new("Orders", "Placed", 1));
    assert_eq!(all[0].stream_id, a);

    // read_all from the middle, with a limit
    assert_eq!(
        payloads(&log.read_all(GlobalPosition(1), 2).unwrap()),
        ["a1", "b0"]
    );
    assert!(log.read_all(GlobalPosition(4), 10).unwrap().is_empty());
    assert!(log.read_all(GlobalPosition(0), 0).unwrap().is_empty());
    assert!(matches!(
        log.read_all(GlobalPosition(5), 10),
        Err(Error::PositionOutOfRange { .. })
    ));

    // backward, clamped
    assert_eq!(
        payloads(&log.read_all_backward(GlobalPosition(u64::MAX), 3).unwrap()),
        ["a2", "b0", "a1"]
    );
    assert_eq!(
        payloads(&log.read_all_backward(GlobalPosition(1), 10).unwrap()),
        ["a1", "a0"]
    );

    // by stream
    let fwd = log
        .read_stream(&a, StreamVersion(0), Direction::Forward, 10)
        .unwrap();
    assert_eq!(payloads(&fwd), ["a0", "a1", "a2"]);
    let fwd1 = log
        .read_stream(&a, StreamVersion(1), Direction::Forward, 1)
        .unwrap();
    assert_eq!(payloads(&fwd1), ["a1"]);
    let bwd = log
        .read_stream(&a, StreamVersion(u64::MAX), Direction::Backward, 2)
        .unwrap();
    assert_eq!(payloads(&bwd), ["a2", "a1"]);
    let bwd0 = log
        .read_stream(&a, StreamVersion(1), Direction::Backward, 10)
        .unwrap();
    assert_eq!(payloads(&bwd0), ["a1", "a0"]);
    assert!(
        log.read_stream(&sid("nope"), StreamVersion(0), Direction::Forward, 10)
            .unwrap()
            .is_empty()
    );
    // b's stream does not leak into a's range
    assert_eq!(
        payloads(
            &log.read_stream(&b, StreamVersion(0), Direction::Forward, 10)
                .unwrap()
        ),
        ["b0"]
    );

    // by type family, versions collapse into the family
    let placed = log
        .read_by_type("Orders.Placed", GlobalPosition(0), 10)
        .unwrap();
    assert_eq!(payloads(&placed), ["a0", "b0"]);
    let placed_from = log
        .read_by_type("Orders.Placed", GlobalPosition(1), 10)
        .unwrap();
    assert_eq!(payloads(&placed_from), ["b0"]);
    assert!(
        log.read_by_type("Orders.Nothing", GlobalPosition(0), 10)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn type_family_ignores_version() {
    let d = tmp();
    let log = create(d.path());
    let s = sid("s");
    log.append(
        &s,
        ExpectedVersion::Any,
        vec![
            NewEvent::new(
                EventType::new("Orders", "Placed", 1),
                Bytes::from_static(b"v1"),
            ),
            NewEvent::new(
                EventType::new("Orders", "Placed", 2),
                Bytes::from_static(b"v2"),
            ),
            NewEvent::new(
                EventType::new("Other", "Placed", 1),
                Bytes::from_static(b"other"),
            ),
        ],
    )
    .unwrap();
    let got = log
        .read_by_type("Orders.Placed", GlobalPosition(0), 10)
        .unwrap();
    assert_eq!(payloads(&got), ["v1", "v2"]);
    assert_eq!(got[1].event_type.version, 2);
}

#[test]
fn expected_version_rules() {
    let d = tmp();
    let log = create(d.path());
    let s = sid("s");

    assert!(matches!(
        log.append(&s, ExpectedVersion::StreamExists, vec![ev("E", "x")]),
        Err(Error::WrongExpectedVersion { actual: None, .. })
    ));
    assert!(matches!(
        log.append(
            &s,
            ExpectedVersion::Exact(StreamVersion(0)),
            vec![ev("E", "x")]
        ),
        Err(Error::WrongExpectedVersion { actual: None, .. })
    ));
    assert_eq!(
        log.head(),
        GlobalPosition(0),
        "a refused append writes nothing"
    );

    log.append(&s, ExpectedVersion::NoStream, vec![ev("E", "0")])
        .unwrap();
    let err = log
        .append(&s, ExpectedVersion::NoStream, vec![ev("E", "1")])
        .unwrap_err();
    match err {
        Error::WrongExpectedVersion {
            stream,
            expected,
            actual,
        } => {
            assert_eq!(stream, "s");
            assert_eq!(expected, ExpectedVersion::NoStream);
            assert_eq!(actual, Some(StreamVersion(0)));
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        log.append(
            &s,
            ExpectedVersion::Exact(StreamVersion(5)),
            vec![ev("E", "1")]
        ),
        Err(Error::WrongExpectedVersion {
            actual: Some(StreamVersion(0)),
            ..
        })
    ));
    log.append(&s, ExpectedVersion::StreamExists, vec![ev("E", "1")])
        .unwrap();
    log.append(
        &s,
        ExpectedVersion::Exact(StreamVersion(1)),
        vec![ev("E", "2")],
    )
    .unwrap();
    assert_eq!(log.stream_head(&s).unwrap(), Some(StreamVersion(2)));
    assert_eq!(log.head(), GlobalPosition(3));
}

#[test]
fn batch_validation() {
    let d = tmp();
    let log = create_with(d.path(), OpenOptions::default().max_record_bytes(200));
    let s = sid("s");
    assert!(matches!(
        log.append(&s, ExpectedVersion::Any, vec![]),
        Err(Error::EmptyBatch)
    ));
    let big = "x".repeat(300);
    let err = log
        .append(&s, ExpectedVersion::Any, vec![ev("E", "ok"), ev("E", &big)])
        .unwrap_err();
    assert!(
        matches!(err, Error::RecordTooLarge { max: 200, .. }),
        "{err:?}"
    );
    assert_eq!(
        log.head(),
        GlobalPosition(0),
        "nothing of the batch was written"
    );
    assert!(log.read_all(GlobalPosition(0), 10).unwrap().is_empty());

    let empty_name = NewEvent::new(EventType::new("Orders", "", 1), Bytes::new());
    assert!(matches!(
        log.append(&s, ExpectedVersion::Any, vec![empty_name]),
        Err(Error::InvalidEventType(_))
    ));

    // a record right at the limit is fine
    let small = ev("E", "fits");
    log.append(&s, ExpectedVersion::Any, vec![small]).unwrap();
}

#[test]
fn supplied_ids_and_metadata_are_kept() {
    let d = tmp();
    let log = create(d.path());
    let s = sid("s");
    let id = EventId(Uuid::from_u128(42));
    let e = ev("E", "p")
        .with_id(id)
        .with_metadata(Bytes::from_static(b"{\"m\":1}"));
    log.append(&s, ExpectedVersion::Any, vec![e, ev("E", "q")])
        .unwrap();
    let all = log.read_all(GlobalPosition(0), 10).unwrap();
    assert_eq!(all[0].id, id);
    assert_eq!(&all[0].metadata[..], b"{\"m\":1}");
    assert_ne!(all[1].id, id);
    assert_eq!(all[1].id.0.get_version(), Some(uuid::Version::SortRand));
    assert!(all[1].metadata.is_empty());
}

#[test]
fn create_open_and_open_or_create() {
    let d = tmp();
    assert!(matches!(
        Log::open(d.path(), NAME, OpenOptions::default()),
        Err(Error::NotFound { .. })
    ));
    {
        let log = Log::open_or_create(d.path(), NAME, OpenOptions::default()).unwrap();
        assert_eq!(log.path(), d.path().join(NAME));
        log.append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "1")])
            .unwrap();
        assert_eq!(log.schema_source().unwrap(), None);
        log.set_schema_source("context Orders {}").unwrap();
        assert_eq!(
            log.schema_source().unwrap().as_deref(),
            Some("context Orders {}")
        );
        assert!(
            std::fs::read_to_string(log.path().join("schema/current.fold")).unwrap()
                == "context Orders {}"
        );
    }
    assert!(matches!(
        Log::create(d.path(), NAME, OpenOptions::default()),
        Err(Error::AlreadyExists { .. })
    ));
    let log = Log::open_or_create(d.path(), NAME, OpenOptions::default()).unwrap();
    assert_eq!(log.head(), GlobalPosition(1));
    assert_eq!(
        log.schema_source().unwrap().as_deref(),
        Some("context Orders {}")
    );
    let again = Log::open(d.path(), NAME, OpenOptions::default()).err();
    assert!(matches!(again, Some(Error::Locked { .. })));
    drop(log);
    let log = open(d.path());
    assert_eq!(
        payloads(&log.read_all(GlobalPosition(0), 10).unwrap()),
        ["1"]
    );
    assert!(format!("{log:?}").contains("head"));
}

#[test]
fn never_fsync_policy_survives_a_clean_close() {
    let d = tmp();
    let opts = OpenOptions::default().fsync(fold_core::FsyncPolicy::Never);
    {
        let log = create_with(d.path(), opts.clone());
        for i in 0..20 {
            log.append(
                &sid("s"),
                ExpectedVersion::Any,
                vec![ev("E", &i.to_string())],
            )
            .unwrap();
        }
        log.flush().unwrap();
    }
    let log = open_with(d.path(), opts);
    assert_eq!(log.head(), GlobalPosition(20));
    assert_eq!(log.read_all(GlobalPosition(0), 100).unwrap().len(), 20);
}

#[test]
fn clones_share_state() {
    let d = tmp();
    let log = create(d.path());
    let other = log.clone();
    log.append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "1")])
        .unwrap();
    assert_eq!(other.head(), GlobalPosition(1));
    assert_eq!(other.read_all(GlobalPosition(0), 1).unwrap().len(), 1);
    assert_eq!(log.log_id(), other.log_id());
}

#[test]
fn an_idempotency_key_is_accepted_once() {
    let d = tmp();
    let log = create(d.path());
    let s = sid("order-k");
    let first = log
        .append_idempotent(&s, ExpectedVersion::Any, vec![ev("Placed", "k0")], b"pm:1")
        .unwrap();
    assert_eq!(first.first.0, 0);
    let head_after = log.head();
    let version_after = log.stream_head(&s).unwrap();

    let err = log
        .append_idempotent(
            &s,
            ExpectedVersion::Any,
            vec![ev("Placed", "again")],
            b"pm:1",
        )
        .unwrap_err();
    assert!(
        matches!(err, Error::DuplicateKey { position } if position.0 == 0),
        "{err}"
    );
    assert_eq!(log.head(), head_after, "nothing appended");
    assert_eq!(log.stream_head(&s).unwrap(), version_after);
    assert_eq!(
        log.idempotency_position(b"pm:1").unwrap(),
        Some(GlobalPosition(0))
    );
    assert_eq!(log.idempotency_position(b"pm:2").unwrap(), None);

    // Another key is another append, and the keys survive a reopen.
    log.append_idempotent(&s, ExpectedVersion::Any, vec![ev("Placed", "k1")], b"pm:2")
        .unwrap();
    assert_eq!(log.head(), GlobalPosition(2));
    drop(log);
    let log = Log::open(d.path(), "testlog", OpenOptions::default()).unwrap();
    let err = log
        .append_idempotent(&s, ExpectedVersion::Any, vec![ev("Placed", "k1")], b"pm:2")
        .unwrap_err();
    assert!(
        matches!(err, Error::DuplicateKey { position } if position.0 == 1),
        "{err}"
    );
    // The refused append left bytes past head; the next real one overwrites them.
    log.append(&s, ExpectedVersion::Any, vec![ev("Placed", "k2")])
        .unwrap();
    assert_eq!(log.read_all(GlobalPosition(0), 10).unwrap().len(), 3);
}
