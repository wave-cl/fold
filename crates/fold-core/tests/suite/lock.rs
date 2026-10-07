use fold_core::{Error, ExpectedVersion, Log, OpenOptions};

use crate::common::*;

#[test]
fn second_open_is_locked_until_the_first_closes() {
    let d = tmp();
    let first = create(d.path());
    first
        .append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "0")])
        .unwrap();

    let err = Log::open(d.path(), NAME, OpenOptions::default())
        .err()
        .unwrap();
    assert!(
        matches!(err, Error::Locked { ref path } if path == &root(d.path())),
        "{err}"
    );
    let err = Log::open_or_create(d.path(), NAME, OpenOptions::default())
        .err()
        .unwrap();
    assert!(matches!(err, Error::Locked { .. }), "{err}");
    // create on a live log is refused by the lock before it can touch anything
    let err = Log::create(d.path(), NAME, OpenOptions::default())
        .err()
        .unwrap();
    assert!(matches!(err, Error::Locked { .. }), "{err}");

    // a clone is not a second open
    let clone = first.clone();
    drop(first);
    assert!(matches!(
        Log::open(d.path(), NAME, OpenOptions::default()),
        Err(Error::Locked { .. })
    ));
    drop(clone);

    let second = open(d.path());
    assert_eq!(second.head().0, 1);
}

#[test]
fn lock_is_released_by_a_failed_open() {
    let d = tmp();
    let log = create(d.path());
    log.append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "0")])
        .unwrap();
    drop(log);
    // break the LOG file so open fails after taking the lock
    let log_file = root(d.path()).join("LOG");
    let mut bytes = std::fs::read(&log_file).unwrap();
    bytes[20] ^= 1;
    std::fs::write(&log_file, &bytes).unwrap();
    assert!(matches!(
        Log::open(d.path(), NAME, OpenOptions::default()),
        Err(Error::Corrupt { .. })
    ));
    bytes[20] ^= 1;
    std::fs::write(&log_file, &bytes).unwrap();
    let log = open(d.path());
    assert_eq!(log.head().0, 1);
}
