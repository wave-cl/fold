//! Read-model rows and checkpoints: one transaction, visible together or
//! not at all, and still there after a reopen.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fold_core::GlobalPosition;
use fold_core::keyenc::{KeyPart, encode_parts};
use uuid::Uuid;

use crate::common::*;

fn key(u: u128) -> Vec<u8> {
    encode_parts(&[KeyPart::Uuid(Uuid::from_u128(u))]).unwrap()
}

#[test]
fn commit_writes_rows_and_checkpoint_together() {
    let d = tmp();
    let log = create(d.path());
    let rm = log.read_models();

    let before = rm.snapshot().unwrap();
    assert_eq!(rm.checkpoint("OrderTotals").unwrap(), None);
    assert_eq!(
        before.get("OrderTotals", "order_totals", &key(1)).unwrap(),
        None
    );
    assert!(
        before
            .scan("OrderTotals", "order_totals", &[], 10)
            .unwrap()
            .is_empty()
    );

    rm.commit(
        "OrderTotals",
        GlobalPosition(7),
        vec![
            ("order_totals".into(), key(1), b"{\"total\":1}".to_vec()),
            ("order_totals".into(), key(2), b"{\"total\":2}".to_vec()),
            ("by_status".into(), b"pending".to_vec(), b"[1,2]".to_vec()),
        ],
        vec![],
    )
    .unwrap();

    // the old snapshot is unchanged
    assert_eq!(
        before.get("OrderTotals", "order_totals", &key(1)).unwrap(),
        None
    );
    assert_eq!(before.checkpoint("OrderTotals").unwrap(), None);

    let after = rm.snapshot().unwrap();
    assert_eq!(
        after.checkpoint("OrderTotals").unwrap(),
        Some(GlobalPosition(7))
    );
    assert_eq!(
        rm.checkpoint("OrderTotals").unwrap(),
        Some(GlobalPosition(7))
    );
    assert_eq!(
        after
            .get("OrderTotals", "order_totals", &key(1))
            .unwrap()
            .as_deref(),
        Some(&b"{\"total\":1}"[..])
    );
    assert_eq!(
        after
            .get("OrderTotals", "by_status", b"pending")
            .unwrap()
            .as_deref(),
        Some(&b"[1,2]"[..])
    );
    // projections do not see each other's tables of the same name
    assert_eq!(after.get("Other", "order_totals", &key(1)).unwrap(), None);
    assert_eq!(after.checkpoint("Other").unwrap(), None);

    // update + delete + checkpoint advance, atomically
    rm.commit(
        "OrderTotals",
        GlobalPosition(9),
        vec![("order_totals".into(), key(2), b"{\"total\":22}".to_vec())],
        vec![
            ("order_totals".into(), key(1)),
            ("order_totals".into(), key(99)),
        ],
    )
    .unwrap();
    let s = rm.snapshot().unwrap();
    assert_eq!(s.get("OrderTotals", "order_totals", &key(1)).unwrap(), None);
    assert_eq!(
        s.get("OrderTotals", "order_totals", &key(2))
            .unwrap()
            .as_deref(),
        Some(&b"{\"total\":22}"[..])
    );
    assert_eq!(
        s.checkpoint("OrderTotals").unwrap(),
        Some(GlobalPosition(9))
    );
    drop((before, after, s));
    drop(rm);
    drop(log);

    let log = open(d.path());
    let s = log.read_models().snapshot().unwrap();
    assert_eq!(
        s.checkpoint("OrderTotals").unwrap(),
        Some(GlobalPosition(9))
    );
    assert_eq!(s.get("OrderTotals", "order_totals", &key(1)).unwrap(), None);
    assert_eq!(
        s.get("OrderTotals", "order_totals", &key(2))
            .unwrap()
            .as_deref(),
        Some(&b"{\"total\":22}"[..])
    );
    assert_eq!(
        s.get("OrderTotals", "by_status", b"pending")
            .unwrap()
            .as_deref(),
        Some(&b"[1,2]"[..])
    );
}

#[test]
fn scan_by_prefix_in_key_order() {
    let d = tmp();
    let log = create(d.path());
    let rm = log.read_models();
    let k = |cust: &str, n: i64| encode_parts(&[KeyPart::Str(cust), KeyPart::I64(n)]).unwrap();
    let puts = vec![
        ("t".to_string(), k("bob", 5), b"b5".to_vec()),
        ("t".to_string(), k("alice", -1), b"a-1".to_vec()),
        ("t".to_string(), k("alice", 10), b"a10".to_vec()),
        ("t".to_string(), k("alice", 2), b"a2".to_vec()),
        ("t".to_string(), k("alicia", 0), b"x".to_vec()),
    ];
    rm.commit("P", GlobalPosition(1), puts, vec![]).unwrap();
    let s = rm.snapshot().unwrap();
    let prefix = encode_parts(&[KeyPart::Str("alice")]).unwrap();
    let rows = s.scan("P", "t", &prefix, 10).unwrap();
    let vals: Vec<&[u8]> = rows.iter().map(|(_, v)| v.as_slice()).collect();
    assert_eq!(
        vals,
        [&b"a-1"[..], b"a2", b"a10"],
        "numeric order, alicia excluded"
    );
    assert_eq!(s.scan("P", "t", &prefix, 2).unwrap().len(), 2);
    assert_eq!(s.scan("P", "t", &prefix, 0).unwrap().len(), 0);
    assert_eq!(s.scan("P", "t", &[], 10).unwrap().len(), 5);
    assert_eq!(s.scan("P", "t", b"zzz", 10).unwrap().len(), 0);
    assert_eq!(s.scan("P", "missing", &[], 10).unwrap().len(), 0);
}

#[test]
fn later_operations_in_one_commit_win() {
    let d = tmp();
    let log = create(d.path());
    let rm = log.read_models();
    rm.commit(
        "P",
        GlobalPosition(1),
        vec![
            ("t".into(), b"k".to_vec(), b"first".to_vec()),
            ("t".into(), b"k".to_vec(), b"second".to_vec()),
            ("t".into(), b"gone".to_vec(), b"x".to_vec()),
        ],
        vec![("t".into(), b"gone".to_vec())],
    )
    .unwrap();
    let s = rm.snapshot().unwrap();
    assert_eq!(
        s.get("P", "t", b"k").unwrap().as_deref(),
        Some(&b"second"[..])
    );
    assert_eq!(s.get("P", "t", b"gone").unwrap(), None);
}

#[test]
fn no_reader_ever_sees_the_checkpoint_without_the_rows() {
    // The commit is one redb transaction; a concurrent reader taking
    // snapshots as fast as it can sees either nothing or everything.
    let d = tmp();
    let log = create(d.path());
    let rm = log.read_models();
    let n = 200u32;
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let rm = rm.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut observed_full = 0;
            let mut observed_empty = 0;
            while !stop.load(Ordering::Acquire) {
                let s = rm.snapshot().unwrap();
                let cp = s.checkpoint("P").unwrap();
                let rows = s.scan("P", "t", &[], 10_000).unwrap();
                match cp {
                    None => {
                        assert!(rows.is_empty(), "rows before the checkpoint");
                        observed_empty += 1;
                    }
                    Some(GlobalPosition(p)) => {
                        assert_eq!(p, 1);
                        assert_eq!(rows.len(), n as usize, "checkpoint without all its rows");
                        observed_full += 1;
                    }
                }
            }
            (observed_empty, observed_full)
        })
    };
    let puts: Vec<_> = (0..n)
        .map(|i| ("t".to_string(), i.to_be_bytes().to_vec(), vec![1u8; 100]))
        .collect();
    rm.commit("P", GlobalPosition(1), puts, vec![]).unwrap();
    // one more read is guaranteed to run after the commit
    let s = rm.snapshot().unwrap();
    assert_eq!(s.scan("P", "t", &[], 10_000).unwrap().len(), n as usize);
    stop.store(true, Ordering::Release);
    let (_, _) = reader.join().unwrap();
}
