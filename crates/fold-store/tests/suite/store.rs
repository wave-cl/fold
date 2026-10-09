//! The store's identity and the resets that a log moving backwards calls
//! for.

use fold_core::{GlobalPosition, StreamVersion};
use fold_store::{Checkpoint, Snapshot};
use uuid::Uuid;

use crate::common::*;

#[test]
fn checkpoint_round_trips_its_fingerprint() {
    let d = tmp();
    let rm = open(d.path());
    rm.commit("P", Checkpoint::at(4).with_event(id(77)), vec![], vec![])
        .unwrap();
    rm.commit("Q", Checkpoint::at(9), vec![], vec![]).unwrap();
    let p = rm.checkpoint("P").unwrap().unwrap();
    assert_eq!(p.next, GlobalPosition(4));
    assert_eq!(p.last_event_id, Some(id(77)));
    let q = rm.checkpoint("Q").unwrap().unwrap();
    assert_eq!(q.next, GlobalPosition(9));
    assert_eq!(q.last_event_id, None, "a nil id reads back as none");
    let all = rm.snapshot().unwrap().checkpoints().unwrap();
    assert_eq!(
        all.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        ["P", "Q"]
    );
    drop(rm);
    let rm = open(d.path());
    assert_eq!(rm.checkpoint("P").unwrap(), Some(p), "durable");
}

#[test]
fn reset_past_drops_only_checkpoints_past_the_cut_and_every_snapshot() {
    let d = tmp();
    let rm = open(d.path());
    let row = |t: &str, k: &str| (t.to_string(), k.as_bytes().to_vec(), b"r".to_vec());
    rm.commit("Behind", cp(5), vec![row("t", "a")], vec![])
        .unwrap();
    rm.commit("AtCut", cp(10), vec![row("t", "b")], vec![])
        .unwrap();
    rm.commit("Past", cp(11), vec![row("t", "c"), row("u", "d")], vec![])
        .unwrap();
    let snaps = rm.snapshots();
    let snap = Snapshot {
        version: StreamVersion(3),
        module_hash: [2; 32],
        event_id: id(3),
        state: b"s".to_vec(),
    };
    snaps.put("C.A", &sid("a-1"), snap.clone()).unwrap();
    snaps.put("C.B", &sid("b-1"), snap).unwrap();

    let report = rm.reset_past(GlobalPosition(10)).unwrap();
    assert_eq!(report.runners_reset, ["Past"]);
    assert_eq!(report.tables_dropped, 2);
    assert_eq!(report.snapshots_dropped, 2);

    let s = rm.snapshot().unwrap();
    assert_eq!(next_of(s.checkpoint("Behind").unwrap()), Some(5));
    assert_eq!(next_of(s.checkpoint("AtCut").unwrap()), Some(10));
    assert_eq!(s.checkpoint("Past").unwrap(), None);
    assert_eq!(s.get("Behind", "t", b"a").unwrap(), Some(b"r".to_vec()));
    assert_eq!(s.get("Past", "t", b"c").unwrap(), None);
    assert_eq!(s.get("Past", "u", b"d").unwrap(), None);
    assert!(snaps.list("C.A").unwrap().is_empty());
    assert!(snaps.list("C.B").unwrap().is_empty());
    // The runner past the cut can start over, and snapshots can be written.
    rm.commit("Past", cp(1), vec![row("t", "c")], vec![])
        .unwrap();
    assert_eq!(next_of(rm.checkpoint("Past").unwrap()), Some(1));
    // Control: a cut nothing is past removes nothing but the snapshots.
    let report = rm.reset_past(GlobalPosition(100)).unwrap();
    assert!(report.runners_reset.is_empty());
    assert_eq!(report.tables_dropped, 0);
}

#[test]
fn bind_to_another_log_resets_everything() {
    let d = tmp();
    let rm = open(d.path());
    assert_eq!(rm.log_id().unwrap(), None);
    let first = Uuid::now_v7();
    assert!(!rm.bind(first).unwrap(), "a fresh store just binds");
    assert_eq!(rm.log_id().unwrap(), Some(first));
    rm.set_generation(3).unwrap();
    rm.commit(
        "P",
        cp(5),
        vec![("t".into(), b"k".to_vec(), b"v".to_vec())],
        vec![],
    )
    .unwrap();
    rm.snapshots()
        .put(
            "C.A",
            &sid("a"),
            Snapshot {
                version: StreamVersion(1),
                module_hash: [0; 32],
                event_id: id(1),
                state: vec![],
            },
        )
        .unwrap();
    assert!(!rm.bind(first).unwrap(), "the same log changes nothing");
    assert_eq!(next_of(rm.checkpoint("P").unwrap()), Some(5));
    assert_eq!(rm.generation().unwrap(), Some(3));

    let second = Uuid::now_v7();
    assert!(rm.bind(second).unwrap(), "another log: reset");
    assert_eq!(rm.log_id().unwrap(), Some(second));
    assert_eq!(rm.checkpoint("P").unwrap(), None);
    assert_eq!(rm.snapshot().unwrap().get("P", "t", b"k").unwrap(), None);
    assert!(rm.snapshots().list("C.A").unwrap().is_empty());
    assert_eq!(
        rm.generation().unwrap(),
        None,
        "the generation is the old log's"
    );
    drop(rm);
    assert_eq!(open(d.path()).log_id().unwrap(), Some(second), "durable");
}
