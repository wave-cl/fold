//! Aggregate instance snapshots: a cache keyed by aggregate and stream,
//! carrying the id of the event at its version.

use fold_core::StreamVersion;
use fold_store::Snapshot;

use crate::common::*;

fn snap(version: u64, hash: u8, state: &str) -> Snapshot {
    Snapshot {
        version: StreamVersion(version),
        module_hash: [hash; 32],
        event_id: id(u128::from(version) + 100),
        state: state.as_bytes().to_vec(),
    }
}

#[test]
fn put_get_overwrite_reopen() {
    let d = tmp();
    let store = open(d.path()).snapshots();
    let a = sid("order-a");
    let b = sid("order-b");

    assert_eq!(store.get("Order", &a).unwrap(), None);
    store
        .put("Order", &a, snap(4, 0xaa, r#"{"status":"Pending"}"#))
        .unwrap();
    assert_eq!(
        store.get("Order", &a).unwrap(),
        Some(snap(4, 0xaa, r#"{"status":"Pending"}"#))
    );
    // keyed by aggregate and stream
    assert_eq!(store.get("Order", &b).unwrap(), None);
    assert_eq!(store.get("Customer", &a).unwrap(), None);

    // overwrite replaces, including the module hash
    store
        .put("Order", &a, snap(9, 0xbb, r#"{"status":"Paid"}"#))
        .unwrap();
    assert_eq!(
        store.get("Order", &a).unwrap(),
        Some(snap(9, 0xbb, r#"{"status":"Paid"}"#))
    );
    // idempotent
    store
        .put("Order", &a, snap(9, 0xbb, r#"{"status":"Paid"}"#))
        .unwrap();
    assert_eq!(
        store.get("Order", &a).unwrap(),
        Some(snap(9, 0xbb, r#"{"status":"Paid"}"#))
    );

    store.put("Order", &b, snap(0, 0, "")).unwrap();
    assert_eq!(store.get("Order", &b).unwrap(), Some(snap(0, 0, "")));
    drop(store);

    let store = open(d.path()).snapshots();
    assert_eq!(
        store.get("Order", &a).unwrap(),
        Some(snap(9, 0xbb, r#"{"status":"Paid"}"#))
    );
    assert_eq!(store.get("Order", &b).unwrap(), Some(snap(0, 0, "")));
    assert!(store.delete("Order", &a).unwrap());
    assert!(!store.delete("Order", &a).unwrap());
    assert_eq!(store.get("Order", &a).unwrap(), None);
    assert_eq!(store.get("Order", &b).unwrap(), Some(snap(0, 0, "")));
}

#[test]
fn list_put_many_and_clear_cover_one_aggregate_only() {
    let d = tmp();
    let st = open(d.path()).snapshots();
    let snap = |v: u64| Snapshot {
        version: StreamVersion(v),
        module_hash: [1u8; 32],
        event_id: id(u128::from(v)),
        state: format!("{{\"v\":{v}}}").into_bytes(),
    };
    st.put_many("C.A", vec![(sid("a-2"), snap(2)), (sid("a-1"), snap(1))])
        .unwrap();
    st.put("C.B", &sid("b-1"), snap(9)).unwrap();

    let listed = st.list("C.A").unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|(s, p)| (s.to_string(), p.version.0))
            .collect::<Vec<_>>(),
        [("a-1".to_string(), 1), ("a-2".to_string(), 2)],
        "key order, this aggregate only"
    );
    assert_eq!(st.clear("C.A").unwrap(), 2);
    assert!(st.list("C.A").unwrap().is_empty());
    assert_eq!(
        st.get("C.B", &sid("b-1")).unwrap().unwrap().version.0,
        9,
        "untouched"
    );
    assert_eq!(st.clear("C.A").unwrap(), 0);
}
