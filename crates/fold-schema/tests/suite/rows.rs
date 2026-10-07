use fold_schema::rows::{self, ColumnOp, MAX_ROW_BYTES, RowError, TruncateFrom};
use fold_schema::{Schema, Table, ValidationError};
use proptest::prelude::*;
use serde_json::{Value, json};

use super::common::{U1, U2, U3, orders, types_schema};

fn nums(s: &Schema) -> &Table {
    &s.projection("T", "P").unwrap().tables["nums"]
}

/// A complete, canonical `nums` row.
fn base_row() -> Value {
    json!({
        "i": 1, "u": 2, "d": "3.50", "oi": null, "s": "x", "se": ["a", "b"], "li": [1, 2, 3],
        "ma": {"EUR": "1.00"}, "mi": {"1": 10}, "ms": {}, "lm": [], "is": [1, 2], "od": null
    })
}

fn apply(s: &Schema, row: &Value, ops: Vec<ColumnOp>) -> Result<Value, RowError> {
    rows::apply(s, nums(s), Some(row), &ops)
}

fn set(column: &str, value: Value) -> ColumnOp {
    ColumnOp::Set {
        column: column.into(),
        value,
    }
}

fn add(column: &str, by: Value) -> ColumnOp {
    ColumnOp::Add {
        column: column.into(),
        map_key: None,
        by,
    }
}

fn add_in(column: &str, key: Value, by: Value) -> ColumnOp {
    ColumnOp::Add {
        column: column.into(),
        map_key: Some(key),
        by,
    }
}

fn set_add(column: &str, value: Value) -> ColumnOp {
    ColumnOp::SetAdd {
        column: column.into(),
        value,
    }
}

fn set_remove(column: &str, value: Value) -> ColumnOp {
    ColumnOp::SetRemove {
        column: column.into(),
        value,
    }
}

fn push(column: &str, value: Value, front: bool) -> ColumnOp {
    ColumnOp::ListPush {
        column: column.into(),
        value,
        front,
    }
}

fn list_remove(column: &str, value: Value, all: bool) -> ColumnOp {
    ColumnOp::ListRemove {
        column: column.into(),
        value,
        all,
    }
}

fn truncate(column: &str, keep: usize, from: TruncateFrom) -> ColumnOp {
    ColumnOp::ListTruncate {
        column: column.into(),
        keep,
        from,
    }
}

fn map_put(column: &str, key: Value, value: Value) -> ColumnOp {
    ColumnOp::MapPut {
        column: column.into(),
        map_key: key,
        value,
    }
}

fn map_remove(column: &str, key: Value) -> ColumnOp {
    ColumnOp::MapRemove {
        column: column.into(),
        map_key: key,
    }
}

#[test]
fn column_op_json_shape() {
    let ops: Vec<ColumnOp> = serde_json::from_value(json!([
        {"op": "set", "column": "s", "value": "y"},
        {"op": "add", "column": "i", "by": 1},
        {"op": "add", "column": "ma", "map_key": "EUR", "by": "2.50"},
        {"op": "set_add", "column": "se", "value": "c"},
        {"op": "set_remove", "column": "se", "value": "a"},
        {"op": "list_push", "column": "li", "value": 4},
        {"op": "list_push", "column": "li", "value": 0, "front": true},
        {"op": "list_remove", "column": "li", "value": 2},
        {"op": "list_remove", "column": "li", "value": 2, "all": true},
        {"op": "list_truncate", "column": "li", "keep": 5, "from": "back"},
        {"op": "map_put", "column": "mi", "map_key": 2, "value": 20},
        {"op": "map_remove", "column": "mi", "map_key": 1},
    ]))
    .unwrap();
    assert_eq!(
        ops,
        [
            set("s", json!("y")),
            add("i", json!(1)),
            add_in("ma", json!("EUR"), json!("2.50")),
            set_add("se", json!("c")),
            set_remove("se", json!("a")),
            push("li", json!(4), false),
            push("li", json!(0), true),
            list_remove("li", json!(2), false),
            list_remove("li", json!(2), true),
            truncate("li", 5, TruncateFrom::Back),
            map_put("mi", json!(2), json!(20)),
            map_remove("mi", json!(1)),
        ]
    );
    // Round trip keeps the tag and omits an absent map_key.
    let back = serde_json::to_value(&ops[1]).unwrap();
    assert_eq!(back, json!({"op": "add", "column": "i", "by": 1}));
    let back = serde_json::to_value(&ops[9]).unwrap();
    assert_eq!(
        back,
        json!({"op": "list_truncate", "column": "li", "keep": 5, "from": "back"})
    );
    assert!(serde_json::from_value::<ColumnOp>(json!({"op": "nope", "column": "x"})).is_err());
    assert!(
        serde_json::from_value::<ColumnOp>(
            json!({"op": "list_truncate", "column": "li", "keep": 1, "from": "middle"})
        )
        .is_err()
    );
}

#[test]
fn default_row_fills_what_it_can() {
    let s = types_schema();
    let p = s.projection("T", "P").unwrap();
    let d = rows::default_row(&s, &p.tables["defaults"]).unwrap();
    assert_eq!(
        d,
        json!({"i": 0, "u": 0, "d": "0", "o": null, "se": [], "li": [], "ma": {}})
    );
    assert_eq!(
        rows::default_row(&s, &p.tables["nodefault"]).unwrap_err(),
        RowError::NoDefault {
            column: "name".into()
        }
    );
    // apply with no row starts from the default row...
    let r = rows::apply(&s, &p.tables["defaults"], None, &[add("i", json!(5))]).unwrap();
    assert_eq!(r["i"], json!(5));
    assert_eq!(r["se"], json!([]));
    // ...and fails where there is none, even with ops.
    assert_eq!(
        rows::apply(&s, &p.tables["nodefault"], None, &[set("name", json!("n"))]).unwrap_err(),
        RowError::NoDefault {
            column: "name".into()
        }
    );
    // The orders example: order_totals has no default (upsert must supply it),
    // customer_orders does.
    let o = orders();
    let co = o.projection("Orders", "CustomerOrders").unwrap();
    assert_eq!(
        rows::default_row(&o, &co.tables["customer_orders"]).unwrap(),
        json!({"name": null, "open_orders": [], "recent_orders": [], "spent_by_currency": {}, "order_count": 0})
    );
    let ot = o.projection("Orders", "OrderTotals").unwrap();
    assert!(matches!(
        rows::default_row(&o, &ot.tables["order_totals"]),
        Err(RowError::NoDefault { .. })
    ));
}

#[test]
fn set_op() {
    let s = types_schema();
    let r = apply(
        &s,
        &base_row(),
        vec![
            set("s", json!("y")),
            set("oi", json!(7)),
            set("se", json!(["z", "y"])),
        ],
    )
    .unwrap();
    assert_eq!(r["s"], json!("y"));
    assert_eq!(r["oi"], json!(7));
    assert_eq!(r["se"], json!(["y", "z"]), "set values are canonicalized");
    // null only on T?
    assert_eq!(
        apply(&s, &base_row(), vec![set("oi", Value::Null)]).unwrap()["oi"],
        Value::Null
    );
    let e = apply(&s, &base_row(), vec![set("s", Value::Null)]).unwrap_err();
    assert!(
        matches!(e, RowError::InvalidValue { ref column, source: ValidationError::Missing { .. } } if column == "s"),
        "{e:?}"
    );
    // wrong value type
    let e = apply(&s, &base_row(), vec![set("i", json!("1"))]).unwrap_err();
    assert!(matches!(e, RowError::InvalidValue { .. }), "{e:?}");
    // unknown column
    assert_eq!(
        apply(&s, &base_row(), vec![set("nope", json!(1))]).unwrap_err(),
        RowError::UnknownColumn {
            column: "nope".into()
        }
    );
}

#[test]
fn add_op() {
    let s = types_schema();
    let r = apply(
        &s,
        &base_row(),
        vec![
            add("i", json!(-3)),
            add("u", json!(5)),
            add("d", json!("1.255")),
            add("oi", json!(2)),
        ],
    )
    .unwrap();
    assert_eq!(r["i"], json!(-2));
    assert_eq!(r["u"], json!(7));
    assert_eq!(r["d"], json!("4.755"), "scale of the larger operand");
    assert_eq!(r["oi"], json!(2), "a null optional number counts as 0");
    assert_eq!(
        apply(&s, &base_row(), vec![add("d", json!("1"))]).unwrap()["d"],
        json!("4.50")
    );
    assert_eq!(
        apply(&s, &base_row(), vec![add("od", json!("0.5"))]).unwrap()["od"],
        json!("0.5")
    );
    // uint below zero
    assert_eq!(
        apply(&s, &base_row(), vec![add("u", json!(-3))]).unwrap_err(),
        RowError::Underflow { column: "u".into() }
    );
    assert_eq!(
        apply(&s, &base_row(), vec![add("u", json!(-2))]).unwrap()["u"],
        json!(0)
    );
    // overflow
    assert_eq!(
        apply(&s, &base_row(), vec![add("i", json!(i64::MAX))]).unwrap_err(),
        RowError::Overflow { column: "i".into() }
    );
    assert_eq!(
        apply(&s, &base_row(), vec![add("u", json!(u64::MAX))]).unwrap_err(),
        RowError::Overflow { column: "u".into() }
    );
    // by must be of the column's kind
    assert!(matches!(
        apply(&s, &base_row(), vec![add("i", json!("1"))]).unwrap_err(),
        RowError::InvalidValue { .. }
    ));
    assert!(matches!(
        apply(&s, &base_row(), vec![add("d", json!(1))]).unwrap_err(),
        RowError::InvalidValue { .. }
    ));
    assert!(matches!(
        apply(&s, &base_row(), vec![add("i", json!(1.5))]).unwrap_err(),
        RowError::InvalidValue { .. }
    ));
    // wrong column type
    assert!(matches!(
        apply(&s, &base_row(), vec![add("s", json!(1))]).unwrap_err(),
        RowError::WrongColumnType { op: "add", .. }
    ));
    assert!(matches!(
        apply(&s, &base_row(), vec![add("li", json!(1))]).unwrap_err(),
        RowError::WrongColumnType { op: "add", .. }
    ));
}

#[test]
fn add_on_map_entries() {
    let s = types_schema();
    let r = apply(
        &s,
        &base_row(),
        vec![
            add_in("ma", json!("EUR"), json!("2.5")),
            add_in("ma", json!("USD"), json!("10.00")),
            add_in("mi", json!(1), json!(5)),
            add_in("mi", json!(2), json!(-5)),
        ],
    )
    .unwrap();
    assert_eq!(
        r["ma"],
        json!({"EUR": "3.50", "USD": "10.00"}),
        "absent entry starts at 0"
    );
    assert_eq!(
        serde_json::to_string(&r["mi"]).unwrap(),
        r#"{"1":15,"2":-5}"#
    );
    // key order is canonical
    let r = apply(
        &s,
        &base_row(),
        vec![
            add_in("mi", json!(10), json!(1)),
            add_in("mi", json!(-4), json!(1)),
        ],
    )
    .unwrap();
    assert_eq!(
        serde_json::to_string(&r["mi"]).unwrap(),
        r#"{"-4":1,"1":10,"10":1}"#
    );
    assert_eq!(
        apply(&s, &base_row(), vec![add("ma", json!("1"))]).unwrap_err(),
        RowError::MissingMapKey {
            column: "ma".into()
        }
    );
    assert_eq!(
        apply(&s, &base_row(), vec![add_in("i", json!("k"), json!(1))]).unwrap_err(),
        RowError::UnexpectedMapKey { column: "i".into() }
    );
    assert!(matches!(
        apply(&s, &base_row(), vec![add_in("ms", json!("k"), json!(1))]).unwrap_err(),
        RowError::WrongColumnType { op: "add", .. }
    ));
    // map key validated against K
    assert!(matches!(
        apply(&s, &base_row(), vec![add_in("mi", json!("x"), json!(1))]).unwrap_err(),
        RowError::InvalidValue { .. }
    ));
}

#[test]
fn set_add_and_remove() {
    let s = types_schema();
    let r = apply(
        &s,
        &base_row(),
        vec![
            set_add("se", json!("c")),
            set_add("se", json!("a")),
            set_add("se", json!("0")),
        ],
    )
    .unwrap();
    assert_eq!(r["se"], json!(["0", "a", "b", "c"]), "sorted, idempotent");
    let r = apply(
        &s,
        &base_row(),
        vec![set_remove("se", json!("a")), set_remove("se", json!("zzz"))],
    )
    .unwrap();
    assert_eq!(
        r["se"],
        json!(["b"]),
        "removing an absent element is a no-op"
    );
    let r = apply(
        &s,
        &base_row(),
        vec![set_add("is", json!(10)), set_add("is", json!(-1))],
    )
    .unwrap();
    assert_eq!(r["is"], json!([-1, 1, 2, 10]), "numeric order");
    // element type checked
    assert!(matches!(
        apply(&s, &base_row(), vec![set_add("se", json!(1))]).unwrap_err(),
        RowError::InvalidValue { .. }
    ));
    assert!(matches!(
        apply(&s, &base_row(), vec![set_remove("is", json!("1"))]).unwrap_err(),
        RowError::InvalidValue { .. }
    ));
    // wrong column type
    assert!(matches!(
        apply(&s, &base_row(), vec![set_add("li", json!(1))]).unwrap_err(),
        RowError::WrongColumnType { op: "set_add", .. }
    ));
    assert!(matches!(
        apply(&s, &base_row(), vec![set_remove("u", json!(1))]).unwrap_err(),
        RowError::WrongColumnType {
            op: "set_remove",
            ..
        }
    ));
}

#[test]
fn list_push_remove_truncate() {
    let s = types_schema();
    let r = apply(
        &s,
        &base_row(),
        vec![push("li", json!(4), false), push("li", json!(0), true)],
    )
    .unwrap();
    assert_eq!(r["li"], json!([0, 1, 2, 3, 4]));
    let r = apply(
        &s,
        &base_row(),
        vec![
            push("li", json!(2), false),
            list_remove("li", json!(2), false),
        ],
    )
    .unwrap();
    assert_eq!(r["li"], json!([1, 3, 2]), "first equal element only");
    let r = apply(
        &s,
        &base_row(),
        vec![
            push("li", json!(2), false),
            list_remove("li", json!(2), true),
        ],
    )
    .unwrap();
    assert_eq!(r["li"], json!([1, 3]), "every equal element");
    let r = apply(&s, &base_row(), vec![list_remove("li", json!(99), false)]).unwrap();
    assert_eq!(
        r["li"],
        json!([1, 2, 3]),
        "removing an absent element is a no-op"
    );
    let r = apply(
        &s,
        &base_row(),
        vec![truncate("li", 2, TruncateFrom::Front)],
    )
    .unwrap();
    assert_eq!(r["li"], json!([1, 2]));
    let r = apply(&s, &base_row(), vec![truncate("li", 2, TruncateFrom::Back)]).unwrap();
    assert_eq!(r["li"], json!([2, 3]));
    let r = apply(
        &s,
        &base_row(),
        vec![truncate("li", 10, TruncateFrom::Back)],
    )
    .unwrap();
    assert_eq!(r["li"], json!([1, 2, 3]));
    let r = apply(&s, &base_row(), vec![truncate("li", 0, TruncateFrom::Back)]).unwrap();
    assert_eq!(r["li"], json!([]));
    // values (nested records) are canonicalized and compared structurally
    let m = json!({"currency": "EUR", "amount": "1.00"});
    let r = apply(
        &s,
        &base_row(),
        vec![
            push("lm", m.clone(), false),
            push("lm", m.clone(), false),
            list_remove("lm", json!({"amount": "1.00", "currency": "EUR"}), false),
        ],
    )
    .unwrap();
    assert_eq!(r["lm"], json!([{"amount": "1.00", "currency": "EUR"}]));
    // element type checked
    assert!(matches!(
        apply(&s, &base_row(), vec![push("li", json!("x"), false)]).unwrap_err(),
        RowError::InvalidValue { .. }
    ));
    assert!(matches!(
        apply(&s, &base_row(), vec![list_remove("li", json!("x"), false)]).unwrap_err(),
        RowError::InvalidValue { .. }
    ));
    // wrong column type
    for op in [
        push("se", json!("a"), false),
        list_remove("se", json!("a"), false),
        truncate("ma", 1, TruncateFrom::Back),
    ] {
        let name = op.name();
        assert!(
            matches!(apply(&s, &base_row(), vec![op]).unwrap_err(), RowError::WrongColumnType { op, .. } if op == name),
            "{name}"
        );
    }
}

#[test]
fn map_put_and_remove() {
    let s = types_schema();
    let r = apply(
        &s,
        &base_row(),
        vec![
            map_put("ma", json!("USD"), json!("2.00")),
            map_put("ma", json!("EUR"), json!("9.99")),
            map_put("mi", json!(0), json!(0)),
            map_remove("ms", json!("absent")),
        ],
    )
    .unwrap();
    assert_eq!(
        serde_json::to_string(&r["ma"]).unwrap(),
        r#"{"EUR":"9.99","USD":"2.00"}"#,
        "replace, sorted"
    );
    assert_eq!(
        serde_json::to_string(&r["mi"]).unwrap(),
        r#"{"0":0,"1":10}"#
    );
    let r = apply(&s, &base_row(), vec![map_remove("mi", json!(1))]).unwrap();
    assert_eq!(r["mi"], json!({}));
    // key and value validated
    assert!(matches!(
        apply(&s, &base_row(), vec![map_put("mi", json!("x"), json!(1))]).unwrap_err(),
        RowError::InvalidValue { .. }
    ));
    assert!(matches!(
        apply(&s, &base_row(), vec![map_put("mi", json!(1), json!("x"))]).unwrap_err(),
        RowError::InvalidValue { .. }
    ));
    assert!(matches!(
        apply(&s, &base_row(), vec![map_remove("mi", json!("x"))]).unwrap_err(),
        RowError::InvalidValue { .. }
    ));
    // wrong column type
    assert!(matches!(
        apply(&s, &base_row(), vec![map_put("li", json!(1), json!(1))]).unwrap_err(),
        RowError::WrongColumnType { op: "map_put", .. }
    ));
    assert!(matches!(
        apply(&s, &base_row(), vec![map_remove("se", json!("a"))]).unwrap_err(),
        RowError::WrongColumnType {
            op: "map_remove",
            ..
        }
    ));
}

#[test]
fn ops_apply_in_order_and_the_result_is_validated() {
    let s = types_schema();
    let r = apply(
        &s,
        &base_row(),
        vec![
            set("i", json!(10)),
            add("i", json!(1)),
            set("i", json!(0)),
            add("i", json!(-1)),
        ],
    )
    .unwrap();
    assert_eq!(r["i"], json!(-1));
    // A starting row that does not validate is rejected.
    let mut bad = base_row();
    bad["i"] = json!("one");
    assert!(matches!(
        apply(&s, &bad, vec![]).unwrap_err(),
        RowError::InvalidRow(_)
    ));
    let mut extra = base_row();
    extra["bogus"] = json!(1);
    assert!(matches!(
        apply(&s, &extra, vec![]).unwrap_err(),
        RowError::InvalidRow(_)
    ));
    // No ops: the row comes back canonical.
    let mut unsorted = base_row();
    unsorted["se"] = json!(["b", "a"]);
    assert_eq!(
        apply(&s, &unsorted, vec![]).unwrap()["se"],
        json!(["a", "b"])
    );
}

#[test]
fn row_size_limit() {
    let s = types_schema();
    let big = "x".repeat(MAX_ROW_BYTES);
    let e = apply(&s, &base_row(), vec![set("s", json!(big))]).unwrap_err();
    assert!(
        matches!(e, RowError::TooLarge { bytes, limit } if bytes > MAX_ROW_BYTES && limit == MAX_ROW_BYTES),
        "{e}"
    );
    let fits = "x".repeat(MAX_ROW_BYTES - 1024);
    apply(&s, &base_row(), vec![set("s", json!(fits))]).unwrap();
}

#[test]
fn the_customer_orders_fold_in_ops() {
    let o = orders();
    let t = &o.projection("Orders", "CustomerOrders").unwrap().tables["customer_orders"];
    let place = |order: &str, amount: &str| {
        vec![
            set_add("open_orders", json!(order)),
            push("recent_orders", json!(order), false),
            truncate("recent_orders", 5, TruncateFrom::Back),
            add_in("spent_by_currency", json!("EUR"), json!(amount)),
            add("order_count", json!(1)),
        ]
    };
    let r1 = rows::apply(&o, t, None, &place(U1, "20.00")).unwrap();
    let r2 = rows::apply(&o, t, Some(&r1), &place(U2, "20.00")).unwrap();
    assert_eq!(
        r2,
        json!({
            "name": null,
            "open_orders": [U1, U2],
            "recent_orders": [U1, U2],
            "spent_by_currency": {"EUR": "40.00"},
            "order_count": 2
        })
    );
    let r3 = rows::apply(
        &o,
        t,
        Some(&r2),
        &[
            set("name", json!("Ada")),
            set_remove("open_orders", json!(U1)),
        ],
    )
    .unwrap();
    assert_eq!(r3["name"], json!("Ada"));
    assert_eq!(r3["open_orders"], json!([U2]));
    assert_eq!(r3["recent_orders"], json!([U1, U2]), "history is kept");
    let mut r = r3;
    for i in 0..6u8 {
        let id = format!("44444444-4444-4444-4444-44444444444{i}");
        r = rows::apply(&o, t, Some(&r), &place(&id, "1.50")).unwrap();
    }
    assert_eq!(r["recent_orders"].as_array().unwrap().len(), 5);
    assert_eq!(
        r["recent_orders"][0],
        json!("44444444-4444-4444-4444-444444444441")
    );
    assert_eq!(r["spent_by_currency"], json!({"EUR": "49.00"}));
    assert_eq!(r["order_count"], json!(8));
    let _ = U3;
}

// -- proptests ---------------------------------------------------------------

fn elem() -> impl Strategy<Value = i64> {
    -20i64..20
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn set_add_is_idempotent_and_order_independent(items in prop::collection::vec(elem(), 0..12), seed in any::<u64>()) {
        let s = types_schema();
        let once: Vec<ColumnOp> = items.iter().map(|i| set_add("is", json!(i))).collect();
        let twice: Vec<ColumnOp> = once.iter().chain(once.iter()).cloned().collect();
        let mut shuffled = items.clone();
        // deterministic shuffle from the seed
        let mut x = seed;
        for i in (1..shuffled.len()).rev() {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            shuffled.swap(i, (x % (i as u64 + 1)) as usize);
        }
        let shuffled_ops: Vec<ColumnOp> = shuffled.iter().map(|i| set_add("is", json!(i))).collect();
        let a = apply(&s, &base_row(), once).unwrap();
        let b = apply(&s, &base_row(), twice).unwrap();
        let c = apply(&s, &base_row(), shuffled_ops).unwrap();
        prop_assert_eq!(&a, &b);
        prop_assert_eq!(&a, &c);
        let mut expected: Vec<i64> = vec![1, 2];
        expected.extend(items.iter().copied());
        expected.sort_unstable();
        expected.dedup();
        prop_assert_eq!(&a["is"], &json!(expected));
    }

    #[test]
    fn set_remove_after_set_add_restores_the_row(item in elem().prop_filter("not already present", |i| *i != 1 && *i != 2)) {
        let s = types_schema();
        let r = apply(&s, &base_row(), vec![set_add("is", json!(item)), set_remove("is", json!(item))]).unwrap();
        prop_assert_eq!(r, apply(&s, &base_row(), vec![]).unwrap());
    }

    #[test]
    fn list_push_then_truncate_keeps_the_right_tail(items in prop::collection::vec(elem(), 0..12), keep in 0usize..8) {
        let s = types_schema();
        let mut ops: Vec<ColumnOp> = items.iter().map(|i| push("li", json!(i), false)).collect();
        ops.push(truncate("li", keep, TruncateFrom::Back));
        let r = apply(&s, &base_row(), ops).unwrap();
        let mut full: Vec<i64> = vec![1, 2, 3];
        full.extend(items.iter().copied());
        let tail: Vec<i64> = full[full.len().saturating_sub(keep)..].to_vec();
        prop_assert_eq!(&r["li"], &json!(tail));
        // and from the front keeps the head
        let mut ops: Vec<ColumnOp> = items.iter().map(|i| push("li", json!(i), false)).collect();
        ops.push(truncate("li", keep, TruncateFrom::Front));
        let r = apply(&s, &base_row(), ops).unwrap();
        let head: Vec<i64> = full[..keep.min(full.len())].to_vec();
        prop_assert_eq!(&r["li"], &json!(head));
    }

    #[test]
    fn map_put_then_map_remove_restores_the_row(key in elem().prop_filter("not already present", |i| *i != 1), value in elem()) {
        let s = types_schema();
        let r = apply(&s, &base_row(), vec![map_put("mi", json!(key), json!(value)), map_remove("mi", json!(key))]).unwrap();
        prop_assert_eq!(r, apply(&s, &base_row(), vec![]).unwrap());
    }
}
