use fold_schema::{Scalar, Type, TypeRef, ValidationError};
use serde_json::{Value, json};

use super::common::{U1, U2, U3, field_ty, json as j, orders, types_schema};

fn errs(r: Result<(), Vec<ValidationError>>) -> Vec<ValidationError> {
    r.err().unwrap_or_default()
}

/// Per scalar: values that validate and values that do not.
#[test]
fn scalars_table() {
    let s = types_schema();
    let cases: &[(Scalar, &[&str], &[&str])] = &[
        (
            Scalar::String,
            &[r#""a""#, r#""""#],
            &["1", "true", "null", "[]", "{}"],
        ),
        (
            Scalar::Int,
            &["0", "-5", "9223372036854775807"],
            &["1.5", "\"1\"", "9223372036854775808", "true", "null"],
        ),
        (
            Scalar::Uint,
            &["0", "5", "18446744073709551615"],
            &["-1", "1.5", "\"1\"", "true"],
        ),
        (
            Scalar::Decimal,
            &[r#""12.50""#, r#""0""#, r#""-3.25""#, r#""40.00""#],
            &[
                "12.5", "12", r#""abc""#, r#""1e5""#, r#""+1""#, r#"".5""#, r#""5.""#, r#""""#,
                "true",
            ],
        ),
        (Scalar::Bool, &["true", "false"], &["1", "\"true\"", "null"]),
        (
            Scalar::Uuid,
            &[&format!("\"{U1}\"")],
            &[
                "\"11111111111111111111111111111111\"",
                "\"11111111-1111-1111-1111-11111111111G\"",
                "\"11111111-1111-1111-1111-11111111111\"",
                "1",
            ],
        ),
        (
            Scalar::Timestamp,
            &[
                r#""2024-01-02T03:04:05Z""#,
                r#""2024-01-02T03:04:05.123+02:00""#,
            ],
            &[
                r#""2024-01-02""#,
                r#""yesterday""#,
                "1704164645",
                r#""2024-01-02T03:04:05""#,
            ],
        ),
        (
            Scalar::Bytes,
            &[r#""aGVsbG8=""#, r#""""#],
            &[r#""aGVsbG8""#, r#""!!!""#, "1"],
        ),
    ];
    for (sc, oks, bads) in cases {
        let ty = Type::Scalar(*sc);
        for ok in *oks {
            assert_eq!(
                errs(s.validate_value(&ty, &j(ok))),
                [],
                "{sc} should accept {ok}"
            );
        }
        for bad in *bads {
            let e = errs(s.validate_value(&ty, &j(bad)));
            assert_eq!(e.len(), 1, "{sc} should reject {bad}: {e:?}");
            assert_eq!(e[0].path(), "$");
        }
    }
}

#[test]
fn decimal_as_json_number_is_rejected_with_a_hint() {
    let s = types_schema();
    let e = errs(s.validate_value(&Type::Scalar(Scalar::Decimal), &json!(12.5)));
    assert!(
        matches!(&e[0], ValidationError::WrongType { expected, found, .. } if expected.contains("string") && found == "a non-integer number"),
        "{e:?}"
    );
}

#[test]
fn optional_fields() {
    let s = types_schema();
    let ty = Type::Optional(Box::new(Type::Scalar(Scalar::String)));
    assert_eq!(errs(s.validate_value(&ty, &Value::Null)), []);
    assert_eq!(errs(s.validate_value(&ty, &json!("x"))), []);
    assert_eq!(errs(s.validate_value(&ty, &json!(1))).len(), 1);
    // In a record: absent and null are both fine for `opt`, not for `s`.
    let v = &s.contexts["T"].values["Everything"];
    let mut full = full_everything();
    full.as_object_mut().unwrap().remove("opt");
    assert_eq!(errs(s.validate_record(&v.fields, &full)), []);
    full["opt"] = Value::Null;
    assert_eq!(errs(s.validate_record(&v.fields, &full)), []);
    full.as_object_mut().unwrap().remove("s");
    assert_eq!(
        errs(s.validate_record(&v.fields, &full)),
        [ValidationError::Missing { path: "$.s".into() }]
    );
    full["s"] = Value::Null;
    assert_eq!(
        errs(s.validate_record(&v.fields, &full)),
        [ValidationError::Missing { path: "$.s".into() }]
    );
}

fn full_everything() -> Value {
    json!({
        "s": "x", "i": -1, "u": 2, "d": "3.50", "b": true, "id": U1, "ts": "2024-01-02T03:04:05Z",
        "by": "aGk=", "opt": "o", "li": [1, 2, 2], "se": ["b", "a"], "ma": {"2": "two", "1": "one"},
        "money": {"amount": "1.00", "currency": "EUR"}, "color": "Red",
        "nested": [{"amount": "2.00", "currency": "USD"}], "mm": {"k": [1]},
        "ts_set": ["2024-01-01T00:00:00Z"], "dec_map": {"1.5": 1}, "bool_map": {"true": 1}, "uuid_set": [U1]
    })
}

#[test]
fn list_set_and_map() {
    let s = types_schema();
    let li = field_ty(&s, "T", "Everything", "li");
    assert_eq!(
        errs(s.validate_value(&li, &json!([1, 2, 2]))),
        [],
        "lists allow duplicates"
    );
    let e = errs(s.validate_value(&li, &json!([1, "x", 2.5])));
    assert_eq!(
        e.iter().map(|e| e.path()).collect::<Vec<_>>(),
        ["$[1]", "$[2]"],
        "all element errors are reported"
    );
    assert_eq!(errs(s.validate_value(&li, &json!({}))).len(), 1);

    let se = field_ty(&s, "T", "Everything", "se");
    assert_eq!(
        errs(s.validate_value(&se, &json!(["b", "a"]))),
        [],
        "unsorted input is accepted"
    );
    assert_eq!(
        errs(s.validate_value(&se, &json!(["a", "b", "a"]))),
        [ValidationError::DuplicateElement {
            path: "$[2]".into()
        }],
        "duplicates in input payloads are rejected"
    );
    let ts_set = field_ty(&s, "T", "Everything", "ts_set");
    assert_eq!(
        errs(s.validate_value(
            &ts_set,
            &json!(["2024-01-01T00:00:00Z", "2024-01-01T01:00:00+01:00"])
        )),
        [ValidationError::DuplicateElement {
            path: "$[1]".into()
        }],
        "equal instants are duplicates"
    );

    let ma = field_ty(&s, "T", "Everything", "ma");
    assert_eq!(
        errs(s.validate_value(&ma, &json!({"2": "two", "1": "one"}))),
        []
    );
    let e = errs(s.validate_value(&ma, &json!({"x": "a", "042": "b", "3": 3})));
    assert_eq!(
        e.iter().map(|e| e.path()).collect::<Vec<_>>(),
        ["$[\"x\"]", "$[\"042\"]", "$[\"3\"]"]
    );
    assert!(matches!(e[0], ValidationError::Invalid { .. }));
    assert!(matches!(e[2], ValidationError::WrongType { .. }));
    assert_eq!(errs(s.validate_value(&ma, &json!([]))).len(), 1);

    let dec_map = field_ty(&s, "T", "Everything", "dec_map");
    assert_eq!(errs(s.validate_value(&dec_map, &json!({"1.5": 1}))), []);
    assert_eq!(
        errs(s.validate_value(&dec_map, &json!({"1.50": 1}))).len(),
        1,
        "non-normalized decimal key"
    );
    let bool_map = field_ty(&s, "T", "Everything", "bool_map");
    assert_eq!(
        errs(s.validate_value(&bool_map, &json!({"true": 1, "false": 0}))),
        []
    );
    assert_eq!(
        errs(s.validate_value(&bool_map, &json!({"yes": 1}))).len(),
        1
    );

    let mm = field_ty(&s, "T", "Everything", "mm");
    let e = errs(s.validate_value(&mm, &json!({"k": [1, "x"]})));
    assert_eq!(e[0].path(), "$[\"k\"][1]");
}

#[test]
fn nested_value_enum_and_unknown_field() {
    let s = types_schema();
    let money = field_ty(&s, "T", "Everything", "money");
    assert_eq!(
        errs(s.validate_value(&money, &json!({"amount": "1.00", "currency": "EUR"}))),
        []
    );
    let e = errs(s.validate_value(
        &money,
        &json!({"amount": 1, "currency": "EUR", "extra": true}),
    ));
    assert_eq!(
        e,
        [
            ValidationError::WrongType {
                path: "$.amount".into(),
                expected: "a decimal as a string like \"12.50\"".into(),
                found: "an integer".into(),
            },
            ValidationError::UnknownField {
                path: "$.extra".into()
            }
        ]
    );
    let nested = field_ty(&s, "T", "Everything", "nested");
    let e = errs(s.validate_value(
        &nested,
        &json!([{"amount": "1.00", "currency": "EUR"}, {"amount": "1.00"}]),
    ));
    assert_eq!(
        e,
        [ValidationError::Missing {
            path: "$[1].currency".into()
        }]
    );

    let color = field_ty(&s, "T", "Everything", "color");
    assert_eq!(errs(s.validate_value(&color, &json!("Green"))), []);
    assert_eq!(
        errs(s.validate_value(&color, &json!("Blue"))),
        [ValidationError::UnknownVariant {
            path: "$".into(),
            variant: "Blue".into(),
            enum_name: "T.Color".into()
        }]
    );
    assert_eq!(errs(s.validate_value(&color, &json!(1))).len(), 1);

    // The whole record, strict.
    let v = &s.contexts["T"].values["Everything"];
    assert_eq!(errs(s.validate_record(&v.fields, &full_everything())), []);
    let mut extra = full_everything();
    extra["bogus"] = json!(1);
    assert_eq!(
        errs(s.validate_record(&v.fields, &extra)),
        [ValidationError::UnknownField {
            path: "$.bogus".into()
        }]
    );
    assert_eq!(errs(s.validate_record(&v.fields, &json!([]))).len(), 1);
}

#[test]
fn entities_in_maps_must_be_keyed_by_their_id() {
    let s = types_schema();
    let a = s.aggregate("T", "A").unwrap();
    let good = json!({
        "lines": { U1: {"lid": U1, "n": 1}, U2: {"lid": U2, "n": 2} },
        "one": null,
        "many": [{"lid": U3, "n": 3}, {"lid": U3, "n": 4}]
    });
    assert_eq!(
        errs(s.validate_state(a, &good)),
        [],
        "lists of entities are not keyed"
    );
    let bad = json!({
        "lines": { U1: {"lid": U2, "n": 1} },
        "one": {"lid": U3, "n": 3},
        "many": []
    });
    assert_eq!(
        errs(s.validate_state(a, &bad)),
        [ValidationError::EntityKeyMismatch {
            path: format!("$.lines[\"{U1}\"]"),
            key: U1.into(),
            id: U2.into(),
        }]
    );
    // The same rule applies to events and commands.
    let e = s.latest_event_type("T", "E").unwrap();
    let payload = json!({"k": U1, "lines": { U1: {"lid": U3, "n": 1} }});
    assert!(matches!(
        errs(s.validate_event(e, &payload)).as_slice(),
        [ValidationError::EntityKeyMismatch { .. }]
    ));
    assert_eq!(
        errs(s.validate_event(e, &json!({"k": U1, "lines": {}}))),
        []
    );
    let cmd = &a.commands["Do"];
    assert_eq!(
        errs(s.validate_command(a, cmd, &json!({"l": {"lid": U1, "n": 1}}))),
        []
    );
    assert_eq!(
        errs(s.validate_command(a, cmd, &json!({"l": {"lid": U1}}))),
        [ValidationError::Missing {
            path: "$.l.n".into()
        }]
    );
    // A missing id is reported as missing, not as a mismatch.
    let e2 = errs(s.validate_state(
        a,
        &json!({"lines": { U1: {"n": 1} }, "one": null, "many": []}),
    ));
    assert_eq!(
        e2,
        [ValidationError::Missing {
            path: format!("$.lines[\"{U1}\"].lid")
        }]
    );
}

#[test]
fn rows_and_keys() {
    let s = types_schema();
    let p = s.projection("T", "P").unwrap();
    let t = &p.tables["defaults"];
    assert_eq!(
        errs(s.validate_row(
            t,
            &json!({"i": 1, "u": 2, "d": "3", "o": null, "se": [], "li": [], "ma": {}})
        )),
        []
    );
    // Keys are not part of the row.
    assert_eq!(
        errs(s.validate_row(
            t,
            &json!({"k": U1, "i": 1, "u": 2, "d": "3", "o": null, "se": [], "li": [], "ma": {}})
        )),
        [ValidationError::UnknownField { path: "$.k".into() }]
    );
    assert_eq!(errs(s.validate_key(t, &json!({"k": U1}))), []);
    assert_eq!(errs(s.validate_key(t, &json!({"k": "x"}))).len(), 1);
    assert_eq!(
        errs(s.validate_key(t, &json!({"k": U1, "i": 1}))),
        [ValidationError::UnknownField { path: "$.i".into() }]
    );
    assert_eq!(
        errs(s.validate_key(t, &json!({}))),
        [ValidationError::Missing { path: "$.k".into() }]
    );
}

#[test]
fn orders_payloads() {
    let s = orders();
    let placed = s.latest_event_type("Orders", "OrderPlaced").unwrap();
    let ok = json!({
        "order_id": U1, "customer_id": U2,
        "lines": [{"line_id": U3, "sku": "A", "qty": 2, "price": {"amount": "20.00", "currency": "EUR"}, "discount": null}],
        "total": {"amount": "40.00", "currency": "EUR"}
    });
    assert_eq!(errs(s.validate_event(placed, &ok)), []);
    let mut bad = ok.clone();
    bad["lines"][0]["discount"] = json!({"percent": -1, "reason": "x"});
    bad["lines"][0]["sku"] = json!(5);
    bad["total"]["amount"] = json!(40);
    let e = errs(s.validate_event(placed, &bad));
    assert_eq!(
        e.iter().map(|e| e.path()).collect::<Vec<_>>(),
        [
            "$.lines[0].sku",
            "$.lines[0].discount.percent",
            "$.total.amount"
        ]
    );
    let order = s.aggregate("Orders", "Order").unwrap();
    let state = json!({
        "customer_id": U2, "status": "Pending",
        "lines": { U3: {"line_id": U3, "sku": "A", "qty": 2, "price": {"amount": "20.00", "currency": "EUR"}} },
        "total": {"amount": "40.00", "currency": "EUR"}
    });
    assert_eq!(errs(s.validate_state(order, &state)), []);
    let cmd = &order.commands["CancelOrder"];
    assert_eq!(errs(s.validate_command(order, cmd, &json!({}))), []);
    assert_eq!(
        errs(s.validate_command(order, cmd, &json!({"reason": "late"}))),
        []
    );
    assert_eq!(
        errs(s.validate_command(order, cmd, &json!({"reasons": "late"}))).len(),
        1
    );
}

#[test]
fn canonicalize_sorts_dedups_and_normalizes() {
    let s = types_schema();
    let se = field_ty(&s, "T", "Everything", "se");
    assert_eq!(
        s.canonicalize(&se, &json!(["b", "a", "b"])).unwrap(),
        json!(["a", "b"])
    );
    let is = Type::Set(Scalar::Int);
    assert_eq!(
        s.canonicalize(&is, &json!([10, 9, -1, 9])).unwrap(),
        json!([-1, 9, 10]),
        "numeric order"
    );
    let uuid_set = field_ty(&s, "T", "Everything", "uuid_set");
    assert_eq!(
        s.canonicalize(&uuid_set, &json!([U2, U1])).unwrap(),
        json!([U1, U2])
    );
    let ts_set = field_ty(&s, "T", "Everything", "ts_set");
    assert_eq!(
        s.canonicalize(
            &ts_set,
            &json!(["2024-01-01T01:00:00+01:00", "2023-12-31T23:00:00Z"])
        )
        .unwrap(),
        json!(["2023-12-31T23:00:00Z", "2024-01-01T00:00:00Z"]),
        "timestamps render in UTC and sort by instant"
    );

    let ma = field_ty(&s, "T", "Everything", "ma");
    let c = s
        .canonicalize(&ma, &json!({"10": "ten", "9": "nine", "-1": "minus"}))
        .unwrap();
    assert_eq!(
        serde_json::to_string(&c).unwrap(),
        r#"{"-1":"minus","9":"nine","10":"ten"}"#
    );

    let d = Type::Scalar(Scalar::Decimal);
    assert_eq!(
        s.canonicalize(&d, &json!("40.00")).unwrap(),
        json!("40.00"),
        "scale kept as given"
    );
    assert!(s.canonicalize(&d, &json!(40)).is_err());

    // Records: fields in declared order, absent optionals as null, nested canonical.
    let v = &s.contexts["T"].values["Everything"];
    let ty = Type::Value(TypeRef::new("T", None, "Everything"));
    let mut input = full_everything();
    input.as_object_mut().unwrap().remove("opt");
    let c = s.canonicalize(&ty, &input).unwrap();
    assert_eq!(c["opt"], Value::Null);
    assert_eq!(c["se"], json!(["a", "b"]));
    assert_eq!(
        serde_json::to_string(&c["ma"]).unwrap(),
        r#"{"1":"one","2":"two"}"#
    );
    assert_eq!(
        c.as_object().unwrap().keys().cloned().collect::<Vec<_>>(),
        v.fields.iter().map(|f| f.name.clone()).collect::<Vec<_>>()
    );
    assert_eq!(
        errs(s.validate_record(&v.fields, &c)),
        [],
        "canonical form validates"
    );

    // Other errors still surface.
    let e = s.canonicalize(&se, &json!(["a", 1])).unwrap_err();
    assert_eq!(e.path(), "$[1]");
}

#[test]
fn canonical_key_strings() {
    assert_eq!(
        Scalar::Int.canonical_key_string(&json!(-42)).unwrap(),
        "-42"
    );
    assert_eq!(Scalar::Uint.canonical_key_string(&json!(42)).unwrap(), "42");
    assert_eq!(
        Scalar::Bool.canonical_key_string(&json!(true)).unwrap(),
        "true"
    );
    assert_eq!(
        Scalar::String.canonical_key_string(&json!("a b")).unwrap(),
        "a b"
    );
    assert_eq!(Scalar::Uuid.canonical_key_string(&json!(U1)).unwrap(), U1);
    assert_eq!(
        Scalar::Decimal
            .canonical_key_string(&json!("1.50"))
            .unwrap(),
        "1.5"
    );
    assert_eq!(
        Scalar::Decimal
            .canonical_key_string(&json!("0.00"))
            .unwrap(),
        "0"
    );
    assert_eq!(
        Scalar::Timestamp
            .canonical_key_string(&json!("2024-01-01T01:00:00+01:00"))
            .unwrap(),
        "2024-01-01T00:00:00Z"
    );
    assert!(Scalar::Int.canonical_key_string(&json!("42")).is_err());
    assert!(Scalar::Uuid.canonical_key_string(&json!("x")).is_err());
}
