use fold_schema::{Scalar, Schema, Type, TypeRef, ValidationError, compile};
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

// -- value rules --------------------------------------------------------------

const RULED: &str = r#"context Shared {
  value Money { amount: decimal, currency: string } rules {
    NonNegative: amount >= 0,
    Iso: currency matches "^[A-Z]{3}$",
  }
}
context C {
  enum Kind { Big, Small }
  value Line { qty: uint, price: Shared.Money, note: string?, tags: [string], kind: Kind } rules {
    HasQty: qty > 0 and qty <= 1000,
    Tagged: len(tags) <= 2 and len(note) < 5,
    Known: kind in ["Big", "Small"],
    Cheap: not price.amount > 1000.00,
  }
  event E v1 { k: uuid, lines: [Line], total: Shared.Money }
  aggregate A {
    key k: uuid
    stream "a-{k}"
    entity Item { id iid: uuid, cost: Shared.Money }
    events E
    state { items: map<uuid, Item> }
    evolve wasm "a.wasm"
  }
}
"#;

fn ruled_schema() -> Schema {
    compile(RULED).unwrap_or_else(|d| panic!("{d}"))
}

fn ruled_event(s: &Schema) -> &fold_schema::EventType {
    s.event_type("C", "E", 1).unwrap()
}

fn line(qty: u64, amount: &str, currency: &str) -> serde_json::Value {
    json!({ "qty": qty, "price": { "amount": amount, "currency": currency }, "tags": [], "kind": "Big" })
}

#[test]
fn rules_hold_for_a_well_formed_instance() {
    let s = ruled_schema();
    let payload = json!({
        "k": "00000000-0000-0000-0000-000000000001",
        "lines": [line(1, "9.99", "EUR")],
        "total": { "amount": "9.99", "currency": "EUR" },
    });
    s.validate_event(ruled_event(&s), &payload)
        .unwrap_or_else(|e| panic!("{e:?}"));
}

#[test]
fn a_violated_rule_names_the_value_the_rule_and_the_path() {
    let s = ruled_schema();
    let payload = json!({
        "k": "00000000-0000-0000-0000-000000000001",
        "lines": [line(1, "9.99", "EUR"), line(1, "-1.00", "eur"), line(0, "1.00", "EUR")],
        "total": { "amount": "0.00", "currency": "EUR" },
    });
    let errs = s.validate_event(ruled_event(&s), &payload).unwrap_err();
    let got: Vec<String> = errs.iter().map(ToString::to_string).collect();
    assert_eq!(
        got,
        [
            "$.lines[1].price: Shared.Money violates rule NonNegative",
            "$.lines[1].price: Shared.Money violates rule Iso",
            "$.lines[2]: C.Line violates rule HasQty",
        ],
        "nested values are checked where they sit"
    );
    // A record whose field failed is not also judged by its own rules: the
    // bad price above produced no HasQty-style cascade on lines[1].
    assert!(!got.iter().any(|e| e.starts_with("$.lines[1]: ")));
}

#[test]
fn rules_see_optional_fields_vacuously_and_lengths_and_sets() {
    let s = ruled_schema();
    let ty = &s.contexts["C"].values["Line"];
    let line_ty = fold_schema::Type::Value(fold_schema::TypeRef::new("C", None, "Line"));
    let _ = ty;
    // note absent: `len(note) < 5` holds vacuously.
    s.validate_value(&line_ty, &line(1, "1.00", "EUR")).unwrap();
    // note too long: Tagged fails.
    let mut v = line(1, "1.00", "EUR");
    v["note"] = json!("hello world");
    let errs = s.validate_value(&line_ty, &v).unwrap_err();
    assert_eq!(errs.len(), 1);
    assert!(
        matches!(&errs[0], ValidationError::RuleViolated { rule, .. } if rule == "Tagged"),
        "{errs:?}"
    );
    // too many tags: Tagged fails.
    let mut v = line(1, "1.00", "EUR");
    v["tags"] = json!(["a", "b", "c"]);
    assert_eq!(s.validate_value(&line_ty, &v).unwrap_err().len(), 1);
    // an enum outside the `in` list is caught by the enum itself first, so
    // test `in` with a kind the enum allows but the rule does not.
    let s2 = compile(&RULED.replace(r#"kind in ["Big", "Small"]"#, r#"kind in ["Big"]"#)).unwrap();
    let mut v = line(1, "1.00", "EUR");
    v["kind"] = json!("Small");
    let errs = s2.validate_value(&line_ty, &v).unwrap_err();
    assert!(matches!(&errs[0], ValidationError::RuleViolated { rule, .. } if rule == "Known"));
    // `not price.amount > 1000.00`
    let errs = s
        .validate_value(&line_ty, &line(1, "1000.01", "EUR"))
        .unwrap_err();
    assert!(matches!(&errs[0], ValidationError::RuleViolated { rule, .. } if rule == "Cheap"));
}

#[test]
fn rules_apply_to_values_inside_entities_and_state() {
    let s = ruled_schema();
    let agg = s.aggregate("C", "A").unwrap();
    let iid = "00000000-0000-0000-0000-000000000009";
    let ok =
        json!({ "items": { iid: { "iid": iid, "cost": { "amount": "5", "currency": "USD" } } } });
    s.validate_state(agg, &ok).unwrap();
    let bad =
        json!({ "items": { iid: { "iid": iid, "cost": { "amount": "-5", "currency": "USD" } } } });
    let errs = s.validate_state(agg, &bad).unwrap_err();
    assert_eq!(errs.len(), 1);
    assert_eq!(
        errs[0].to_string(),
        format!("$.items[\"{iid}\"].cost: Shared.Money violates rule NonNegative")
    );
}

// -- enums with payloads --------------------------------------------------------

#[test]
fn payload_enum_variants_validate_and_canonicalize() {
    let s = types_schema();
    let shape = Type::Enum(TypeRef::new("T", None, "Shape"));
    assert_eq!(s.canonicalize(&shape, &json!("Dot")).unwrap(), json!("Dot"));
    assert_eq!(
        s.canonicalize(&shape, &json!({ "Box": { "h": 2, "w": 1 } }))
            .unwrap(),
        json!({ "Box": { "w": 1, "h": 2 } }),
        "the payload is a record: canonical field order"
    );
    assert_eq!(
        s.canonicalize(&shape, &json!({ "Tag": { "label": "x", "color": "Red" } }))
            .unwrap(),
        json!({ "Tag": { "label": "x", "color": "Red" } })
    );
    let bad: &[(Value, &str)] = &[
        (
            json!("Box"),
            "$: expected an object {\"Box\": {...}}: variant `Box` of Shape carries a payload",
        ),
        (
            json!({ "Dot": {} }),
            "$: expected the string \"Dot\": variant `Dot` of Shape carries no payload",
        ),
        (
            json!({ "Nope": {} }),
            "$: `Nope` is not a variant of T.Shape",
        ),
        (json!("Nope"), "$: `Nope` is not a variant of T.Shape"),
        (
            json!({ "Box": { "w": 1 } }),
            "$.Box.h: required field is missing",
        ),
        (
            json!({ "Box": { "w": 1, "h": 2, "z": 3 } }),
            "$.Box.z: unknown field",
        ),
        (
            json!({ "Box": {}, "Dot": {} }),
            "$: expected an object with exactly one key naming a variant of Shape",
        ),
        (json!(1), "$: expected a variant of Shape"),
    ];
    for (v, want) in bad {
        let errs = s.validate_value(&shape, v).unwrap_err();
        assert!(
            errs[0].to_string().starts_with(want),
            "{v}: got {:?}, want {want}",
            errs[0].to_string()
        );
    }
}

#[test]
fn rules_compare_payload_enums_by_variant_name() {
    let s = types_schema();
    let shaped = Type::Value(TypeRef::new("T", None, "Shaped"));
    s.validate_value(&shaped, &json!({ "shape": "Dot" }))
        .unwrap();
    s.validate_value(&shaped, &json!({ "shape": { "Box": { "w": 1, "h": 1 } } }))
        .unwrap();
    let errs = s
        .validate_value(
            &shaped,
            &json!({ "shape": { "Tag": { "label": "x", "color": "Red" } } }),
        )
        .unwrap_err();
    assert!(
        matches!(&errs[0], ValidationError::RuleViolated { rule, .. } if rule == "Flat"),
        "{errs:?}"
    );
}

// -- field defaults -------------------------------------------------------------

#[test]
fn defaults_fill_absent_and_null_fields() {
    let s = types_schema();
    let ty = Type::Value(TypeRef::new("T", None, "Defaulted"));
    let want = json!({ "n": 7, "s": "", "c": "Red", "d": "2.50", "o": null });
    assert_eq!(s.canonicalize(&ty, &json!({})).unwrap(), want, "absent");
    assert_eq!(
        s.canonicalize(&ty, &json!({ "n": null, "s": null, "c": null, "d": null }))
            .unwrap(),
        want,
        "null"
    );
    assert_eq!(
        s.canonicalize(&ty, &json!({ "n": 1, "c": "Green" }))
            .unwrap(),
        json!({ "n": 1, "s": "", "c": "Green", "d": "2.50", "o": null }),
        "present values win"
    );
    s.validate_value(&ty, &json!({})).unwrap();
    // A wrong value is still wrong; a default does not paper over it.
    assert!(s.validate_value(&ty, &json!({ "n": "x" })).is_err());
}

#[test]
fn defaults_apply_inside_values_lists_maps_and_payloads() {
    let s = types_schema();
    let nested = &s.contexts["T"].values["Nested"];
    let mut v = json!({
        "inner": {},
        "many": [{ "n": 1 }, {}],
        "by": { "a": { "s": "q" } },
        "shape": { "Box": { "w": 1 } },
    });
    s.apply_defaults(&nested.fields, &mut v);
    assert_eq!(
        v,
        json!({
            "inner": { "n": 7, "s": "", "c": "Red", "d": "2.50" },
            "many": [{ "n": 1, "s": "", "c": "Red", "d": "2.50" }, { "n": 7, "s": "", "c": "Red", "d": "2.50" }],
            "by": { "a": { "n": 7, "s": "q", "c": "Red", "d": "2.50" } },
            "shape": { "Box": { "w": 1 } },
        }),
        "fills without validating: Box.h stays missing, optionals stay absent"
    );
    // Canonicalizing the same input validates the whole thing.
    let ty = Type::Value(TypeRef::new("T", None, "Nested"));
    let errs = s.validate_value(&ty, &v).unwrap_err();
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert_eq!(
        errs[0].to_string(),
        "$.shape.Box.h: required field is missing"
    );
}

#[test]
fn rules_see_defaults() {
    let s = types_schema();
    // `Pos: n >= 1` with default 0: an absent `n` violates the rule.
    let bad = Type::Value(TypeRef::new("T", None, "BadDefault"));
    let errs = s.validate_value(&bad, &json!({})).unwrap_err();
    assert!(
        matches!(&errs[0], ValidationError::RuleViolated { rule, .. } if rule == "Pos"),
        "{errs:?}"
    );
    s.validate_value(&bad, &json!({ "n": 3 })).unwrap();
}

// -- guards --------------------------------------------------------------------

#[test]
fn guards_evaluate_against_state_and_command() {
    use fold_schema::rules::{eval, eval_guard};
    let src = r#"context C {
  enum St { Open, Closed }
  event E v1 { k: uuid }
  aggregate A {
    key k: uuid
    stream "a-{k}"
    events E
    state { st: St, items: list<int>, tag: string? }
    evolve wasm "w"
    commands
      Do { n: int, st: St } requires {
        IsOpen: state.st == Open,
        Big: command.n > 0 and command.st in [Open, Closed],
        Fresh: not state exists or state.tag == "t",
        TagOk: state.tag matches "^t",
      } -> wasm "w"
    invariants Few: len(items) <= 2, NotClosedWithItems: not (st == Closed and len(items) > 0)
  }
}"#;
    let s = compile(src).unwrap_or_else(|d| panic!("{d}"));
    let a = &s.contexts["C"].aggregates["A"];
    let g = &a.commands["Do"].requires;
    let guard = |name: &str| &g.iter().find(|g| g.name == name).unwrap().expr;
    let open = json!({ "st": "Open", "items": [], "tag": "t" });
    let closed = json!({ "st": "Closed", "items": [1], "tag": null });
    let cmd = json!({ "n": 1, "st": "Closed" });

    assert!(eval_guard(guard("IsOpen"), Some(&open), &cmd));
    assert!(!eval_guard(guard("IsOpen"), Some(&closed), &cmd));
    assert!(eval_guard(guard("Big"), Some(&open), &cmd));
    assert!(!eval_guard(
        guard("Big"),
        Some(&open),
        &json!({ "n": 0, "st": "Open" })
    ));
    // Without state a required operand is absent: the comparison is false,
    // `state exists` is false, and `not state exists or ..` holds.
    assert!(!eval_guard(guard("IsOpen"), None, &cmd));
    assert!(eval_guard(guard("Fresh"), None, &cmd));
    assert!(eval_guard(guard("Fresh"), Some(&open), &cmd));
    // `tag` is optional and unset: its comparison holds vacuously.
    assert!(eval_guard(guard("Fresh"), Some(&closed), &cmd));
    assert!(!eval_guard(
        guard("Fresh"),
        Some(&json!({ "st": "Open", "items": [], "tag": "x" })),
        &cmd
    ));
    // An absent optional operand still holds vacuously, with or without state.
    assert!(eval_guard(guard("TagOk"), Some(&closed), &cmd));
    assert!(eval_guard(guard("TagOk"), Some(&open), &cmd));
    assert!(!eval_guard(
        guard("TagOk"),
        Some(&json!({ "st": "Open", "items": [], "tag": "x" })),
        &cmd
    ));
    assert!(eval_guard(guard("TagOk"), None, &cmd));

    let inv = |name: &str| match &a.invariants[name].check {
        fold_schema::InvariantCheck::Expr { expr, .. } => expr,
        _ => panic!("declarative"),
    };
    assert!(eval(inv("Few"), &open));
    assert!(!eval(
        inv("Few"),
        &json!({ "st": "Open", "items": [1, 2, 3] })
    ));
    assert!(eval(inv("NotClosedWithItems"), &open));
    assert!(!eval(inv("NotClosedWithItems"), &closed));
}
