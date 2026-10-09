use fold_schema::ast::{BaseType, Expr, Item, Literal, LocalItem, Term};
use fold_schema::{Scalar, parse};

use super::common::ORDERS;

#[test]
fn example_schema_parses() {
    let file = parse(ORDERS).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(file.contexts.len(), 4);
    let orders = &file.contexts[3];
    assert_eq!(orders.name.name, "Orders");
    let kinds: Vec<&str> = orders
        .items
        .iter()
        .map(|i| match i {
            Item::Value(_) => "value",
            Item::Enum(_) => "enum",
            Item::Event(_) => "event",
            Item::Aggregate(_) => "aggregate",
            Item::Projection(_) => "projection",
            Item::Invariant(_) => "invariant",
            Item::Process(_) => "process",
        })
        .collect();
    assert_eq!(
        kinds,
        [
            "enum",
            "event",
            "event",
            "event",
            "event",
            "aggregate",
            "process",
            "invariant",
            "projection",
            "projection"
        ]
    );
    let Item::Aggregate(order) = &orders.items[5] else {
        panic!()
    };
    assert_eq!(order.commands.len(), 4);
    assert_eq!(order.invariants.len(), 1);
    assert_eq!(order.items.len(), 2);
    assert_eq!(order.snapshot_every.as_ref().map(|s| s.value), Some(100));
    assert_eq!(
        order.evolve.export.as_ref().map(|e| e.value.as_str()),
        Some("evolve_order")
    );
    let Item::Invariant(max_open) = &orders.items[7] else {
        panic!()
    };
    assert_eq!(max_open.on.name, "Order");
    assert_eq!(max_open.scope.name, "customer_id");
    let Item::Projection(co) = &orders.items[9] else {
        panic!()
    };
    assert_eq!(co.from.len(), 3);
    assert_eq!(
        co.from[0].qualifier.as_ref().map(|q| q.name.as_str()),
        Some("Customers")
    );
    assert_eq!(co.tables.len(), 2);
    assert!(co.tables[0].fields[0].key);
    assert!(!co.tables[0].fields[1].key);
}

#[test]
fn list_sugar_and_keyword_both_parse_to_list() {
    let a = parse("context C { value V { a: [int] } }")
        .unwrap()
        .strip_spans();
    let b = parse("context C { value V { a: list<int> } }")
        .unwrap()
        .strip_spans();
    assert_eq!(a, b);
    let Item::Value(v) = &a.contexts[0].items[0] else {
        panic!()
    };
    assert!(matches!(v.fields[0].ty.base, BaseType::List(_)));
}

#[test]
fn optional_and_nested_collections() {
    let f = parse("context C { value V { a: map<string, [set<uuid>]>?, b: string? } }").unwrap();
    let Item::Value(v) = &f.contexts[0].items[0] else {
        panic!()
    };
    assert!(v.fields[0].ty.optional);
    let BaseType::Map(Scalar::String, inner) = &v.fields[0].ty.base else {
        panic!("{:?}", v.fields[0].ty.base)
    };
    let BaseType::List(inner) = &inner.base else {
        panic!()
    };
    assert_eq!(inner.base, BaseType::Set(Scalar::Uuid));
    assert!(v.fields[1].ty.optional);
}

#[test]
fn a_column_named_key_is_a_column() {
    let f = parse(
        r#"context C { projection P { from E fold wasm "w" table t { key id: uuid, key: string, key name: int } } }"#,
    )
    .unwrap();
    let Item::Projection(p) = &f.contexts[0].items[0] else {
        panic!()
    };
    let t = &p.tables[0];
    assert_eq!(
        t.fields
            .iter()
            .map(|f| (f.key, f.field.name.name.as_str()))
            .collect::<Vec<_>>(),
        [(true, "id"), (false, "key"), (true, "name")]
    );
}

#[test]
fn trailing_commas_and_empty_blocks() {
    let f = parse(
        r#"context C {
            value V { a: int, }
            value W {}
            enum E { A, B, }
            aggregate G {
              key k: uuid
              stream "g-{k}"
              entity N { id n: uuid, }
              events X
              state {}
              evolve wasm "w"
              commands C {} -> wasm "w",
            }
        }"#,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(f.contexts[0].items.len(), 4);
}

#[test]
fn spans_point_at_the_source() {
    let src = "context C {\n  value V { a: int }\n}";
    let f = parse(src).unwrap();
    let Item::Value(v) = &f.contexts[0].items[0] else {
        panic!()
    };
    assert_eq!(&src[v.name.span.start..v.name.span.end], "V");
    assert_eq!(&src[v.span.start..v.span.end], "value V { a: int }");
    assert_eq!(
        &src[v.fields[0].ty.span.start..v.fields[0].ty.span.end],
        "int"
    );
    assert_eq!(v.name.span.line_col(src), (2, 9));
}

const AGG: &str =
    r#"context A { aggregate G { key k: uuid stream "k" events E state {} evolve wasm "w" "#;

/// Malformed inputs and the exact message each must produce.
const MALFORMED: &[(&str, &str, (usize, usize))] = &[
    (
        "context A { event E v2 {} upcast v1 }",
        "expected `from`, found identifier `v1`",
        (1, 34),
    ),
    (
        "context A { event E v2 {} upcast from v1 foo }",
        "expected `{` or `wasm`, found identifier `foo`",
        (1, 42),
    ),
    (
        "context A { event E v2 {} upcast from v1 { foo } }",
        "expected `set`, `rename` or `}`, found identifier `foo`",
        (1, 44),
    ),
    (
        "context A { event E v2 {} upcast from v1 { rename a b } }",
        "expected `as`, found identifier `b`",
        (1, 53),
    ),
    (
        "context A { event E v2 {} upcast from v1 { set a 1 } }",
        "expected `:`, found integer `1`",
        (1, 50),
    ),
    (
        "context A { enum X { A { } } }",
        "expected a field name, found `}`",
        (1, 26),
    ),
    (
        "context A { value V { a: int = } }",
        "expected a number, a string, `true`, `false` or a variant name, found `}`",
        (1, 32),
    ),
    (
        "context A { /// x\n}",
        "expected `value`, `enum`, `event`, `aggregate`, `projection`, `invariant`, `process` or `}`, found doc comment",
        (1, 13),
    ),
    (
        "context A {} /// x",
        "expected `context` or end of input, found doc comment",
        (1, 14),
    ),
    (
        "context A {} //! x",
        "expected `context` or end of input, found inner doc comment `//!`",
        (1, 14),
    ),
    (
        "context A { aggregate G { key k: uuid stream \"k\" /// x\n events E state {} evolve wasm \"w\" } }",
        "expected `value`, `enum`, `entity` or `events`, found doc comment",
        (1, 50),
    ),
    (
        "context A { value V { a: int, /// x\n } }",
        "expected a field name or `}`, found doc comment",
        (1, 31),
    ),
    (
        "context",
        "expected a context name, found end of input",
        (1, 8),
    ),
    ("context A", "expected `{`, found end of input", (1, 10)),
    (
        "context A { foo }",
        "expected `value`, `enum`, `event`, `aggregate`, `projection`, `invariant`, `process` or `}`, found identifier `foo`",
        (1, 13),
    ),
    (
        "context A { value V { a } }",
        "expected `:`, found `}`",
        (1, 25),
    ),
    (
        "context A { value V { a: } }",
        "expected a type, found `}`",
        (1, 26),
    ),
    (
        "context A { value V { a: int b: int } }",
        "expected `,` or `}`, found identifier `b`",
        (1, 30),
    ),
    (
        "context A { value V { a: [int } }",
        "expected `]`, found `}`",
        (1, 31),
    ),
    (
        "context A { value V { a: set<Money> } }",
        "expected a scalar type, found identifier `Money`",
        (1, 30),
    ),
    (
        "context A { value V { a: map<string> } }",
        "expected `,`, found `>`",
        (1, 36),
    ),
    (
        "context A { value V { a: \"x\" } }",
        "expected a type, found string \"x\"",
        (1, 26),
    ),
    (
        "context A { event E { } }",
        "expected a version like `v1`, found `{`",
        (1, 21),
    ),
    (
        "context A { event E version1 { } }",
        "expected a version like `v1`, found identifier `version1`",
        (1, 21),
    ),
    (
        "context A { enum X { } }",
        "expected a variant name, found `}`",
        (1, 22),
    ),
    (
        "context A { enum X { A B } }",
        "expected `,` or `}`, found identifier `B`",
        (1, 24),
    ),
    (
        "context A { aggregate G { stream \"x\" } }",
        "expected `key`, found identifier `stream`",
        (1, 27),
    ),
    (
        "context A { aggregate G { key k: uuid events E state {} evolve wasm \"w\" } }",
        "expected `stream`, found identifier `events`",
        (1, 39),
    ),
    (
        "context A { aggregate G { key k: uuid stream \"k\" foo } }",
        "expected `value`, `enum`, `entity` or `events`, found identifier `foo`",
        (1, 50),
    ),
    (
        "context A { aggregate G { key k: uuid stream \"k\" events E state {} evolve wasm \"w\" foo } }",
        "expected `snapshot`, `commands`, `invariants` or `}`, found identifier `foo`",
        (1, 84),
    ),
    (
        "context A { aggregate G { key k: uuid stream \"k\" events E state {} evolve wasm \"w\" snapshot every 2 commands C {} -> wasm \"w\" foo } }",
        "expected `,`, `invariants` or `}`, found identifier `foo`",
        (1, 127),
    ),
    (
        "context A { aggregate G { key k: uuid stream \"k\" events E state {} evolve wasm \"w\" snapshot 2 } }",
        "expected `every`, found integer `2`",
        (1, 93),
    ),
    (
        "context A { aggregate G { key k: uuid stream \"k\" events E state {} evolve wasm \"w\" commands C {} wasm \"w\" } }",
        "expected `->`, found identifier `wasm`",
        (1, 98),
    ),
    (
        "context A { aggregate G { key k: uuid stream \"k\" entity N { n: uuid } events E state {} evolve wasm \"w\" } }",
        "expected `id`, found identifier `n`",
        (1, 61),
    ),
    (
        "context A { projection P { fold wasm \"w\" table t { key k: uuid } } }",
        "expected `from`, found identifier `fold`",
        (1, 28),
    ),
    (
        "context A { projection P { from E fold wasm \"w\" } }",
        "expected `table`, found `}`",
        (1, 49),
    ),
    (
        "context A { projection P { from E fold wasm \"w\" table t { key k: uuid } extra } }",
        "expected `table` or `}`, found identifier `extra`",
        (1, 73),
    ),
    (
        "context A { projection P { from E fold \"w\" table t { key k: uuid } } }",
        "expected `wasm`, found string \"w\"",
        (1, 40),
    ),
    (
        "context A { value V { a: int, } } extra",
        "expected `context` or end of input, found identifier `extra`",
        (1, 35),
    ),
    (
        "context A { aggregate G { key k: uuid stream \"x",
        "expected a closing `\"`, found end of the string literal",
        (1, 46),
    ),
    (
        "context A { aggregate G { key k: uuid stream \"x\\q\" } }",
        "expected an escape sequence (`\\\\`, `\\\"`, `\\n`, `\\t`, `\\r`, `\\0`, `\\u{..}`), found `\\q`",
        (1, 48),
    ),
    (
        "context A { value V { a: int } } /* open",
        "expected a closing `*/`, found end of input",
        (1, 34),
    ),
    (
        "context A { value V { a: int } } @",
        "expected a token, found `@`",
        (1, 34),
    ),
];

#[test]
fn malformed_inputs_name_what_was_expected_and_found() {
    assert!(MALFORMED.len() >= 10);
    for (src, want, (line, col)) in MALFORMED {
        let err = parse(src).expect_err(src);
        assert_eq!(err.to_string(), *want, "message for {src:?}");
        assert_eq!(
            err.span.line_col(src),
            (*line, *col),
            "position for {src:?}"
        );
    }
}

#[test]
fn a_valid_aggregate_prefix_parses_once_closed() {
    // Negative control for the fixtures built on AGG: the prefix itself is fine.
    parse(&format!("{AGG} }} }}")).unwrap();
}

#[test]
fn parse_error_display_with_one_expected() {
    let err = parse("context A {").unwrap_err();
    assert_eq!(
        err.to_string(),
        "expected `value`, `enum`, `event`, `aggregate`, `projection`, `invariant`, `process` or `}`, found end of input"
    );
    let err = parse("context 5").unwrap_err();
    assert_eq!(
        err.to_string(),
        "expected a context name, found integer `5`"
    );
}

#[test]
fn rules_parse_with_precedence_and_parentheses() {
    let src = r#"context C {
  value V { a: int, s: string, m: Money } rules {
    A: a >= 0 and s != "" or not len(s) > 3,
    B: (a < 1 or a > 9) and m.amount <= -2.50,
    C: s matches "^[A-Z]{3}$",
    D: a in [1, 2, 3], E: s in [],
  }
}"#;
    let file = parse(src).unwrap_or_else(|e| panic!("{e}"));
    let Item::Value(v) = &file.contexts[0].items[0] else {
        panic!()
    };
    assert_eq!(v.rules.len(), 5);
    // `and` binds tighter than `or`; `not` tighter than `and`.
    assert!(matches!(&v.rules[0].expr, Expr::Or(l, r)
        if matches!(**l, Expr::And(..)) && matches!(**r, Expr::Not(..))));
    assert!(matches!(&v.rules[1].expr, Expr::And(l, _) if matches!(**l, Expr::Or(..))));
    let Expr::And(_, right) = &v.rules[1].expr else {
        panic!()
    };
    let Expr::Cmp { rhs, .. } = right.as_ref() else {
        panic!()
    };
    assert!(matches!(rhs, Term::Lit(Literal::Number(t, _)) if t == "-2.50"));
    assert!(matches!(&v.rules[2].expr, Expr::Matches { .. }));
    assert!(matches!(&v.rules[3].expr, Expr::In { items, .. } if items.len() == 3));
    assert!(matches!(&v.rules[4].expr, Expr::In { items, .. } if items.is_empty()));
}

#[test]
fn malformed_rules_name_what_was_expected() {
    let err = parse("context C { value V { a: int } rules { A: a } }").unwrap_err();
    assert!(err.to_string().starts_with("expected `<`, `<=`"), "{err}");
    let err = parse(r#"context C { value V { a: int } rules { A: 3 matches "x" } }"#).unwrap_err();
    assert!(
        err.to_string().contains("field path before `matches`"),
        "{err}"
    );
    let err = parse("context C { value V { a: int } rules { A: (a > 1 } }").unwrap_err();
    assert!(err.to_string().contains("expected `)`"), "{err}");
}

#[test]
fn doc_comments_attach_to_declarations_fields_rules_and_tables() {
    let src = "//! file\n//! two\n/// ctx\ncontext C {\n  /// val\n  /// more\n  value V {\n    /// f\n    a: int,\n  } rules {\n    /// r\n    R: a > 0,\n  }\n  /// ev\n  event E v1 { ///x\n k: uuid }\n  /// agg\n  aggregate A {\n    key k: uuid\n    stream \"a-{k}\"\n    /// ent\n    entity N { /// idd\n id n: uuid, /// nf\n m: int }\n    events E\n    state {}\n    evolve wasm \"w\"\n    commands\n      /// cmd\n      Do { /// cf\n x: int } -> wasm \"w\"\n    invariants\n      /// inv\n      I -> wasm \"w\"\n  }\n  /// proj\n  projection P {\n    from E\n    fold wasm \"w\"\n    /// tbl\n    table t {\n      /// col\n      key k: uuid,\n      ///\n      n: int,\n    }\n  }\n  /// ci\n  invariant X { on A projection P scope k check wasm \"w\" }\n  /// proc\n  process Q { key k: uuid from E state {} react wasm \"w\" }\n}\n";
    let file = parse(src).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(file.docs, ["file", "two"]);
    let ctx = &file.contexts[0];
    assert_eq!(ctx.docs, ["ctx"]);
    let Item::Value(v) = &ctx.items[0] else {
        panic!()
    };
    assert_eq!(v.docs, ["val", "more"]);
    assert_eq!(v.fields[0].docs, ["f"]);
    assert_eq!(v.rules[0].docs, ["r"]);
    let Item::Event(e) = &ctx.items[1] else {
        panic!()
    };
    assert_eq!(e.docs, ["ev"]);
    assert_eq!(e.fields[0].docs, ["x"], "no space after /// is fine");
    let Item::Aggregate(a) = &ctx.items[2] else {
        panic!()
    };
    assert_eq!(a.docs, ["agg"]);
    let LocalItem::Entity(n) = &a.items[0] else {
        panic!()
    };
    assert_eq!(n.docs, ["ent"]);
    assert_eq!(n.id.docs, ["idd"]);
    assert_eq!(n.fields[0].docs, ["nf"]);
    assert_eq!(a.commands[0].docs, ["cmd"]);
    assert_eq!(a.commands[0].fields[0].docs, ["cf"]);
    assert_eq!(a.invariants[0].docs, ["inv"]);
    let Item::Projection(p) = &ctx.items[3] else {
        panic!()
    };
    assert_eq!(p.docs, ["proj"]);
    assert_eq!(p.tables[0].docs, ["tbl"]);
    assert_eq!(p.tables[0].fields[0].field.docs, ["col"]);
    assert_eq!(p.tables[0].fields[1].field.docs, [""], "an empty doc line");
    let Item::Invariant(i) = &ctx.items[4] else {
        panic!()
    };
    assert_eq!(i.docs, ["ci"]);
    let Item::Process(q) = &ctx.items[5] else {
        panic!()
    };
    assert_eq!(q.docs, ["proc"]);
    // Spans still start at the keyword, not at the docs.
    assert_eq!(&src[v.span.start..v.span.start + 5], "value");
    assert_eq!(&src[ctx.span.start..ctx.span.start + 7], "context");
}

#[test]
fn enum_payloads_and_defaults_parse() {
    let src = "context C {\n  enum Status { Pending, Shipped { carrier: string, at: timestamp }, /// gone\n Cancelled { reason: string? }, }\n  value V { qty: uint = 1, name: string = \"x\", on: bool = true, status: Status = Pending, d: decimal = -1.50 }\n}";
    let file = parse(src).unwrap_or_else(|e| panic!("{e}"));
    let Item::Enum(e) = &file.contexts[0].items[0] else {
        panic!()
    };
    let names: Vec<&str> = e.variants.iter().map(|v| v.name.name.as_str()).collect();
    assert_eq!(names, ["Pending", "Shipped", "Cancelled"]);
    assert!(e.variants[0].payload.is_none());
    let shipped = e.variants[1].payload.as_ref().unwrap();
    assert_eq!(shipped.len(), 2);
    assert_eq!(shipped[1].name.name, "at");
    assert_eq!(e.variants[2].docs, ["gone"]);
    assert!(e.variants[2].payload.as_ref().unwrap()[0].ty.optional);
    let Item::Value(v) = &file.contexts[0].items[1] else {
        panic!()
    };
    let defaults: Vec<String> = v
        .fields
        .iter()
        .map(|f| match &f.default {
            Some(Literal::Number(t, _)) => format!("num {t}"),
            Some(Literal::Str(s)) => format!("str {}", s.value),
            Some(Literal::Bool(b, _)) => format!("bool {b}"),
            Some(Literal::Variant(i)) => format!("variant {}", i.name),
            None => "none".into(),
        })
        .collect();
    assert_eq!(
        defaults,
        [
            "num 1",
            "str x",
            "bool true",
            "variant Pending",
            "num -1.50"
        ]
    );
    // The field's span covers its default.
    let f = &v.fields[4];
    assert_eq!(&src[f.span.start..f.span.end], "d: decimal = -1.50");
}

#[test]
fn upcast_clauses_parse() {
    use fold_schema::ast::{UpcastHow, UpcastOp, UpcastValue};
    let src = "context C {\n  event E v2 { k: uuid } upcast from v1 { set note: \"x\", rename a as b, set n: null, set l: [1, Red], set o: { x: 1, y: { z: true } } }\n  event E v3 { k: uuid } upcast from v2 wasm \"w\" export \"up\"\n}";
    let file = parse(src).unwrap_or_else(|e| panic!("{e}"));
    let Item::Event(e2) = &file.contexts[0].items[0] else {
        panic!()
    };
    let up = e2.upcast.as_ref().unwrap();
    assert_eq!(up.from.value, 1);
    let UpcastHow::Ops(ops) = &up.how else {
        panic!()
    };
    assert_eq!(ops.len(), 5);
    assert!(
        matches!(&ops[0], UpcastOp::Set { field, value: UpcastValue::Lit(Literal::Str(s)), .. } if field.name == "note" && s.value == "x")
    );
    assert!(
        matches!(&ops[1], UpcastOp::Rename { from, to, .. } if from.name == "a" && to.name == "b")
    );
    assert!(matches!(
        &ops[2],
        UpcastOp::Set {
            value: UpcastValue::Null(_),
            ..
        }
    ));
    let UpcastOp::Set {
        value: UpcastValue::List(items, _),
        ..
    } = &ops[3]
    else {
        panic!()
    };
    assert!(matches!(&items[1], UpcastValue::Lit(Literal::Variant(i)) if i.name == "Red"));
    let UpcastOp::Set {
        value: UpcastValue::Object(entries, _),
        ..
    } = &ops[4]
    else {
        panic!()
    };
    assert_eq!(entries[1].0.name, "y");
    assert!(matches!(&entries[1].1, UpcastValue::Object(inner, _) if inner.len() == 1));
    assert_eq!(&src[e2.span.start..e2.span.start + 5], "event");
    assert!(
        src[..e2.span.end].ends_with("} } }"),
        "the event's span covers its upcast"
    );
    let Item::Event(e3) = &file.contexts[0].items[1] else {
        panic!()
    };
    let UpcastHow::Wasm(w) = &e3.upcast.as_ref().unwrap().how else {
        panic!()
    };
    assert_eq!(w.export.as_ref().unwrap().value, "up");
}
