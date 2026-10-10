use fold_schema::ast::{BaseType, Expr, Item, Layer, LayerItem, Literal, LocalItem, Term};
use fold_schema::{Scalar, parse};

use super::common::{ORDERS_DERIVE, ORDERS_DOMAIN};

/// `src` under a `layer domain` header.
fn dom(src: &str) -> String {
    format!("layer domain\n{src}")
}

#[test]
fn example_schema_parses() {
    let domain = parse(ORDERS_DOMAIN).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(domain.layer.layer, Layer::Domain);
    assert!(domain.imports.is_empty());
    assert_eq!(domain.contexts.len(), 4);
    assert!(domain.items.is_empty());
    let orders = &domain.contexts[3];
    assert_eq!(orders.name.name, "Orders");
    let kinds: Vec<&str> = orders
        .items
        .iter()
        .map(|i| match i {
            Item::Value(_) => "value",
            Item::Enum(_) => "enum",
            Item::Event(_) => "event",
            Item::Aggregate(_) => "aggregate",
        })
        .collect();
    assert_eq!(
        kinds,
        ["enum", "event", "event", "event", "event", "aggregate"]
    );
    let Item::Aggregate(order) = &orders.items[5] else {
        panic!()
    };
    assert_eq!(order.items.len(), 2);
    assert_eq!(order.events.len(), 4);

    let derive = parse(ORDERS_DERIVE).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(derive.layer.layer, Layer::Derivation);
    assert_eq!(derive.imports[0].path.value, "domain.fold");
    assert!(derive.contexts.is_empty());
    let kinds: Vec<&str> = derive.items.iter().map(LayerItem::keyword).collect();
    assert_eq!(
        kinds,
        ["state", "state", "state", "projection", "projection"]
    );
    let LayerItem::State(order_state) = &derive.items[2] else {
        panic!()
    };
    assert_eq!(order_state.aggregate.context.name, "Orders");
    assert_eq!(order_state.aggregate.aggregate.name, "Order");
    assert_eq!(order_state.fields.len(), 4);
    assert_eq!(
        order_state.snapshot_every.as_ref().map(|s| s.value),
        Some(100)
    );
    assert_eq!(
        order_state.evolve.export.as_ref().map(|e| e.value.as_str()),
        Some("evolve_order")
    );
    let LayerItem::Projection(path, co) = &derive.items[4] else {
        panic!()
    };
    assert_eq!(
        (path.context.name.as_str(), path.name.name.as_str()),
        ("Orders", "CustomerOrders")
    );
    assert_eq!(co.name.name, "CustomerOrders");
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
fn the_layer_header_is_required_and_names_a_layer() {
    let err = parse("context C {}").unwrap_err();
    assert_eq!(
        err.to_string(),
        "expected `layer domain` or `layer derivation`, found identifier `context`"
    );
    let err = parse("//! docs\n").unwrap_err();
    assert_eq!(
        err.to_string(),
        "expected `layer domain` or `layer derivation`, found end of input"
    );
    let err = parse("layer storage\n").unwrap_err();
    assert_eq!(
        err.to_string(),
        "expected `domain` or `derivation`, found identifier `storage`"
    );
    assert_eq!(err.span.line_col("layer storage\n"), (1, 7));
    for (text, layer) in [("domain", Layer::Domain), ("derivation", Layer::Derivation)] {
        let src = format!("//! d\nlayer {text}\n");
        let f = parse(&src).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(f.layer.layer, layer);
        assert_eq!(
            &src[f.layer.span.start..f.layer.span.end],
            format!("layer {text}")
        );
        assert_eq!(f.docs, ["d"]);
    }
}

#[test]
fn the_application_layer_is_gone() {
    // Commands, invariants and processes are Rust code now: a file that
    // still declares the layer is a syntax error naming the two that exist.
    let src = "layer application\n\nimport \"derive.fold\"\n";
    let err = parse(src).unwrap_err();
    assert_eq!(
        err.to_string(),
        "expected `domain` or `derivation`, found identifier `application`"
    );
    assert_eq!(err.span.line_col(src), (1, 7));
    // Its declarations are unknown keywords in either layer.
    let err = parse("layer derivation\ncommands C.A { Do {} -> wasm \"w\" }\n").unwrap_err();
    assert_eq!(
        err.to_string(),
        "expected `import`, `state`, `projection` or end of input, found identifier `commands`"
    );
    let err = parse("layer domain\nprocess C.P { key k: uuid from E state {} react wasm \"w\" }\n")
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "expected `import`, `context` or end of input, found identifier `process`"
    );
}

#[test]
fn qualified_declarations_parse_in_any_file() {
    // The parser accepts every declaration everywhere; the layer rule
    // (S058) is the resolver's.
    let src = "layer domain\nstate C.A { n: int } evolve wasm \"w\"\nprojection C.P { from E fold wasm \"w\" table t { key k: uuid } }\n";
    let f = parse(src).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(f.items.len(), 2);
    assert_eq!(f.items[0].layer(), Layer::Derivation);
    assert_eq!(f.items[1].layer(), Layer::Derivation);
}

#[test]
fn state_declarations_parse() {
    let src = "layer derivation\n/// doc\nstate Orders.Order {\n  n: int,\n}\n  evolve wasm \"w\" export \"e\"\n  snapshot every 7\nstate Orders.Other {} evolve wasm \"w\"\n";
    let f = parse(src).unwrap_or_else(|e| panic!("{e}"));
    let LayerItem::State(a) = &f.items[0] else {
        panic!()
    };
    assert_eq!(a.docs, ["doc"]);
    assert_eq!(a.fields.len(), 1);
    assert_eq!(a.snapshot_every.as_ref().unwrap().value, 7);
    assert_eq!(
        &src[a.span.start..a.span.end],
        "state Orders.Order {\n  n: int,\n}\n  evolve wasm \"w\" export \"e\"\n  snapshot every 7"
    );
    assert_eq!(
        &src[a.aggregate.span.start..a.aggregate.span.end],
        "Orders.Order"
    );
    let LayerItem::State(b) = &f.items[1] else {
        panic!()
    };
    assert!(b.fields.is_empty());
    assert!(b.snapshot_every.is_none());
}

#[test]
fn list_sugar_and_keyword_both_parse_to_list() {
    let a = parse(&dom("context C { value V { a: [int] } }"))
        .unwrap()
        .strip_spans();
    let b = parse(&dom("context C { value V { a: list<int> } }"))
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
    let f = parse(&dom(
        "context C { value V { a: map<string, [set<uuid>]>?, b: string? } }",
    ))
    .unwrap();
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
        r#"layer derivation
projection C.P { from E fold wasm "w" table t { key id: uuid, key: string, key name: int } }"#,
    )
    .unwrap();
    let LayerItem::Projection(_, p) = &f.items[0] else {
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
        r#"layer domain
        context C {
            value V { a: int, }
            value W {}
            enum E { A, B, }
            aggregate G {
              key k: uuid
              stream "g-{k}"
              entity N { id n: uuid, }
              events X
            }
        }"#,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(f.contexts[0].items.len(), 4);
    let f = parse(
        r#"layer derivation
        state C.G { n: int, } evolve wasm "w"
        projection C.P { from X fold wasm "w" table t { key k: uuid, } }"#,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(f.items.len(), 2);
}

#[test]
fn spans_point_at_the_source() {
    let src = "layer domain\ncontext C {\n  value V { a: int }\n}";
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
    assert_eq!(v.name.span.line_col(src), (3, 9));
}

const AGG: &str = r#"layer domain
context A { aggregate G { key k: uuid stream "k" events E "#;

/// Malformed inputs and the exact message each must produce. Each sits
/// under a `layer domain` line the test adds (positions are in the input
/// as written here).
const MALFORMED: &[(&str, &str, (usize, usize))] = &[
    (
        "import shared.fold\ncontext A {}",
        "expected a file path, found identifier `shared`",
        (1, 8),
    ),
    (
        "context A {}\nimport \"b.fold\"",
        "expected `context` or end of input, found identifier `import`",
        (2, 1),
    ),
    (
        "state C.A {} evolve wasm \"w\"\nimport \"b.fold\"",
        "expected `context` or end of input, found identifier `import`",
        (2, 1),
    ),
    (
        "/// doc\nimport \"b.fold\"",
        "expected `context`, found doc comment",
        (1, 1),
    ),
    (
        "foo",
        "expected `import`, `context` or end of input, found identifier `foo`",
        (1, 1),
    ),
    (
        "state A { n: int } evolve wasm \"w\"",
        "expected `.`, found `{`",
        (1, 9),
    ),
    (
        "state A. { n: int } evolve wasm \"w\"",
        "expected an aggregate name, found `{`",
        (1, 10),
    ),
    (
        "projection P { from E fold wasm \"w\" table t { key k: uuid } }",
        "expected `.`, found `{`",
        (1, 14),
    ),
    (
        "state A.B { n: int }",
        "expected `evolve`, found end of input",
        (1, 21),
    ),
    (
        "state A.B { n: int } evolve wasm \"w\" snapshot 2",
        "expected `every`, found integer `2`",
        (1, 47),
    ),
    (
        "state A.B { n: int } evolve wasm \"w\" foo",
        "expected `context` or end of input, found identifier `foo`",
        (1, 38),
    ),
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
        "expected `value`, `enum`, `event`, `aggregate` or `}`, found doc comment",
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
        "context A { aggregate G { key k: uuid stream \"k\" /// x\n events E } }",
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
        "expected `value`, `enum`, `event`, `aggregate` or `}`, found identifier `foo`",
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
        "context A { aggregate G { key k: uuid events E } }",
        "expected `stream`, found identifier `events`",
        (1, 39),
    ),
    (
        "context A { aggregate G { key k: uuid stream \"k\" foo } }",
        "expected `value`, `enum`, `entity` or `events`, found identifier `foo`",
        (1, 50),
    ),
    (
        "context A { aggregate G { key k: uuid stream \"k\" events E state {} } }",
        "expected `}`, found identifier `state`",
        (1, 59),
    ),
    (
        "context A { aggregate G { key k: uuid stream \"k\" events E, } }",
        "expected an event name, found `}`",
        (1, 60),
    ),
    (
        "context A { aggregate G { key k: uuid stream \"k\" entity N { n: uuid } events E } }",
        "expected `id`, found identifier `n`",
        (1, 61),
    ),
    (
        "projection A.P { fold wasm \"w\" table t { key k: uuid } }",
        "expected `from`, found identifier `fold`",
        (1, 18),
    ),
    (
        "projection A.P { from E fold wasm \"w\" }",
        "expected `table`, found `}`",
        (1, 39),
    ),
    (
        "projection A.P { from E fold wasm \"w\" table t { key k: uuid } extra }",
        "expected `table` or `}`, found identifier `extra`",
        (1, 63),
    ),
    (
        "projection A.P { from E fold \"w\" table t { key k: uuid } }",
        "expected `wasm`, found string \"w\"",
        (1, 30),
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
        let full = dom(src);
        let err = parse(&full).expect_err(src);
        assert_eq!(err.to_string(), *want, "message for {src:?}");
        assert_eq!(
            err.span.line_col(&full),
            (*line + 1, *col),
            "position for {src:?}"
        );
    }
}

#[test]
fn the_expected_declarations_follow_the_files_layer() {
    let err = parse("layer derivation\nfoo").unwrap_err();
    assert_eq!(
        err.to_string(),
        "expected `import`, `state`, `projection` or end of input, found identifier `foo`"
    );
    let err = parse("layer derivation\nimport \"a.fold\"\n/// x\n").unwrap_err();
    assert_eq!(
        err.to_string(),
        "expected `state`, `projection` or end of input, found doc comment"
    );
    assert_eq!(
        err.span
            .line_col("layer derivation\nimport \"a.fold\"\n/// x\n"),
        (3, 1)
    );
}

#[test]
fn a_valid_aggregate_prefix_parses_once_closed() {
    // Negative control for the fixtures built on AGG: the prefix itself is fine.
    parse(&format!("{AGG} }} }}")).unwrap();
}

#[test]
fn parse_error_display_with_one_expected() {
    let err = parse(&dom("context A {")).unwrap_err();
    assert_eq!(
        err.to_string(),
        "expected `value`, `enum`, `event`, `aggregate` or `}`, found end of input"
    );
    let err = parse(&dom("context 5")).unwrap_err();
    assert_eq!(
        err.to_string(),
        "expected a context name, found integer `5`"
    );
}

#[test]
fn rules_parse_with_precedence_and_parentheses() {
    let src = r#"layer domain
context C {
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
    let err = parse(&dom("context C { value V { a: int } rules { A: a } }")).unwrap_err();
    assert!(err.to_string().starts_with("expected `<`, `<=`"), "{err}");
    let err = parse(&dom(
        r#"context C { value V { a: int } rules { A: 3 matches "x" } }"#,
    ))
    .unwrap_err();
    assert!(
        err.to_string().contains("field path before `matches`"),
        "{err}"
    );
    let err = parse(&dom("context C { value V { a: int } rules { A: (a > 1 } }")).unwrap_err();
    assert!(err.to_string().contains("expected `)`"), "{err}");
}

#[test]
fn doc_comments_attach_to_declarations_fields_rules_and_tables() {
    let src = "//! file\n//! two\nlayer domain\n/// ctx\ncontext C {\n  /// val\n  /// more\n  value V {\n    /// f\n    a: int,\n  } rules {\n    /// r\n    R: a > 0,\n  }\n  /// ev\n  event E v1 { ///x\n k: uuid }\n  /// agg\n  aggregate A {\n    key k: uuid\n    stream \"a-{k}\"\n    /// ent\n    entity N { /// idd\n id n: uuid, /// nf\n m: int }\n    events E\n  }\n}\n";
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
    // Spans still start at the keyword, not at the docs.
    assert_eq!(&src[v.span.start..v.span.start + 5], "value");
    assert_eq!(&src[ctx.span.start..ctx.span.start + 7], "context");

    let src = "layer derivation\n/// st\nstate C.A {\n  /// sf\n  n: int,\n}\n  evolve wasm \"w\"\n/// proj\nprojection C.P {\n  from E\n  fold wasm \"w\"\n  /// tbl\n  table t {\n    /// col\n    key k: uuid,\n    ///\n    n: int,\n  }\n}\n";
    let file = parse(src).unwrap_or_else(|e| panic!("{e}"));
    let LayerItem::State(st) = &file.items[0] else {
        panic!()
    };
    assert_eq!(st.docs, ["st"]);
    assert_eq!(st.fields[0].docs, ["sf"]);
    let LayerItem::Projection(_, p) = &file.items[1] else {
        panic!()
    };
    assert_eq!(p.docs, ["proj"]);
    assert_eq!(p.tables[0].docs, ["tbl"]);
    assert_eq!(p.tables[0].fields[0].field.docs, ["col"]);
    assert_eq!(p.tables[0].fields[1].field.docs, [""], "an empty doc line");
    assert_eq!(&src[p.span.start..p.span.start + 10], "projection");
}

#[test]
fn enum_payloads_and_defaults_parse() {
    let src = "layer domain\ncontext C {\n  enum Status { Pending, Shipped { carrier: string, at: timestamp }, /// gone\n Cancelled { reason: string? }, }\n  value V { qty: uint = 1, name: string = \"x\", on: bool = true, status: Status = Pending, d: decimal = -1.50 }\n}";
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
    let src = "layer domain\ncontext C {\n  event E v2 { k: uuid } upcast from v1 { set note: \"x\", rename a as b, set n: null, set l: [1, Red], set o: { x: 1, y: { z: true } } }\n  event E v3 { k: uuid } upcast from v2 wasm \"w\" export \"up\"\n}";
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

#[test]
fn imports_parse_before_the_contexts() {
    let src =
        "//! root\nlayer domain\nimport \"shared.fold\"\nimport \"sub/b.fold\"\n\ncontext A {}\n";
    let file = parse(src).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(file.docs, ["root"]);
    assert_eq!(
        file.imports
            .iter()
            .map(|i| i.path.value.as_str())
            .collect::<Vec<_>>(),
        ["shared.fold", "sub/b.fold"]
    );
    assert_eq!(
        &src[file.imports[0].span.start..file.imports[0].span.end],
        "import \"shared.fold\""
    );
    assert_eq!(file.contexts.len(), 1);
    // A file of imports alone parses.
    let only = parse("layer domain\nimport \"a.fold\"").unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(only.imports.len(), 1);
    assert!(only.contexts.is_empty());
}
