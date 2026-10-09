use fold_schema::ast::*;
use fold_schema::fmt::{format, format_source};
use fold_schema::lexer::{TokenKind, lex, lex_with_comments};
use fold_schema::{Scalar, Span, parse};
use proptest::prelude::*;

use super::common::{ORDERS_APP, ORDERS_DERIVE, ORDERS_DOMAIN};

const EXAMPLE_FILES: [&str; 3] = [ORDERS_DOMAIN, ORDERS_DERIVE, ORDERS_APP];

#[test]
fn example_schema_round_trips_through_the_formatter() {
    for src in EXAMPLE_FILES {
        let ast = parse(src).unwrap().strip_spans();
        let printed = format(&ast);
        let again = parse(&printed)
            .unwrap_or_else(|e| panic!("{e}\n{printed}"))
            .strip_spans();
        assert_eq!(again, ast);
        assert_eq!(format(&again), printed, "formatting is idempotent");
    }
}

#[test]
fn canonical_layout() {
    let src = r#"layer   domain
    context  C { value   V {a:int,b:[string]?}
      enum E{A,B} event Ev v2 {k:uuid}
      aggregate A { key k:uuid stream "a-{k}" entity N { id n: uuid, q: list<int> } events Ev } }"#;
    let printed = format(&parse(src).unwrap());
    let want = r#"layer domain

context C {
  value V {
    a: int,
    b: [string]?,
  }

  enum E { A, B }

  event Ev v2 {
    k: uuid,
  }

  aggregate A {
    key k: uuid
    stream "a-{k}"

    entity N {
      id n: uuid,
      q: [int],
    }

    events Ev
  }
}
"#;
    assert_eq!(printed, want);

    let src = r#"layer derivation import "d.fold"
      state C.A { } evolve wasm "w" export "e"
      state C.B { n: int, m: string } evolve wasm "w" snapshot every 5
      projection C.P { from Ev, C.Ev fold wasm "w" table t { key k: uuid, n: set<int> } }"#;
    let printed = format(&parse(src).unwrap());
    let want = r#"layer derivation

import "d.fold"

state C.A {}
  evolve wasm "w" export "e"

state C.B {
  n: int,
  m: string,
}
  evolve wasm "w"
  snapshot every 5

projection C.P {
  from Ev, C.Ev
  fold wasm "w"
  table t {
    key k: uuid,
    n: set<int>,
  }
}
"#;
    assert_eq!(printed, want);

    let src = r#"layer application
      commands C.A { Do { } -> wasm "w", Undo { x: int } requires { Pos: command.x > 0 } -> wasm "w" export "u" }
      invariants C.A { Small: n < 10, Checked -> wasm "w" }
      invariant C.X { on A projection P scope k check wasm "w" }
      process C.Q { key k: uuid from Ev, C.Ev by k state { n: int } react wasm "w" timers T }"#;
    let printed = format(&parse(src).unwrap());
    let want = r#"layer application

commands C.A {
  Do {} -> wasm "w",
  Undo { x: int } requires { Pos: command.x > 0 } -> wasm "w" export "u"
}

invariants C.A {
  Small: n < 10,
  Checked -> wasm "w"
}

invariant C.X {
  on A
  projection P
  scope k
  check wasm "w"
}

process C.Q {
  key k: uuid
  from Ev, C.Ev by k
  state {
    n: int,
  }
  react wasm "w"
  timers T
}
"#;
    assert_eq!(printed, want);
}

#[test]
fn doc_comments_print_before_their_nodes() {
    let src = "//! file\n//!\nlayer domain\ncontext C {\n  /// value\n  value V {\n    /// the field\n    a: int,\n  } rules {\n    /// positive\n    R: a > 0,\n  }\n}\n";
    let printed = format(&parse(src).unwrap());
    let want = r#"//! file
//!

layer domain

context C {
  /// value
  value V {
    /// the field
    a: int,
  } rules {
    /// positive
    R: a > 0,
  }
}
"#;
    assert_eq!(printed, want);
}

#[test]
fn comments_are_preserved_and_formatting_is_idempotent() {
    for src in EXAMPLE_FILES {
        let out = format_source(src).unwrap();
        let (_, comments) = lex_with_comments(src).unwrap();
        assert!(!comments.is_empty(), "the example has comments to keep");
        for c in &comments {
            assert!(out.contains(c.text.trim_end()), "lost {:?}:\n{out}", c.text);
        }
        assert_eq!(
            parse(&out).unwrap().strip_spans(),
            parse(src).unwrap().strip_spans()
        );
        assert_eq!(format_source(&out).unwrap(), out, "idempotent");
    }
}

#[test]
fn trailing_comments_stay_on_their_line() {
    let src = "layer domain\ncontext C {\n  value V {\n    a: int,  // first\n    b: int, // second\n    // dangling\n  }\n}\n";
    let out = format_source(src).unwrap();
    let want = r#"layer domain

context C {
  value V {
    a: int,  // first
    b: int,  // second
    // dangling
  }
}
"#;
    assert_eq!(out, want);
    assert_eq!(format_source(&out).unwrap(), out);
}

#[test]
fn comments_inside_inline_constructs_force_block_form() {
    let src = "layer domain\ncontext C {\n  enum E { A, // a\n B }\n  aggregate G {\n    key k: uuid\n    stream \"g-{k}\"\n    events E, // one\n      F\n  }\n}\n";
    let out = format_source(src).unwrap();
    let want = r#"layer domain

context C {
  enum E {
    A,  // a
    B,
  }

  aggregate G {
    key k: uuid
    stream "g-{k}"

    events E,  // one
      F
  }
}
"#;
    assert_eq!(out, want);
    assert_eq!(format_source(&out).unwrap(), out);

    let src = "layer application\ncommands C.G { Do { x: int, /* why */ y: int } -> wasm \"w\" }\n";
    let out = format_source(src).unwrap();
    let want = r#"layer application

commands C.G {
  Do {
    x: int,  /* why */
    y: int,
  } -> wasm "w"
}
"#;
    assert_eq!(out, want);
    assert_eq!(format_source(&out).unwrap(), out);
}

#[test]
fn defaults_and_payload_variants_print_canonically() {
    let src = r#"layer domain
context C {
  enum Status { Pending, Shipped { carrier: string, at: timestamp }, /// gone
   Cancelled { reason: string?, by: string, note: string, code: uint, extra: [string] } }
  value V { qty: uint = 1, name: string = "x", on: bool = true, status: Status = Pending, d: decimal = 1.50 }
}"#;
    let printed = format(&parse(src).unwrap());
    let want = r#"layer domain

context C {
  enum Status {
    Pending,
    Shipped { carrier: string, at: timestamp },
    /// gone
    Cancelled {
      reason: string?,
      by: string,
      note: string,
      code: uint,
      extra: [string],
    },
  }

  value V {
    qty: uint = 1,
    name: string = "x",
    on: bool = true,
    status: Status = Pending,
    d: decimal = 1.50,
  }
}
"#;
    assert_eq!(printed, want);
    assert_eq!(
        parse(&printed).unwrap().strip_spans(),
        parse(src).unwrap().strip_spans()
    );
    let src = "layer application\ncommands C.A { Do { n: int = -2 } -> wasm \"w\" }";
    let printed = format(&parse(src).unwrap());
    assert_eq!(
        printed,
        "layer application\n\ncommands C.A {\n  Do { n: int = -2 } -> wasm \"w\"\n}\n"
    );
}

#[test]
fn upcast_clauses_print_canonically() {
    let src = r#"layer domain
context C {
  event E v1 { k: uuid, cust: string }
  event E v2 { k: uuid, customer: string, note: string, tags: [string], m: Shared.Money, n: int? }
    upcast from v1 { set note: "legacy", rename cust as customer, set tags: ["a", "b"], set m: { amount: 1.50, currency: "EUR" }, set n: null, }
  event E v3 { k: uuid } upcast from v2 wasm "w" export "up3"
  event E v4 { k: uuid } upcast from v3 {}
}"#;
    let printed = format(&parse(src).unwrap());
    let want = r#"layer domain

context C {
  event E v1 {
    k: uuid,
    cust: string,
  }

  event E v2 {
    k: uuid,
    customer: string,
    note: string,
    tags: [string],
    m: Shared.Money,
    n: int?,
  } upcast from v1 {
    set note: "legacy",
    rename cust as customer,
    set tags: ["a", "b"],
    set m: { amount: 1.50, currency: "EUR" },
    set n: null,
  }

  event E v3 {
    k: uuid,
  } upcast from v2 wasm "w" export "up3"

  event E v4 {
    k: uuid,
  } upcast from v3 {}
}
"#;
    assert_eq!(printed, want);
    assert_eq!(
        parse(&printed).unwrap().strip_spans(),
        parse(src).unwrap().strip_spans()
    );
}

#[test]
fn strings_are_escaped() {
    let src = "layer domain\ncontext C { aggregate A { key k: string stream \"a\\\"b\\\\c\\n{k}\\t\\u{e9}\" events E } }";
    let ast = parse(src).unwrap();
    let Item::Aggregate(a) = &ast.contexts[0].items[0] else {
        panic!()
    };
    assert_eq!(a.stream.value, "a\"b\\c\n{k}\té");
    let printed = format(&ast);
    assert!(
        printed.contains("stream \"a\\\"b\\\\c\\n{k}\\té\""),
        "{printed}"
    );
    assert_eq!(parse(&printed).unwrap().strip_spans(), ast.strip_spans());
}

// -- proptest: parse(format(ast)) == ast ------------------------------------------

const KEYWORDS: &[&str] = &[
    "layer",
    "domain",
    "derivation",
    "application",
    "invariants",
    "invariant",
    "process",
    "check",
    "scope",
    "on",
    "react",
    "by",
    "rules",
    "and",
    "or",
    "not",
    "in",
    "matches",
    "len",
    "true",
    "false",
    "context",
    "value",
    "enum",
    "event",
    "aggregate",
    "projection",
    "entity",
    "key",
    "stream",
    "events",
    "state",
    "evolve",
    "wasm",
    "export",
    "snapshot",
    "every",
    "commands",
    "from",
    "fold",
    "table",
    "id",
    "list",
    "set",
    "map",
    "string",
    "int",
    "uint",
    "decimal",
    "bool",
    "uuid",
    "timestamp",
    "bytes",
    "upcast",
    "import",
    "timers",
    "requires",
    "exists",
    "rename",
    "as",
    "null",
];

fn sp() -> Span {
    Span::default()
}

/// Doc comment lines: printable text, possibly empty or starting with a
/// space (the printer and lexer keep both as they are).
fn docs() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec("[ -~]{0,16}", 0..=2)
}

fn ident() -> impl Strategy<Value = Ident> {
    "[a-zA-Z_][a-zA-Z0-9_]{0,7}"
        .prop_filter("not a keyword", |s| !KEYWORDS.contains(&s.as_str()))
        .prop_map(|name| Ident { name, span: sp() })
}

fn str_lit() -> impl Strategy<Value = StrLit> {
    "[ -~\u{e9}\u{1F600}\n\t\r\0]{0,12}".prop_map(|value| StrLit { value, span: sp() })
}

fn int_lit(max: u64) -> impl Strategy<Value = IntLit> {
    (0..=max).prop_map(|value| IntLit { value, span: sp() })
}

fn scalar() -> impl Strategy<Value = Scalar> {
    prop::sample::select(Scalar::ALL.to_vec())
}

fn type_ref() -> impl Strategy<Value = TypeRefSyntax> {
    (prop::option::of(ident()), ident())
        .prop_map(|(qualifier, name)| TypeRefSyntax { qualifier, name })
}

fn ty() -> impl Strategy<Value = Type> {
    let leaf = prop_oneof![
        scalar().prop_map(BaseType::Scalar),
        type_ref().prop_map(BaseType::Ref),
        scalar().prop_map(BaseType::Set),
    ];
    let base = leaf.prop_recursive(3, 12, 2, |inner| {
        let wrap = inner.prop_map(|base| Type {
            base,
            optional: false,
            span: sp(),
        });
        prop_oneof![
            wrap.clone().prop_map(|t| BaseType::List(Box::new(t))),
            (scalar(), wrap).prop_map(|(k, v)| BaseType::Map(k, Box::new(v))),
        ]
    });
    (base, any::<bool>()).prop_map(|(base, optional)| Type {
        base,
        optional,
        span: sp(),
    })
}

/// A field default: a number, string, bool or bare variant name.
fn default_literal() -> impl Strategy<Value = Literal> {
    prop_oneof![
        number_text().prop_map(|t| Literal::Number(t, sp())),
        str_lit().prop_map(Literal::Str),
        any::<bool>().prop_map(|b| Literal::Bool(b, sp())),
        rule_ident().prop_map(Literal::Variant),
    ]
}

fn field() -> impl Strategy<Value = Field> {
    (docs(), ident(), ty(), prop::option::of(default_literal())).prop_map(
        |(docs, name, ty, default)| Field {
            docs,
            name,
            ty,
            default,
            span: sp(),
        },
    )
}

fn fields(max: usize) -> impl Strategy<Value = Vec<Field>> {
    prop::collection::vec(field(), 0..=max)
}

fn value_decl() -> impl Strategy<Value = ValueDecl> {
    (
        docs(),
        ident(),
        fields(4),
        prop::collection::vec(rule_decl(), 0..=2),
    )
        .prop_map(|(docs, name, fields, rules)| ValueDecl {
            docs,
            name,
            fields,
            rules,
            span: sp(),
        })
}

/// Identifiers that are not contextual keywords inside a rule.
fn rule_ident() -> impl Strategy<Value = Ident> {
    ident().prop_filter("rule keyword", |i| {
        !matches!(
            i.name.as_str(),
            "and"
                | "or"
                | "not"
                | "len"
                | "in"
                | "matches"
                | "true"
                | "false"
                | "null"
                | "exists"
                | "requires"
        )
    })
}

fn field_path() -> impl Strategy<Value = FieldPath> {
    prop::collection::vec(rule_ident(), 1..=3).prop_map(|segments| FieldPath {
        segments,
        span: sp(),
    })
}

fn number_text() -> impl Strategy<Value = String> {
    (any::<bool>(), 0u32..1000, prop::option::of("[0-9]{1,3}")).prop_map(|(neg, int, frac)| {
        let mut s = String::new();
        if neg {
            s.push('-');
        }
        s.push_str(&int.to_string());
        if let Some(f) = frac {
            s.push('.');
            s.push_str(&f);
        }
        s
    })
}

/// Literals that can stand as a comparison term (a bare variant would
/// parse as a path there).
fn term_literal() -> impl Strategy<Value = Literal> {
    prop_oneof![
        number_text().prop_map(|t| Literal::Number(t, sp())),
        str_lit().prop_map(Literal::Str),
        any::<bool>().prop_map(|b| Literal::Bool(b, sp())),
    ]
}

/// Literals inside `in [...]`, where a bare variant name is one.
fn literal() -> impl Strategy<Value = Literal> {
    prop_oneof![term_literal(), rule_ident().prop_map(Literal::Variant)]
}

fn term() -> impl Strategy<Value = Term> {
    prop_oneof![
        term_literal().prop_map(Term::Lit),
        field_path().prop_map(Term::Path),
        field_path().prop_map(|p| Term::Len(p, sp())),
    ]
}

fn cmp_op() -> impl Strategy<Value = CmpOp> {
    prop_oneof![
        Just(CmpOp::Lt),
        Just(CmpOp::Le),
        Just(CmpOp::Gt),
        Just(CmpOp::Ge),
        Just(CmpOp::Eq),
        Just(CmpOp::Ne),
    ]
}

fn leaf_expr() -> impl Strategy<Value = Expr> {
    prop_oneof![
        (term(), cmp_op(), term()).prop_map(|(lhs, op, rhs)| Expr::Cmp {
            lhs,
            op,
            rhs,
            span: sp()
        }),
        (field_path(), str_lit()).prop_map(|(path, pattern)| Expr::Matches {
            path,
            pattern,
            span: sp()
        }),
        (field_path(), prop::collection::vec(literal(), 0..=3)).prop_map(|(path, items)| {
            Expr::In {
                path,
                items,
                span: sp(),
            }
        }),
        rule_ident().prop_map(|root| Expr::Exists { root, span: sp() }),
    ]
}

fn expr() -> impl Strategy<Value = Expr> {
    leaf_expr().prop_recursive(3, 16, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::Or(Box::new(a), Box::new(b))),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::And(Box::new(a), Box::new(b))),
            inner.prop_map(|a| Expr::Not(Box::new(a))),
        ]
    })
}

fn rule_decl() -> impl Strategy<Value = RuleDecl> {
    (docs(), rule_ident(), expr()).prop_map(|(docs, name, expr)| RuleDecl {
        docs,
        name,
        expr,
        span: sp(),
    })
}

fn variant() -> impl Strategy<Value = Variant> {
    (
        docs(),
        ident(),
        prop::option::of(prop::collection::vec(field(), 1..=3)),
    )
        .prop_map(|(docs, name, payload)| Variant {
            docs,
            name,
            payload,
            span: sp(),
        })
}

fn enum_decl() -> impl Strategy<Value = EnumDecl> {
    (docs(), ident(), prop::collection::vec(variant(), 1..=4)).prop_map(|(docs, name, variants)| {
        EnumDecl {
            docs,
            name,
            variants,
            span: sp(),
        }
    })
}

fn upcast_value() -> impl Strategy<Value = UpcastValue> {
    let leaf = prop_oneof![
        default_literal().prop_map(UpcastValue::Lit),
        Just(UpcastValue::Null(sp())),
    ];
    leaf.prop_recursive(2, 8, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..=3).prop_map(|v| UpcastValue::List(v, sp())),
            prop::collection::vec((rule_ident(), inner), 0..=3)
                .prop_map(|v| UpcastValue::Object(v, sp())),
        ]
    })
}

fn upcast_decl() -> impl Strategy<Value = UpcastDecl> {
    let op = prop_oneof![
        (rule_ident(), upcast_value()).prop_map(|(field, value)| UpcastOp::Set {
            field,
            value,
            span: sp()
        }),
        (rule_ident(), rule_ident()).prop_map(|(from, to)| UpcastOp::Rename {
            from,
            to,
            span: sp()
        }),
    ];
    let how = prop_oneof![
        prop::collection::vec(op, 0..=3).prop_map(UpcastHow::Ops),
        wasm_ref().prop_map(UpcastHow::Wasm),
    ];
    (int_lit(u64::from(u16::MAX) + 5), how).prop_map(|(from, how)| UpcastDecl {
        from,
        how,
        span: sp(),
    })
}

fn event_decl() -> impl Strategy<Value = EventDecl> {
    (
        docs(),
        ident(),
        int_lit(u64::from(u16::MAX) + 5),
        fields(4),
        prop::option::of(upcast_decl()),
    )
        .prop_map(|(docs, name, version, fields, upcast)| EventDecl {
            docs,
            name,
            version,
            fields,
            upcast,
            span: sp(),
        })
}

fn wasm_ref() -> impl Strategy<Value = WasmRef> {
    (str_lit(), prop::option::of(str_lit())).prop_map(|(module, export)| WasmRef {
        module,
        export,
        span: sp(),
    })
}

fn event_ref() -> impl Strategy<Value = EventRef> {
    (prop::option::of(ident()), ident()).prop_map(|(qualifier, name)| EventRef {
        qualifier,
        name,
        span: sp(),
    })
}

fn entity_decl() -> impl Strategy<Value = EntityDecl> {
    (docs(), ident(), field(), fields(3)).prop_map(|(docs, name, id, fields)| EntityDecl {
        docs,
        name,
        id,
        fields,
        span: sp(),
    })
}

fn command_decl() -> impl Strategy<Value = CommandDecl> {
    (
        docs(),
        ident(),
        fields(5),
        prop::collection::vec(rule_decl(), 0..=2),
        wasm_ref(),
    )
        .prop_map(|(docs, name, fields, requires, handler)| CommandDecl {
            docs,
            name,
            fields,
            requires,
            handler,
            span: sp(),
        })
}

fn invariant_check() -> impl Strategy<Value = InvariantCheckSyntax> {
    prop_oneof![
        wasm_ref().prop_map(InvariantCheckSyntax::Wasm),
        expr().prop_map(InvariantCheckSyntax::Expr),
    ]
}

fn invariant_ref() -> impl Strategy<Value = InvariantRef> {
    (docs(), ident(), invariant_check()).prop_map(|(docs, name, check)| InvariantRef {
        docs,
        name,
        check,
        span: sp(),
    })
}

fn agg_path() -> impl Strategy<Value = AggPath> {
    (ident(), ident()).prop_map(|(context, aggregate)| AggPath {
        context,
        aggregate,
        span: sp(),
    })
}

fn ctx_path() -> impl Strategy<Value = CtxPath> {
    (ident(), ident()).prop_map(|(context, name)| CtxPath {
        context,
        name,
        span: sp(),
    })
}

fn state_decl() -> impl Strategy<Value = StateDecl> {
    (
        docs(),
        agg_path(),
        fields(4),
        wasm_ref(),
        prop::option::of(int_lit(1 << 40)),
    )
        .prop_map(
            |(docs, aggregate, fields, evolve, snapshot_every)| StateDecl {
                docs,
                aggregate,
                fields,
                evolve,
                snapshot_every,
                span: sp(),
            },
        )
}

fn commands_decl() -> impl Strategy<Value = CommandsDecl> {
    (
        docs(),
        agg_path(),
        prop::collection::vec(command_decl(), 0..=3),
    )
        .prop_map(|(docs, aggregate, commands)| CommandsDecl {
            docs,
            aggregate,
            commands,
            span: sp(),
        })
}

fn invariants_decl() -> impl Strategy<Value = InvariantsDecl> {
    (
        docs(),
        agg_path(),
        prop::collection::vec(invariant_ref(), 0..=2),
    )
        .prop_map(|(docs, aggregate, invariants)| InvariantsDecl {
            docs,
            aggregate,
            invariants,
            span: sp(),
        })
}

/// `invariant Ctx.Name { .. }`: the path's name doubles as the decl's.
fn invariant_decl() -> impl Strategy<Value = (CtxPath, InvariantDecl)> {
    (
        docs(),
        ctx_path(),
        ident(),
        event_ref(),
        ident(),
        wasm_ref(),
    )
        .prop_map(|(docs, path, on, projection, scope, check)| {
            let name = path.name.clone();
            (
                path,
                InvariantDecl {
                    docs,
                    name,
                    on,
                    projection,
                    scope,
                    check,
                    span: sp(),
                },
            )
        })
}

fn process_decl() -> impl Strategy<Value = (CtxPath, ProcessDecl)> {
    (
        docs(),
        ctx_path(),
        key_field(),
        prop::collection::vec(
            (event_ref(), prop::option::of(ident())).prop_map(|(event, by)| ProcessSource {
                event,
                by,
                span: sp(),
            }),
            1..=3,
        ),
        fields(4),
        wasm_ref(),
        prop::option::of(int_lit(1 << 40)),
        prop::collection::vec(ident(), 0..=2),
    )
        .prop_map(
            |(docs, path, key, from, state, react, snapshot_every, timers)| {
                let name = path.name.clone();
                (
                    path,
                    ProcessDecl {
                        docs,
                        name,
                        key,
                        from,
                        state,
                        react,
                        snapshot_every,
                        timers,
                        span: sp(),
                    },
                )
            },
        )
}

fn local_item() -> impl Strategy<Value = LocalItem> {
    prop_oneof![
        value_decl().prop_map(LocalItem::Value),
        enum_decl().prop_map(LocalItem::Enum),
        entity_decl().prop_map(|e| LocalItem::Entity(Box::new(e))),
    ]
}

/// `key k: T` carries no docs (nothing can precede the keyword).
fn key_field() -> impl Strategy<Value = Field> {
    field().prop_map(|mut f| {
        f.docs.clear();
        f
    })
}

fn aggregate_decl() -> impl Strategy<Value = AggregateDecl> {
    (
        docs(),
        ident(),
        key_field(),
        str_lit(),
        prop::collection::vec(local_item(), 0..=3),
        prop::collection::vec(event_ref(), 1..=3),
    )
        .prop_map(|(docs, name, key, stream, items, events)| AggregateDecl {
            docs,
            name,
            key,
            stream,
            items,
            events,
            span: sp(),
        })
}

fn table_decl() -> impl Strategy<Value = TableDecl> {
    (
        docs(),
        ident(),
        prop::collection::vec(
            (any::<bool>(), field()).prop_map(|(key, field)| TableField { key, field }),
            0..=4,
        ),
    )
        .prop_map(|(docs, name, fields)| TableDecl {
            docs,
            name,
            fields,
            span: sp(),
        })
}

fn projection_decl() -> impl Strategy<Value = (CtxPath, ProjectionDecl)> {
    (
        docs(),
        ctx_path(),
        prop::collection::vec(event_ref(), 1..=3),
        wasm_ref(),
        prop::option::of(int_lit(1 << 40)),
        prop::collection::vec(table_decl(), 1..=2),
    )
        .prop_map(|(docs, path, from, fold, snapshot_every, tables)| {
            let name = path.name.clone();
            (
                path,
                ProjectionDecl {
                    docs,
                    name,
                    from,
                    fold,
                    snapshot_every,
                    tables,
                    span: sp(),
                },
            )
        })
}

fn item() -> impl Strategy<Value = Item> {
    prop_oneof![
        value_decl().prop_map(Item::Value),
        enum_decl().prop_map(Item::Enum),
        event_decl().prop_map(Item::Event),
        aggregate_decl().prop_map(|a| Item::Aggregate(Box::new(a))),
    ]
}

fn layer_item() -> impl Strategy<Value = LayerItem> {
    prop_oneof![
        state_decl().prop_map(LayerItem::State),
        projection_decl().prop_map(|(p, d)| LayerItem::Projection(p, d)),
        commands_decl().prop_map(LayerItem::Commands),
        invariants_decl().prop_map(LayerItem::Invariants),
        invariant_decl().prop_map(|(p, d)| LayerItem::Invariant(p, d)),
        process_decl().prop_map(|(p, d)| LayerItem::Process(p, d)),
    ]
}

fn layer() -> impl Strategy<Value = LayerDecl> {
    prop::sample::select(vec![Layer::Domain, Layer::Derivation, Layer::Application])
        .prop_map(|layer| LayerDecl { layer, span: sp() })
}

fn import() -> impl Strategy<Value = Import> {
    str_lit().prop_map(|path| Import { path, span: sp() })
}

/// Any file the grammar admits: the layer rule (S058) is the resolver's,
/// so a file may mix contexts and items of every layer here.
fn file() -> impl Strategy<Value = File> {
    (
        docs(),
        layer(),
        prop::collection::vec(import(), 0..=2),
        prop::collection::vec(
            (docs(), ident(), prop::collection::vec(item(), 0..=4)).prop_map(
                |(docs, name, items)| Context {
                    docs,
                    name,
                    items,
                    span: sp(),
                },
            ),
            0..=2,
        ),
        prop::collection::vec(layer_item(), 0..=3),
    )
        .prop_map(|(docs, layer, imports, contexts, items)| File {
            docs,
            layer,
            imports,
            contexts,
            items,
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn parse_of_format_is_identity_modulo_spans(ast in file()) {
        let printed = format(&ast);
        let parsed = parse(&printed).map_err(|e| TestCaseError::fail(format!("{e}\n---\n{printed}")))?;
        prop_assert_eq!(parsed.strip_spans(), ast);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Comments dropped between any tokens of a formatted file all survive
    /// a reformat, which changes nothing else and is idempotent.
    #[test]
    fn comments_survive_formatting(ast in file(), picks in prop::collection::vec((0usize..64, any::<bool>()), 0..=6)) {
        let printed = format(&ast);
        // Doc comments run to the end of their line, so nothing can follow
        // them on it; every other token end is a spot between tokens.
        let toks: Vec<_> = lex(&printed)
            .map_err(|e| TestCaseError::fail(e.to_string()))?
            .into_iter()
            .filter(|t| !matches!(t.kind, TokenKind::Doc(_) | TokenKind::InnerDoc(_)))
            .collect();
        // Insert from the back so earlier offsets stay valid.
        let mut spots: Vec<(usize, bool)> = picks
            .iter()
            .map(|(i, block)| (toks[i % toks.len()].span.end, *block))
            .collect();
        spots.sort_by_key(|s| std::cmp::Reverse(s.0));
        spots.dedup_by_key(|s| s.0);
        let mut src = printed.clone();
        let mut names = Vec::new();
        for (n, (at, block)) in spots.iter().enumerate() {
            let name = format!("c{n}");
            let text = if *block { format!(" /* {name} */ ") } else { format!(" // {name}\n") };
            src.insert_str(*at, &text);
            names.push(name);
        }
        let out = format_source(&src).map_err(|e| TestCaseError::fail(format!("{e}\n---\n{src}")))?;
        for name in &names {
            prop_assert!(out.contains(name.as_str()), "lost {name}:\n{src}\n---\n{out}");
        }
        let parsed = parse(&out).map_err(|e| TestCaseError::fail(format!("{e}\n---\n{out}")))?;
        prop_assert_eq!(parsed.strip_spans(), ast.clone());
        let again = format_source(&out).map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(again, out);
    }
}

#[test]
fn imports_are_printed_after_the_file_docs_with_their_comments() {
    let src = "//! Root.\nlayer domain // which\n\n// the shared types\nimport   \"shared.fold\"\nimport \"sub/b.fold\" // local\ncontext A {}\n";
    let out = format_source(src).unwrap();
    assert_eq!(
        out,
        "//! Root.\n\nlayer domain  // which\n\n// the shared types\nimport \"shared.fold\"\nimport \"sub/b.fold\"  // local\n\ncontext A {\n}\n"
    );
    assert_eq!(format_source(&out).unwrap(), out, "idempotent");
}
