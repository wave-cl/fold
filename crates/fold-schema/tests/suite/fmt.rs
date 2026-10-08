use fold_schema::ast::*;
use fold_schema::fmt::format;
use fold_schema::{Scalar, Span, parse};
use proptest::prelude::*;

use super::common::ORDERS;

#[test]
fn example_schema_round_trips_through_the_formatter() {
    let ast = parse(ORDERS).unwrap().strip_spans();
    let printed = format(&ast);
    let again = parse(&printed)
        .unwrap_or_else(|e| panic!("{e}\n{printed}"))
        .strip_spans();
    assert_eq!(again, ast);
    assert_eq!(format(&again), printed, "formatting is idempotent");
}

#[test]
fn canonical_layout() {
    let src = r#"context  C { value   V {a:int,b:[string]?}
      enum E{A,B} event Ev v2 {k:uuid}
      aggregate A { key k:uuid stream "a-{k}" entity N { id n: uuid, q: list<int> } events Ev state { } evolve wasm "w" export "e"
        commands Do { } -> wasm "w" }
      projection P { from Ev, C.Ev fold wasm "w" table t { key k: uuid, n: set<int> } } }"#;
    let printed = format(&parse(src).unwrap());
    let want = r#"context C {
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
    state {}
    evolve wasm "w" export "e"
    commands
      Do {} -> wasm "w"
  }

  projection P {
    from Ev, C.Ev
    fold wasm "w"
    table t {
      key k: uuid,
      n: set<int>,
    }
  }
}
"#;
    assert_eq!(printed, want);
}

#[test]
fn strings_are_escaped() {
    let src = "context C { aggregate A { key k: string stream \"a\\\"b\\\\c\\n{k}\\t\\u{e9}\" events E state {} evolve wasm \"w\" } }";
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
];

fn sp() -> Span {
    Span::default()
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

fn field() -> impl Strategy<Value = Field> {
    (ident(), ty()).prop_map(|(name, ty)| Field {
        name,
        ty,
        span: sp(),
    })
}

fn fields(max: usize) -> impl Strategy<Value = Vec<Field>> {
    prop::collection::vec(field(), 0..=max)
}

fn value_decl() -> impl Strategy<Value = ValueDecl> {
    (
        ident(),
        fields(4),
        prop::collection::vec(rule_decl(), 0..=2),
    )
        .prop_map(|(name, fields, rules)| ValueDecl {
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
            "and" | "or" | "not" | "len" | "in" | "matches" | "true" | "false"
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

fn literal() -> impl Strategy<Value = Literal> {
    prop_oneof![
        number_text().prop_map(|t| Literal::Number(t, sp())),
        str_lit().prop_map(Literal::Str),
        any::<bool>().prop_map(|b| Literal::Bool(b, sp())),
    ]
}

fn term() -> impl Strategy<Value = Term> {
    prop_oneof![
        literal().prop_map(Term::Lit),
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
    (rule_ident(), expr()).prop_map(|(name, expr)| RuleDecl {
        name,
        expr,
        span: sp(),
    })
}

fn enum_decl() -> impl Strategy<Value = EnumDecl> {
    (ident(), prop::collection::vec(ident(), 1..=4)).prop_map(|(name, variants)| EnumDecl {
        name,
        variants,
        span: sp(),
    })
}

fn event_decl() -> impl Strategy<Value = EventDecl> {
    (ident(), int_lit(u64::from(u16::MAX) + 5), fields(4)).prop_map(|(name, version, fields)| {
        EventDecl {
            name,
            version,
            fields,
            span: sp(),
        }
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
    (ident(), field(), fields(3)).prop_map(|(name, id, fields)| EntityDecl {
        name,
        id,
        fields,
        span: sp(),
    })
}

fn command_decl() -> impl Strategy<Value = CommandDecl> {
    (ident(), fields(5), wasm_ref()).prop_map(|(name, fields, handler)| CommandDecl {
        name,
        fields,
        handler,
        span: sp(),
    })
}

fn invariant_ref() -> impl Strategy<Value = InvariantRef> {
    (ident(), wasm_ref()).prop_map(|(name, check)| InvariantRef {
        name,
        check,
        span: sp(),
    })
}

fn invariant_decl() -> impl Strategy<Value = InvariantDecl> {
    (ident(), ident(), event_ref(), ident(), wasm_ref()).prop_map(
        |(name, on, projection, scope, check)| InvariantDecl {
            name,
            on,
            projection,
            scope,
            check,
            span: sp(),
        },
    )
}

fn process_decl() -> impl Strategy<Value = ProcessDecl> {
    (
        ident(),
        field(),
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
    )
        .prop_map(|(name, key, from, state, react)| ProcessDecl {
            name,
            key,
            from,
            state,
            react,
            span: sp(),
        })
}

fn local_item() -> impl Strategy<Value = LocalItem> {
    prop_oneof![
        value_decl().prop_map(LocalItem::Value),
        enum_decl().prop_map(LocalItem::Enum),
        entity_decl().prop_map(LocalItem::Entity),
    ]
}

fn aggregate_decl() -> impl Strategy<Value = AggregateDecl> {
    (
        ident(),
        field(),
        str_lit(),
        prop::collection::vec(local_item(), 0..=3),
        prop::collection::vec(event_ref(), 1..=3),
        fields(4),
        wasm_ref(),
        prop::option::of(int_lit(1 << 40)),
        prop::collection::vec(command_decl(), 0..=3),
        prop::collection::vec(invariant_ref(), 0..=2),
    )
        .prop_map(
            |(
                name,
                key,
                stream,
                items,
                events,
                state,
                evolve,
                snapshot_every,
                commands,
                invariants,
            )| {
                AggregateDecl {
                    name,
                    key,
                    stream,
                    items,
                    events,
                    state,
                    evolve,
                    snapshot_every,
                    commands,
                    invariants,
                    span: sp(),
                }
            },
        )
}

fn table_decl() -> impl Strategy<Value = TableDecl> {
    (
        ident(),
        prop::collection::vec(
            (any::<bool>(), field()).prop_map(|(key, field)| TableField { key, field }),
            0..=4,
        ),
    )
        .prop_map(|(name, fields)| TableDecl {
            name,
            fields,
            span: sp(),
        })
}

fn projection_decl() -> impl Strategy<Value = ProjectionDecl> {
    (
        ident(),
        prop::collection::vec(event_ref(), 1..=3),
        wasm_ref(),
        prop::option::of(int_lit(1 << 40)),
        prop::collection::vec(table_decl(), 1..=2),
    )
        .prop_map(
            |(name, from, fold, snapshot_every, tables)| ProjectionDecl {
                name,
                from,
                fold,
                snapshot_every,
                tables,
                span: sp(),
            },
        )
}

fn item() -> impl Strategy<Value = Item> {
    prop_oneof![
        value_decl().prop_map(Item::Value),
        enum_decl().prop_map(Item::Enum),
        event_decl().prop_map(Item::Event),
        aggregate_decl().prop_map(|a| Item::Aggregate(Box::new(a))),
        projection_decl().prop_map(Item::Projection),
        invariant_decl().prop_map(Item::Invariant),
        process_decl().prop_map(Item::Process),
    ]
}

fn file() -> impl Strategy<Value = File> {
    prop::collection::vec(
        (ident(), prop::collection::vec(item(), 0..=4)).prop_map(|(name, items)| Context {
            name,
            items,
            span: sp(),
        }),
        0..=3,
    )
    .prop_map(|contexts| File { contexts })
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
