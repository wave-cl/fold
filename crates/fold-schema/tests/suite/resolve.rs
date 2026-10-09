use fold_schema::{Diagnostics, Scalar, Type, compile};

use super::common::{ORDERS, line_of, orders};

/// A minimal valid schema; every fixture below is a one-rule mutation of it.
const BASE: &str = r#"context C {
  value V { a: int }
  enum En { X, Y }
  event E v1 { k: uuid, v: V }
  aggregate A {
    key k: uuid
    stream "a-{k}"
    value LV { b: int }
    entity Ent { id eid: uuid, n: int }
    events E
    state { lines: map<uuid, Ent> }
    evolve wasm "a.wasm"
    commands Do { e: Ent } -> wasm "a.wasm"
  }
  projection P {
    from E
    fold wasm "a.wasm"
    table t { key k: uuid, n: int }
  }
}
"#;

#[test]
fn base_fixture_compiles() {
    compile(BASE).unwrap_or_else(|d| panic!("{d}"));
}

fn diags(src: &str) -> Diagnostics {
    match compile(src) {
        Ok(_) => panic!("expected diagnostics for:\n{src}"),
        Err(d) => d,
    }
}

/// Assert that `src` fails with exactly `expected`, each a (code, needle)
/// pair where the diagnostic must sit on the line containing `needle`.
fn check(src: &str, expected: &[(&str, &str)]) {
    let d = diags(src);
    let got: Vec<(&str, usize)> = d.iter().map(|x| (x.code, x.span.line_col(src).0)).collect();
    let want: Vec<(&str, usize)> = expected
        .iter()
        .map(|(code, needle)| (*code, line_of(src, needle)))
        .collect();
    assert_eq!(got, want, "diagnostics for:\n{src}\n{d}");
}

#[test]
fn s001_duplicate_type_name() {
    check(
        &BASE.replace(
            "enum En { X, Y }",
            "enum En { X, Y }\n  value En { z: int }",
        ),
        &[("S001", "value En")],
    );
    check(
        &BASE.replace(
            "value LV { b: int }",
            "value LV { b: int }\n    entity LV { id q: uuid }",
        ),
        &[("S001", "entity LV")],
    );
}

#[test]
fn s002_local_type_shadows_context_type() {
    check(
        &BASE.replace("value LV { b: int }", "value V { b: int }"),
        &[("S002", "value V { b: int }")],
    );
}

#[test]
fn s003_duplicate_event_version() {
    check(
        &BASE.replace(
            "event E v1 { k: uuid, v: V }",
            "event E v1 { k: uuid, v: V }\n  event E v1 { k: uuid }",
        ),
        &[("S003", "event E v1 { k: uuid }")],
    );
    // Another version of the same family is fine.
    compile(&BASE.replace(
        "event E v1 { k: uuid, v: V }",
        "event E v1 { k: uuid, v: V }\n  event E v2 { k: uuid }",
    ))
    .unwrap_or_else(|d| panic!("{d}"));
}

#[test]
fn s004_duplicate_aggregate() {
    let src = BASE.replace(
        "  projection P {",
        "  aggregate A {\n    key k: uuid\n    stream \"b-{k}\"\n    events E\n    state {}\n    evolve wasm \"a.wasm\"\n  }\n  projection P {",
    );
    let d = diags(&src);
    assert_eq!(d.codes(), ["S004"]);
    // The second declaration is the duplicate: the one right before `stream "b-{k}"`.
    assert_eq!(
        d[0].span.line_col(&src).0,
        line_of(&src, "stream \"b-{k}\"") - 2
    );
}

#[test]
fn s005_duplicate_projection() {
    let src = format!(
        "{}  projection P {{\n    from E\n    fold wasm \"b.wasm\"\n    table u {{ key k: uuid }}\n  }}\n}}\n",
        BASE.strip_suffix("}\n").unwrap()
    );
    let d = diags(&src);
    assert_eq!(d.codes(), ["S005"]);
    assert_eq!(
        d[0].span.line_col(&src).0,
        line_of(&src, "fold wasm \"b.wasm\"") - 2
    );
}

#[test]
fn s006_duplicate_command() {
    check(
        &BASE.replace(
            "commands Do { e: Ent } -> wasm \"a.wasm\"",
            "commands Do { e: Ent } -> wasm \"a.wasm\",\n      Do { } -> wasm \"a.wasm\"",
        ),
        &[("S006", "Do { } ->")],
    );
}

#[test]
fn s007_duplicate_field() {
    check(
        &BASE.replace("value V { a: int }", "value V { a: int, a: string }"),
        &[("S007", "a: string")],
    );
    check(
        &BASE.replace(
            "entity Ent { id eid: uuid, n: int }",
            "entity Ent { id eid: uuid, eid: int }",
        ),
        &[("S007", "eid: int")],
    );
    check(
        &BASE.replace(
            "table t { key k: uuid, n: int }",
            "table t { key k: uuid, k: int }",
        ),
        &[("S007", "k: int }")],
    );
}

#[test]
fn s008_duplicate_variant() {
    check(
        &BASE.replace("enum En { X, Y }", "enum En { X, Y, X }"),
        &[("S008", "X, Y, X")],
    );
}

#[test]
fn s009_duplicate_table() {
    check(
        &BASE.replace(
            "table t { key k: uuid, n: int }",
            "table t { key k: uuid, n: int }\n    table t { key k: uuid }",
        ),
        &[("S009", "table t { key k: uuid }")],
    );
}

#[test]
fn s010_duplicate_context() {
    let src = format!("{BASE}context C {{ }}\n");
    check(&src, &[("S010", "context C { }")]);
}

#[test]
fn s011_unresolved_type() {
    check(
        &BASE.replace("value V { a: int }", "value V { a: Nope }"),
        &[("S011", "a: Nope")],
    );
    check(
        &BASE.replace("value V { a: int }", "value V { a: Nope.X }"),
        &[("S011", "a: Nope.X")],
    );
    check(
        &BASE.replace("value V { a: int }", "value V { a: C.Nope }"),
        &[("S011", "a: C.Nope")],
    );
    check(
        &BASE.replace("value V { a: int }", "value V { a: A.Nope }"),
        &[("S011", "a: A.Nope")],
    );
    // Aggregate-local types are not visible unqualified outside the aggregate.
    check(
        &BASE.replace(
            "event E v1 { k: uuid, v: V }",
            "event E v1 { k: uuid, v: Ent }",
        ),
        &[("S011", "v: Ent")],
    );
}

#[test]
fn s012_ambiguous_qualifier() {
    let src = format!("{BASE}context A {{ value W {{ x: int }} }}\n")
        .replace("value V { a: int }", "value V { a: A.LV }");
    let d = diags(&src);
    assert_eq!(d.codes(), ["S012"]);
    assert_eq!(d[0].span.line_col(&src).0, line_of(&src, "a: A.LV"));
}

#[test]
fn s013_entity_misplaced() {
    // in a projection table
    check(
        &BASE.replace(
            "table t { key k: uuid, n: int }",
            "table t { key k: uuid, n: A.Ent }",
        ),
        &[("S013", "n: A.Ent")],
    );
    // in another aggregate
    let src = BASE.replace(
        "  projection P {",
        "  aggregate B {\n    key k: uuid\n    stream \"b-{k}\"\n    events E\n    state { e: A.Ent }\n    evolve wasm \"a.wasm\"\n  }\n  projection P {",
    );
    // B also lists E, which A owns: that is S023 on both, plus the S013.
    let d = diags(&src);
    assert_eq!(d.codes(), ["S023", "S023", "S013"]);
    assert_eq!(
        d[2].span.line_col(&src).0,
        line_of(&src, "state { e: A.Ent }")
    );
    // in an event the aggregate does not list
    check(
        &BASE.replace(
            "event E v1 { k: uuid, v: V }",
            "event E v1 { k: uuid, v: V }\n  event F v1 { e: A.Ent }",
        ),
        &[("S013", "event F v1")],
    );
}

#[test]
fn s014_local_value_misplaced() {
    check(
        &BASE.replace("value V { a: int }", "value V { a: A.LV }"),
        &[("S014", "a: A.LV")],
    );
    check(
        &BASE.replace(
            "table t { key k: uuid, n: int }",
            "table t { key k: uuid, n: A.LV }",
        ),
        &[("S014", "n: A.LV")],
    );
    check(
        &BASE.replace(
            "event E v1 { k: uuid, v: V }",
            "event E v1 { k: uuid, v: V }\n  event F v1 { e: A.LV }",
        ),
        &[("S014", "event F v1")],
    );
    // Local enums follow the same rule.
    check(
        &BASE
            .replace(
                "value LV { b: int }",
                "value LV { b: int }\n    enum LE { P, Q }",
            )
            .replace("value V { a: int }", "value V { a: A.LE }"),
        &[("S014", "a: A.LE")],
    );
    // Allowed: in the aggregate's own events and commands.
    compile(
        &BASE
            .replace(
                "event E v1 { k: uuid, v: V }",
                "event E v1 { k: uuid, v: A.LV }",
            )
            .replace("commands Do { e: Ent }", "commands Do { e: Ent, l: LV }"),
    )
    .unwrap_or_else(|d| panic!("{d}"));
}

#[test]
fn s015_value_contains_entity() {
    check(
        &BASE.replace("value V { a: int }", "value V { a: A.Ent }"),
        &[("S015", "a: A.Ent")],
    );
    check(
        &BASE.replace("value LV { b: int }", "value LV { b: [Ent] }"),
        &[("S015", "b: [Ent]")],
    );
    check(
        &BASE.replace("value LV { b: int }", "value LV { b: map<uuid, Ent?> }"),
        &[("S015", "b: map<uuid, Ent?>")],
    );
}

#[test]
fn s016_cycles() {
    check(
        &BASE.replace(
            "value V { a: int }",
            "value V { a: W }\n  value W { a: V? }",
        ),
        &[("S016", "value V { a: W }")],
    );
    check(
        &BASE.replace(
            "entity Ent { id eid: uuid, n: int }",
            "entity Ent { id eid: uuid, child: [Ent] }",
        ),
        &[("S016", "entity Ent")],
    );
    // A cycle through a map value counts too; one report per cycle.
    check(
        &BASE.replace(
            "value V { a: int }",
            "value V { a: map<string, W> }\n  value W { a: X }\n  value X { a: V }",
        ),
        &[("S016", "value V { a: map<string, W> }")],
    );
}

#[test]
fn s017_map_key_must_be_entity_id_type() {
    check(
        &BASE.replace(
            "state { lines: map<uuid, Ent> }",
            "state { lines: map<string, Ent> }",
        ),
        &[("S017", "map<string, Ent>")],
    );
    check(
        &BASE.replace(
            "event E v1 { k: uuid, v: V }",
            "event E v1 { k: uuid, v: V, m: map<int, A.Ent> }",
        ),
        &[("S017", "map<int, A.Ent>")],
    );
}

#[test]
fn s018_entity_id_must_be_scalar() {
    check(
        &BASE.replace(
            "entity Ent { id eid: uuid, n: int }",
            "entity Ent { id eid: V, n: int }",
        ),
        &[("S018", "id eid: V")],
    );
    check(
        &BASE.replace(
            "entity Ent { id eid: uuid, n: int }",
            "entity Ent { id eid: uuid?, n: int }",
        ),
        &[("S018", "id eid: uuid?")],
    );
}

#[test]
fn s019_aggregate_key_type() {
    for bad in [
        "decimal",
        "bool",
        "timestamp",
        "bytes",
        "V",
        "uuid?",
        "[uuid]",
    ] {
        let src = BASE
            .replace("key k: uuid\n", &format!("key k: {bad}\n"))
            .replace(
                "event E v1 { k: uuid, v: V }",
                &format!("event E v1 {{ k: {bad}, v: V }}"),
            )
            .replace("state { lines: map<uuid, Ent> }", "state { }");
        let d = diags(&src);
        assert!(d.codes().contains(&"S019"), "{bad}: {d}");
        assert_eq!(
            d.iter()
                .find(|x| x.code == "S019")
                .unwrap()
                .span
                .line_col(&src)
                .0,
            line_of(&src, &format!("key k: {bad}")),
            "{bad}"
        );
    }
    for ok in ["uuid", "string", "int", "uint"] {
        let src = BASE
            .replace("key k: uuid\n", &format!("key k: {ok}\n"))
            .replace(
                "event E v1 { k: uuid, v: V }",
                &format!("event E v1 {{ k: {ok}, v: V }}"),
            );
        compile(&src).unwrap_or_else(|d| panic!("{ok}: {d}"));
    }
}

#[test]
fn s020_stream_template() {
    for bad in [
        "a",
        "a-{other}",
        "{k}-{k}",
        "a-{k",
        "a-}",
        "a-{}",
        "a-{k k}",
    ] {
        check(
            &BASE.replace("stream \"a-{k}\"", &format!("stream \"{bad}\"")),
            &[("S020", "stream \"")],
        );
    }
}

#[test]
fn s021_aggregate_event_ref() {
    check(
        &BASE.replace("events E\n", "events E, Nope\n"),
        &[("S021", "events E, Nope")],
    );
    let src = format!("{BASE}context D {{ event E v1 {{ k: uuid }} }}\n")
        .replace("events E\n", "events D.E\n");
    check(&src, &[("S021", "events D.E")]);
    check(
        &BASE.replace("events E\n", "events Zzz.E\n"),
        &[("S021", "events Zzz.E")],
    );
    // Qualifying with the own context is allowed.
    compile(&BASE.replace("events E\n", "events C.E\n")).unwrap_or_else(|d| panic!("{d}"));
}

#[test]
fn s022_aggregate_event_key_field() {
    check(
        &BASE.replace("event E v1 { k: uuid, v: V }", "event E v1 { v: V }"),
        &[("S022", "events E")],
    );
    check(
        &BASE.replace(
            "event E v1 { k: uuid, v: V }",
            "event E v1 { k: string, v: V }",
        ),
        &[("S022", "events E")],
    );
    check(
        &BASE.replace(
            "event E v1 { k: uuid, v: V }",
            "event E v1 { k: uuid?, v: V }",
        ),
        &[("S022", "events E")],
    );
    // Every version is checked.
    check(
        &BASE.replace(
            "event E v1 { k: uuid, v: V }",
            "event E v1 { k: uuid, v: V }\n  event E v2 { v: V }",
        ),
        &[("S022", "events E")],
    );
}

#[test]
fn s023_event_owned_by_two_aggregates() {
    let src = BASE.replace(
        "  projection P {",
        "  aggregate B {\n    key k: uuid\n    stream \"b-{k}\"\n    events E\n    state {}\n    evolve wasm \"a.wasm\"\n  }\n  projection P {",
    );
    let d = diags(&src);
    assert_eq!(d.codes(), ["S023", "S023"]);
    // Reported on both aggregates' `events` lines.
    let lines: Vec<usize> = d.iter().map(|x| x.span.line_col(&src).0).collect();
    let a_events = line_of(&src, "stream \"a-{k}\"") + 3;
    let b_events = line_of(&src, "stream \"b-{k}\"") + 1;
    assert_eq!(lines, [a_events, b_events]);
}

#[test]
fn s024_wasm_paths() {
    for bad in ["/abs.wasm", "../up.wasm", "x/../y.wasm", "", "C:\\x.wasm"] {
        let escaped = bad.replace('\\', "\\\\");
        check(
            &BASE.replace(
                "evolve wasm \"a.wasm\"",
                &format!("evolve wasm \"{escaped}\""),
            ),
            &[("S024", "evolve wasm")],
        );
    }
    check(
        &BASE.replace("fold wasm \"a.wasm\"", "fold wasm \"../a.wasm\""),
        &[("S024", "fold wasm")],
    );
    check(
        &BASE.replace("-> wasm \"a.wasm\"", "-> wasm \"/a.wasm\""),
        &[("S024", "-> wasm")],
    );
    compile(&BASE.replace("evolve wasm \"a.wasm\"", "evolve wasm \"sub/dir/a.wasm\""))
        .unwrap_or_else(|d| panic!("{d}"));
}

#[test]
fn s025_projection_from() {
    check(
        &BASE.replace("from E\n", "from E, Nope\n"),
        &[("S025", "from E, Nope")],
    );
    check(
        &BASE.replace("from E\n", "from Zzz.E\n"),
        &[("S025", "from Zzz.E")],
    );
    check(
        &BASE.replace("from E\n", "from C.Nope\n"),
        &[("S025", "from C.Nope")],
    );
}

#[test]
fn s026_table_keys() {
    check(
        &BASE.replace(
            "table t { key k: uuid, n: int }",
            "table t { k: uuid, n: int }",
        ),
        &[("S026", "table t")],
    );
    check(
        &BASE.replace(
            "table t { key k: uuid, n: int }",
            "table t { key k: V, n: int }",
        ),
        &[("S026", "key k: V")],
    );
    check(
        &BASE.replace(
            "table t { key k: uuid, n: int }",
            "table t { key k: uuid?, n: int }",
        ),
        &[("S026", "key k: uuid?")],
    );
    check(
        &BASE.replace(
            "table t { key k: uuid, n: int }",
            "table t { key k: [uuid], n: int }",
        ),
        &[("S026", "key k: [uuid]")],
    );
}

#[test]
fn s027_optional_collection() {
    check(
        &BASE.replace("value V { a: int }", "value V { a: [int]? }"),
        &[("S027", "a: [int]?")],
    );
    check(
        &BASE.replace("value V { a: int }", "value V { a: set<int>? }"),
        &[("S027", "a: set<int>?")],
    );
    check(
        &BASE.replace(
            "table t { key k: uuid, n: int }",
            "table t { key k: uuid, n: map<string, int>? }",
        ),
        &[("S027", "map<string, int>?")],
    );
}

#[test]
fn s028_bytes_as_element_or_key() {
    check(
        &BASE.replace("value V { a: int }", "value V { a: set<bytes> }"),
        &[("S028", "set<bytes>")],
    );
    check(
        &BASE.replace("value V { a: int }", "value V { a: map<bytes, int> }"),
        &[("S028", "map<bytes, int>")],
    );
    // bytes as a map *value* is fine.
    compile(&BASE.replace("value V { a: int }", "value V { a: map<string, bytes> }"))
        .unwrap_or_else(|d| panic!("{d}"));
}

#[test]
fn s029_integer_range() {
    check(
        &BASE.replace(
            "event E v1 { k: uuid, v: V }",
            "event E v70000 { k: uuid, v: V }",
        ),
        &[("S029", "event E v70000")],
    );
    check(
        &BASE.replace(
            "evolve wasm \"a.wasm\"",
            "evolve wasm \"a.wasm\"\n    snapshot every 5000000000",
        ),
        &[("S029", "snapshot every")],
    );
    compile(&BASE.replace(
        "event E v1 { k: uuid, v: V }",
        "event E v65535 { k: uuid, v: V }",
    ))
    .unwrap_or_else(|d| panic!("{d}"));
}

#[test]
fn all_errors_are_collected_not_just_the_first() {
    let src = BASE
        .replace("value V { a: int }", "value V { a: Nope }")
        .replace("enum En { X, Y }", "enum En { X, X }")
        .replace("key k: uuid", "key k: bool")
        .replace(
            "event E v1 { k: uuid, v: V }",
            "event E v1 { k: bool, v: V }",
        )
        .replace("evolve wasm \"a.wasm\"", "evolve wasm \"/a.wasm\"");
    let d = diags(&src);
    assert_eq!(d.codes(), ["S011", "S008", "S019", "S024"]);
}

#[test]
fn diagnostics_render_line_col_and_source() {
    let src = BASE.replace("value V { a: int }", "value V { a: Nope }");
    let d = diags(&src);
    let text = d.to_string();
    assert!(
        text.starts_with("2:16: S011: unknown type `Nope`\n  |   value V { a: Nope }\n  |"),
        "{text}"
    );
    assert!(text.ends_with("^^^^"), "{text}");
}

#[test]
fn syntax_errors_are_p001() {
    let d = compile("context {").unwrap_err();
    assert_eq!(d.codes(), ["P001"]);
    assert!(
        d.to_string().contains("expected a context name, found `{`"),
        "{d}"
    );
}

// -- the example schema, resolved -----------------------------------------------

#[test]
fn orders_schema_resolves_as_the_plan_describes() {
    let s = orders();
    assert_eq!(
        s.contexts.keys().collect::<Vec<_>>(),
        ["Shared", "Customers", "Shipping", "Orders"]
    );
    let flow = s.process("Orders", "Fulfilment").unwrap();
    assert_eq!(flow.key.name, "order_id");
    assert_eq!(flow.from.len(), 5);
    assert_eq!(
        flow.source("Shipping", "ShipmentShipped").unwrap().by,
        "order_id"
    );
    let order = s.aggregate("Orders", "Order").unwrap();
    assert_eq!(order.key.name, "order_id");
    assert_eq!(order.key.ty, Type::Scalar(Scalar::Uuid));
    assert_eq!(order.snapshot_every, 100);
    assert_eq!(
        s.aggregate("Customers", "Customer").unwrap().snapshot_every,
        100
    );
    assert_eq!(
        order
            .events
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        [
            "Orders.OrderPlaced",
            "Orders.LineAdded",
            "Orders.LineRemoved",
            "Orders.OrderCancelled"
        ]
    );
    assert_eq!(
        order.commands.keys().collect::<Vec<_>>(),
        ["PlaceOrder", "AddLine", "RemoveLine", "CancelOrder"]
    );
    assert_eq!(
        order.invariants.keys().collect::<Vec<_>>(),
        ["LinesNotEmpty"]
    );
    let max_open = &s.contexts["Orders"].invariants["MaxOpenOrders"];
    assert_eq!(max_open.aggregate, "Order");
    assert_eq!(max_open.projection.to_string(), "Orders.CustomerOrders");
    assert_eq!(max_open.scope.name, "customer_id");
    assert_eq!(max_open.check.export_or("x"), "check_max_open_orders");
    assert_eq!(
        order.commands["AddLine"].handler.export.as_deref(),
        Some("handle_add_line")
    );
    assert_eq!(order.evolve.export_or("evolve_Order"), "evolve_order");
    assert_eq!(order.values["Discount"].fields.len(), 2);
    let line = &order.entities["Line"];
    assert_eq!(line.id.name, "line_id");
    assert_eq!(
        line.fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        ["line_id", "sku", "qty", "price", "discount"]
    );
    assert_eq!(
        line.fields[3].ty,
        Type::Value(fold_schema::TypeRef::new("Shared", None, "Money"))
    );
    assert_eq!(
        line.fields[4].ty,
        Type::Optional(Box::new(Type::Value(fold_schema::TypeRef::new(
            "Orders",
            Some("Order".into()),
            "Discount"
        ))))
    );
    let state_lines = order.state.iter().find(|f| f.name == "lines").unwrap();
    assert_eq!(
        state_lines.ty,
        Type::Map(
            Scalar::Uuid,
            Box::new(Type::Entity(fold_schema::TypeRef::new(
                "Orders",
                Some("Order".into()),
                "Line"
            )))
        )
    );
    let placed = s.latest_event_type("Orders", "OrderPlaced").unwrap();
    assert_eq!(placed.id.to_string(), "Orders.OrderPlaced@v1");
    assert!(
        matches!(&placed.fields[2].ty, Type::List(inner) if matches!(**inner, Type::Entity(_)))
    );

    let co = s.projection("Orders", "CustomerOrders").unwrap();
    assert_eq!(
        co.from.iter().map(ToString::to_string).collect::<Vec<_>>(),
        [
            "Customers.CustomerRegistered",
            "Orders.OrderPlaced",
            "Orders.OrderCancelled"
        ]
    );
    assert_eq!(
        co.tables.keys().collect::<Vec<_>>(),
        ["customer_orders", "order_owner"]
    );
    let t = &co.tables["customer_orders"];
    assert_eq!(t.keys.len(), 1);
    assert_eq!(
        t.columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        [
            "name",
            "open_orders",
            "recent_orders",
            "spent_by_currency",
            "order_count"
        ]
    );
    assert_eq!(t.columns[1].ty, Type::Set(Scalar::Uuid));
    assert_eq!(
        t.columns[2].ty,
        Type::List(Box::new(Type::Scalar(Scalar::Uuid)))
    );
    assert_eq!(
        t.columns[3].ty,
        Type::Map(Scalar::String, Box::new(Type::Scalar(Scalar::Decimal)))
    );
    let owner = &co.tables["order_owner"];
    assert_eq!(owner.keys[0].name, "order_id");
    assert_eq!(owner.columns[0].name, "customer_id");

    assert_eq!(s.aggregates().count(), 3);
    assert_eq!(s.processes().count(), 1);
    assert_eq!(s.projections().count(), 2);
    let (ctx, agg) = s.aggregate_for_event("Orders", "LineAdded").unwrap();
    assert_eq!((ctx.name.as_str(), agg.name.as_str()), ("Orders", "Order"));
    assert!(s.aggregate_for_event("Orders", "Nope").is_none());
    assert!(s.aggregate_for_event("Customers", "OrderPlaced").is_none());
    assert_eq!(
        s.event_type("Orders", "OrderPlaced", 1).unwrap().id.version,
        1
    );
    assert!(s.event_type("Orders", "OrderPlaced", 2).is_none());
    assert!(s.event_family("Orders", "LineAdded").is_some());
    assert_eq!(
        s.resolve_event_ref("Orders.OrderPlaced")
            .unwrap()
            .unwrap()
            .id
            .version,
        1
    );
    assert!(
        s.resolve_event_ref("Orders.OrderPlaced@v2")
            .unwrap()
            .is_none()
    );
    assert!(s.dir().is_none());
}

#[test]
fn from_file_records_the_directory() {
    let dir = std::env::temp_dir().join(format!("fold-schema-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("schema.fold");
    std::fs::write(&path, ORDERS).unwrap();
    let s = fold_schema::Schema::from_file(&path).unwrap();
    assert_eq!(s.dir(), Some(dir.as_path()));
    std::fs::write(&path, "context {").unwrap();
    let err = fold_schema::Schema::from_file(&path).unwrap_err();
    assert!(matches!(err, fold_schema::Error::Compile { .. }), "{err}");
    let err = fold_schema::Schema::from_file(dir.join("missing.fold")).unwrap_err();
    assert!(matches!(err, fold_schema::Error::Io { .. }), "{err}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn event_type_id_parsing() {
    use fold_schema::{EventTypeId, parse_event_ref};
    let id: EventTypeId = "Orders.OrderPlaced@v2".parse().unwrap();
    assert_eq!(
        id,
        EventTypeId {
            context: "Orders".into(),
            name: "OrderPlaced".into(),
            version: 2
        }
    );
    assert_eq!(id.to_string(), "Orders.OrderPlaced@v2");
    assert_eq!(
        parse_event_ref("Orders.OrderPlaced").unwrap(),
        ("Orders".to_string(), "OrderPlaced".to_string(), None)
    );
    assert_eq!(
        parse_event_ref("Orders.OrderPlaced@v7").unwrap(),
        ("Orders".to_string(), "OrderPlaced".to_string(), Some(7))
    );
    assert!("Orders.OrderPlaced".parse::<EventTypeId>().is_err());
    for bad in [
        "OrderPlaced",
        "Orders.OrderPlaced@2",
        "Orders.OrderPlaced@v",
        "Orders.OrderPlaced@v70000",
        "a.b.c",
        ".x",
        "Orders.Order Placed",
    ] {
        assert!(parse_event_ref(bad).is_err(), "{bad}");
    }
}

// -- invariants ---------------------------------------------------------------

/// BASE with a keyable state field, a state invariant and a context invariant.
fn with_invariants() -> String {
    BASE.replace(
        "state { lines: map<uuid, Ent> }",
        "state { lines: map<uuid, Ent>, owner: uuid }",
    )
    .replace(
        "commands Do { e: Ent } -> wasm \"a.wasm\"\n",
        "commands Do { e: Ent } -> wasm \"a.wasm\"\n    invariants NotEmpty -> wasm \"a.wasm\" export \"check_not_empty\"\n",
    )
    .replace(
        "    table t { key k: uuid, n: int }\n  }\n",
        "    table t { key k: uuid, n: int }\n  }\n  invariant MaxPerOwner {\n    on A\n    projection P\n    scope owner\n    check wasm \"a.wasm\" export \"check_max\"\n  }\n",
    )
}

#[test]
fn invariants_resolve() {
    let src = with_invariants();
    let schema = compile(&src).unwrap_or_else(|d| panic!("{d}"));
    let a = &schema.contexts["C"].aggregates["A"];
    assert_eq!(a.invariants.len(), 1);
    assert_eq!(
        a.invariants["NotEmpty"].check.export_or("x"),
        "check_not_empty"
    );
    let inv = &schema.contexts["C"].invariants["MaxPerOwner"];
    assert_eq!(inv.aggregate, "A");
    assert_eq!(inv.projection.to_string(), "C.P");
    assert_eq!(inv.scope.name, "owner");
    assert_eq!(inv.check.export_or("x"), "check_max");
    let on_a: Vec<_> = schema
        .invariants_on("C", "A")
        .map(|i| i.name.as_str())
        .collect();
    assert_eq!(on_a, ["MaxPerOwner"]);
    assert_eq!(schema.invariants_on("C", "Nope").count(), 0);
}

#[test]
fn s030_duplicate_context_invariant() {
    let src = with_invariants();
    let dup = src.replace(
        "  invariant MaxPerOwner {",
        "  invariant MaxPerOwner {\n    on A\n    projection P\n    scope owner\n    check wasm \"a.wasm\"\n  }\n  invariant MaxPerOwner {",
    );
    let d = diags(&dup);
    assert_eq!(d.codes(), ["S030"], "{d}");
}

#[test]
fn s031_duplicate_aggregate_invariant() {
    let src = with_invariants().replace(
        "invariants NotEmpty -> wasm \"a.wasm\" export \"check_not_empty\"",
        "invariants NotEmpty -> wasm \"a.wasm\", NotEmpty -> wasm \"a.wasm\"",
    );
    let d = diags(&src);
    assert_eq!(d.codes(), ["S031"], "{d}");
}

#[test]
fn s032_invariant_on_unknown_aggregate() {
    let src = with_invariants().replace("    on A\n", "    on Zed\n");
    check(&src, &[("S032", "on Zed")]);
}

#[test]
fn s033_invariant_on_unknown_projection() {
    check(
        &with_invariants().replace("    projection P\n", "    projection Nope\n"),
        &[("S033", "projection Nope")],
    );
    check(
        &with_invariants().replace("    projection P\n", "    projection Zzz.P\n"),
        &[("S033", "projection Zzz.P")],
    );
}

#[test]
fn s034_invariant_scope_must_be_a_keyable_state_field() {
    check(
        &with_invariants().replace("    scope owner\n", "    scope nope\n"),
        &[("S034", "scope nope")],
    );
    check(
        &with_invariants().replace("    scope owner\n", "    scope lines\n"),
        &[("S034", "scope lines")],
    );
}

#[test]
fn a_context_invariant_may_read_another_contexts_projection() {
    let src = with_invariants().replace("    projection P\n", "    projection D.Q\n")
        + "context D {\n  event F v1 { k: uuid }\n  projection Q {\n    from F\n    fold wasm \"d.wasm\"\n    table u { key k: uuid, n: int }\n  }\n}\n";
    let schema = compile(&src).unwrap_or_else(|d| panic!("{d}"));
    assert_eq!(
        schema.contexts["C"].invariants["MaxPerOwner"]
            .projection
            .to_string(),
        "D.Q"
    );
}

// -- processes ----------------------------------------------------------------

/// BASE plus a second context with an event correlated by another field name,
/// and a process reacting to both.
fn with_process() -> String {
    BASE.replace(
        "  projection P {",
        "  process Flow {\n    key k: uuid\n    from E, D.F by ref\n    state { seen: uint }\n    react wasm \"a.wasm\" export \"react_flow\"\n  }\n  projection P {",
    ) + "context D {\n  event F v1 { ref: uuid, n: int }\n  event G v1 { k: string }\n}\n"
}

#[test]
fn processes_resolve() {
    let src = with_process();
    let schema = compile(&src).unwrap_or_else(|d| panic!("{d}"));
    let p = &schema.contexts["C"].processes["Flow"];
    assert_eq!(p.key.name, "k");
    assert_eq!(p.key.ty, Type::Scalar(Scalar::Uuid));
    let sources: Vec<(String, String)> = p
        .from
        .iter()
        .map(|s| (s.family.to_string(), s.by.clone()))
        .collect();
    assert_eq!(
        sources,
        [
            ("C.E".to_string(), "k".to_string()),
            ("D.F".to_string(), "ref".to_string())
        ]
    );
    assert_eq!(p.source("D", "F").unwrap().by, "ref");
    assert!(p.source("D", "G").is_none());
    assert_eq!(p.react.export_or("x"), "react_flow");
    assert_eq!(schema.processes().count(), 1);
    assert_eq!(schema.process("C", "Flow").unwrap().state.len(), 1);
}

#[test]
fn s035_duplicate_or_projection_named_process() {
    let src = with_process().replace("  process Flow {", "  process Flow {\n    key k: uuid\n    from E\n    state {}\n    react wasm \"a.wasm\"\n  }\n  process Flow {");
    let d = diags(&src);
    assert_eq!(d.codes(), ["S035"], "{d}");
    let src = with_process().replace("process Flow", "process P");
    check(&src, &[("S035", "process P")]);
}

#[test]
fn s036_process_key_type() {
    // A bad key type reports once; the per-source checks that depend on the
    // key type are skipped rather than cascading.
    check(
        &with_process().replace(
            "    key k: uuid\n    from E, D.F by ref",
            "    key k: decimal\n    from E, D.F by ref",
        ),
        &[("S036", "key k: decimal")],
    );
}

#[test]
fn s037_process_source_unknown() {
    check(
        &with_process().replace("from E, D.F by ref", "from E, Nope"),
        &[("S037", "from E, Nope")],
    );
    check(
        &with_process().replace("from E, D.F by ref", "from E, Zzz.F by ref"),
        &[("S037", "from E, Zzz.F by ref")],
    );
}

#[test]
fn s038_process_source_must_carry_the_key() {
    // D.F has no field `k`, so without `by` it cannot correlate.
    check(
        &with_process().replace("from E, D.F by ref", "from E, D.F"),
        &[("S038", "from E, D.F")],
    );
    // D.G has `k`, but as a string while the process keys by uuid.
    check(
        &with_process().replace("from E, D.F by ref", "from E, D.G"),
        &[("S038", "from E, D.G")],
    );
}

// -- value rules --------------------------------------------------------------

const RULES: &str = r#"context Shared {
  value Money { amount: decimal, currency: string } rules {
    NonNegative: amount >= 0,
    Iso: currency matches "^[A-Z]{3}$",
  }
}
context C {
  enum Kind { Big, Small }
  value Line { qty: uint, price: Shared.Money, note: string?, tags: [string], kind: Kind } rules {
    HasQty: qty > 0 and qty <= 1000,
    Priced: price.amount > 0 or kind == "Small",
    Tagged: len(tags) <= 5 and len(note) < 80,
    Known: kind in ["Big", "Small"],
    Cheap: not price.amount > 1000.00,
  }
  event E v1 { k: uuid, line: Line }
}
"#;

#[test]
fn value_rules_resolve_through_nested_values() {
    let s = compile(RULES).unwrap_or_else(|d| panic!("{d}"));
    let money = &s.contexts["Shared"].values["Money"];
    assert_eq!(money.rules.len(), 2);
    assert_eq!(money.rules[0].name, "NonNegative");
    let line = &s.contexts["C"].values["Line"];
    let names: Vec<&str> = line.rules.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(names, ["HasQty", "Priced", "Tagged", "Known", "Cheap"]);
    let fold_schema::RuleExpr::Or(l, _) = &line.rules[1].expr else {
        panic!()
    };
    let fold_schema::RuleExpr::Cmp {
        lhs: fold_schema::RuleTerm::Field(p),
        ..
    } = l.as_ref()
    else {
        panic!()
    };
    assert_eq!(p.segments, ["price", "amount"]);
    assert_eq!(p.kind, fold_schema::OperandKind::Number);
    let fold_schema::RuleExpr::And(_, note) = &line.rules[2].expr else {
        panic!()
    };
    let fold_schema::RuleExpr::Cmp {
        lhs: fold_schema::RuleTerm::Len { optional, .. },
        ..
    } = note.as_ref()
    else {
        panic!()
    };
    assert!(optional, "note is `string?`");
}

#[test]
fn s039_rule_path_must_name_fields() {
    check(
        &RULES.replace("qty > 0 and", "qyt > 0 and"),
        &[("S039", "qyt > 0")],
    );
    check(
        &RULES.replace("price.amount > 0", "tags.amount > 0"),
        &[("S039", "tags.amount > 0")],
    );
}

#[test]
fn s040_rule_operands_must_agree() {
    check(
        &RULES.replace("qty > 0 and", r#"qty > "0" and"#),
        &[("S040", r#"qty > "0""#)],
    );
    check(
        &RULES.replace("Iso: currency matches", "Iso: amount matches"),
        &[("S040", "amount matches")],
    );
    check(
        &RULES.replace(r#"kind in ["Big", "Small"]"#, "kind in [1]"),
        &[("S040", "kind in [1]")],
    );
    check(
        &RULES.replace("len(tags) <= 5", "tags <= 5"),
        &[("S040", "tags <= 5")],
    );
    check(
        &RULES.replace("qty > 0 and", r#"kind > "A" and"#),
        &[("S040", r#"kind > "A""#)],
    );
    check(
        &RULES.replace("price.amount > 0", "price > 0"),
        &[("S040", "price > 0")],
    );
}

#[test]
fn s041_bad_regex() {
    check(
        &RULES.replace(r#""^[A-Z]{3}$""#, r#""[""#),
        &[("S041", r#"currency matches "[""#)],
    );
}

#[test]
fn s042_duplicate_rule() {
    check(
        &RULES.replace("Iso: currency", "NonNegative: currency"),
        &[("S042", "NonNegative: currency")],
    );
}

#[test]
fn projection_snapshot_every_defaults_to_never() {
    let s = compile(BASE).unwrap();
    assert_eq!(s.contexts["C"].projections["P"].snapshot_every, 0);
    let s = compile(&BASE.replace(
        "fold wasm \"a.wasm\"\n    table",
        "fold wasm \"a.wasm\"\n    snapshot every 250\n    table",
    ))
    .unwrap();
    assert_eq!(s.contexts["C"].projections["P"].snapshot_every, 250);
    check(
        &BASE.replace(
            "fold wasm \"a.wasm\"\n    table",
            "fold wasm \"a.wasm\"\n    snapshot every 99999999999\n    table",
        ),
        &[("S029", "snapshot every 99999999999")],
    );
}

#[test]
fn process_snapshot_every() {
    let s = compile(&with_process()).unwrap();
    assert_eq!(s.contexts["C"].processes["Flow"].snapshot_every, 0);
    let s = compile(&with_process().replace(
        "react wasm \"a.wasm\" export \"react_flow\"\n",
        "react wasm \"a.wasm\" export \"react_flow\"\n    snapshot every 50\n",
    ))
    .unwrap();
    assert_eq!(s.contexts["C"].processes["Flow"].snapshot_every, 50);
}

#[test]
fn doc_comments_reach_the_model() {
    let src = r#"//! the file
/// the context
context C {
  /// a value
  value V {
    /// its field
    a: int,
  } rules {
    /// its rule
    R: a > 0,
  }
  /// an enum
  enum En { X, Y }
  /// an event
  event E v1 {
    /// the key
    k: uuid,
  }
  /// an aggregate
  aggregate A {
    key k: uuid
    stream "a-{k}"
    /// an entity
    entity Ent {
      /// its id
      id eid: uuid,
      n: int,
    }
    events E
    state { k: uuid }
    evolve wasm "a.wasm"
    commands
      /// a command
      Do {
        /// its field
        e: Ent,
      } -> wasm "a.wasm"
    invariants
      /// an invariant
      I -> wasm "a.wasm"
  }
  /// a projection
  projection P {
    from E
    fold wasm "a.wasm"
    /// a table
    table t {
      /// the key column
      key k: uuid,
      /// a column
      n: int,
    }
  }
  /// a context invariant
  invariant X { on A projection P scope k check wasm "a.wasm" }
  /// a process
  process Q { key k: uuid from E state {} react wasm "a.wasm" }
}
"#;
    let schema = compile(src).unwrap_or_else(|d| panic!("{d}"));
    assert_eq!(schema.docs, ["the file"]);
    let c = &schema.contexts["C"];
    assert_eq!(c.docs, ["the context"]);
    let v = &c.values["V"];
    assert_eq!(v.docs, ["a value"]);
    assert_eq!(v.fields[0].docs, ["its field"]);
    assert_eq!(v.rules[0].docs, ["its rule"]);
    assert_eq!(c.enums["En"].docs, ["an enum"]);
    let e = &c.events["E"].versions[&1];
    assert_eq!(e.docs, ["an event"]);
    assert_eq!(e.fields[0].docs, ["the key"]);
    let a = &c.aggregates["A"];
    assert_eq!(a.docs, ["an aggregate"]);
    let ent = &a.entities["Ent"];
    assert_eq!(ent.docs, ["an entity"]);
    assert_eq!(ent.id.docs, ["its id"]);
    assert_eq!(ent.fields[0].docs, ["its id"], "the id is the first field");
    let cmd = &a.commands["Do"];
    assert_eq!(cmd.docs, ["a command"]);
    assert_eq!(cmd.fields[0].docs, ["its field"]);
    assert_eq!(a.invariants["I"].docs, ["an invariant"]);
    let p = &c.projections["P"];
    assert_eq!(p.docs, ["a projection"]);
    let t = &p.tables["t"];
    assert_eq!(t.docs, ["a table"]);
    assert_eq!(t.keys[0].docs, ["the key column"]);
    assert_eq!(t.columns[0].docs, ["a column"]);
    assert_eq!(c.invariants["X"].docs, ["a context invariant"]);
    assert_eq!(c.processes["Q"].docs, ["a process"]);
}

// -- enums with payloads --------------------------------------------------------

#[test]
fn enums_with_payloads_resolve() {
    let src = BASE.replace("enum En { X, Y }", "enum En { X, Y { n: int, v: V }, Z }");
    let s = compile(&src).unwrap_or_else(|d| panic!("{d}"));
    let en = &s.contexts["C"].enums["En"];
    assert_eq!(en.variant_names(), ["X", "Y", "Z"]);
    assert!(en.has_payloads());
    assert!(en.variant("X").unwrap().payload.is_none());
    let y = en.variant("Y").unwrap().payload.as_ref().unwrap();
    assert_eq!(y.len(), 2);
    assert_eq!(y[0].name, "n");
    assert_eq!(
        y[1].ty,
        Type::Value(fold_schema::TypeRef::new("C", None, "V"))
    );
}

#[test]
fn s007_duplicate_payload_field() {
    check(
        &BASE.replace("enum En { X, Y }", "enum En { X, Y { n: int, n: int } }"),
        &[("S007", "n: int, n: int")],
    );
}

#[test]
fn s008_duplicate_variant_with_payload() {
    check(
        &BASE.replace("enum En { X, Y }", "enum En { X, X { n: int } }"),
        &[("S008", "X { n: int }")],
    );
}

#[test]
fn s011_unknown_type_in_payload() {
    check(
        &BASE.replace("enum En { X, Y }", "enum En { X, Y { n: Nope } }"),
        &[("S011", "n: Nope")],
    );
}

#[test]
fn s015_context_enum_payload_may_not_hold_an_entity() {
    check(
        &BASE.replace("enum En { X, Y }", "enum En { X, Y { e: A.Ent } }"),
        &[("S015", "e: A.Ent")],
    );
}

#[test]
fn s014_context_enum_payload_may_not_use_a_local_value() {
    check(
        &BASE.replace("enum En { X, Y }", "enum En { X, Y { lv: A.LV } }"),
        &[("S014", "lv: A.LV")],
    );
}

#[test]
fn a_local_enum_payload_may_hold_the_aggregates_entity_but_not_leave_it() {
    let local = BASE.replace(
        "value LV { b: int }",
        "value LV { b: int }\n    enum LE { P { e: Ent } }",
    );
    compile(&local).unwrap_or_else(|d| panic!("{d}"));
    // Used from another aggregate's state: the local enum stays local.
    let elsewhere = local.replace(
        "    table t { key k: uuid, n: int }\n  }",
        "    table t { key k: uuid, n: int }\n  }\n  aggregate B {\n    key k: uuid\n    stream \"b-{k}\"\n    events E2\n    state { le: A.LE }\n    evolve wasm \"a.wasm\"\n  }\n  event E2 v1 { k: uuid }",
    );
    check(&elsewhere, &[("S014", "le: A.LE")]);
}

#[test]
fn s016_cycle_through_an_enum_payload() {
    check(
        &BASE
            .replace("value V { a: int }", "value V { a: int, e: En }")
            .replace("enum En { X, Y }", "enum En { X, Y { v: V } }"),
        &[("S016", "value V { a: int, e: En }")],
    );
}

// -- field defaults -------------------------------------------------------------

#[test]
fn defaults_resolve_to_canonical_json() {
    let src = BASE.replace(
        "value V { a: int }",
        r#"value V {
    a: int = -3, u: uint = 1, d: decimal = 1.50, s: string = "x", b: bool = true,
    id: uuid = "11111111-1111-1111-1111-111111111111",
    ts: timestamp = "2024-01-02T03:04:05+02:00", by: bytes = "aGVsbG8=", en: En = Y,
  }"#,
    );
    let s = compile(&src).unwrap_or_else(|d| panic!("{d}"));
    let v = &s.contexts["C"].values["V"];
    let got: Vec<(String, Option<serde_json::Value>)> = v
        .fields
        .iter()
        .map(|f| (f.name.clone(), f.default.clone()))
        .collect();
    use serde_json::json;
    assert_eq!(
        got,
        [
            ("a".into(), Some(json!(-3))),
            ("u".into(), Some(json!(1))),
            ("d".into(), Some(json!("1.50"))),
            ("s".into(), Some(json!("x"))),
            ("b".into(), Some(json!(true))),
            (
                "id".into(),
                Some(json!("11111111-1111-1111-1111-111111111111"))
            ),
            ("ts".into(), Some(json!("2024-01-02T01:04:05Z"))),
            ("by".into(), Some(json!("aGVsbG8="))),
            ("en".into(), Some(json!("Y"))),
        ]
    );
}

#[test]
fn s043_default_on_optional_collection_or_record() {
    check(
        &BASE.replace(
            "value V { a: int }",
            "value V { a: int? = 1, l: [int] = 1, v: En? = X, m: map<int, int> = 0 }",
        ),
        &[
            ("S043", "a: int? = 1"),
            ("S043", "a: int? = 1"),
            ("S043", "a: int? = 1"),
            ("S043", "a: int? = 1"),
        ],
    );
    check(
        &BASE.replace(
            "  event E v1 { k: uuid, v: V }",
            "  event E v1 { k: uuid, v: V = X }",
        ),
        &[("S043", "v: V = X")],
    );
}

#[test]
fn s044_default_on_key_or_id() {
    check(
        &BASE.replace(
            "key k: uuid\n    stream \"a-{k}\"",
            "key k: uuid = \"11111111-1111-1111-1111-111111111111\"\n    stream \"a-{k}\"",
        ),
        &[("S044", "key k: uuid =")],
    );
    check(
        &BASE.replace(
            "entity Ent { id eid: uuid, n: int }",
            "entity Ent { id eid: uuid = \"11111111-1111-1111-1111-111111111111\", n: int }",
        ),
        &[("S044", "id eid: uuid =")],
    );
    check(
        &BASE.replace(
            "table t { key k: uuid, n: int }",
            "table t { key k: uuid = \"11111111-1111-1111-1111-111111111111\", n: int }",
        ),
        &[("S044", "table t { key k: uuid =")],
    );
    check(
        &with_process().replace(
            "key k: uuid\n    from E",
            "key k: uuid = \"11111111-1111-1111-1111-111111111111\"\n    from E",
        ),
        &[("S044", "key k: uuid =")],
    );
}

#[test]
fn s045_default_literal_mismatch() {
    for (field, needle) in [
        ("a: int = \"x\"", "a: int = \"x\""),
        ("a: uint = -1", "a: uint = -1"),
        ("a: bool = 1", "a: bool = 1"),
        ("a: decimal = \"x\"", "a: decimal = \"x\""),
        ("a: uuid = \"nope\"", "a: uuid = \"nope\""),
        ("a: int = X", "a: int = X"),
        ("a: En = Nope", "a: En = Nope"),
        ("a: En = 1", "a: En = 1"),
    ] {
        check(
            &BASE.replace("value V { a: int }", &format!("value V {{ {field} }}")),
            &[("S045", needle)],
        );
    }
    // A payload-carrying variant cannot be a default.
    check(
        &BASE
            .replace("enum En { X, Y }", "enum En { X, Y { n: int } }")
            .replace("value V { a: int }", "value V { a: En = Y }"),
        &[("S045", "a: En = Y")],
    );
    // Control: a decimal default may be written as a number or as the string
    // the JSON form uses.
    compile(&BASE.replace("value V { a: int }", "value V { a: decimal = 2 }"))
        .unwrap_or_else(|d| panic!("{d}"));
    compile(&BASE.replace("value V { a: int }", "value V { a: decimal = \"1.5\" }"))
        .unwrap_or_else(|d| panic!("{d}"));
}
