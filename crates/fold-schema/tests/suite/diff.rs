use fold_schema::{
    Action, AssumeData, ChangeKind as K, Compatibility as C, EventFamilyRef, Facts, SchemaDiff,
    compile, diff, diff_with,
};

const BASE: &str = r#"context Shared {
  value Money { amount: decimal, currency: string } rules { NonNeg: amount >= 0 }
}
context C {
  enum En { X, Y, Z { n: int } }
  event E v1 { k: uuid, v: Shared.Money, e: En }
  event E v2 { k: uuid, v: Shared.Money, e: En, note: string? } upcast from v1 {}
  event G v1 { k: uuid }
  aggregate A {
    key k: uuid
    stream "a-{k}"
    entity Ent { id eid: uuid, n: int }
    events E
    state { lines: map<uuid, Ent>, st: En, owner: uuid }
    evolve wasm "a.wasm"
    snapshot every 10
    commands Do { e: Ent } requires state.st == X -> wasm "a.wasm"
    invariants Few: len(lines) <= 10, W -> wasm "a.wasm"
  }
  projection P {
    from E
    fold wasm "a.wasm"
    table t { key k: uuid, n: int, o: string? }
    table u { key k: uuid, m: int }
  }
  invariant Max { on A projection P scope owner check wasm "a.wasm" }
  process Flow {
    key k: uuid
    from E
    state { seen: uint }
    react wasm "a.wasm"
    timers T1, T2
  }
}
"#;

/// A log that holds nothing.
struct NoData;

impl Facts for NoData {
    fn has_events(&self, _: &EventFamilyRef, _: Option<u16>) -> bool {
        false
    }
    fn has_streams(&self, _: &str, _: &str) -> bool {
        false
    }
}

fn schema(src: &str) -> fold_schema::Schema {
    compile(src).unwrap_or_else(|d| panic!("{d}\n---\n{src}"))
}

fn edited(needle: &str, replacement: &str) -> String {
    assert!(BASE.contains(needle), "needle not in BASE: {needle}");
    BASE.replace(needle, replacement)
}

fn d(new: &str) -> SchemaDiff {
    diff(&schema(BASE), &schema(new))
}

fn d_no_data(new: &str) -> SchemaDiff {
    diff_with(&schema(BASE), &schema(new), &NoData)
}

fn kinds(diff: &SchemaDiff) -> Vec<(K, C)> {
    diff.changes
        .iter()
        .map(|c| (c.kind, c.compatibility))
        .collect()
}

/// Edit `needle` into `replacement` and expect exactly `want` (kind,
/// compatibility) pairs in path order.
fn expect(needle: &str, replacement: &str, want: &[(K, C)]) -> SchemaDiff {
    let diff = d(&edited(needle, replacement));
    assert_eq!(kinds(&diff), want, "{needle:?} -> {replacement:?}\n{diff}");
    diff
}

#[test]
fn base_vs_base_is_empty() {
    let s = schema(BASE);
    let diff = diff(&s, &s);
    assert!(diff.is_empty());
    assert_eq!(diff.worst(), None);
    assert!(!diff.has_breaking());
    assert_eq!(diff.summary(), "no changes");
    assert!(diff.actions().is_empty());
}

#[test]
fn textual_changes_are_no_changes() {
    // Comments, doc comments, whitespace and declaration order.
    let reordered = r#"//! docs
/// the C context
context C {
  // a comment
  process Flow {
    key k: uuid
    from E
    state { seen: uint }
    react wasm "a.wasm"
    timers T1, T2
  }
  invariant Max { on A projection P scope owner check wasm "a.wasm" }
  projection P {
    from E
    fold wasm "a.wasm"
    /// first table
    table t { key k: uuid, n: int, o: string? }
    table u { key k: uuid, m: int }
  }
  aggregate A {
    key k: uuid
    stream "a-{k}"
    entity Ent { id eid: uuid, n: int }
    events E
    state { lines: map<uuid, Ent>, st: En, owner: uuid }
    evolve wasm "a.wasm"
    snapshot every 10
    commands Do { e: Ent } requires { Requires: state.st == X } -> wasm "a.wasm"
    invariants Few: len(lines) <= 10, W -> wasm "a.wasm"
  }
  event G v1 { k: uuid }
  event E v2 { k: uuid, v: Shared.Money, e: En, note: string? } upcast from v1 {}
  event E v1 { k: uuid, v: Shared.Money, e: En }
  /// an enum
  enum En { X, Y, Z { n: int } }
}
context Shared {
  value Money { amount: decimal, currency: string } rules { NonNeg: amount >= 0 }
}
"#;
    let diff = d(reordered);
    assert!(diff.is_empty(), "{diff}");
}

#[test]
fn stored_record_field_rules() {
    // (edit, expected)
    type Case = (&'static str, &'static str, &'static [(K, C)]);
    let cases: &[Case] = &[
        // value fields
        (
            "value Money { amount: decimal, currency: string }",
            "value Money { amount: decimal, currency: string, note: string? }",
            &[(K::FieldAdded, C::Compatible)],
        ),
        (
            "value Money { amount: decimal, currency: string }",
            "value Money { amount: decimal, currency: string, n: int = 0 }",
            &[(K::FieldAdded, C::Compatible)],
        ),
        (
            "value Money { amount: decimal, currency: string }",
            "value Money { amount: decimal, currency: string, n: int }",
            &[(K::FieldAdded, C::Breaking)],
        ),
        (
            "value Money { amount: decimal, currency: string }",
            "value Money { amount: decimal }",
            &[(K::FieldRemoved, C::Breaking)],
        ),
        (
            "value Money { amount: decimal, currency: string }",
            "value Money { amount: decimal, currency: string? }",
            &[(K::FieldTypeChanged, C::Compatible)],
        ),
        (
            "value Money { amount: decimal, currency: string }",
            "value Money { amount: int, currency: string }",
            &[(K::FieldTypeChanged, C::Breaking)],
        ),
        (
            "value Money { amount: decimal, currency: string }",
            "value Money { amount: decimal, currency: string = \"EUR\" }",
            &[(K::FieldDefaultChanged, C::Compatible)],
        ),
        (
            "rules { NonNeg: amount >= 0 }",
            "rules { NonNeg: amount > 0 }",
            &[(K::RulesChanged, C::Compatible)],
        ),
        // an optional event field removed is tolerated
        (
            "e: En, note: string? } upcast from v1 {}",
            "e: En } upcast from v1 {}",
            &[(K::FieldRemoved, C::Compatible)],
        ),
        // entity fields are stored
        (
            "entity Ent { id eid: uuid, n: int }",
            "entity Ent { id eid: uuid, n: int, m: int }",
            &[(K::FieldAdded, C::Breaking)],
        ),
        // enum payloads are stored
        (
            "Z { n: int } }",
            "Z { n: int, m: int } }",
            &[(K::FieldAdded, C::Breaking)],
        ),
        (
            "enum En { X, Y, Z { n: int } }",
            "enum En { X, Y, Z { n: int }, Q }",
            &[(K::VariantAdded, C::Compatible)],
        ),
        (
            "enum En { X, Y, Z { n: int } }",
            "enum En { X, Z { n: int } }",
            &[(K::VariantRemoved, C::Breaking)],
        ),
        (
            "enum En { X, Y, Z { n: int } }",
            "enum En { X, Y, Z }",
            &[(K::FieldRemoved, C::Breaking)],
        ),
        (
            "enum En { X, Y, Z { n: int } }",
            "enum En { X, Y { q: int? }, Z { n: int } }",
            &[(K::FieldAdded, C::Compatible)],
        ),
    ];
    for (needle, replacement, want) in cases {
        expect(needle, replacement, want);
    }
    // An entity id change (the state map's key must follow it).
    let src =
        edited("id eid: uuid", "id eid: string").replace("map<uuid, Ent>", "map<string, Ent>");
    assert_eq!(
        kinds(&d(&src)),
        [
            (K::EntityIdChanged, C::Breaking),
            (K::AggregateStateChanged, C::NeedsRebuild)
        ]
    );
}

#[test]
fn command_fields_are_transient() {
    expect(
        "commands Do { e: Ent }",
        "commands Do { e: Ent, n: int }",
        &[(K::FieldAdded, C::Compatible)],
    );
    expect(
        "commands Do { e: Ent }",
        "commands Do { e: string }",
        &[(K::FieldTypeChanged, C::Compatible)],
    );
    expect(
        "commands Do { e: Ent }",
        "commands Do {}",
        &[(K::FieldRemoved, C::Compatible)],
    );
    expect(
        "requires state.st == X",
        "requires state.st == Y",
        &[(K::CommandChanged, C::Compatible)],
    );
    expect(
        "commands Do { e: Ent } requires state.st == X -> wasm \"a.wasm\"",
        "commands Do { e: Ent } requires state.st == X -> wasm \"a.wasm\", Undo {} -> wasm \"a.wasm\"",
        &[(K::CommandAdded, C::Compatible)],
    );
}

#[test]
fn event_removals_depend_on_the_log() {
    let src = edited("  event G v1 { k: uuid }\n", "");
    let with = d(&src);
    assert_eq!(
        kinds(&with),
        [(K::EventFamilyRemoved, C::Breaking)],
        "{with}"
    );
    assert!(with.has_breaking());
    let without = d_no_data(&src);
    assert_eq!(kinds(&without), [(K::EventFamilyRemoved, C::Compatible)]);

    // A version removed.
    let src = edited(
        "  event E v2 { k: uuid, v: Shared.Money, e: En, note: string? } upcast from v1 {}\n",
        "",
    );
    assert_eq!(kinds(&d(&src)), [(K::EventVersionRemoved, C::Breaking)]);
    assert_eq!(
        kinds(&d_no_data(&src)),
        [(K::EventVersionRemoved, C::Compatible)]
    );

    // A version added is compatible and says what consumers see.
    let diff = expect(
        "  event G v1 { k: uuid }",
        "  event G v1 { k: uuid }\n  event G v2 { k: uuid, n: int } upcast from v1 { set n: 1 }",
        &[(K::EventVersionAdded, C::Compatible)],
    );
    assert!(
        diff.changes[0].description.contains("upcast from v1"),
        "{diff}"
    );
    assert_eq!(diff.changes[0].path, "C.G@v2");

    // The upcast of a recorded version changed.
    let src = edited("upcast from v1 {}", "upcast from v1 { set note: \"x\" }");
    assert_eq!(kinds(&d(&src)), [(K::UpcastChanged, C::Breaking)]);
    assert_eq!(kinds(&d_no_data(&src)), [(K::UpcastChanged, C::Compatible)]);
}

#[test]
fn aggregate_rules() {
    // The events must carry the new key field, which is its own change.
    let src = edited(
        "key k: uuid\n    stream \"a-{k}\"",
        "key id: uuid\n    stream \"a-{id}\"",
    )
    .replace("e: En }", "e: En, id: uuid }")
    .replace("e: En, note: string? }", "e: En, note: string?, id: uuid }");
    let diff = d(&src);
    assert_eq!(
        kinds(&diff),
        [
            (K::AggregateKeyChanged, C::Breaking),
            (K::AggregateStreamChanged, C::Breaking),
            (K::FieldAdded, C::Breaking),
            (K::FieldAdded, C::Breaking),
        ],
        "{diff}"
    );
    assert_eq!(diff.changes[0].path, "C.A.key");
    expect(
        "stream \"a-{k}\"",
        "stream \"agg-{k}\"",
        &[(K::AggregateStreamChanged, C::Breaking)],
    );
    let diff = expect(
        "state { lines: map<uuid, Ent>, st: En, owner: uuid }",
        "state { lines: map<uuid, Ent>, st: En, owner: uuid, n: int }",
        &[(K::AggregateStateChanged, C::NeedsRebuild)],
    );
    assert_eq!(
        diff.actions(),
        [Action::ClearAggregateSnapshots {
            context: "C".into(),
            name: "A".into()
        }]
    );
    expect(
        "evolve wasm \"a.wasm\"",
        "evolve wasm \"a.wasm\" export \"ev\"",
        &[(K::WasmChanged, C::Compatible)],
    );
    expect(
        "snapshot every 10",
        "snapshot every 20",
        &[(K::SnapshotEveryChanged, C::Compatible)],
    );
    expect(
        "invariants Few: len(lines) <= 10, W -> wasm \"a.wasm\"",
        "invariants Few: len(lines) <= 9, W -> wasm \"a.wasm\"",
        &[(K::InvariantChanged, C::Compatible)],
    );
    expect(
        "invariants Few: len(lines) <= 10, W -> wasm \"a.wasm\"",
        "invariants Few: len(lines) <= 10",
        &[(K::InvariantRemoved, C::Compatible)],
    );
    // The events list.
    let diff = expect(
        "    events E\n    state",
        "    events E, G\n    state",
        &[(K::AggregateEventsChanged, C::Compatible)],
    );
    assert_eq!(diff.changes[0].path, "C.A.events");
    let both = edited("    events E\n    state", "    events E, G\n    state");
    let back = diff_with(&schema(&both), &schema(BASE), &AssumeData);
    assert_eq!(kinds(&back), [(K::AggregateEventsChanged, C::Breaking)]);
    let back = diff_with(&schema(&both), &schema(BASE), &NoData);
    assert_eq!(kinds(&back), [(K::AggregateEventsChanged, C::Compatible)]);
}

#[test]
fn aggregate_removal_depends_on_streams() {
    // Removing A also removes what refers to it: the context invariant, and
    // nothing else depends on A (P and Flow read events).
    let src = BASE
        .replace(
            "  aggregate A {\n    key k: uuid\n    stream \"a-{k}\"\n    entity Ent { id eid: uuid, n: int }\n    events E\n    state { lines: map<uuid, Ent>, st: En, owner: uuid }\n    evolve wasm \"a.wasm\"\n    snapshot every 10\n    commands Do { e: Ent } requires state.st == X -> wasm \"a.wasm\"\n    invariants Few: len(lines) <= 10, W -> wasm \"a.wasm\"\n  }\n",
            "",
        )
        .replace(
            "  invariant Max { on A projection P scope owner check wasm \"a.wasm\" }\n",
            "",
        );
    let with = d(&src);
    assert_eq!(
        kinds(&with),
        [
            (K::AggregateRemoved, C::Breaking),
            (K::InvariantRemoved, C::Compatible)
        ],
        "{with}"
    );
    assert_eq!(
        with.actions(),
        [Action::DropAggregate {
            context: "C".into(),
            name: "A".into()
        }]
    );
    let without = d_no_data(&src);
    assert_eq!(without.worst(), Some(C::Compatible));
}

#[test]
fn projection_rules() {
    let rebuild = Action::RebuildProjection {
        context: "C".into(),
        name: "P".into(),
    };
    let diff = expect(
        "    from E\n    fold",
        "    from E, G\n    fold",
        &[(K::ProjectionSourcesChanged, C::NeedsRebuild)],
    );
    assert_eq!(diff.actions(), std::slice::from_ref(&rebuild));
    expect(
        "fold wasm \"a.wasm\"",
        "fold wasm \"b.wasm\"",
        &[(K::WasmChanged, C::Compatible)],
    );
    let diff = expect(
        "table u { key k: uuid, m: int }",
        "table u { key k: uuid, m: int }\n    table w { key k: uuid }",
        &[(K::TableAdded, C::NeedsRebuild)],
    );
    assert_eq!(diff.actions(), std::slice::from_ref(&rebuild));
    expect(
        "table t { key k: uuid, n: int, o: string? }",
        "table t { key k: string, n: int, o: string? }",
        &[(K::TableKeyChanged, C::NeedsRebuild)],
    );
    expect(
        "table t { key k: uuid, n: int, o: string? }",
        "table t { key k: uuid, o: string? }",
        &[(K::ColumnRemoved, C::NeedsRebuild)],
    );
    expect(
        "table t { key k: uuid, n: int, o: string? }",
        "table t { key k: uuid, n: string, o: string? }",
        &[(K::ColumnTypeChanged, C::NeedsRebuild)],
    );
    expect(
        "table t { key k: uuid, n: int, o: string? }",
        "table t { key k: uuid, n: int, o: string?, p: int }",
        &[(K::ColumnAdded, C::NeedsRebuild)],
    );
    expect(
        "table t { key k: uuid, n: int, o: string? }",
        "table t { key k: uuid, n: int, o: string?, p: int?, q: int = 1 }",
        &[
            (K::ColumnAdded, C::Compatible),
            (K::ColumnAdded, C::Compatible),
        ],
    );
    // Two rebuild-worthy changes in one projection give one action.
    let diff = expect(
        "table t { key k: uuid, n: int, o: string? }\n    table u { key k: uuid, m: int }",
        "table t { key k: uuid, o: string? }\n    table u { key k: uuid, m: string }",
        &[
            (K::ColumnRemoved, C::NeedsRebuild),
            (K::ColumnTypeChanged, C::NeedsRebuild),
        ],
    );
    assert_eq!(diff.actions(), std::slice::from_ref(&rebuild));
    // A table removed is dropped; the projection keeps going.
    let diff = expect(
        "\n    table u { key k: uuid, m: int }",
        "",
        &[(K::TableRemoved, C::Compatible)],
    );
    assert_eq!(
        diff.actions(),
        [Action::DropTable {
            context: "C".into(),
            projection: "P".into(),
            table: "u".into()
        }]
    );
    // A projection removed (and the invariant that reads it).
    let src = BASE
        .replace(
            "  projection P {\n    from E\n    fold wasm \"a.wasm\"\n    table t { key k: uuid, n: int, o: string? }\n    table u { key k: uuid, m: int }\n  }\n",
            "",
        )
        .replace(
            "  invariant Max { on A projection P scope owner check wasm \"a.wasm\" }\n",
            "",
        );
    let diff = d(&src);
    assert_eq!(
        kinds(&diff),
        [
            (K::InvariantRemoved, C::Compatible),
            (K::ProjectionRemoved, C::Compatible)
        ],
        "{diff}"
    );
    assert_eq!(
        diff.actions(),
        [Action::DropProjection {
            context: "C".into(),
            name: "P".into(),
            tables: vec!["t".into(), "u".into()]
        }]
    );
    // A new projection needs no action: it starts from the log's beginning.
    let diff = expect(
        "  invariant Max",
        "  projection Q { from G fold wasm \"a.wasm\" table q { key k: uuid } }\n  invariant Max",
        &[(K::ProjectionAdded, C::Compatible)],
    );
    assert!(diff.actions().is_empty());
}

#[test]
fn process_rules() {
    let rebuild = Action::RebuildProcess {
        context: "C".into(),
        name: "Flow".into(),
    };
    let diff = expect(
        "    key k: uuid\n    from E\n    state { seen: uint }",
        "    key id: uuid\n    from E by k\n    state { seen: uint }",
        &[(K::ProcessKeyChanged, C::NeedsRebuild)],
    );
    assert_eq!(diff.actions(), std::slice::from_ref(&rebuild));
    expect(
        "    from E\n    state { seen: uint }",
        "    from E, G\n    state { seen: uint }",
        &[(K::ProcessSourcesChanged, C::NeedsRebuild)],
    );
    expect(
        "state { seen: uint }",
        "state { seen: uint, last: uuid? }",
        &[(K::ProcessStateChanged, C::NeedsRebuild)],
    );
    expect(
        "react wasm \"a.wasm\"\n    timers",
        "react wasm \"a.wasm\" export \"r\"\n    timers",
        &[(K::WasmChanged, C::Compatible)],
    );
    let diff = expect(
        "timers T1, T2",
        "timers T1",
        &[(K::TimerRemoved, C::Compatible)],
    );
    assert_eq!(
        diff.actions(),
        [Action::DropTimer {
            context: "C".into(),
            process: "Flow".into(),
            timer: "T2".into()
        }]
    );
    expect(
        "timers T1, T2",
        "timers T1, T2, T3",
        &[(K::TimerAdded, C::Compatible)],
    );
    let diff = expect(
        "  process Flow {\n    key k: uuid\n    from E\n    state { seen: uint }\n    react wasm \"a.wasm\"\n    timers T1, T2\n  }\n",
        "",
        &[(K::ProcessRemoved, C::Compatible)],
    );
    assert_eq!(
        diff.actions(),
        [Action::DropProcess {
            context: "C".into(),
            name: "Flow".into()
        }]
    );
}

#[test]
fn renames_are_remove_plus_add() {
    let diff = expect(
        "value Money { amount: decimal, currency: string }",
        "value Money { amount: decimal, ccy: string }",
        &[(K::FieldAdded, C::Breaking), (K::FieldRemoved, C::Breaking)],
    );
    assert_eq!(diff.changes[0].path, "Shared.Money.ccy");
    assert_eq!(diff.changes[1].path, "Shared.Money.currency");
    // A renamed value: removed and added (nothing refers to Money elsewhere
    // once every use is renamed too).
    let src = BASE
        .replace("value Money", "value Cash")
        .replace("Shared.Money", "Shared.Cash");
    let diff = d(&src);
    assert_eq!(
        kinds(&diff),
        [
            (K::FieldTypeChanged, C::Breaking),
            (K::FieldTypeChanged, C::Breaking),
            (K::ValueAdded, C::Compatible),
            (K::ValueRemoved, C::Compatible),
        ],
        "{diff}"
    );
}

#[test]
fn contexts_added_or_removed_expand_to_their_members() {
    let src = format!(
        "{BASE}context D {{\n  event H v1 {{ k: uuid }}\n  projection Q {{ from H fold wasm \"d.wasm\" table q {{ key k: uuid }} }}\n}}\n"
    );
    let diff = d(&src);
    assert_eq!(
        kinds(&diff),
        [
            (K::ContextAdded, C::Compatible),
            (K::EventFamilyAdded, C::Compatible),
            (K::ProjectionAdded, C::Compatible),
        ],
        "{diff}"
    );
    let back = diff_with(&schema(&src), &schema(BASE), &NoData);
    assert_eq!(
        kinds(&back),
        [
            (K::ContextRemoved, C::Compatible),
            (K::EventFamilyRemoved, C::Compatible),
            (K::ProjectionRemoved, C::Compatible),
        ],
        "{back}"
    );
    assert_eq!(
        back.actions(),
        [Action::DropProjection {
            context: "D".into(),
            name: "Q".into(),
            tables: vec!["q".into()]
        }]
    );
    let back = fold_schema::diff(&schema(&src), &schema(BASE));
    assert!(back.has_breaking(), "{back}");
}

#[test]
fn ordering_is_deterministic_and_by_path() {
    let src = edited(
        "value Money { amount: decimal, currency: string }",
        "value Money { amount: int, currency: string, zz: int?, aa: int? }",
    )
    .replace("timers T1, T2", "timers T2");
    let a = d(&src);
    let b = d(&src);
    assert_eq!(a, b);
    let paths: Vec<&str> = a.changes.iter().map(|c| c.path.as_str()).collect();
    let mut sorted = paths.clone();
    sorted.sort();
    assert_eq!(paths, sorted, "{a}");
    assert_eq!(
        paths,
        [
            "C.Flow.timers.T1",
            "Shared.Money.aa",
            "Shared.Money.amount",
            "Shared.Money.zz"
        ]
    );
}

#[test]
fn display_summary_and_serde() {
    let src = edited(
        "value Money { amount: decimal, currency: string }",
        "value Money { amount: int, currency: string, zz: int? }",
    )
    .replace("state { seen: uint }", "state { seen: uint, n: int }");
    let diff = d(&src);
    assert_eq!(diff.worst(), Some(C::Breaking));
    assert_eq!(
        diff.summary(),
        "3 change(s): 1 breaking, 1 rebuild, 1 compatible"
    );
    let text = diff.to_string();
    assert!(
        text.contains("[breaking] Shared.Money.amount: field `amount` changed from decimal to int"),
        "{text}"
    );
    assert!(text.contains("[rebuild] C.Flow.state:"), "{text}");
    assert!(
        text.contains("[compatible] Shared.Money.zz: field `zz` added (optional)"),
        "{text}"
    );
    assert!(text.ends_with(&diff.summary()), "{text}");
    assert_eq!(diff.breaking().count(), 1);
    let json = serde_json::to_value(&diff).unwrap();
    assert_eq!(json["changes"][0]["path"], "C.Flow.state");
    assert_eq!(json["changes"][0]["compatibility"], "needs_rebuild");
    assert_eq!(json["changes"][0]["action"]["action"], "rebuild_process");
    assert_eq!(json["changes"][0]["kind"], "process_state_changed");
    assert_eq!(json["changes"][1]["compatibility"], "breaking");
    assert_eq!(
        json["changes"][1]["action"],
        serde_json::json!({ "action": "none" })
    );
    let back: SchemaDiff = serde_json::from_value(json).unwrap();
    assert_eq!(back, diff);
}
