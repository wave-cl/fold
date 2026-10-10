use fold_schema::{
    Action, AssumeData, ChangeKind as K, Compatibility as C, EventFamilyRef, Facts, Layer,
    SchemaDiff, compile, diff, diff_derivation, diff_domain, diff_with,
};

/// The fixture as a two-file bundle (root first, domain last).
const BASE: &str = r#"// ---- file: derive.fold
layer derivation

import "domain.fold"

state C.A { lines: map<uuid, Ent>, st: En, owner: uuid }
  evolve wasm "a.wasm"
  snapshot every 10
projection C.P {
  from E
  fold wasm "a.wasm"
  table t { key k: uuid, n: int, o: string? }
  table u { key k: uuid, m: int }
}
// ---- file: domain.fold
layer domain

context Shared {
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
    let reordered = r#"// ---- file: derive.fold
//! docs
layer derivation
import "domain.fold"
// a comment
/// the projection
projection C.P {
  from E
  fold wasm "a.wasm"
  /// first table
  table t { key k: uuid, n: int, o: string? }
  table u { key k: uuid, m: int }
}
state C.A { lines: map<uuid, Ent>, st: En, owner: uuid }
  evolve wasm "a.wasm"
  snapshot every 10
// ---- file: domain.fold
layer domain
/// the C context
context C {
  aggregate A {
    key k: uuid
    stream "a-{k}"
    entity Ent { id eid: uuid, n: int }
    events E
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
        "state C.A { lines: map<uuid, Ent>, st: En, owner: uuid }",
        "state C.A { lines: map<uuid, Ent>, st: En, owner: uuid, n: int }",
        &[(K::AggregateStateChanged, C::NeedsRebuild)],
    );
    assert_eq!(diff.changes[0].path, "C.A.state");
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
    // A state removed or added.
    let diff = d(&without_state());
    assert_eq!(kinds(&diff), [(K::StateRemoved, C::Compatible)], "{diff}");
    assert_eq!(
        diff.actions(),
        [Action::ClearAggregateSnapshots {
            context: "C".into(),
            name: "A".into()
        }]
    );
    let back = fold_schema::diff(&schema(&without_state()), &schema(BASE));
    assert_eq!(kinds(&back), [(K::StateAdded, C::Compatible)], "{back}");
    assert!(back.actions().is_empty());
    // The events list.
    let diff = expect(
        "    events E\n  }",
        "    events E, G\n  }",
        &[(K::AggregateEventsChanged, C::Compatible)],
    );
    assert_eq!(diff.changes[0].path, "C.A.events");
    let both = edited("    events E\n  }", "    events E, G\n  }");
    let back = diff_with(&schema(&both), &schema(BASE), &AssumeData);
    assert_eq!(kinds(&back), [(K::AggregateEventsChanged, C::Breaking)]);
    let back = diff_with(&schema(&both), &schema(BASE), &NoData);
    assert_eq!(kinds(&back), [(K::AggregateEventsChanged, C::Compatible)]);
}

/// BASE without A's state.
fn without_state() -> String {
    BASE.replace(
        "state C.A { lines: map<uuid, Ent>, st: En, owner: uuid }\n  evolve wasm \"a.wasm\"\n  snapshot every 10\n",
        "",
    )
}

#[test]
fn aggregate_removal_depends_on_streams() {
    // Removing A also removes what rests on it: its state; P reads events.
    let src = without_state().replace(
        "  aggregate A {\n    key k: uuid\n    stream \"a-{k}\"\n    entity Ent { id eid: uuid, n: int }\n    events E\n  }\n",
        "",
    );
    let with = d(&src);
    assert_eq!(
        kinds(&with),
        [
            (K::AggregateRemoved, C::Breaking),
            (K::StateRemoved, C::Compatible),
        ],
        "{with}"
    );
    assert_eq!(
        with.actions(),
        [
            Action::ClearAggregateSnapshots {
                context: "C".into(),
                name: "A".into()
            },
            Action::DropAggregate {
                context: "C".into(),
                name: "A".into()
            }
        ]
    );
    assert_eq!(
        with.actions_for(Layer::Derivation),
        with.actions(),
        "both actions are the derivation node's"
    );
    assert!(with.actions_for(Layer::Domain).is_empty());
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
        "  from E\n  fold",
        "  from E, G\n  fold",
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
        "table u { key k: uuid, m: int }\n  table w { key k: uuid }",
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
        "table t { key k: uuid, n: int, o: string? }\n  table u { key k: uuid, m: int }",
        "table t { key k: uuid, o: string? }\n  table u { key k: uuid, m: string }",
        &[
            (K::ColumnRemoved, C::NeedsRebuild),
            (K::ColumnTypeChanged, C::NeedsRebuild),
        ],
    );
    assert_eq!(diff.actions(), std::slice::from_ref(&rebuild));
    // A table removed is dropped; the projection keeps going.
    let diff = expect(
        "\n  table u { key k: uuid, m: int }",
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
    // A projection removed.
    let src = BASE.replace(
        "projection C.P {\n  from E\n  fold wasm \"a.wasm\"\n  table t { key k: uuid, n: int, o: string? }\n  table u { key k: uuid, m: int }\n}\n",
        "",
    );
    let diff = d(&src);
    assert_eq!(
        kinds(&diff),
        [(K::ProjectionRemoved, C::Compatible)],
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
        "projection C.P {",
        "projection C.Q { from G fold wasm \"a.wasm\" table q { key k: uuid } }\nprojection C.P {",
        &[(K::ProjectionAdded, C::Compatible)],
    );
    assert!(diff.actions().is_empty());
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
    let src = format!("{BASE}context D {{\n  event H v1 {{ k: uuid }}\n}}\n").replace(
        "projection C.P {",
        "projection D.Q { from H fold wasm \"d.wasm\" table q { key k: uuid } }\nprojection C.P {",
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
    .replace("table u { key k: uuid, m: int }", "table u { key k: uuid }");
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
            "C.P.u.m",
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
    .replace(
        "table u { key k: uuid, m: int }",
        "table u { key k: uuid, m: string }",
    );
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
    assert!(text.contains("[rebuild] C.P.u.m:"), "{text}");
    assert!(
        text.contains("[compatible] Shared.Money.zz: field `zz` added (optional)"),
        "{text}"
    );
    assert!(text.ends_with(&diff.summary()), "{text}");
    assert_eq!(diff.breaking().count(), 1);
    let json = serde_json::to_value(&diff).unwrap();
    assert_eq!(json["changes"][0]["path"], "C.P.u.m");
    assert_eq!(json["changes"][0]["compatibility"], "needs_rebuild");
    assert_eq!(json["changes"][0]["action"]["action"], "rebuild_projection");
    assert_eq!(json["changes"][0]["kind"], "column_type_changed");
    assert_eq!(json["changes"][1]["compatibility"], "breaking");
    assert_eq!(
        json["changes"][1]["action"],
        serde_json::json!({ "action": "none" })
    );
    let back: SchemaDiff = serde_json::from_value(json).unwrap();
    assert_eq!(back, diff);
}

#[test]
fn each_layer_diffs_what_it_knows() {
    // A domain change and a derivation change.
    let src = edited(
        "value Money { amount: decimal, currency: string }",
        "value Money { amount: int, currency: string }",
    )
    .replace("snapshot every 10", "snapshot every 20")
    .replace("table u { key k: uuid, m: int }", "table u { key k: uuid }");
    let old = schema(BASE);
    let new = schema(&src);
    let dom = diff_domain(&old, &new, &AssumeData);
    assert_eq!(kinds(&dom), [(K::FieldTypeChanged, C::Breaking)], "{dom}");
    let der = diff_derivation(&old, &new, &AssumeData);
    assert_eq!(
        kinds(&der),
        [
            (K::SnapshotEveryChanged, C::Compatible),
            (K::ColumnRemoved, C::NeedsRebuild),
            (K::FieldTypeChanged, C::Breaking),
        ],
        "{der}"
    );
    assert_eq!(der, diff(&old, &new));
    assert_eq!(
        der.actions_for(Layer::Derivation),
        [Action::RebuildProjection {
            context: "C".into(),
            name: "P".into()
        }]
    );
    assert!(der.actions_for(Layer::Domain).is_empty());
    assert_eq!(Action::None.layer(), Layer::Domain);
    // The lower layer compares through the deref: a derivation schema
    // diffs against another's domain.
    let dom_only = diff_domain(&old.domain, &new.domain, &AssumeData);
    assert_eq!(dom_only, dom);
}
