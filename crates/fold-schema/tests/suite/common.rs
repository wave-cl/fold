#![allow(dead_code)]

use fold_schema::{Schema, Type, compile};
use serde_json::Value;

pub const ORDERS_DOMAIN: &str = include_str!("../../../../examples/orders/domain.fold");
pub const ORDERS_DERIVE: &str = include_str!("../../../../examples/orders/derive.fold");
pub const ORDERS_APP: &str = include_str!("../../../../examples/orders/app.fold");

/// The example schema, verbatim, as the bundle `Sources::bundle` writes for
/// it (root first, then each import in load order).
pub const ORDERS: &str = concat!(
    "// ---- file: app.fold\n",
    include_str!("../../../../examples/orders/app.fold"),
    "// ---- file: derive.fold\n",
    include_str!("../../../../examples/orders/derive.fold"),
    "// ---- file: domain.fold\n",
    include_str!("../../../../examples/orders/domain.fold"),
);

/// A three-file bundle: `app` (layer application, imports derive.fold),
/// `derive` (layer derivation, imports domain.fold) and `domain`. Each
/// argument is the body after its `layer` and `import` lines.
pub fn bundle(domain: &str, derive: &str, app: &str) -> String {
    format!(
        "// ---- file: app.fold\nlayer application\n\nimport \"derive.fold\"\n\n{app}\n\
         // ---- file: derive.fold\nlayer derivation\n\nimport \"domain.fold\"\n\n{derive}\n\
         // ---- file: domain.fold\nlayer domain\n\n{domain}\n"
    )
}

pub fn orders() -> Schema {
    compile(ORDERS).unwrap_or_else(|d| panic!("orders schema must compile:\n{d}"))
}

/// A schema exercising every type shape the validator knows.
pub const TYPES: &str = r#"// ---- file: app.fold
layer application

import "derive.fold"

commands T.A { Do { l: L } -> wasm "a.wasm" }
// ---- file: derive.fold
layer derivation

import "domain.fold"

state T.A { lines: map<uuid, L>, one: L?, many: [L] }
  evolve wasm "a.wasm"

projection T.P {
  from E
  fold wasm "a.wasm"
  table nums { key k: uuid, i: int, u: uint, d: decimal, oi: int?, s: string, se: set<string>, li: [int],
               ma: map<string, decimal>, mi: map<int, int>, ms: map<string, string>, lm: [Shared.Money],
               is: set<int>, od: decimal? }
  table defaults { key k: uuid, i: int, u: uint, d: decimal, o: string?, se: set<int>, li: [string], ma: map<string, int> }
  table nodefault { key k: uuid, name: string }
  table with_defaults { key k: uuid, n: int = 7, s: string = "x", c: Color = Red, o: string? }
}
// ---- file: domain.fold
layer domain

context Shared { value Money { amount: decimal, currency: string } }

context T {
  enum Color { Red, Green }
  enum Shape { Dot, Box { w: int, h: int }, Tag { label: string, color: Color } }
  value Shaped { shape: Shape } rules { Flat: shape in [Dot, Box] }
  value Defaulted { n: int = 7, s: string = "", c: Color = Red, d: decimal = 2.50, o: string? }
  value Nested { inner: Defaulted, many: [Defaulted], by: map<string, Defaulted>, shape: Shape }
  value BadDefault { n: int = 0 } rules { Pos: n >= 1 }
  value Everything {
    s: string, i: int, u: uint, d: decimal, b: bool, id: uuid, ts: timestamp, by: bytes,
    opt: string?, li: [int], se: set<string>, ma: map<int, string>, money: Shared.Money,
    color: Color, nested: [Shared.Money], mm: map<string, [int]>, ts_set: set<timestamp>,
    dec_map: map<decimal, int>, bool_map: map<bool, int>, uuid_set: set<uuid>,
  }
  event E v1 { k: uuid, lines: map<uuid, A.L> }
  aggregate A {
    key k: uuid
    stream "a-{k}"
    entity L { id lid: uuid, n: int }
    events E
  }
}
"#;

pub fn types_schema() -> Schema {
    compile(TYPES).unwrap_or_else(|d| panic!("types schema must compile:\n{d}"))
}

/// The type of field `field` of the context-level value `value` in `ctx`.
pub fn field_ty(schema: &Schema, ctx: &str, value: &str, field: &str) -> Type {
    schema.contexts[ctx].values[value]
        .fields
        .iter()
        .find(|f| f.name == field)
        .unwrap_or_else(|| panic!("no field {field}"))
        .ty
        .clone()
}

pub fn json(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or_else(|e| panic!("bad test JSON {s}: {e}"))
}

/// 1-based line of the first line containing `needle` (which must be unique).
pub fn line_of(src: &str, needle: &str) -> usize {
    let hits: Vec<usize> = src
        .lines()
        .enumerate()
        .filter(|(_, l)| l.contains(needle))
        .map(|(i, _)| i + 1)
        .collect();
    assert_eq!(
        hits.len(),
        1,
        "needle {needle:?} must occur on exactly one line, found {hits:?}"
    );
    hits[0]
}

pub const U1: &str = "11111111-1111-1111-1111-111111111111";
pub const U2: &str = "22222222-2222-2222-2222-222222222222";
pub const U3: &str = "33333333-3333-3333-3333-333333333333";
