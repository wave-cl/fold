//! Applying declarative upcasts through a compiled schema.

use fold_schema::{UpcastHow, compile, upcast::apply_declarative};
use serde_json::json;

#[test]
fn a_declarative_upcast_rewrites_a_v1_payload_as_v2() {
    let src = r#"context C {
  value Money { amount: decimal, currency: string }
  event Placed v1 { id: uuid, cust: string, total: Money, legacy: int }
  event Placed v2 { id: uuid, customer: string, total: Money, note: string, qty: uint = 1, tag: string? }
    upcast from v1 { rename cust as customer, set note: "migrated" }
}"#;
    let s = compile(src).unwrap_or_else(|d| panic!("{d}"));
    let fam = &s.contexts["C"].events["Placed"];
    let v2 = &fam.versions[&2];
    let UpcastHow::Declarative(up) = &v2.upcast.as_ref().unwrap().how else {
        panic!()
    };
    let v1 = json!({ "id": "11111111-1111-1111-1111-111111111111", "cust": "ada", "total": { "amount": "1.00", "currency": "EUR" }, "legacy": 3 });
    let out = apply_declarative(up, &v2.fields, &v1);
    assert_eq!(
        out,
        json!({ "id": "11111111-1111-1111-1111-111111111111", "customer": "ada", "total": { "amount": "1.00", "currency": "EUR" }, "note": "migrated", "qty": 1, "tag": null })
    );
    s.validate_event(v2, &out)
        .unwrap_or_else(|e| panic!("{e:?}"));
}
