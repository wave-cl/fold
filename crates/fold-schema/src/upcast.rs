//! Applying a declarative upcast: the previous version's payload as this
//! version's. The resolver has checked that the result is a valid record
//! of the target version, so this is a pure rearrangement.

use serde_json::{Map, Value};

use crate::model::{DeclarativeUpcast, Field};

/// `payload` (a record of the previous version) as a record of the version
/// with `target` fields: `set` literals win, renamed fields move, fields
/// of the same name carry over, a target field nothing supplies takes its
/// default or `null`. Source fields the target lacks are dropped. Field
/// order is the target's.
pub fn apply_declarative(up: &DeclarativeUpcast, target: &[Field], payload: &Value) -> Value {
    let source = payload.as_object();
    let mut out = Map::with_capacity(target.len());
    for t in target {
        let v = if let Some((_, v)) = up.set.iter().find(|(f, _)| *f == t.name) {
            v.clone()
        } else if let Some((old, _)) = up.rename.iter().find(|(_, new)| *new == t.name) {
            source
                .and_then(|s| s.get(old))
                .cloned()
                .unwrap_or(Value::Null)
        } else if let Some(v) = source
            .filter(|_| !up.rename.iter().any(|(old, _)| *old == t.name))
            .and_then(|s| s.get(&t.name))
        {
            v.clone()
        } else if let Some(d) = &t.default {
            d.clone()
        } else {
            Value::Null
        };
        out.insert(t.name.clone(), v);
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::types::{Scalar, Type};

    fn field(name: &str, ty: Type, default: Option<Value>) -> Field {
        Field {
            name: name.into(),
            ty,
            docs: vec![],
            default,
        }
    }

    #[test]
    fn set_rename_carry_default_null_and_drop() {
        let up = DeclarativeUpcast {
            set: vec![("note".into(), json!("legacy"))],
            rename: vec![("cust".into(), "customer".into())],
        };
        let target = vec![
            field("id", Type::Scalar(Scalar::Uuid), None),
            field("customer", Type::Scalar(Scalar::String), None),
            field("note", Type::Scalar(Scalar::String), None),
            field("qty", Type::Scalar(Scalar::Uint), Some(json!(1))),
            field(
                "reason",
                Type::Optional(Box::new(Type::Scalar(Scalar::String))),
                None,
            ),
        ];
        let out = apply_declarative(
            &up,
            &target,
            &json!({ "id": "x", "cust": "c", "gone": true, "note": "old" }),
        );
        assert_eq!(
            out,
            json!({ "id": "x", "customer": "c", "note": "legacy", "qty": 1, "reason": null })
        );
        let keys: Vec<&String> = out.as_object().unwrap().keys().collect();
        assert_eq!(
            keys,
            ["id", "customer", "note", "qty", "reason"],
            "target order"
        );
    }

    #[test]
    fn a_renamed_away_field_does_not_also_carry_under_its_old_name() {
        let up = DeclarativeUpcast {
            set: vec![],
            rename: vec![("a".into(), "b".into())],
        };
        let target = vec![
            field("a", Type::Scalar(Scalar::Int), Some(json!(0))),
            field("b", Type::Scalar(Scalar::Int), None),
        ];
        assert_eq!(
            apply_declarative(&up, &target, &json!({ "a": 5 })),
            json!({ "a": 0, "b": 5 })
        );
    }
}
