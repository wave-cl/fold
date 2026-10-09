//! Table keys: JSON key objects ⇄ the order-preserving byte encoding the
//! read-model store indexes by.

use fold_core::keyenc::{self, KeyPart};
use fold_schema::{Field, Scalar, Schema, Table, Type};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    #[error("key must be a JSON object")]
    NotAnObject,
    #[error("key field {0} is missing")]
    Missing(String),
    #[error("key field {field}: {reason}")]
    Bad { field: String, reason: String },
    #[error("key has unknown field {0}")]
    Unknown(String),
}

fn scalar_of(f: &Field) -> Option<Scalar> {
    match &f.ty {
        Type::Scalar(s) => Some(*s),
        _ => None,
    }
}

fn part<'a>(field: &Field, v: &'a Value) -> Result<KeyPart<'a>, KeyError> {
    let bad = |reason: String| KeyError::Bad {
        field: field.name.clone(),
        reason,
    };
    let scalar = scalar_of(field).ok_or_else(|| bad("key columns must be scalars".into()))?;
    Ok(match scalar {
        Scalar::Uuid => {
            let s = v
                .as_str()
                .ok_or_else(|| bad("expected a uuid string".into()))?;
            let u = uuid::Uuid::parse_str(s).map_err(|e| bad(e.to_string()))?;
            KeyPart::Uuid(u)
        }
        Scalar::String | Scalar::Decimal | Scalar::Bytes => {
            let s = v.as_str().ok_or_else(|| bad("expected a string".into()))?;
            KeyPart::Str(s)
        }
        Scalar::Int => KeyPart::I64(
            v.as_i64()
                .ok_or_else(|| bad("expected an integer".into()))?,
        ),
        Scalar::Uint => KeyPart::U64(
            v.as_u64()
                .ok_or_else(|| bad("expected a non-negative integer".into()))?,
        ),
        Scalar::Bool => KeyPart::Bool(
            v.as_bool()
                .ok_or_else(|| bad("expected a boolean".into()))?,
        ),
        Scalar::Timestamp => {
            let s = v
                .as_str()
                .ok_or_else(|| bad("expected an RFC 3339 string".into()))?;
            let ts: jiff::Timestamp = s.parse().map_err(|e: jiff::Error| bad(e.to_string()))?;
            KeyPart::I64(ts.as_nanosecond() as i64)
        }
    })
}

/// Encodes one scalar field's value on its own (a process correlation key).
pub fn encode_field(field: &Field, v: &Value) -> Result<Vec<u8>, KeyError> {
    keyenc::encode_parts(&[part(field, v)?]).map_err(|e| KeyError::Bad {
        field: field.name.clone(),
        reason: e.to_string(),
    })
}

/// The row key of a process instance's timer: the instance's encoded key,
/// a zero byte, the timer's name.
pub fn encode_timer_key(instance: &[u8], name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(instance.len() + 1 + name.len());
    out.extend_from_slice(instance);
    out.push(0);
    out.extend_from_slice(name.as_bytes());
    out
}

/// Encodes a full key object: every key field present, nothing extra.
pub fn encode(_schema: &Schema, table: &Table, key: &Value) -> Result<Vec<u8>, KeyError> {
    let obj = key.as_object().ok_or(KeyError::NotAnObject)?;
    for k in obj.keys() {
        if !table.keys.iter().any(|f| &f.name == k) {
            return Err(KeyError::Unknown(k.clone()));
        }
    }
    let mut parts = Vec::with_capacity(table.keys.len());
    for f in &table.keys {
        let v = obj
            .get(&f.name)
            .ok_or_else(|| KeyError::Missing(f.name.clone()))?;
        parts.push(part(f, v)?);
    }
    keyenc::encode_parts(&parts).map_err(|e| KeyError::Bad {
        field: String::new(),
        reason: e.to_string(),
    })
}

/// Encodes a key prefix: the leading key fields, in declaration order.
pub fn encode_prefix(_schema: &Schema, table: &Table, prefix: &Value) -> Result<Vec<u8>, KeyError> {
    if prefix.is_null() {
        return Ok(Vec::new());
    }
    let obj = prefix.as_object().ok_or(KeyError::NotAnObject)?;
    for k in obj.keys() {
        if !table.keys.iter().any(|f| &f.name == k) {
            return Err(KeyError::Unknown(k.clone()));
        }
    }
    let mut parts = Vec::new();
    for f in &table.keys {
        match obj.get(&f.name) {
            Some(v) => parts.push(part(f, v)?),
            None => break,
        }
    }
    if parts.len() != obj.len() {
        return Err(KeyError::Bad {
            field: table.keys[parts.len()].name.clone(),
            reason: "a prefix must name the leading key fields without gaps".into(),
        });
    }
    keyenc::encode_parts(&parts).map_err(|e| KeyError::Bad {
        field: String::new(),
        reason: e.to_string(),
    })
}
