//! Strict JSON validation and canonicalization against resolved types.
//!
//! One record validator serves events, value objects, entities, state,
//! commands and table rows. Validation and canonicalization share one walk:
//! the walk collects every error with its JSON path and, when it finds none,
//! has also built the canonical form (sets sorted and deduplicated, map
//! entries sorted, timestamps in UTC, absent optionals as `null`).

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use base64::Engine;
use rust_decimal::Decimal;
use serde_json::{Map, Value};

use crate::model::{Aggregate, Command, EventType, Field, Schema, Table};
use crate::types::{Scalar, Type};

/// A validation failure at a JSON path (`$`, `$.lines[0].sku`,
/// `$.lines["<key>"]`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidationError {
    /// A required field is absent or `null`.
    Missing { path: String },
    /// A field the record does not declare.
    UnknownField { path: String },
    /// The JSON value has the wrong shape.
    WrongType {
        path: String,
        expected: String,
        found: String,
    },
    /// The shape is right but the content is not (bad uuid, bad base64,
    /// non-canonical map key, ...).
    Invalid {
        path: String,
        expected: String,
        message: String,
    },
    UnknownVariant {
        path: String,
        variant: String,
        enum_name: String,
    },
    /// A set element occurs twice.
    DuplicateElement { path: String },
    /// In `map<K, E>` the key differs from the entity's id.
    EntityKeyMismatch {
        path: String,
        key: String,
        id: String,
    },
    /// A type reference the schema does not define (cannot happen for a
    /// compiled schema; reported rather than panicking).
    UnknownType { path: String, name: String },
}

impl ValidationError {
    pub fn path(&self) -> &str {
        match self {
            ValidationError::Missing { path }
            | ValidationError::UnknownField { path }
            | ValidationError::WrongType { path, .. }
            | ValidationError::Invalid { path, .. }
            | ValidationError::UnknownVariant { path, .. }
            | ValidationError::DuplicateElement { path }
            | ValidationError::EntityKeyMismatch { path, .. }
            | ValidationError::UnknownType { path, .. } => path,
        }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::Missing { path } => write!(f, "{path}: required field is missing"),
            ValidationError::UnknownField { path } => write!(f, "{path}: unknown field"),
            ValidationError::WrongType {
                path,
                expected,
                found,
            } => write!(f, "{path}: expected {expected}, found {found}"),
            ValidationError::Invalid {
                path,
                expected,
                message,
            } => write!(f, "{path}: invalid {expected}: {message}"),
            ValidationError::UnknownVariant {
                path,
                variant,
                enum_name,
            } => write!(f, "{path}: `{variant}` is not a variant of {enum_name}"),
            ValidationError::DuplicateElement { path } => {
                write!(f, "{path}: duplicate set element")
            }
            ValidationError::EntityKeyMismatch { path, key, id } => {
                write!(
                    f,
                    "{path}: map key `{key}` does not equal the entity id `{id}`"
                )
            }
            ValidationError::UnknownType { path, name } => write!(f, "{path}: unknown type {name}"),
        }
    }
}

impl std::error::Error for ValidationError {}

/// A scalar value in its typed form, ordered the way sets and map entries
/// are sorted (numbers numerically, strings bytewise, timestamps by instant).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScalarKey {
    Bool(bool),
    Int(i64),
    Uint(u64),
    Decimal(Decimal),
    Str(String),
    Uuid(uuid::Uuid),
    Timestamp(jiff::Timestamp),
    Bytes(Vec<u8>),
}

impl ScalarKey {
    /// The canonical string form used for map keys: `"42"`, `"true"`, a
    /// lower-case hyphenated uuid, a normalized decimal (`"12.5"`), an RFC
    /// 3339 UTC timestamp.
    pub fn canonical_string(&self) -> String {
        match self {
            ScalarKey::Bool(b) => b.to_string(),
            ScalarKey::Int(i) => i.to_string(),
            ScalarKey::Uint(u) => u.to_string(),
            ScalarKey::Decimal(d) => d.normalize().to_string(),
            ScalarKey::Str(s) => s.clone(),
            ScalarKey::Uuid(u) => u.hyphenated().to_string(),
            ScalarKey::Timestamp(t) => t.to_string(),
            ScalarKey::Bytes(b) => base64::engine::general_purpose::STANDARD.encode(b),
        }
    }

    /// The canonical JSON form. Decimals keep their scale (`"40.00"` stays
    /// `"40.00"`); timestamps are rendered in UTC.
    pub fn to_value(&self) -> Value {
        match self {
            ScalarKey::Bool(b) => Value::Bool(*b),
            ScalarKey::Int(i) => Value::from(*i),
            ScalarKey::Uint(u) => Value::from(*u),
            ScalarKey::Decimal(d) => Value::String(d.to_string()),
            ScalarKey::Str(s) => Value::String(s.clone()),
            ScalarKey::Uuid(u) => Value::String(u.hyphenated().to_string()),
            ScalarKey::Timestamp(t) => Value::String(t.to_string()),
            ScalarKey::Bytes(b) => {
                Value::String(base64::engine::general_purpose::STANDARD.encode(b))
            }
        }
    }

    /// Parse a map key written in canonical form.
    pub fn parse_canonical(sc: Scalar, s: &str) -> Option<ScalarKey> {
        let key = match sc {
            Scalar::String => ScalarKey::Str(s.to_string()),
            Scalar::Int => ScalarKey::Int(s.parse().ok()?),
            Scalar::Uint => ScalarKey::Uint(s.parse().ok()?),
            Scalar::Decimal => ScalarKey::Decimal(Decimal::from_str(s).ok()?),
            Scalar::Bool => ScalarKey::Bool(s.parse().ok()?),
            Scalar::Uuid => ScalarKey::Uuid(uuid::Uuid::parse_str(s).ok()?),
            Scalar::Timestamp => ScalarKey::Timestamp(s.parse().ok()?),
            Scalar::Bytes => {
                ScalarKey::Bytes(base64::engine::general_purpose::STANDARD.decode(s).ok()?)
            }
        };
        (key.canonical_string() == s).then_some(key)
    }
}

/// `-?[0-9]+(\.[0-9]+)?`: no exponent, no sign `+`, no bare `.`.
fn is_plain_decimal(s: &str) -> bool {
    let digits = s.strip_prefix('-').unwrap_or(s);
    let (int, frac) = match digits.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (digits, None),
    };
    let all_digits = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit());
    all_digits(int) && frac.is_none_or(all_digits)
}

fn describe(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "an integer",
        Value::Number(_) => "a non-integer number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

fn wrong(path: &str, expected: impl Into<String>, v: &Value) -> ValidationError {
    ValidationError::WrongType {
        path: path.to_string(),
        expected: expected.into(),
        found: describe(v).to_string(),
    }
}

fn invalid(path: &str, expected: impl Into<String>, message: impl Into<String>) -> ValidationError {
    ValidationError::Invalid {
        path: path.to_string(),
        expected: expected.into(),
        message: message.into(),
    }
}

/// Check a JSON value against a scalar type and return its typed form.
pub fn check_scalar(sc: Scalar, v: &Value, path: &str) -> Result<ScalarKey, ValidationError> {
    match sc {
        Scalar::String => match v {
            Value::String(s) => Ok(ScalarKey::Str(s.clone())),
            _ => Err(wrong(path, "a string", v)),
        },
        Scalar::Int => match v {
            Value::Number(n) => n.as_i64().map(ScalarKey::Int).ok_or_else(|| {
                if n.is_u64() {
                    invalid(path, "int", "out of range for a 64-bit signed integer")
                } else {
                    wrong(path, "an integer", v)
                }
            }),
            _ => Err(wrong(path, "an integer", v)),
        },
        Scalar::Uint => match v {
            Value::Number(n) => n.as_u64().map(ScalarKey::Uint).ok_or_else(|| {
                if n.is_i64() {
                    invalid(path, "uint", "negative")
                } else {
                    wrong(path, "a non-negative integer", v)
                }
            }),
            _ => Err(wrong(path, "a non-negative integer", v)),
        },
        Scalar::Decimal => match v {
            Value::String(s) => {
                if !is_plain_decimal(s) {
                    return Err(invalid(
                        path,
                        "decimal",
                        format!("{s:?} is not of the form -?digits[.digits]"),
                    ));
                }
                Decimal::from_str(s)
                    .map(ScalarKey::Decimal)
                    .map_err(|e| invalid(path, "decimal", e.to_string()))
            }
            Value::Number(_) => Err(wrong(path, "a decimal as a string like \"12.50\"", v)),
            _ => Err(wrong(path, "a decimal as a string like \"12.50\"", v)),
        },
        Scalar::Bool => match v {
            Value::Bool(b) => Ok(ScalarKey::Bool(*b)),
            _ => Err(wrong(path, "a boolean", v)),
        },
        Scalar::Uuid => match v {
            Value::String(s) => {
                let u =
                    uuid::Uuid::parse_str(s).map_err(|e| invalid(path, "uuid", e.to_string()))?;
                if u.hyphenated().to_string() != *s {
                    return Err(invalid(path, "uuid", "must be lower-case and hyphenated"));
                }
                Ok(ScalarKey::Uuid(u))
            }
            _ => Err(wrong(path, "a uuid string", v)),
        },
        Scalar::Timestamp => match v {
            Value::String(s) => s
                .parse::<jiff::Timestamp>()
                .map(ScalarKey::Timestamp)
                .map_err(|e| invalid(path, "timestamp", e.to_string())),
            _ => Err(wrong(path, "an RFC 3339 timestamp string", v)),
        },
        Scalar::Bytes => match v {
            Value::String(s) => base64::engine::general_purpose::STANDARD
                .decode(s)
                .map(ScalarKey::Bytes)
                .map_err(|e| invalid(path, "bytes", e.to_string())),
            _ => Err(wrong(path, "a base64 string", v)),
        },
    }
}

impl Scalar {
    /// The canonical string form of a JSON value of this scalar type, as
    /// used for map keys; an error if the value is not of this type.
    pub fn canonical_key_string(self, v: &Value) -> Result<String, ValidationError> {
        check_scalar(self, v, "$").map(|k| k.canonical_string())
    }

    /// The canonical JSON form of a JSON value of this scalar type.
    pub fn canonical_value(self, v: &Value) -> Result<Value, ValidationError> {
        check_scalar(self, v, "$").map(|k| k.to_value())
    }
}

fn field_path(path: &str, name: &str) -> String {
    format!("{path}.{name}")
}

fn index_path(path: &str, i: usize) -> String {
    format!("{path}[{i}]")
}

fn key_path(path: &str, key: &str) -> String {
    format!("{path}[{key:?}]")
}

/// The validating/canonicalizing walk.
struct Walk<'s> {
    schema: &'s Schema,
    errors: Vec<ValidationError>,
    /// Merge duplicate set elements instead of rejecting them
    /// (canonicalization does, validation does not).
    dedup: bool,
}

impl Walk<'_> {
    fn err(&mut self, e: ValidationError) -> Option<Value> {
        self.errors.push(e);
        None
    }

    fn record(&mut self, fields: &[Field], v: &Value, path: &str) -> Option<Value> {
        let Value::Object(obj) = v else {
            return self.err(wrong(path, "an object", v));
        };
        let mut out = Map::with_capacity(fields.len());
        let mut ok = true;
        for field in fields {
            let fpath = field_path(path, &field.name);
            match self.value(
                &field.ty,
                obj.get(&field.name).unwrap_or(&Value::Null),
                &fpath,
            ) {
                Some(canon) => {
                    out.insert(field.name.clone(), canon);
                }
                None => ok = false,
            }
        }
        for key in obj.keys() {
            if !fields.iter().any(|f| &f.name == key) {
                self.errors.push(ValidationError::UnknownField {
                    path: field_path(path, key),
                });
                ok = false;
            }
        }
        ok.then_some(Value::Object(out))
    }

    fn value(&mut self, ty: &Type, v: &Value, path: &str) -> Option<Value> {
        match ty {
            Type::Optional(inner) => {
                if v.is_null() {
                    Some(Value::Null)
                } else {
                    self.value(inner, v, path)
                }
            }
            _ if v.is_null() => self.err(ValidationError::Missing {
                path: path.to_string(),
            }),
            Type::Scalar(sc) => match check_scalar(*sc, v, path) {
                Ok(k) => Some(k.to_value()),
                Err(e) => self.err(e),
            },
            Type::Enum(r) => {
                let Some(en) = self.schema.enum_type(r) else {
                    return self.err(ValidationError::UnknownType {
                        path: path.to_string(),
                        name: r.to_string(),
                    });
                };
                let Value::String(s) = v else {
                    return self.err(wrong(path, format!("a variant of {}", en.name), v));
                };
                if en.variants.iter().any(|variant| variant == s) {
                    Some(v.clone())
                } else {
                    self.err(ValidationError::UnknownVariant {
                        path: path.to_string(),
                        variant: s.clone(),
                        enum_name: r.to_string(),
                    })
                }
            }
            Type::Value(r) => {
                let Some(vt) = self.schema.value_type(r) else {
                    return self.err(ValidationError::UnknownType {
                        path: path.to_string(),
                        name: r.to_string(),
                    });
                };
                self.record(&vt.fields, v, path)
            }
            Type::Entity(r) => {
                let Some(en) = self.schema.entity(r) else {
                    return self.err(ValidationError::UnknownType {
                        path: path.to_string(),
                        name: r.to_string(),
                    });
                };
                self.record(&en.fields, v, path)
            }
            Type::List(elem) => {
                let Value::Array(items) = v else {
                    return self.err(wrong(path, "an array", v));
                };
                let mut out = Vec::with_capacity(items.len());
                let mut ok = true;
                for (i, item) in items.iter().enumerate() {
                    match self.value(elem, item, &index_path(path, i)) {
                        Some(c) => out.push(c),
                        None => ok = false,
                    }
                }
                ok.then_some(Value::Array(out))
            }
            Type::Set(sc) => {
                let Value::Array(items) = v else {
                    return self.err(wrong(path, "an array", v));
                };
                let mut seen = BTreeSet::new();
                let mut ok = true;
                for (i, item) in items.iter().enumerate() {
                    let ipath = index_path(path, i);
                    match check_scalar(*sc, item, &ipath) {
                        Ok(k) => {
                            if !seen.insert(k) && !self.dedup {
                                self.errors
                                    .push(ValidationError::DuplicateElement { path: ipath });
                                ok = false;
                            }
                        }
                        Err(e) => {
                            self.errors.push(e);
                            ok = false;
                        }
                    }
                }
                ok.then(|| Value::Array(seen.iter().map(ScalarKey::to_value).collect()))
            }
            Type::Map(k, vty) => {
                let Value::Object(obj) = v else {
                    return self.err(wrong(path, "an object", v));
                };
                let mut entries: Vec<(ScalarKey, String, Value)> = Vec::with_capacity(obj.len());
                let mut ok = true;
                let entity = vty.entity().and_then(|r| self.schema.entity(r));
                for (key, val) in obj {
                    let kpath = key_path(path, key);
                    let Some(kk) = ScalarKey::parse_canonical(*k, key) else {
                        self.errors.push(invalid(
                            &kpath,
                            format!("map key of type {k}"),
                            format!("`{key}` is not the canonical form of a {k}"),
                        ));
                        ok = false;
                        continue;
                    };
                    let Some(canon) = self.value(vty, val, &kpath) else {
                        ok = false;
                        continue;
                    };
                    if let Some(en) = entity
                        && let Value::Object(fields) = &canon
                        && let Type::Scalar(id_sc) = en.id.ty.required()
                        && let Some(id) = fields.get(&en.id.name)
                        && let Ok(id_key) = check_scalar(*id_sc, id, &kpath)
                        && id_key != kk
                    {
                        self.errors.push(ValidationError::EntityKeyMismatch {
                            path: kpath.clone(),
                            key: key.clone(),
                            id: id_key.canonical_string(),
                        });
                        ok = false;
                        continue;
                    }
                    entries.push((kk, key.clone(), canon));
                }
                if !ok {
                    return None;
                }
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                Some(Value::Object(
                    entries
                        .into_iter()
                        .map(|(_, key, val)| (key, val))
                        .collect(),
                ))
            }
        }
    }
}

fn finish(walk: Walk<'_>) -> Result<(), Vec<ValidationError>> {
    if walk.errors.is_empty() {
        Ok(())
    } else {
        Err(walk.errors)
    }
}

impl Schema {
    fn walk(&self, dedup: bool) -> Walk<'_> {
        Walk {
            schema: self,
            errors: Vec::new(),
            dedup,
        }
    }

    /// Validate a record (an object) against `fields`: every required field
    /// present and typed, no unknown fields. All errors are returned.
    pub fn validate_record(&self, fields: &[Field], v: &Value) -> Result<(), Vec<ValidationError>> {
        let mut walk = self.walk(false);
        walk.record(fields, v, "$");
        finish(walk)
    }

    /// Validate and canonicalize a record in one pass.
    pub fn canonicalize_record(
        &self,
        fields: &[Field],
        v: &Value,
    ) -> Result<Value, Vec<ValidationError>> {
        let mut walk = self.walk(false);
        match walk.record(fields, v, "$") {
            Some(c) if walk.errors.is_empty() => Ok(c),
            _ => Err(walk.errors),
        }
    }

    /// Validate one value against a type.
    pub fn validate_value(&self, ty: &Type, v: &Value) -> Result<(), Vec<ValidationError>> {
        let mut walk = self.walk(false);
        walk.value(ty, v, "$");
        finish(walk)
    }

    /// The canonical JSON form of `v` as a `ty`: sets sorted and
    /// deduplicated, map entries sorted by key, timestamps in UTC, absent
    /// optional fields present as `null`, decimals as written. The first
    /// error if `v` is not a valid `ty` (duplicate set elements are
    /// deduplicated here, not rejected).
    pub fn canonicalize(&self, ty: &Type, v: &Value) -> Result<Value, ValidationError> {
        let mut walk = self.walk(true);
        match walk.value(ty, v, "$") {
            Some(c) if walk.errors.is_empty() => Ok(c),
            _ => Err(walk.errors.remove(0)),
        }
    }

    pub fn validate_event(
        &self,
        ty: &EventType,
        payload: &Value,
    ) -> Result<(), Vec<ValidationError>> {
        self.validate_record(&ty.fields, payload)
    }

    pub fn validate_state(
        &self,
        agg: &Aggregate,
        state: &Value,
    ) -> Result<(), Vec<ValidationError>> {
        self.validate_record(&agg.state, state)
    }

    pub fn validate_command(
        &self,
        agg: &Aggregate,
        cmd: &Command,
        payload: &Value,
    ) -> Result<(), Vec<ValidationError>> {
        let _ = agg;
        self.validate_record(&cmd.fields, payload)
    }

    /// Validate a table row: the columns only (keys travel separately).
    pub fn validate_row(&self, table: &Table, row: &Value) -> Result<(), Vec<ValidationError>> {
        self.validate_record(&table.columns, row)
    }

    /// Validate a table key: an object with exactly the key fields.
    pub fn validate_key(&self, table: &Table, key: &Value) -> Result<(), Vec<ValidationError>> {
        self.validate_record(&table.keys, key)
    }
}
