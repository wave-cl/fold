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
    /// A value's rule does not hold for this instance.
    RuleViolated {
        path: String,
        value: String,
        rule: String,
    },
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
            | ValidationError::UnknownType { path, .. }
            | ValidationError::RuleViolated { path, .. } => path,
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
            ValidationError::RuleViolated { path, value, rule } => {
                write!(f, "{path}: {value} violates rule {rule}")
            }
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
            // Absent and `null` are the same thing; a default fills either.
            let given = obj.get(&field.name).filter(|v| !v.is_null());
            let v = given.or(field.default.as_ref()).unwrap_or(&Value::Null);
            match self.value(&field.ty, v, &fpath) {
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
                match v {
                    Value::String(s) => match en.variant(s) {
                        None => self.err(ValidationError::UnknownVariant {
                            path: path.to_string(),
                            variant: s.clone(),
                            enum_name: r.to_string(),
                        }),
                        Some(variant) if variant.payload.is_some() => self.err(wrong(
                            path,
                            format!(
                                "an object {{\"{s}\": {{...}}}}: variant `{s}` of {} carries a payload",
                                en.name
                            ),
                            v,
                        )),
                        Some(_) => Some(v.clone()),
                    },
                    Value::Object(obj) if obj.len() == 1 => {
                        let (name, inner) = obj.iter().next().expect("one entry");
                        match en.variant(name) {
                            None => self.err(ValidationError::UnknownVariant {
                                path: path.to_string(),
                                variant: name.clone(),
                                enum_name: r.to_string(),
                            }),
                            Some(variant) => match &variant.payload {
                                None => self.err(wrong(
                                    path,
                                    format!(
                                        "the string \"{name}\": variant `{name}` of {} carries no payload",
                                        en.name
                                    ),
                                    v,
                                )),
                                Some(fields) => {
                                    let canon =
                                        self.record(fields, inner, &field_path(path, name))?;
                                    let mut out = Map::with_capacity(1);
                                    out.insert(name.clone(), canon);
                                    Some(Value::Object(out))
                                }
                            },
                        }
                    }
                    Value::Object(_) => self.err(wrong(
                        path,
                        format!(
                            "an object with exactly one key naming a variant of {}",
                            en.name
                        ),
                        v,
                    )),
                    _ => self.err(wrong(path, format!("a variant of {}", en.name), v)),
                }
            }
            Type::Value(r) => {
                let Some(vt) = self.schema.value_type(r) else {
                    return self.err(ValidationError::UnknownType {
                        path: path.to_string(),
                        name: r.to_string(),
                    });
                };
                let canon = self.record(&vt.fields, v, path)?;
                // Rules see the canonical record, after every field validated.
                let mut ok = true;
                for rule in &vt.rules {
                    if !rules::eval(&rule.expr, &canon) {
                        self.errors.push(ValidationError::RuleViolated {
                            path: path.to_string(),
                            value: r.to_string(),
                            rule: rule.name.clone(),
                        });
                        ok = false;
                    }
                }
                ok.then_some(canon)
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

    /// The canonical form of an event payload (defaults filled in, sets
    /// sorted, maps ordered), or every error.
    pub fn canonicalize_event(
        &self,
        ty: &EventType,
        payload: &Value,
    ) -> Result<Value, Vec<ValidationError>> {
        self.canonicalize_record(&ty.fields, payload)
    }

    pub fn canonicalize_state(
        &self,
        agg: &Aggregate,
        state: &Value,
    ) -> Result<Value, Vec<ValidationError>> {
        self.canonicalize_record(&agg.state, state)
    }

    pub fn canonicalize_command(
        &self,
        cmd: &Command,
        payload: &Value,
    ) -> Result<Value, Vec<ValidationError>> {
        self.canonicalize_record(&cmd.fields, payload)
    }

    /// The canonical form of a table row's columns.
    pub fn canonicalize_row(
        &self,
        table: &Table,
        row: &Value,
    ) -> Result<Value, Vec<ValidationError>> {
        self.canonicalize_record(&table.columns, row)
    }

    /// Fills absent or `null` fields that have defaults, recursively
    /// (through values, entities, lists, maps and enum payloads), without
    /// validating anything else: for records stored before a default was
    /// declared.
    pub fn apply_defaults(&self, fields: &[Field], v: &mut Value) {
        let Value::Object(obj) = v else {
            return;
        };
        for field in fields {
            let present = obj.get(&field.name).is_some_and(|v| !v.is_null());
            if !present {
                if let Some(d) = &field.default {
                    obj.insert(field.name.clone(), d.clone());
                }
                continue;
            }
            if let Some(inner) = obj.get_mut(&field.name) {
                self.apply_defaults_in(&field.ty, inner);
            }
        }
    }

    fn apply_defaults_in(&self, ty: &Type, v: &mut Value) {
        match ty {
            Type::Optional(inner) => {
                if !v.is_null() {
                    self.apply_defaults_in(inner, v);
                }
            }
            Type::Value(r) => {
                if let Some(vt) = self.value_type(r) {
                    self.apply_defaults(&vt.fields, v);
                }
            }
            Type::Entity(r) => {
                if let Some(en) = self.entity(r) {
                    self.apply_defaults(&en.fields, v);
                }
            }
            Type::Enum(r) => {
                if let Some(en) = self.enum_type(r)
                    && let Value::Object(obj) = v
                    && obj.len() == 1
                {
                    let (name, inner) = obj.iter_mut().next().expect("one entry");
                    if let Some(fields) = en.variant(name).and_then(|v| v.payload.as_ref()) {
                        self.apply_defaults(fields, inner);
                    }
                }
            }
            Type::List(elem) => {
                if let Value::Array(items) = v {
                    for item in items {
                        self.apply_defaults_in(elem, item);
                    }
                }
            }
            Type::Map(_, vty) => {
                if let Value::Object(obj) = v {
                    for item in obj.values_mut() {
                        self.apply_defaults_in(vty, item);
                    }
                }
            }
            Type::Scalar(_) | Type::Set(_) => {}
        }
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

/// Rule evaluation over a canonical record.
pub mod rules {
    use rust_decimal::Decimal;
    use serde_json::Value;

    use crate::model::{RuleExpr, RuleOp, RuleTerm};

    enum Operand {
        Number(Decimal),
        Text(String),
        Bool(bool),
    }

    fn lookup<'a>(root: &'a Value, segments: &[String]) -> Option<&'a Value> {
        let mut cur = root;
        for s in segments {
            cur = cur.get(s)?;
            if cur.is_null() {
                return None;
            }
        }
        Some(cur)
    }

    fn number(v: &Value) -> Option<Decimal> {
        match v {
            Value::Number(n) => n
                .as_i64()
                .map(Decimal::from)
                .or_else(|| n.as_u64().map(Decimal::from))
                .or_else(|| n.as_f64().and_then(|f| Decimal::try_from(f).ok())),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    fn operand(t: &RuleTerm, root: &Value) -> Option<Operand> {
        match t {
            RuleTerm::Number(d) => Some(Operand::Number(*d)),
            RuleTerm::Text(s) => Some(Operand::Text(s.clone())),
            RuleTerm::Bool(b) => Some(Operand::Bool(*b)),
            RuleTerm::Field(p) => {
                let v = lookup(root, &p.segments)?;
                match p.kind {
                    crate::model::OperandKind::Number => number(v).map(Operand::Number),
                    crate::model::OperandKind::Text => match v {
                        Value::String(s) => Some(Operand::Text(s.clone())),
                        // A payload-carrying enum variant compares by its name.
                        Value::Object(o) if o.len() == 1 => {
                            o.keys().next().map(|k| Operand::Text(k.clone()))
                        }
                        _ => None,
                    },
                    crate::model::OperandKind::Bool => v.as_bool().map(Operand::Bool),
                }
            }
            RuleTerm::Len { segments, .. } => {
                let v = lookup(root, segments)?;
                let n = match v {
                    Value::String(s) => s.chars().count(),
                    Value::Array(a) => a.len(),
                    Value::Object(o) => o.len(),
                    _ => return None,
                };
                Some(Operand::Number(Decimal::from(n)))
            }
        }
    }

    fn compare(l: &Operand, op: RuleOp, r: &Operand) -> bool {
        use std::cmp::Ordering;
        let ord = match (l, r) {
            (Operand::Number(a), Operand::Number(b)) => a.cmp(b),
            (Operand::Text(a), Operand::Text(b)) => a.cmp(b),
            (Operand::Bool(a), Operand::Bool(b)) => a.cmp(b),
            _ => return false,
        };
        match op {
            RuleOp::Lt => ord == Ordering::Less,
            RuleOp::Le => ord != Ordering::Greater,
            RuleOp::Gt => ord == Ordering::Greater,
            RuleOp::Ge => ord != Ordering::Less,
            RuleOp::Eq => ord == Ordering::Equal,
            RuleOp::Ne => ord != Ordering::Equal,
        }
    }

    /// True when the rule holds. An absent optional operand makes a
    /// comparison, `matches` or `in` hold vacuously.
    pub fn eval(e: &RuleExpr, root: &Value) -> bool {
        match e {
            RuleExpr::Or(a, b) => eval(a, root) || eval(b, root),
            RuleExpr::And(a, b) => eval(a, root) && eval(b, root),
            RuleExpr::Not(inner) => !eval(inner, root),
            RuleExpr::Cmp { lhs, op, rhs } => match (operand(lhs, root), operand(rhs, root)) {
                (Some(l), Some(r)) => compare(&l, *op, &r),
                _ => true,
            },
            RuleExpr::Matches { path, pattern } => {
                match lookup(root, &path.segments).and_then(Value::as_str) {
                    Some(s) => pattern.0.is_match(s),
                    None => true,
                }
            }
            RuleExpr::In { path, items } => match operand(&RuleTerm::Field(path.clone()), root) {
                None => true,
                Some(v) => items
                    .iter()
                    .filter_map(|i| operand(i, root))
                    .any(|i| compare(&v, RuleOp::Eq, &i)),
            },
        }
    }
}
