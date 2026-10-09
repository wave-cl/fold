//! Typed column operations on read-model rows, applied atomically by the host.
//!
//! [`apply`] is a pure function: it starts from the stored row or from the
//! table's default row, applies the ops in order, validates and
//! canonicalizes the result, and enforces [`MAX_ROW_BYTES`].

use std::collections::BTreeSet;
use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::model::{Schema, Table};
use crate::types::{Scalar, Type};
use crate::validate::{ScalarKey, ValidationError, check_scalar};

/// Rows are stored inline as JSON; this bounds one row.
pub const MAX_ROW_BYTES: usize = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TruncateFrom {
    Front,
    Back,
}

/// One column operation, as a fold emits it (`{"op": "set_add", "column":
/// "open_orders", "value": "..."}`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ColumnOp {
    /// Replace the column; `null` only on `T?`.
    Set {
        column: String,
        value: Value,
    },
    /// Numeric add on `int`/`uint`/`decimal`, or on one entry of a map of
    /// those with `map_key` (an absent entry starts at 0).
    Add {
        column: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        map_key: Option<Value>,
        by: Value,
    },
    SetAdd {
        column: String,
        value: Value,
    },
    SetRemove {
        column: String,
        value: Value,
    },
    ListPush {
        column: String,
        value: Value,
        #[serde(default)]
        front: bool,
    },
    ListRemove {
        column: String,
        value: Value,
        #[serde(default)]
        all: bool,
    },
    ListTruncate {
        column: String,
        keep: usize,
        from: TruncateFrom,
    },
    MapPut {
        column: String,
        map_key: Value,
        value: Value,
    },
    MapRemove {
        column: String,
        map_key: Value,
    },
}

impl ColumnOp {
    pub fn column(&self) -> &str {
        match self {
            ColumnOp::Set { column, .. }
            | ColumnOp::Add { column, .. }
            | ColumnOp::SetAdd { column, .. }
            | ColumnOp::SetRemove { column, .. }
            | ColumnOp::ListPush { column, .. }
            | ColumnOp::ListRemove { column, .. }
            | ColumnOp::ListTruncate { column, .. }
            | ColumnOp::MapPut { column, .. }
            | ColumnOp::MapRemove { column, .. } => column,
        }
    }

    /// The op's wire name.
    pub fn name(&self) -> &'static str {
        match self {
            ColumnOp::Set { .. } => "set",
            ColumnOp::Add { .. } => "add",
            ColumnOp::SetAdd { .. } => "set_add",
            ColumnOp::SetRemove { .. } => "set_remove",
            ColumnOp::ListPush { .. } => "list_push",
            ColumnOp::ListRemove { .. } => "list_remove",
            ColumnOp::ListTruncate { .. } => "list_truncate",
            ColumnOp::MapPut { .. } => "map_put",
            ColumnOp::MapRemove { .. } => "map_remove",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowError {
    UnknownColumn {
        column: String,
    },
    /// The op does not apply to the column's type.
    WrongColumnType {
        column: String,
        op: &'static str,
        expected: &'static str,
        found: String,
    },
    /// `add` on a map column without `map_key`.
    MissingMapKey {
        column: String,
    },
    /// `add` with `map_key` on a column that is not a map.
    UnexpectedMapKey {
        column: String,
    },
    /// An element, key or value does not validate against the column type.
    InvalidValue {
        column: String,
        source: ValidationError,
    },
    /// `add` took a `uint` below zero.
    Underflow {
        column: String,
    },
    Overflow {
        column: String,
    },
    /// The default row needs a value for this column (no `?`, not a
    /// collection, not numeric).
    NoDefault {
        column: String,
    },
    /// The starting row or the result does not validate against the table.
    InvalidRow(Vec<ValidationError>),
    TooLarge {
        bytes: usize,
        limit: usize,
    },
}

impl fmt::Display for RowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RowError::UnknownColumn { column } => write!(f, "unknown column `{column}`"),
            RowError::WrongColumnType {
                column,
                op,
                expected,
                found,
            } => write!(
                f,
                "`{op}` needs a {expected} column, but `{column}` is {found}"
            ),
            RowError::MissingMapKey { column } => {
                write!(f, "`add` on map column `{column}` needs `map_key`")
            }
            RowError::UnexpectedMapKey { column } => {
                write!(
                    f,
                    "`add` on column `{column}` takes no `map_key` (not a map)"
                )
            }
            RowError::InvalidValue { column, source } => write!(f, "column `{column}`: {source}"),
            RowError::Underflow { column } => {
                write!(f, "column `{column}`: uint would go below zero")
            }
            RowError::Overflow { column } => write!(f, "column `{column}`: arithmetic overflow"),
            RowError::NoDefault { column } => {
                write!(f, "column `{column}` has no default and must be supplied")
            }
            RowError::InvalidRow(errors) => {
                write!(f, "row does not validate:")?;
                for e in errors {
                    write!(f, " {e};")?;
                }
                Ok(())
            }
            RowError::TooLarge { bytes, limit } => {
                write!(f, "row is {bytes} bytes, over the {limit} byte limit")
            }
        }
    }
}

impl std::error::Error for RowError {}

/// The row a table starts from when a key has no row yet: `null` for
/// optionals, empty collections, `0` for `int`/`uint` and `"0"` for
/// `decimal`. Any other column must be supplied (`NoDefault`).
pub fn default_row(schema: &Schema, table: &Table) -> Result<Value, RowError> {
    let _ = schema;
    let mut row = Map::with_capacity(table.columns.len());
    for col in &table.columns {
        if let Some(d) = &col.default {
            row.insert(col.name.clone(), d.clone());
            continue;
        }
        let v = match &col.ty {
            Type::Optional(_) => Value::Null,
            Type::List(_) | Type::Set(_) => Value::Array(Vec::new()),
            Type::Map(..) => Value::Object(Map::new()),
            Type::Scalar(Scalar::Int | Scalar::Uint) => Value::from(0),
            Type::Scalar(Scalar::Decimal) => Value::String("0".to_string()),
            _ => {
                return Err(RowError::NoDefault {
                    column: col.name.clone(),
                });
            }
        };
        row.insert(col.name.clone(), v);
    }
    Ok(Value::Object(row))
}

/// Apply `ops` in order to `row` (or to the table's default row) and return
/// the canonical result.
pub fn apply(
    schema: &Schema,
    table: &Table,
    row: Option<&Value>,
    ops: &[ColumnOp],
) -> Result<Value, RowError> {
    let start = match row {
        Some(r) => r.clone(),
        None => default_row(schema, table)?,
    };
    let canon = schema
        .canonicalize_record(&table.columns, &start)
        .map_err(RowError::InvalidRow)?;
    let Value::Object(mut obj) = canon else {
        unreachable!("canonicalize_record returns an object")
    };
    for op in ops {
        apply_one(schema, table, &mut obj, op)?;
    }
    let result = schema
        .canonicalize_record(&table.columns, &Value::Object(obj))
        .map_err(RowError::InvalidRow)?;
    let bytes = serde_json::to_vec(&result)
        .map(|b| b.len())
        .unwrap_or(usize::MAX);
    if bytes > MAX_ROW_BYTES {
        return Err(RowError::TooLarge {
            bytes,
            limit: MAX_ROW_BYTES,
        });
    }
    Ok(result)
}

fn wrong(column: &str, op: &'static str, expected: &'static str, ty: &Type) -> RowError {
    RowError::WrongColumnType {
        column: column.to_string(),
        op,
        expected,
        found: ty.to_string(),
    }
}

fn invalid(column: &str, source: ValidationError) -> RowError {
    RowError::InvalidValue {
        column: column.to_string(),
        source,
    }
}

fn apply_one(
    schema: &Schema,
    table: &Table,
    row: &mut Map<String, Value>,
    op: &ColumnOp,
) -> Result<(), RowError> {
    let column = op.column();
    let field = table
        .column(column)
        .ok_or_else(|| RowError::UnknownColumn {
            column: column.to_string(),
        })?;
    let ty = &field.ty;
    let name = op.name();
    match op {
        ColumnOp::Set { value, .. } => {
            let canon = schema
                .canonicalize(ty, value)
                .map_err(|e| invalid(column, e))?;
            row.insert(column.to_string(), canon);
        }
        ColumnOp::Add { map_key, by, .. } => match (ty, map_key) {
            (Type::Map(k, v), Some(key)) => {
                let Type::Scalar(sc) = v.required() else {
                    return Err(wrong(column, name, "map<K, int|uint|decimal>", ty));
                };
                if !sc.is_numeric() {
                    return Err(wrong(column, name, "map<K, int|uint|decimal>", ty));
                }
                let key = check_scalar(*k, key, "$").map_err(|e| invalid(column, e))?;
                let key_str = key.canonical_string();
                let map = map_mut(row, column);
                let current = map.get(&key_str).cloned().unwrap_or(Value::Null);
                let sum = add_numeric(column, *sc, &current, by)?;
                map.insert(key_str, sum);
                sort_map(*k, map);
            }
            (Type::Map(..), None) => {
                return Err(RowError::MissingMapKey {
                    column: column.to_string(),
                });
            }
            (_, Some(_)) => {
                return Err(RowError::UnexpectedMapKey {
                    column: column.to_string(),
                });
            }
            (_, None) => {
                let Type::Scalar(sc) = ty.required() else {
                    return Err(wrong(column, name, "int, uint or decimal", ty));
                };
                if !sc.is_numeric() {
                    return Err(wrong(column, name, "int, uint or decimal", ty));
                }
                let current = row.get(column).cloned().unwrap_or(Value::Null);
                let sum = add_numeric(column, *sc, &current, by)?;
                row.insert(column.to_string(), sum);
            }
        },
        ColumnOp::SetAdd { value, .. } | ColumnOp::SetRemove { value, .. } => {
            let Type::Set(sc) = ty else {
                return Err(wrong(column, name, "set<T>", ty));
            };
            let elem = check_scalar(*sc, value, "$").map_err(|e| invalid(column, e))?;
            let mut set: BTreeSet<ScalarKey> = array_mut(row, column)
                .iter()
                .filter_map(|v| check_scalar(*sc, v, "$").ok())
                .collect();
            if matches!(op, ColumnOp::SetAdd { .. }) {
                set.insert(elem);
            } else {
                set.remove(&elem);
            }
            row.insert(
                column.to_string(),
                Value::Array(set.iter().map(ScalarKey::to_value).collect()),
            );
        }
        ColumnOp::ListPush { value, front, .. } => {
            let Type::List(elem_ty) = ty else {
                return Err(wrong(column, name, "list<T>", ty));
            };
            let elem = schema
                .canonicalize(elem_ty, value)
                .map_err(|e| invalid(column, e))?;
            let list = array_mut(row, column);
            if *front {
                list.insert(0, elem);
            } else {
                list.push(elem);
            }
        }
        ColumnOp::ListRemove { value, all, .. } => {
            let Type::List(elem_ty) = ty else {
                return Err(wrong(column, name, "list<T>", ty));
            };
            let elem = schema
                .canonicalize(elem_ty, value)
                .map_err(|e| invalid(column, e))?;
            let list = array_mut(row, column);
            if *all {
                list.retain(|v| *v != elem);
            } else if let Some(i) = list.iter().position(|v| *v == elem) {
                list.remove(i);
            }
        }
        ColumnOp::ListTruncate { keep, from, .. } => {
            let Type::List(_) = ty else {
                return Err(wrong(column, name, "list<T>", ty));
            };
            let list = array_mut(row, column);
            match from {
                TruncateFrom::Front => list.truncate(*keep),
                TruncateFrom::Back => {
                    let excess = list.len().saturating_sub(*keep);
                    list.drain(..excess);
                }
            }
        }
        ColumnOp::MapPut { map_key, value, .. } => {
            let Type::Map(k, v) = ty else {
                return Err(wrong(column, name, "map<K, V>", ty));
            };
            let key = check_scalar(*k, map_key, "$").map_err(|e| invalid(column, e))?;
            let val = schema
                .canonicalize(v, value)
                .map_err(|e| invalid(column, e))?;
            let map = map_mut(row, column);
            map.insert(key.canonical_string(), val);
            sort_map(*k, map);
        }
        ColumnOp::MapRemove { map_key, .. } => {
            let Type::Map(k, _) = ty else {
                return Err(wrong(column, name, "map<K, V>", ty));
            };
            let key = check_scalar(*k, map_key, "$").map_err(|e| invalid(column, e))?;
            map_mut(row, column).remove(&key.canonical_string());
        }
    }
    Ok(())
}

fn array_mut<'a>(row: &'a mut Map<String, Value>, column: &str) -> &'a mut Vec<Value> {
    let slot = row
        .entry(column.to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    if !slot.is_array() {
        *slot = Value::Array(Vec::new());
    }
    slot.as_array_mut().expect("just made it an array")
}

fn map_mut<'a>(row: &'a mut Map<String, Value>, column: &str) -> &'a mut Map<String, Value> {
    let slot = row
        .entry(column.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if !slot.is_object() {
        *slot = Value::Object(Map::new());
    }
    slot.as_object_mut().expect("just made it an object")
}

/// Re-sort a map's entries by their typed key (the canonical order).
fn sort_map(k: Scalar, map: &mut Map<String, Value>) {
    let mut entries: Vec<(Option<ScalarKey>, String, Value)> = std::mem::take(map)
        .into_iter()
        .map(|(key, v)| (ScalarKey::parse_canonical(k, &key), key, v))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    for (_, key, v) in entries {
        map.insert(key, v);
    }
}

/// `current + by` for a numeric scalar; `current` may be `null` (absent),
/// which counts as zero.
fn add_numeric(column: &str, sc: Scalar, current: &Value, by: &Value) -> Result<Value, RowError> {
    match sc {
        Scalar::Int => {
            let cur = if current.is_null() {
                0
            } else {
                as_i64(column, current)?
            };
            let by = as_i64(column, by)?;
            cur.checked_add(by)
                .map(Value::from)
                .ok_or_else(|| RowError::Overflow {
                    column: column.to_string(),
                })
        }
        Scalar::Uint => {
            let cur: i128 = if current.is_null() {
                0
            } else {
                check_scalar(Scalar::Uint, current, "$")
                    .map_err(|e| invalid(column, e))
                    .and_then(|k| match k {
                        ScalarKey::Uint(u) => Ok(i128::from(u)),
                        _ => unreachable!(),
                    })?
            };
            let by: i128 = match by {
                Value::Number(n) if n.is_u64() => i128::from(n.as_u64().expect("checked")),
                Value::Number(n) if n.is_i64() => i128::from(n.as_i64().expect("checked")),
                _ => {
                    return Err(invalid(
                        column,
                        check_scalar(Scalar::Int, by, "$").unwrap_err(),
                    ));
                }
            };
            let sum = cur + by;
            if sum < 0 {
                return Err(RowError::Underflow {
                    column: column.to_string(),
                });
            }
            u64::try_from(sum)
                .map(Value::from)
                .map_err(|_| RowError::Overflow {
                    column: column.to_string(),
                })
        }
        Scalar::Decimal => {
            let cur = if current.is_null() {
                Decimal::ZERO
            } else {
                as_decimal(column, current)?
            };
            let by = as_decimal(column, by)?;
            let mut sum = cur.checked_add(by).ok_or_else(|| RowError::Overflow {
                column: column.to_string(),
            })?;
            sum.rescale(cur.scale().max(by.scale()));
            Ok(Value::String(sum.to_string()))
        }
        _ => unreachable!("add_numeric is only called for numeric scalars"),
    }
}

fn as_i64(column: &str, v: &Value) -> Result<i64, RowError> {
    match check_scalar(Scalar::Int, v, "$").map_err(|e| invalid(column, e))? {
        ScalarKey::Int(i) => Ok(i),
        _ => unreachable!(),
    }
}

fn as_decimal(column: &str, v: &Value) -> Result<Decimal, RowError> {
    match check_scalar(Scalar::Decimal, v, "$").map_err(|e| invalid(column, e))? {
        ScalarKey::Decimal(d) => Ok(d),
        _ => unreachable!(),
    }
}
