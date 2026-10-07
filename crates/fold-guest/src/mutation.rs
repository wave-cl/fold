//! Read-model mutations a projection step returns.
//!
//! A [`Row`] names a table row by its key; each method produces one
//! [`Mutation`] against it. The host applies them in order inside the batch
//! transaction, validating every value against the table's declared types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Which end of a list `list_truncate` keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TruncateFrom {
    Front,
    Back,
}

/// One change to one row. Serialized as
/// `{"table": ..., "key": {...}, "op": "...", ...fields}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Mutation {
    pub table: String,
    /// JSON object of the table's key fields.
    pub key: Value,
    #[serde(flatten)]
    pub op: Op,
}

/// The operation part of a [`Mutation`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// Replace the whole row (all non-key columns).
    Upsert {
        row: Value,
    },
    /// Remove the row.
    Delete,
    /// Replace one column.
    Set {
        column: String,
        value: Value,
    },
    /// Add to a numeric column, or to one entry of a numeric map.
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
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        front: bool,
    },
    ListRemove {
        column: String,
        value: Value,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
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

/// A row address: table plus key. Cheap to clone; every method borrows.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    table: String,
    key: Value,
}

impl Row {
    /// `key` is a JSON object of the table's key fields, e.g.
    /// `json!({"customer_id": id})`.
    pub fn new(table: impl Into<String>, key: Value) -> Self {
        Row {
            table: table.into(),
            key,
        }
    }

    fn with(&self, op: Op) -> Mutation {
        Mutation {
            table: self.table.clone(),
            key: self.key.clone(),
            op,
        }
    }

    pub fn upsert(&self, row: Value) -> Mutation {
        self.with(Op::Upsert { row })
    }

    pub fn delete(&self) -> Mutation {
        self.with(Op::Delete)
    }

    pub fn set(&self, column: &str, value: impl Into<Value>) -> Mutation {
        self.with(Op::Set {
            column: column.into(),
            value: value.into(),
        })
    }

    /// Add `by` to a numeric column. Decimals are JSON strings, e.g. `"12.50"`.
    pub fn add(&self, column: &str, by: impl Into<Value>) -> Mutation {
        self.with(Op::Add {
            column: column.into(),
            map_key: None,
            by: by.into(),
        })
    }

    /// Add `by` to the `map_key` entry of a numeric map column.
    pub fn add_in(
        &self,
        column: &str,
        map_key: impl Into<Value>,
        by: impl Into<Value>,
    ) -> Mutation {
        self.with(Op::Add {
            column: column.into(),
            map_key: Some(map_key.into()),
            by: by.into(),
        })
    }

    pub fn set_add(&self, column: &str, value: impl Into<Value>) -> Mutation {
        self.with(Op::SetAdd {
            column: column.into(),
            value: value.into(),
        })
    }

    pub fn set_remove(&self, column: &str, value: impl Into<Value>) -> Mutation {
        self.with(Op::SetRemove {
            column: column.into(),
            value: value.into(),
        })
    }

    pub fn push(&self, column: &str, value: impl Into<Value>) -> Mutation {
        self.with(Op::ListPush {
            column: column.into(),
            value: value.into(),
            front: false,
        })
    }

    pub fn push_front(&self, column: &str, value: impl Into<Value>) -> Mutation {
        self.with(Op::ListPush {
            column: column.into(),
            value: value.into(),
            front: true,
        })
    }

    /// Remove the first element equal to `value`.
    pub fn remove(&self, column: &str, value: impl Into<Value>) -> Mutation {
        self.with(Op::ListRemove {
            column: column.into(),
            value: value.into(),
            all: false,
        })
    }

    /// Remove every element equal to `value`.
    pub fn remove_all(&self, column: &str, value: impl Into<Value>) -> Mutation {
        self.with(Op::ListRemove {
            column: column.into(),
            value: value.into(),
            all: true,
        })
    }

    /// Keep only the last `keep` elements.
    pub fn truncate_back(&self, column: &str, keep: usize) -> Mutation {
        self.with(Op::ListTruncate {
            column: column.into(),
            keep,
            from: TruncateFrom::Back,
        })
    }

    /// Keep only the first `keep` elements.
    pub fn truncate_front(&self, column: &str, keep: usize) -> Mutation {
        self.with(Op::ListTruncate {
            column: column.into(),
            keep,
            from: TruncateFrom::Front,
        })
    }

    pub fn map_put(
        &self,
        column: &str,
        map_key: impl Into<Value>,
        value: impl Into<Value>,
    ) -> Mutation {
        self.with(Op::MapPut {
            column: column.into(),
            map_key: map_key.into(),
            value: value.into(),
        })
    }

    pub fn map_remove(&self, column: &str, map_key: impl Into<Value>) -> Mutation {
        self.with(Op::MapRemove {
            column: column.into(),
            map_key: map_key.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn mutations_serialize_flat_with_an_op_tag() {
        let r = Row::new("customer_orders", json!({"customer_id": "c1"}));
        let m = r.add_in("spent_by_currency", "EUR", "12.50");
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(
            v,
            json!({
                "table": "customer_orders",
                "key": {"customer_id": "c1"},
                "op": "add",
                "column": "spent_by_currency",
                "map_key": "EUR",
                "by": "12.50"
            })
        );
        let back: Mutation = serde_json::from_value(v).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn defaults_are_omitted_and_read_back() {
        let r = Row::new("t", json!({"k": 1}));
        let v = serde_json::to_value(r.push("recent", "x")).unwrap();
        assert_eq!(v.get("front"), None, "a default false is not written");
        let v = serde_json::to_value(r.push_front("recent", "x")).unwrap();
        assert_eq!(v["front"], json!(true));
        let v = serde_json::to_value(r.truncate_back("recent", 5)).unwrap();
        assert_eq!(v["op"], "list_truncate");
        assert_eq!(v["from"], "back");
        assert_eq!(v["keep"], 5);
        let v = serde_json::to_value(r.delete()).unwrap();
        assert_eq!(v, json!({"table": "t", "key": {"k": 1}, "op": "delete"}));
    }

    #[test]
    fn an_unknown_op_is_rejected() {
        let bad = json!({"table": "t", "key": {}, "op": "explode"});
        assert!(serde_json::from_value::<Mutation>(bad).is_err());
    }
}
