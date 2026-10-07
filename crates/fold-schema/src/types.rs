//! Resolved types: scalars, type references and the `Type` tree.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// The eight scalar types of the schema language.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scalar {
    String,
    Int,
    Uint,
    Decimal,
    Bool,
    Uuid,
    Timestamp,
    Bytes,
}

impl Scalar {
    pub const ALL: [Scalar; 8] = [
        Scalar::String,
        Scalar::Int,
        Scalar::Uint,
        Scalar::Decimal,
        Scalar::Bool,
        Scalar::Uuid,
        Scalar::Timestamp,
        Scalar::Bytes,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Scalar::String => "string",
            Scalar::Int => "int",
            Scalar::Uint => "uint",
            Scalar::Decimal => "decimal",
            Scalar::Bool => "bool",
            Scalar::Uuid => "uuid",
            Scalar::Timestamp => "timestamp",
            Scalar::Bytes => "bytes",
        }
    }

    /// `int`, `uint` and `decimal`: the types `add` and the default `0` apply to.
    pub fn is_numeric(self) -> bool {
        matches!(self, Scalar::Int | Scalar::Uint | Scalar::Decimal)
    }

    /// Scalars with a canonical string form, usable as set elements and map keys.
    pub fn is_keyable(self) -> bool {
        self != Scalar::Bytes
    }
}

impl fmt::Display for Scalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for Scalar {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Scalar::ALL.into_iter().find(|sc| sc.name() == s).ok_or(())
    }
}

/// A reference to a named type: a context-level `value`/`enum`
/// (`aggregate: None`) or an aggregate-local `value`/`enum`/`entity`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TypeRef {
    pub context: String,
    pub aggregate: Option<String>,
    pub name: String,
}

impl TypeRef {
    pub fn new(
        context: impl Into<String>,
        aggregate: Option<String>,
        name: impl Into<String>,
    ) -> Self {
        TypeRef {
            context: context.into(),
            aggregate,
            name: name.into(),
        }
    }
}

impl fmt::Display for TypeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.aggregate {
            Some(agg) => write!(f, "{}.{}.{}", self.context, agg, self.name),
            None => write!(f, "{}.{}", self.context, self.name),
        }
    }
}

/// A fully resolved field type.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Type {
    Scalar(Scalar),
    Value(TypeRef),
    Enum(TypeRef),
    Entity(TypeRef),
    List(Box<Type>),
    Set(Scalar),
    Map(Scalar, Box<Type>),
    Optional(Box<Type>),
}

impl Type {
    pub fn is_optional(&self) -> bool {
        matches!(self, Type::Optional(_))
    }

    pub fn is_collection(&self) -> bool {
        matches!(self, Type::List(_) | Type::Set(_) | Type::Map(..))
    }

    /// The type with an outer `Optional` removed.
    pub fn required(&self) -> &Type {
        match self {
            Type::Optional(inner) => inner,
            other => other,
        }
    }

    /// The entity this type holds directly (through an optional), if any.
    pub fn entity(&self) -> Option<&TypeRef> {
        match self.required() {
            Type::Entity(r) => Some(r),
            _ => None,
        }
    }

    /// Every named type referenced anywhere in this type tree.
    pub fn refs(&self) -> Vec<&TypeRef> {
        let mut out = Vec::new();
        self.collect_refs(&mut out);
        out
    }

    fn collect_refs<'a>(&'a self, out: &mut Vec<&'a TypeRef>) {
        match self {
            Type::Scalar(_) | Type::Set(_) => {}
            Type::Value(r) | Type::Enum(r) | Type::Entity(r) => out.push(r),
            Type::List(t) | Type::Optional(t) | Type::Map(_, t) => t.collect_refs(out),
        }
    }

    /// Whether an entity type occurs anywhere in this type tree.
    pub fn contains_entity(&self) -> bool {
        match self {
            Type::Entity(_) => true,
            Type::Scalar(_) | Type::Set(_) | Type::Value(_) | Type::Enum(_) => false,
            Type::List(t) | Type::Optional(t) | Type::Map(_, t) => t.contains_entity(),
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Scalar(s) => write!(f, "{s}"),
            Type::Value(r) | Type::Enum(r) | Type::Entity(r) => write!(f, "{r}"),
            Type::List(t) => write!(f, "list<{t}>"),
            Type::Set(s) => write!(f, "set<{s}>"),
            Type::Map(k, v) => write!(f, "map<{k}, {v}>"),
            Type::Optional(t) => write!(f, "{t}?"),
        }
    }
}
