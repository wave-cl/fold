//! The resolved schema: what [`crate::compile`] produces and the rest of the
//! system reads.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::template::StreamTemplate;
use crate::types::{Type, TypeRef};

/// A named, typed field of a record (value, entity, event, state, command,
/// table), or an aggregate key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub ty: Type,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueType {
    pub name: String,
    pub fields: Vec<Field>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnumType {
    pub name: String,
    pub variants: Vec<String>,
}

/// `Context.Name@vN`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EventTypeId {
    pub context: String,
    pub name: String,
    pub version: u16,
}

impl EventTypeId {
    pub fn family(&self) -> EventFamilyRef {
        EventFamilyRef {
            context: self.context.clone(),
            name: self.name.clone(),
        }
    }
}

impl fmt::Display for EventTypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}@v{}", self.context, self.name, self.version)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EventRefError {
    #[error("event reference `{0}` must look like `Context.Event` or `Context.Event@vN`")]
    Malformed(String),
    #[error("event reference `{0}` has no `@vN` version")]
    MissingVersion(String),
}

/// Split `Context.Event` or `Context.Event@vN` into its parts; the version is
/// `None` when absent, and the caller then picks the family's latest version.
pub fn parse_event_ref(s: &str) -> Result<(String, String, Option<u16>), EventRefError> {
    let malformed = || EventRefError::Malformed(s.to_string());
    let (path, version) = match s.split_once('@') {
        Some((path, v)) => {
            let digits = v.strip_prefix('v').ok_or_else(malformed)?;
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(malformed());
            }
            let version = digits.parse::<u16>().map_err(|_| malformed())?;
            (path, Some(version))
        }
        None => (s, None),
    };
    let (context, name) = path.split_once('.').ok_or_else(malformed)?;
    if !is_ident(context) || !is_ident(name) {
        return Err(malformed());
    }
    Ok((context.to_string(), name.to_string(), version))
}

fn is_ident(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl FromStr for EventTypeId {
    type Err = EventRefError;

    /// Accepts `Context.Event@vN`. A reference without a version is an
    /// error here because an id always carries one; use [`parse_event_ref`]
    /// (or [`Schema::resolve_event_ref`]) to accept `Context.Event` and
    /// pick the latest version.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (context, name, version) = parse_event_ref(s)?;
        let version = version.ok_or_else(|| EventRefError::MissingVersion(s.to_string()))?;
        Ok(EventTypeId {
            context,
            name,
            version,
        })
    }
}

/// `Context.Name`: an event family across its versions.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EventFamilyRef {
    pub context: String,
    pub name: String,
}

impl fmt::Display for EventFamilyRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.context, self.name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventType {
    pub id: EventTypeId,
    pub fields: Vec<Field>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventFamily {
    pub context: String,
    pub name: String,
    pub versions: BTreeMap<u16, EventType>,
}

impl EventFamily {
    pub fn latest(&self) -> &EventType {
        self.versions
            .values()
            .next_back()
            .expect("an event family has at least one version")
    }

    pub fn as_ref(&self) -> EventFamilyRef {
        EventFamilyRef {
            context: self.context.clone(),
            name: self.name.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmRef {
    /// Path relative to the schema file's directory.
    pub module: String,
    /// The export name as written, if any; the default is the caller's.
    pub export: Option<String>,
}

impl WasmRef {
    pub fn export_or<'a>(&'a self, default: &'a str) -> &'a str {
        self.export.as_deref().unwrap_or(default)
    }
}

/// An entity: identity within its aggregate. `fields` holds every field
/// including the id (as its first element); `id` repeats that field for
/// convenience.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entity {
    pub name: String,
    pub id: Field,
    pub fields: Vec<Field>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Command {
    pub name: String,
    pub fields: Vec<Field>,
    pub handler: WasmRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Aggregate {
    pub name: String,
    pub key: Field,
    pub stream: StreamTemplate,
    pub values: IndexMap<String, ValueType>,
    pub enums: IndexMap<String, EnumType>,
    pub entities: IndexMap<String, Entity>,
    pub events: Vec<EventFamilyRef>,
    pub state: Vec<Field>,
    pub evolve: WasmRef,
    /// `0` means never.
    pub snapshot_every: u32,
    pub commands: IndexMap<String, Command>,
}

impl Aggregate {
    pub fn owns_event(&self, family: &EventFamilyRef) -> bool {
        self.events.iter().any(|e| e == family)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    pub keys: Vec<Field>,
    pub columns: Vec<Field>,
}

impl Table {
    pub fn column(&self, name: &str) -> Option<&Field> {
        self.columns.iter().find(|c| c.name == name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Projection {
    pub name: String,
    pub from: Vec<EventFamilyRef>,
    pub fold: WasmRef,
    pub tables: IndexMap<String, Table>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Context {
    pub name: String,
    pub values: IndexMap<String, ValueType>,
    pub enums: IndexMap<String, EnumType>,
    pub events: IndexMap<String, EventFamily>,
    pub aggregates: IndexMap<String, Aggregate>,
    pub projections: IndexMap<String, Projection>,
}

/// A compiled schema.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Schema {
    pub contexts: IndexMap<String, Context>,
    dir: Option<PathBuf>,
}

impl Schema {
    pub fn new(contexts: IndexMap<String, Context>) -> Self {
        Schema {
            contexts,
            dir: None,
        }
    }

    /// Compile the schema at `path`, remembering its directory for
    /// [`Schema::dir`].
    pub fn from_file(path: impl AsRef<Path>) -> Result<Schema, crate::Error> {
        let path = path.as_ref();
        let src = std::fs::read_to_string(path).map_err(|source| crate::Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let mut schema = crate::compile(&src).map_err(|diagnostics| crate::Error::Compile {
            path: path.to_path_buf(),
            diagnostics,
        })?;
        schema.dir = Some(
            path.parent()
                .map_or_else(|| PathBuf::from("."), Path::to_path_buf),
        );
        Ok(schema)
    }

    /// The directory the schema was loaded from, against which wasm paths
    /// resolve. `None` for a schema compiled from a string.
    pub fn dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    pub fn with_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.dir = Some(dir.into());
        self
    }

    pub fn context(&self, ctx: &str) -> Option<&Context> {
        self.contexts.get(ctx)
    }

    pub fn event_family(&self, ctx: &str, name: &str) -> Option<&EventFamily> {
        self.contexts.get(ctx)?.events.get(name)
    }

    pub fn event_type(&self, ctx: &str, name: &str, version: u16) -> Option<&EventType> {
        self.event_family(ctx, name)?.versions.get(&version)
    }

    pub fn latest_event_type(&self, ctx: &str, name: &str) -> Option<&EventType> {
        self.event_family(ctx, name).map(EventFamily::latest)
    }

    pub fn event_type_by_id(&self, id: &EventTypeId) -> Option<&EventType> {
        self.event_type(&id.context, &id.name, id.version)
    }

    /// Resolve `Context.Event` (latest version) or `Context.Event@vN`.
    pub fn resolve_event_ref(&self, s: &str) -> Result<Option<&EventType>, EventRefError> {
        let (ctx, name, version) = parse_event_ref(s)?;
        Ok(match version {
            Some(v) => self.event_type(&ctx, &name, v),
            None => self.latest_event_type(&ctx, &name),
        })
    }

    pub fn aggregate(&self, ctx: &str, name: &str) -> Option<&Aggregate> {
        self.contexts.get(ctx)?.aggregates.get(name)
    }

    /// The aggregate (in context `ctx`) whose `events` list the family
    /// `ctx.event_name`.
    pub fn aggregate_for_event(
        &self,
        ctx: &str,
        event_name: &str,
    ) -> Option<(&Context, &Aggregate)> {
        let family = EventFamilyRef {
            context: ctx.to_string(),
            name: event_name.to_string(),
        };
        self.aggregates().find(|(_, agg)| agg.owns_event(&family))
    }

    /// The aggregate whose stream template renders `stream_id`, and the key
    /// it was rendered from.
    pub fn aggregate_for_stream(&self, stream_id: &str) -> Option<(&Context, &Aggregate, Value)> {
        self.aggregates()
            .find_map(|(ctx, agg)| agg.stream.matches(stream_id).map(|key| (ctx, agg, key)))
    }

    pub fn projection(&self, ctx: &str, name: &str) -> Option<&Projection> {
        self.contexts.get(ctx)?.projections.get(name)
    }

    pub fn projections(&self) -> impl Iterator<Item = (&Context, &Projection)> {
        self.contexts
            .values()
            .flat_map(|c| c.projections.values().map(move |p| (c, p)))
    }

    pub fn aggregates(&self) -> impl Iterator<Item = (&Context, &Aggregate)> {
        self.contexts
            .values()
            .flat_map(|c| c.aggregates.values().map(move |a| (c, a)))
    }

    pub fn event_families(&self) -> impl Iterator<Item = (&Context, &EventFamily)> {
        self.contexts
            .values()
            .flat_map(|c| c.events.values().map(move |e| (c, e)))
    }

    pub fn value_type(&self, r: &TypeRef) -> Option<&ValueType> {
        let ctx = self.contexts.get(&r.context)?;
        match &r.aggregate {
            Some(agg) => ctx.aggregates.get(agg)?.values.get(&r.name),
            None => ctx.values.get(&r.name),
        }
    }

    pub fn enum_type(&self, r: &TypeRef) -> Option<&EnumType> {
        let ctx = self.contexts.get(&r.context)?;
        match &r.aggregate {
            Some(agg) => ctx.aggregates.get(agg)?.enums.get(&r.name),
            None => ctx.enums.get(&r.name),
        }
    }

    pub fn entity(&self, r: &TypeRef) -> Option<&Entity> {
        let ctx = self.contexts.get(&r.context)?;
        ctx.aggregates
            .get(r.aggregate.as_ref()?)?
            .entities
            .get(&r.name)
    }
}
