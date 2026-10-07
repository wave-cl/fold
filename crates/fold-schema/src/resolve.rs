//! Name resolution and the schema rules, as numbered diagnostics. Every rule
//! is checked and every violation reported; nothing stops at the first.
//!
//! | code | rule |
//! |---|---|
//! | S001 | duplicate type name (`value`/`enum`/`entity` share one namespace per scope) |
//! | S002 | an aggregate-local type shadows a context-level type |
//! | S003 | duplicate event `Name vN` in a context |
//! | S004 | duplicate aggregate name in a context |
//! | S005 | duplicate projection name in a context |
//! | S006 | duplicate command name in an aggregate |
//! | S007 | duplicate field name in a record |
//! | S008 | duplicate enum variant |
//! | S009 | duplicate table name in a projection |
//! | S010 | duplicate context name |
//! | S011 | unresolved type reference |
//! | S012 | `X.Y` where `X` is both a context and an aggregate of the current context |
//! | S013 | an entity used where entities may not appear |
//! | S014 | an aggregate-local value or enum used outside its aggregate |
//! | S015 | a value contains an entity |
//! | S016 | cyclic value/entity graph |
//! | S017 | `map<K, E>` with an entity `E` whose id is not of type `K` |
//! | S018 | an entity id that is not a scalar |
//! | S019 | an aggregate key that is not `uuid`, `string`, `int` or `uint` |
//! | S020 | a stream template without exactly one placeholder naming the key |
//! | S021 | an aggregate `events` entry that is not an event family of its context |
//! | S022 | an aggregate event without the key field, or with another type for it |
//! | S023 | an event family listed by more than one aggregate |
//! | S024 | a wasm path that is empty, absolute or contains `..` |
//! | S025 | a projection `from` entry that resolves to no event family |
//! | S026 | a table without a key, or with a key that is not a required scalar |
//! | S027 | an optional collection (`[T]?`, `set<T>?`, `map<K, V>?`) |
//! | S028 | `bytes` as a set element or map key |
//! | S029 | an integer out of range (event version > 65535, `snapshot every` > 2^32-1) |

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;

use crate::ast;
use crate::diag::{Diagnostic, Diagnostics};
use crate::model::*;
use crate::span::Span;
use crate::template::StreamTemplate;
use crate::types::{Scalar, Type, TypeRef};

/// Resolve a parsed file into a [`Schema`], or every diagnostic found.
pub fn resolve(src: &str, file: &ast::File) -> Result<Schema, Diagnostics> {
    let mut r = Resolver {
        index: Index::default(),
        owners: HashMap::new(),
        diags: Vec::new(),
        decl_spans: HashMap::new(),
    };
    r.index(file);
    r.owners(file);
    let mut contexts = IndexMap::new();
    for ctx in &file.contexts {
        if contexts.contains_key(&ctx.name.name) {
            continue; // S010 already reported
        }
        let resolved = r.context(ctx);
        contexts.insert(ctx.name.name.clone(), resolved);
    }
    let schema = Schema::new(contexts);
    r.cycles(&schema);
    if r.diags.is_empty() {
        Ok(schema)
    } else {
        Err(Diagnostics::new(src, r.diags))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Value,
    Enum,
    Entity,
}

#[derive(Default)]
struct AggIndex {
    values: HashSet<String>,
    enums: HashSet<String>,
    /// Entity name → its id's scalar type (None if the id is not a scalar).
    entities: HashMap<String, Option<Scalar>>,
}

impl AggIndex {
    fn kind_of(&self, name: &str) -> Option<Kind> {
        if self.values.contains(name) {
            Some(Kind::Value)
        } else if self.enums.contains(name) {
            Some(Kind::Enum)
        } else if self.entities.contains_key(name) {
            Some(Kind::Entity)
        } else {
            None
        }
    }
}

#[derive(Default)]
struct CtxIndex {
    values: HashSet<String>,
    enums: HashSet<String>,
    /// Event family names.
    events: HashSet<String>,
    aggregates: HashMap<String, AggIndex>,
}

impl CtxIndex {
    fn kind_of(&self, name: &str) -> Option<Kind> {
        if self.values.contains(name) {
            Some(Kind::Value)
        } else if self.enums.contains(name) {
            Some(Kind::Enum)
        } else {
            None
        }
    }
}

#[derive(Default)]
struct Index {
    contexts: HashMap<String, CtxIndex>,
}

/// Where a type is being used, for the entity and local-type placement rules.
#[derive(Clone, Copy, Debug)]
enum Place<'a> {
    ContextValue,
    LocalValue { agg: &'a str },
    Entity { agg: &'a str },
    Event { family: &'a str },
    State { agg: &'a str },
    Command { agg: &'a str },
    Table,
}

#[derive(Clone, Copy)]
struct Scope<'a> {
    ctx: &'a str,
    agg: Option<&'a str>,
    place: Place<'a>,
}

struct Resolver {
    index: Index,
    /// (context, family) → aggregates listing it.
    owners: HashMap<(String, String), Vec<String>>,
    diags: Vec<Diagnostic>,
    decl_spans: HashMap<TypeRef, Span>,
}

impl Resolver {
    fn diag(&mut self, code: &'static str, span: Span, message: impl Into<String>) {
        self.diags.push(Diagnostic {
            code,
            span,
            message: message.into(),
        });
    }

    // -- phase A: declared names ---------------------------------------------

    fn index(&mut self, file: &ast::File) {
        let mut seen_ctx = HashSet::new();
        for ctx in &file.contexts {
            if !seen_ctx.insert(ctx.name.name.clone()) {
                self.diag(
                    "S010",
                    ctx.name.span,
                    format!("duplicate context `{}`", ctx.name.name),
                );
                continue;
            }
            let mut ci = CtxIndex::default();
            let mut events: HashSet<(String, u64)> = HashSet::new();
            let mut projections: HashSet<String> = HashSet::new();
            for item in &ctx.items {
                match item {
                    ast::Item::Value(v) => {
                        if ci.kind_of(&v.name.name).is_some() {
                            self.dup_type(&v.name, &ctx.name.name, None);
                        } else {
                            ci.values.insert(v.name.name.clone());
                            self.decl_spans.insert(
                                TypeRef::new(&ctx.name.name, None, &v.name.name),
                                v.name.span,
                            );
                        }
                    }
                    ast::Item::Enum(e) => {
                        if ci.kind_of(&e.name.name).is_some() {
                            self.dup_type(&e.name, &ctx.name.name, None);
                        } else {
                            ci.enums.insert(e.name.name.clone());
                        }
                    }
                    ast::Item::Event(e) => {
                        if !events.insert((e.name.name.clone(), e.version.value)) {
                            self.diag(
                                "S003",
                                e.name.span,
                                format!(
                                    "duplicate event `{} v{}` in context `{}`",
                                    e.name.name, e.version.value, ctx.name.name
                                ),
                            );
                        }
                        ci.events.insert(e.name.name.clone());
                    }
                    ast::Item::Aggregate(a) => {
                        if ci.aggregates.contains_key(&a.name.name) {
                            self.diag(
                                "S004",
                                a.name.span,
                                format!(
                                    "duplicate aggregate `{}` in context `{}`",
                                    a.name.name, ctx.name.name
                                ),
                            );
                            continue;
                        }
                        let ai = self.index_aggregate(&ctx.name.name, &ci, a);
                        ci.aggregates.insert(a.name.name.clone(), ai);
                    }
                    ast::Item::Projection(p) => {
                        if !projections.insert(p.name.name.clone()) {
                            self.diag(
                                "S005",
                                p.name.span,
                                format!(
                                    "duplicate projection `{}` in context `{}`",
                                    p.name.name, ctx.name.name
                                ),
                            );
                        }
                    }
                }
            }
            self.index.contexts.insert(ctx.name.name.clone(), ci);
        }
    }

    fn dup_type(&mut self, name: &ast::Ident, ctx: &str, agg: Option<&str>) {
        let scope = match agg {
            Some(a) => format!("aggregate `{a}`"),
            None => format!("context `{ctx}`"),
        };
        self.diag(
            "S001",
            name.span,
            format!("duplicate type name `{}` in {scope}", name.name),
        );
    }

    fn index_aggregate(&mut self, ctx: &str, ci: &CtxIndex, a: &ast::AggregateDecl) -> AggIndex {
        let mut ai = AggIndex::default();
        for item in &a.items {
            let (name, kind) = match item {
                ast::LocalItem::Value(v) => (&v.name, Kind::Value),
                ast::LocalItem::Enum(e) => (&e.name, Kind::Enum),
                ast::LocalItem::Entity(e) => (&e.name, Kind::Entity),
            };
            if ai.kind_of(&name.name).is_some() {
                self.dup_type(name, ctx, Some(&a.name.name));
                continue;
            }
            if ci.kind_of(&name.name).is_some() {
                self.diag(
                    "S002",
                    name.span,
                    format!(
                        "aggregate-local type `{}` shadows the context-level type `{}.{}`",
                        name.name, ctx, name.name
                    ),
                );
                continue;
            }
            let r = TypeRef::new(ctx, Some(a.name.name.clone()), &name.name);
            match (kind, item) {
                (Kind::Value, _) => {
                    ai.values.insert(name.name.clone());
                    self.decl_spans.insert(r, name.span);
                }
                (Kind::Enum, _) => {
                    ai.enums.insert(name.name.clone());
                }
                (Kind::Entity, ast::LocalItem::Entity(e)) => {
                    let id_scalar = match (&e.id.ty.base, e.id.ty.optional) {
                        (ast::BaseType::Scalar(sc), false) => Some(*sc),
                        _ => None,
                    };
                    ai.entities.insert(name.name.clone(), id_scalar);
                    self.decl_spans.insert(r, name.span);
                }
                _ => unreachable!(),
            }
        }
        ai
    }

    // -- phase B: which aggregate owns which event family -------------------

    fn owners(&mut self, file: &ast::File) {
        let mut seen_ctx = HashSet::new();
        for ctx in &file.contexts {
            if !seen_ctx.insert(&ctx.name.name) {
                continue;
            }
            let mut seen_agg = HashSet::new();
            for item in &ctx.items {
                let ast::Item::Aggregate(a) = item else {
                    continue;
                };
                if !seen_agg.insert(&a.name.name) {
                    continue;
                }
                for e in &a.events {
                    if let Some(family) = self.aggregate_event_ref(&ctx.name.name, e) {
                        let owners = self
                            .owners
                            .entry((ctx.name.name.clone(), family.name))
                            .or_default();
                        if !owners.contains(&a.name.name) {
                            owners.push(a.name.name.clone());
                        }
                    }
                }
            }
        }
    }

    /// Resolve an aggregate's `events` entry (S021).
    fn aggregate_event_ref(&mut self, ctx: &str, e: &ast::EventRef) -> Option<EventFamilyRef> {
        if let Some(q) = &e.qualifier
            && q.name != ctx
        {
            let msg = if self.index.contexts.contains_key(&q.name) {
                format!(
                    "aggregate events must be declared in the aggregate's own context `{ctx}`, not `{}`",
                    q.name
                )
            } else {
                format!("unknown context `{}`", q.name)
            };
            self.diag("S021", e.span, msg);
            return None;
        }
        let exists = self
            .index
            .contexts
            .get(ctx)
            .is_some_and(|c| c.events.contains(&e.name.name));
        if !exists {
            self.diag(
                "S021",
                e.span,
                format!("unknown event `{}` in context `{ctx}`", e.name.name),
            );
            return None;
        }
        Some(EventFamilyRef {
            context: ctx.to_string(),
            name: e.name.name.clone(),
        })
    }

    // -- phase C: the model --------------------------------------------------

    fn context(&mut self, ctx: &ast::Context) -> Context {
        let name = ctx.name.name.as_str();
        let mut out = Context {
            name: name.to_string(),
            values: IndexMap::new(),
            enums: IndexMap::new(),
            events: IndexMap::new(),
            aggregates: IndexMap::new(),
            projections: IndexMap::new(),
        };
        // Types and events first so aggregates can check their event fields.
        for item in &ctx.items {
            match item {
                ast::Item::Value(v) => {
                    if out.values.contains_key(&v.name.name) || out.enums.contains_key(&v.name.name)
                    {
                        continue;
                    }
                    let scope = Scope {
                        ctx: name,
                        agg: None,
                        place: Place::ContextValue,
                    };
                    let fields = self.fields(&v.fields, scope);
                    out.values.insert(
                        v.name.name.clone(),
                        ValueType {
                            name: v.name.name.clone(),
                            fields,
                        },
                    );
                }
                ast::Item::Enum(e) => {
                    if out.values.contains_key(&e.name.name) || out.enums.contains_key(&e.name.name)
                    {
                        continue;
                    }
                    let en = self.enum_decl(e);
                    out.enums.insert(e.name.name.clone(), en);
                }
                ast::Item::Event(e) => {
                    let Some(version) =
                        self.int_in_range(&e.version, u64::from(u16::MAX), "event version")
                    else {
                        continue;
                    };
                    let version = version as u16;
                    let family =
                        out.events
                            .entry(e.name.name.clone())
                            .or_insert_with(|| EventFamily {
                                context: name.to_string(),
                                name: e.name.name.clone(),
                                versions: Default::default(),
                            });
                    if family.versions.contains_key(&version) {
                        continue; // S003 already reported
                    }
                    let scope = Scope {
                        ctx: name,
                        agg: None,
                        place: Place::Event {
                            family: &e.name.name,
                        },
                    };
                    let fields = self.fields(&e.fields, scope);
                    family.versions.insert(
                        version,
                        EventType {
                            id: EventTypeId {
                                context: name.to_string(),
                                name: e.name.name.clone(),
                                version,
                            },
                            fields,
                        },
                    );
                }
                _ => {}
            }
        }
        for item in &ctx.items {
            match item {
                ast::Item::Aggregate(a) => {
                    if out.aggregates.contains_key(&a.name.name) {
                        continue;
                    }
                    let agg = self.aggregate(&out, a);
                    out.aggregates.insert(a.name.name.clone(), agg);
                }
                ast::Item::Projection(p) => {
                    if out.projections.contains_key(&p.name.name) {
                        continue;
                    }
                    let proj = self.projection(name, p);
                    out.projections.insert(p.name.name.clone(), proj);
                }
                _ => {}
            }
        }
        out
    }

    fn enum_decl(&mut self, e: &ast::EnumDecl) -> EnumType {
        let mut variants: Vec<String> = Vec::new();
        for v in &e.variants {
            if variants.contains(&v.name) {
                self.diag(
                    "S008",
                    v.span,
                    format!("duplicate variant `{}` in enum `{}`", v.name, e.name.name),
                );
            } else {
                variants.push(v.name.clone());
            }
        }
        EnumType {
            name: e.name.name.clone(),
            variants,
        }
    }

    fn int_in_range(&mut self, lit: &ast::IntLit, max: u64, what: &str) -> Option<u64> {
        if lit.value > max {
            self.diag(
                "S029",
                lit.span,
                format!("{what} {} is out of range (max {max})", lit.value),
            );
            None
        } else {
            Some(lit.value)
        }
    }

    fn wasm_ref(&mut self, w: &ast::WasmRef) -> WasmRef {
        let path = &w.module.value;
        let bad = if path.is_empty() {
            Some("wasm path is empty")
        } else if path.starts_with('/') || path.starts_with('\\') || path.contains(':') {
            Some("wasm path must be relative to the schema file")
        } else if path.split(['/', '\\']).any(|seg| seg == "..") {
            Some("wasm path may not contain `..`")
        } else {
            None
        };
        if let Some(msg) = bad {
            self.diag("S024", w.module.span, format!("{msg}: {path:?}"));
        }
        WasmRef {
            module: path.clone(),
            export: w.export.as_ref().map(|e| e.value.clone()),
        }
    }

    fn fields(&mut self, fields: &[ast::Field], scope: Scope<'_>) -> Vec<Field> {
        let mut out: Vec<Field> = Vec::with_capacity(fields.len());
        let mut seen = HashSet::new();
        for f in fields {
            if !seen.insert(f.name.name.as_str()) {
                self.diag(
                    "S007",
                    f.name.span,
                    format!("duplicate field `{}`", f.name.name),
                );
                continue;
            }
            if let Some(ty) = self.ty(&f.ty, scope) {
                out.push(Field {
                    name: f.name.name.clone(),
                    ty,
                });
            }
        }
        out
    }

    fn aggregate(&mut self, ctx: &Context, a: &ast::AggregateDecl) -> Aggregate {
        let ctx_name = ctx.name.as_str();
        let agg_name = a.name.name.as_str();
        // key
        let key_scope = Scope {
            ctx: ctx_name,
            agg: Some(agg_name),
            place: Place::State { agg: agg_name },
        };
        let key_ty = self.ty(&a.key.ty, key_scope);
        let key_scalar = match &key_ty {
            Some(Type::Scalar(
                sc @ (Scalar::Uuid | Scalar::String | Scalar::Int | Scalar::Uint),
            )) => Some(*sc),
            Some(_) => {
                self.diag(
                    "S019",
                    a.key.ty.span,
                    format!(
                        "aggregate key `{}` must be uuid, string, int or uint, not {}",
                        a.key.name.name,
                        key_ty.as_ref().expect("checked")
                    ),
                );
                None
            }
            None => None,
        };
        let key = Field {
            name: a.key.name.name.clone(),
            ty: key_ty.unwrap_or(Type::Scalar(Scalar::String)),
        };

        // stream template
        let stream = match StreamTemplate::parse(&a.stream.value) {
            Ok(t) if t.placeholder() != key.name => {
                self.diag(
                    "S020",
                    a.stream.span,
                    format!(
                        "stream template placeholder `{{{}}}` must be the key `{{{}}}`",
                        t.placeholder(),
                        key.name
                    ),
                );
                t
            }
            Ok(t) => t,
            Err(e) => {
                self.diag("S020", a.stream.span, e.to_string());
                StreamTemplate::parse("{key}").expect("constant template parses")
            }
        }
        .with_key_type(key_scalar.unwrap_or(Scalar::String));

        // local types
        let mut values = IndexMap::new();
        let mut enums = IndexMap::new();
        let mut entities = IndexMap::new();
        for item in &a.items {
            let item_name = match item {
                ast::LocalItem::Value(v) => &v.name.name,
                ast::LocalItem::Enum(e) => &e.name.name,
                ast::LocalItem::Entity(e) => &e.name.name,
            };
            // Only names that made it into the index are declared here.
            if self.index.contexts[ctx_name].aggregates[agg_name]
                .kind_of(item_name)
                .is_none()
                || values.contains_key(item_name)
                || enums.contains_key(item_name)
                || entities.contains_key(item_name)
            {
                continue;
            }
            match item {
                ast::LocalItem::Value(v) => {
                    let scope = Scope {
                        ctx: ctx_name,
                        agg: Some(agg_name),
                        place: Place::LocalValue { agg: agg_name },
                    };
                    let fields = self.fields(&v.fields, scope);
                    values.insert(
                        v.name.name.clone(),
                        ValueType {
                            name: v.name.name.clone(),
                            fields,
                        },
                    );
                }
                ast::LocalItem::Enum(e) => {
                    let en = self.enum_decl(e);
                    enums.insert(e.name.name.clone(), en);
                }
                ast::LocalItem::Entity(e) => {
                    let scope = Scope {
                        ctx: ctx_name,
                        agg: Some(agg_name),
                        place: Place::Entity { agg: agg_name },
                    };
                    if !matches!(
                        (&e.id.ty.base, e.id.ty.optional),
                        (ast::BaseType::Scalar(_), false)
                    ) {
                        self.diag(
                            "S018",
                            e.id.ty.span,
                            format!(
                                "entity `{}` id `{}` must be a required scalar",
                                e.name.name, e.id.name.name
                            ),
                        );
                    }
                    let mut all: Vec<ast::Field> = Vec::with_capacity(e.fields.len() + 1);
                    all.push(e.id.clone());
                    all.extend(e.fields.iter().cloned());
                    let fields = self.fields(&all, scope);
                    let id = fields
                        .iter()
                        .find(|f| f.name == e.id.name.name)
                        .cloned()
                        .unwrap_or(Field {
                            name: e.id.name.name.clone(),
                            ty: Type::Scalar(Scalar::String),
                        });
                    entities.insert(
                        e.name.name.clone(),
                        Entity {
                            name: e.name.name.clone(),
                            id,
                            fields,
                        },
                    );
                }
            }
        }

        // events (resolved in phase B; re-resolve silently for the list)
        let mut events: Vec<EventFamilyRef> = Vec::new();
        for e in &a.events {
            let in_ctx = e.qualifier.as_ref().is_none_or(|q| q.name == ctx_name);
            if in_ctx && ctx.events.contains_key(&e.name.name) {
                let family = EventFamilyRef {
                    context: ctx_name.to_string(),
                    name: e.name.name.clone(),
                };
                if !events.contains(&family) {
                    events.push(family);
                }
                let owners = &self.owners[&(ctx_name.to_string(), e.name.name.clone())];
                if owners.len() > 1 {
                    let others: Vec<&str> = owners
                        .iter()
                        .filter(|o| *o != agg_name)
                        .map(String::as_str)
                        .collect();
                    self.diag(
                        "S023",
                        e.span,
                        format!(
                            "event `{}` is also listed by aggregate {}; an event family belongs to one aggregate",
                            e.name.name,
                            others
                                .iter()
                                .map(|o| format!("`{o}`"))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    );
                }
                // S022: every version carries the key field with the key's type.
                let family = &ctx.events[&e.name.name];
                for (version, ty) in &family.versions {
                    match ty.fields.iter().find(|f| f.name == key.name) {
                        None => self.diag(
                            "S022",
                            e.span,
                            format!(
                                "event `{} v{version}` lacks the aggregate key field `{}: {}`",
                                e.name.name, key.name, key.ty
                            ),
                        ),
                        Some(f) if f.ty != key.ty => self.diag(
                            "S022",
                            e.span,
                            format!(
                                "event `{} v{version}` field `{}` is {}, but the aggregate key is {}",
                                e.name.name, key.name, f.ty, key.ty
                            ),
                        ),
                        Some(_) => {}
                    }
                }
            }
        }

        // state
        let state_scope = Scope {
            ctx: ctx_name,
            agg: Some(agg_name),
            place: Place::State { agg: agg_name },
        };
        let state = self.fields(&a.state, state_scope);

        let evolve = self.wasm_ref(&a.evolve);
        let snapshot_every = match &a.snapshot_every {
            Some(lit) => self
                .int_in_range(lit, u64::from(u32::MAX), "snapshot every")
                .map_or(100, |v| v as u32),
            None => 100,
        };

        let mut commands = IndexMap::new();
        for c in &a.commands {
            if commands.contains_key(&c.name.name) {
                self.diag(
                    "S006",
                    c.name.span,
                    format!(
                        "duplicate command `{}` in aggregate `{agg_name}`",
                        c.name.name
                    ),
                );
                continue;
            }
            let scope = Scope {
                ctx: ctx_name,
                agg: Some(agg_name),
                place: Place::Command { agg: agg_name },
            };
            let fields = self.fields(&c.fields, scope);
            let handler = self.wasm_ref(&c.handler);
            commands.insert(
                c.name.name.clone(),
                Command {
                    name: c.name.name.clone(),
                    fields,
                    handler,
                },
            );
        }

        Aggregate {
            name: agg_name.to_string(),
            key,
            stream,
            values,
            enums,
            entities,
            events,
            state,
            evolve,
            snapshot_every,
            commands,
        }
    }

    fn projection(&mut self, ctx_name: &str, p: &ast::ProjectionDecl) -> Projection {
        let mut from = Vec::new();
        for e in &p.from {
            let target_ctx = e.qualifier.as_ref().map_or(ctx_name, |q| q.name.as_str());
            match self.index.contexts.get(target_ctx) {
                None => self.diag("S025", e.span, format!("unknown context `{target_ctx}`")),
                Some(ci) if !ci.events.contains(&e.name.name) => self.diag(
                    "S025",
                    e.span,
                    format!("unknown event `{}` in context `{target_ctx}`", e.name.name),
                ),
                Some(_) => {
                    let family = EventFamilyRef {
                        context: target_ctx.to_string(),
                        name: e.name.name.clone(),
                    };
                    if !from.contains(&family) {
                        from.push(family);
                    }
                }
            }
        }
        let fold = self.wasm_ref(&p.fold);
        let mut tables = IndexMap::new();
        for t in &p.tables {
            if tables.contains_key(&t.name.name) {
                self.diag(
                    "S009",
                    t.name.span,
                    format!(
                        "duplicate table `{}` in projection `{}`",
                        t.name.name, p.name.name
                    ),
                );
                continue;
            }
            let table = self.table(ctx_name, t);
            tables.insert(t.name.name.clone(), table);
        }
        Projection {
            name: p.name.name.clone(),
            from,
            fold,
            tables,
        }
    }

    fn table(&mut self, ctx_name: &str, t: &ast::TableDecl) -> Table {
        let scope = Scope {
            ctx: ctx_name,
            agg: None,
            place: Place::Table,
        };
        let all: Vec<ast::Field> = t.fields.iter().map(|f| f.field.clone()).collect();
        let resolved = self.fields(&all, scope);
        let mut keys = Vec::new();
        let mut columns = Vec::new();
        for tf in &t.fields {
            let Some(field) = resolved.iter().find(|f| f.name == tf.field.name.name) else {
                continue;
            };
            // A duplicate name keeps the first declaration only.
            if keys
                .iter()
                .chain(columns.iter())
                .any(|f: &Field| f.name == field.name)
            {
                continue;
            }
            if tf.key {
                if !matches!(field.ty, Type::Scalar(_)) {
                    self.diag(
                        "S026",
                        tf.field.ty.span,
                        format!(
                            "table key `{}` must be a required scalar, not {}",
                            field.name, field.ty
                        ),
                    );
                }
                keys.push(field.clone());
            } else {
                columns.push(field.clone());
            }
        }
        if !t.fields.iter().any(|f| f.key) {
            self.diag(
                "S026",
                t.name.span,
                format!("table `{}` has no `key` column", t.name.name),
            );
        }
        Table {
            name: t.name.name.clone(),
            keys,
            columns,
        }
    }

    // -- types -----------------------------------------------------------------

    fn ty(&mut self, t: &ast::Type, scope: Scope<'_>) -> Option<Type> {
        let base = match &t.base {
            ast::BaseType::Scalar(sc) => Type::Scalar(*sc),
            ast::BaseType::Ref(r) => {
                let (tr, kind) = self.type_ref(r, t.span, scope)?;
                self.check_placement(&tr, kind, t.span, scope);
                match kind {
                    Kind::Value => Type::Value(tr),
                    Kind::Enum => Type::Enum(tr),
                    Kind::Entity => Type::Entity(tr),
                }
            }
            ast::BaseType::List(inner) => Type::List(Box::new(self.ty(inner, scope)?)),
            ast::BaseType::Set(sc) => {
                if !sc.is_keyable() {
                    self.diag("S028", t.span, "set elements may not be `bytes`");
                }
                Type::Set(*sc)
            }
            ast::BaseType::Map(k, v) => {
                if !k.is_keyable() {
                    self.diag("S028", t.span, "map keys may not be `bytes`");
                }
                let value = self.ty(v, scope)?;
                if let Some(er) = value.entity()
                    && let Some(agg) = &er.aggregate
                    && let Some(Some(id_sc)) = self
                        .index
                        .contexts
                        .get(&er.context)
                        .and_then(|c| c.aggregates.get(agg))
                        .and_then(|a| a.entities.get(&er.name))
                    && id_sc != k
                {
                    self.diag(
                        "S017",
                        t.span,
                        format!(
                            "map key type {k} must equal the id type {id_sc} of entity `{}`",
                            er.name
                        ),
                    );
                }
                Type::Map(*k, Box::new(value))
            }
        };
        if t.optional {
            if base.is_collection() {
                self.diag(
                    "S027",
                    t.span,
                    format!(
                        "a collection is never optional: `{base}?` (its empty value is the default)"
                    ),
                );
            }
            Some(Type::Optional(Box::new(base)))
        } else {
            Some(base)
        }
    }

    fn type_ref(
        &mut self,
        r: &ast::TypeRefSyntax,
        span: Span,
        scope: Scope<'_>,
    ) -> Option<(TypeRef, Kind)> {
        let ci = &self.index.contexts[scope.ctx];
        match &r.qualifier {
            None => {
                if let Some(agg) = scope.agg
                    && let Some(kind) = ci.aggregates[agg].kind_of(&r.name.name)
                {
                    return Some((
                        TypeRef::new(scope.ctx, Some(agg.to_string()), &r.name.name),
                        kind,
                    ));
                }
                if let Some(kind) = ci.kind_of(&r.name.name) {
                    return Some((TypeRef::new(scope.ctx, None, &r.name.name), kind));
                }
                self.diag("S011", span, format!("unknown type `{}`", r.name.name));
                None
            }
            Some(q) => {
                let is_ctx = self.index.contexts.contains_key(&q.name);
                let is_agg = ci.aggregates.contains_key(&q.name);
                match (is_ctx, is_agg) {
                    (true, true) => {
                        self.diag(
                            "S012",
                            span,
                            format!(
                                "`{}` is both a context and an aggregate of context `{}`; the reference `{}.{}` is ambiguous",
                                q.name, scope.ctx, q.name, r.name.name
                            ),
                        );
                        None
                    }
                    (true, false) => match self.index.contexts[&q.name].kind_of(&r.name.name) {
                        Some(kind) => Some((TypeRef::new(&q.name, None, &r.name.name), kind)),
                        None => {
                            self.diag(
                                "S011",
                                span,
                                format!("unknown type `{}` in context `{}`", r.name.name, q.name),
                            );
                            None
                        }
                    },
                    (false, true) => match ci.aggregates[&q.name].kind_of(&r.name.name) {
                        Some(kind) => Some((
                            TypeRef::new(scope.ctx, Some(q.name.clone()), &r.name.name),
                            kind,
                        )),
                        None => {
                            self.diag(
                                "S011",
                                span,
                                format!("unknown type `{}` in aggregate `{}`", r.name.name, q.name),
                            );
                            None
                        }
                    },
                    (false, false) => {
                        self.diag(
                            "S011",
                            span,
                            format!("unknown context or aggregate `{}`", q.name),
                        );
                        None
                    }
                }
            }
        }
    }

    /// The entity and aggregate-local placement rules (S013, S014, S015).
    fn check_placement(&mut self, tr: &TypeRef, kind: Kind, span: Span, scope: Scope<'_>) {
        let Some(owner) = &tr.aggregate else {
            return; // context-level types go anywhere
        };
        let owner = owner.as_str();
        let owned_event = |me: &Self, family: &str| {
            me.owners
                .get(&(scope.ctx.to_string(), family.to_string()))
                .is_some_and(|o| o.iter().any(|a| a == owner))
        };
        if kind == Kind::Entity {
            let allowed = match scope.place {
                Place::ContextValue | Place::LocalValue { .. } => {
                    self.diag(
                        "S015",
                        span,
                        format!("a value may not contain the entity `{}`", tr.name),
                    );
                    return;
                }
                Place::Table => {
                    self.diag(
                        "S013",
                        span,
                        format!("entity `{}` may not appear in a projection table", tr.name),
                    );
                    return;
                }
                Place::Entity { agg } | Place::State { agg } | Place::Command { agg } => {
                    agg == owner
                }
                Place::Event { family } => owned_event(self, family),
            };
            if !allowed {
                self.diag(
                    "S013",
                    span,
                    format!(
                        "entity `{}` belongs to aggregate `{owner}` and may appear only in its state, entities, commands and events",
                        tr.name
                    ),
                );
            }
            return;
        }
        let allowed = match scope.place {
            Place::ContextValue | Place::Table => false,
            Place::LocalValue { agg }
            | Place::Entity { agg }
            | Place::State { agg }
            | Place::Command { agg } => agg == owner,
            Place::Event { family } => owned_event(self, family),
        };
        if !allowed {
            let what = if kind == Kind::Value { "value" } else { "enum" };
            self.diag(
                "S014",
                span,
                format!(
                    "aggregate-local {what} `{}` may be used only inside aggregate `{owner}` and in its events and commands",
                    tr.name
                ),
            );
        }
    }

    // -- phase D: cycles -------------------------------------------------------

    fn cycles(&mut self, schema: &Schema) {
        // Nodes: every value and entity. Edges: the named types its fields hold.
        let mut nodes: Vec<(TypeRef, Vec<TypeRef>)> = Vec::new();
        for ctx in schema.contexts.values() {
            for v in ctx.values.values() {
                nodes.push((
                    TypeRef::new(&ctx.name, None, &v.name),
                    record_edges(schema, &v.fields),
                ));
            }
            for agg in ctx.aggregates.values() {
                for v in agg.values.values() {
                    nodes.push((
                        TypeRef::new(&ctx.name, Some(agg.name.clone()), &v.name),
                        record_edges(schema, &v.fields),
                    ));
                }
                for e in agg.entities.values() {
                    nodes.push((
                        TypeRef::new(&ctx.name, Some(agg.name.clone()), &e.name),
                        record_edges(schema, &e.fields),
                    ));
                }
            }
        }
        let graph: HashMap<&TypeRef, &Vec<TypeRef>> = nodes.iter().map(|(n, e)| (n, e)).collect();
        #[derive(Clone, Copy, PartialEq)]
        enum Color {
            White,
            Grey,
            Black,
        }
        let mut color: HashMap<&TypeRef, Color> =
            graph.keys().map(|k| (*k, Color::White)).collect();
        let mut reported: HashSet<TypeRef> = HashSet::new();
        for (start, _) in &nodes {
            if color[start] != Color::White {
                continue;
            }
            // Iterative DFS with an explicit path for the cycle message.
            let mut stack: Vec<(&TypeRef, usize)> = vec![(start, 0)];
            color.insert(start, Color::Grey);
            while let Some(&(node, i)) = stack.last() {
                let edges = graph[node];
                if i == edges.len() {
                    color.insert(node, Color::Black);
                    stack.pop();
                    continue;
                }
                stack.last_mut().expect("non-empty").1 += 1;
                let next = &edges[i];
                let Some(&c) = color.get(next) else { continue };
                match c {
                    Color::White => {
                        color.insert(next, Color::Grey);
                        stack.push((next, 0));
                    }
                    Color::Grey => {
                        if reported.insert(next.clone()) {
                            let pos = stack.iter().position(|(n, _)| *n == next).unwrap_or(0);
                            let path: Vec<String> = stack[pos..]
                                .iter()
                                .map(|(n, _)| n.name.clone())
                                .chain(std::iter::once(next.name.clone()))
                                .collect();
                            let span = self.decl_spans.get(next).copied().unwrap_or_default();
                            self.diag(
                                "S016",
                                span,
                                format!(
                                    "type `{}` is part of a cycle: {}",
                                    next.name,
                                    path.join(" -> ")
                                ),
                            );
                        }
                    }
                    Color::Black => {}
                }
            }
        }
    }
}

/// The values and entities referenced by a record's fields.
fn record_edges(schema: &Schema, fields: &[Field]) -> Vec<TypeRef> {
    let mut out = Vec::new();
    for f in fields {
        for r in f.ty.refs() {
            if schema.enum_type(r).is_some() {
                continue;
            }
            if !out.contains(r) {
                out.push(r.clone());
            }
        }
    }
    out
}
