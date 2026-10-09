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
//! | S030 | duplicate invariant name in a context |
//! | S031 | duplicate invariant name in an aggregate |
//! | S032 | an invariant `on` an aggregate that is not in its context |
//! | S033 | an invariant `projection` that resolves to no projection |
//! | S034 | an invariant `scope` that is not a required keyable scalar field of the aggregate's state |
//! | S035 | duplicate process name in a context, or a process named like one of its projections |
//! | S036 | a process key that is not uuid, string, int or uint |
//! | S037 | a process `from` entry that resolves to no event family |
//! | S038 | a process source event lacking the correlating field, or carrying it with another type |
//! | S039 | a rule path naming no field, or descending into something that is not a value |
//! | S040 | a rule comparing operands of different kinds, or an operator its operands do not support |
//! | S041 | a rule `matches` pattern that is not a valid regular expression |
//! | S042 | duplicate rule name in a value |
//! | S043 | a default on a field that is optional, a collection, a value or an entity |
//! | S044 | a default on an aggregate key, process key, entity id or table key |
//! | S045 | a default literal that does not fit its type, or names an unknown or payload-carrying variant |
//! | S048 | an upcast from a version the family does not have |
//! | S049 | an upcast not from the immediately preceding version, or on the first version |
//! | S050 | an upcast op naming a field that does not exist, or a duplicate op |
//! | S051 | an upcast whose result would not be a valid record of its version |
//! | S052 | a version after the first with neither an upcast nor an implicit one |
//! | S053 | a bare name that is not a variant of the enum it is compared with, or a variant against a non-enum |
//! | S054 | a `requires` path without a `state.`/`command.` root, a bare `state`, or `exists` outside a `requires` |
//! | S055 | duplicate guard name in a command |
//! | S046 | an import that cannot be read, or an import in a schema compiled from text (`source.rs`) |
//! | S047 | an import path that is empty, absolute, holds `:` or `..` (`source.rs`) |
//! | S056 | duplicate timer name in a process |
//! | S057 | a context named `Fold` (reserved for the daemon's own events) |

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;

use crate::ast;
use crate::diag::{Diagnostic, Diagnostics};
use serde_json::Value;

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
        process_checks: Vec::new(),
        pending_rules: Vec::new(),
        pending_upcasts: Vec::new(),
        pending_guards: Vec::new(),
    };
    r.index(file);
    r.collect_rules(file);
    r.collect_upcasts(file);
    r.collect_guards(file);
    r.owners(file);
    let mut contexts = IndexMap::new();
    for ctx in &file.contexts {
        if contexts.contains_key(&ctx.name.name) {
            continue; // S010 already reported
        }
        let resolved = r.context(ctx);
        contexts.insert(ctx.name.name.clone(), resolved);
    }
    let mut schema = Schema::new(contexts);
    schema.docs = file.docs.clone();
    r.cycles(&schema);
    r.check_processes(&schema);
    r.resolve_rules(&mut schema);
    r.resolve_upcasts(&mut schema);
    r.resolve_guards(&mut schema);
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

/// A variant name and whether it carries a payload.
type VariantList = Vec<(String, bool)>;

#[derive(Default)]
struct AggIndex {
    values: HashSet<String>,
    enums: HashMap<String, VariantList>,
    /// Entity name → its id's scalar type (None if the id is not a scalar).
    entities: HashMap<String, Option<Scalar>>,
}

impl AggIndex {
    fn kind_of(&self, name: &str) -> Option<Kind> {
        if self.values.contains(name) {
            Some(Kind::Value)
        } else if self.enums.contains_key(name) {
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
    enums: HashMap<String, VariantList>,
    /// Event family names.
    events: HashSet<String>,
    aggregates: HashMap<String, AggIndex>,
    projections: HashSet<String>,
}

impl CtxIndex {
    fn kind_of(&self, name: &str) -> Option<Kind> {
        if self.values.contains(name) {
            Some(Kind::Value)
        } else if self.enums.contains_key(name) {
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
    LocalValue {
        agg: &'a str,
    },
    /// A payload of a context-level enum: a record like a context value.
    ContextEnum,
    /// A payload of an aggregate-local enum: a record like that aggregate's
    /// entities.
    LocalEnum {
        agg: &'a str,
    },
    Entity {
        agg: &'a str,
    },
    Event {
        family: &'a str,
    },
    State {
        agg: &'a str,
    },
    Command {
        agg: &'a str,
    },
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
    /// Process sources whose correlating field is checked once every
    /// context's events are lowered (they may live in a later context).
    process_checks: Vec<ProcessCheck>,
    /// Value rules, lowered once every value type is known (a rule may
    /// descend into a value of a later context).
    pending_rules: Vec<PendingRules>,
    /// Event upcasts, resolved once every version and type is known.
    pending_upcasts: Vec<PendingUpcast>,
    /// Aggregate invariants and command guards, lowered once every type is
    /// known (their paths may descend into values of later contexts).
    pending_guards: Vec<PendingGuards>,
}

struct PendingGuards {
    ctx: String,
    agg: String,
    invariants: Vec<ast::InvariantRef>,
    commands: Vec<(String, Vec<ast::RuleDecl>)>,
}

/// What a rule expression's paths start from.
#[derive(Clone, Copy)]
enum ExprScope<'a> {
    /// A record: paths start at its fields (value rules, invariants).
    Record(&'a [Field]),
    /// Named roots: paths start with one of them (`state.`, `command.`).
    Rooted(&'a [(&'a str, &'a [Field])]),
}

/// The two versions an upcast bridges, for the checks.
#[derive(Clone, Copy)]
struct UpcastSides<'a> {
    label: &'a str,
    from: u16,
    source: &'a [Field],
    target: &'a [Field],
}

struct PendingUpcast {
    ctx: String,
    name: String,
    version: u64,
    decl: Option<ast::UpcastDecl>,
    name_span: Span,
}

struct PendingRules {
    ctx: String,
    agg: Option<String>,
    value: String,
    rules: Vec<ast::RuleDecl>,
}

/// What a rule path points at.
enum PathKind {
    Operand(OperandKind),
    Collection,
    Record,
}

struct ProcessCheck {
    process: String,
    context: String,
    event: String,
    by: String,
    key: Scalar,
    span: Span,
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
            if ctx.name.name == RESERVED_CONTEXT {
                self.diag(
                    "S057",
                    ctx.name.span,
                    format!("context `{RESERVED_CONTEXT}` is reserved for the daemon's own events"),
                );
            }
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
            let mut invariants: HashSet<String> = HashSet::new();
            let mut processes: HashSet<String> = HashSet::new();
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
                            ci.enums.insert(e.name.name.clone(), variant_list(e));
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
                        ci.projections.insert(p.name.name.clone());
                    }
                    ast::Item::Process(p) => {
                        if !processes.insert(p.name.name.clone()) {
                            self.diag(
                                "S035",
                                p.name.span,
                                format!(
                                    "duplicate process `{}` in context `{}`",
                                    p.name.name, ctx.name.name
                                ),
                            );
                        }
                    }
                    ast::Item::Invariant(i) => {
                        if !invariants.insert(i.name.name.clone()) {
                            self.diag(
                                "S030",
                                i.name.span,
                                format!(
                                    "duplicate invariant `{}` in context `{}`",
                                    i.name.name, ctx.name.name
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
                (Kind::Enum, ast::LocalItem::Enum(e)) => {
                    ai.enums.insert(name.name.clone(), variant_list(e));
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
            docs: ctx.docs.clone(),
            values: IndexMap::new(),
            enums: IndexMap::new(),
            events: IndexMap::new(),
            aggregates: IndexMap::new(),
            projections: IndexMap::new(),
            invariants: IndexMap::new(),
            processes: IndexMap::new(),
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
                            docs: v.docs.clone(),
                            fields,
                            rules: Vec::new(),
                        },
                    );
                }
                ast::Item::Enum(e) => {
                    if out.values.contains_key(&e.name.name) || out.enums.contains_key(&e.name.name)
                    {
                        continue;
                    }
                    let scope = Scope {
                        ctx: name,
                        agg: None,
                        place: Place::ContextEnum,
                    };
                    let en = self.enum_decl(e, scope);
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
                            docs: e.docs.clone(),
                            fields,
                            upcast: None,
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
        // Processes: they share the read-model namespace with projections.
        for item in &ctx.items {
            let ast::Item::Process(p) = item else {
                continue;
            };
            if out.processes.contains_key(&p.name.name) {
                continue;
            }
            if out.projections.contains_key(&p.name.name) {
                self.diag(
                    "S035",
                    p.name.span,
                    format!(
                        "process `{}` is named like a projection of context `{name}`; they share one namespace",
                        p.name.name
                    ),
                );
                continue;
            }
            let proc = self.process(name, p);
            out.processes.insert(p.name.name.clone(), proc);
        }
        // Context invariants last: they refer to aggregates and projections.
        for item in &ctx.items {
            let ast::Item::Invariant(i) = item else {
                continue;
            };
            if out.invariants.contains_key(&i.name.name) {
                continue;
            }
            if let Some(inv) = self.invariant(name, &out, i) {
                out.invariants.insert(i.name.name.clone(), inv);
            }
        }
        out
    }

    fn invariant(
        &mut self,
        ctx_name: &str,
        out: &Context,
        i: &ast::InvariantDecl,
    ) -> Option<ContextInvariant> {
        let mut ok = true;
        let aggregate = match out.aggregates.get(&i.on.name) {
            Some(a) => Some(a),
            None => {
                self.diag(
                    "S032",
                    i.on.span,
                    format!(
                        "invariant `{}` is on `{}`, which is not an aggregate of context `{ctx_name}`",
                        i.name.name, i.on.name
                    ),
                );
                ok = false;
                None
            }
        };
        let target_ctx = i
            .projection
            .qualifier
            .as_ref()
            .map_or(ctx_name, |q| q.name.as_str());
        let projection_exists = if target_ctx == ctx_name {
            out.projections.contains_key(&i.projection.name.name)
        } else {
            self.index
                .contexts
                .get(target_ctx)
                .is_some_and(|c| c.projections.contains(&i.projection.name.name))
        };
        if !projection_exists {
            self.diag(
                "S033",
                i.projection.span,
                format!(
                    "invariant `{}` reads projection `{target_ctx}.{}`, which does not exist",
                    i.name.name, i.projection.name.name
                ),
            );
            ok = false;
        }
        let scope = aggregate.and_then(|a| {
            let field = a.state.iter().find(|f| f.name == i.scope.name);
            match field {
                Some(f) if matches!(&f.ty, Type::Scalar(sc) if sc.is_keyable()) => Some(f.clone()),
                Some(f) => {
                    self.diag(
                        "S034",
                        i.scope.span,
                        format!(
                            "invariant `{}` scope `{}` has type {}, which cannot key a scope; it must be a required keyable scalar",
                            i.name.name, i.scope.name, f.ty
                        ),
                    );
                    None
                }
                None => {
                    self.diag(
                        "S034",
                        i.scope.span,
                        format!(
                            "invariant `{}` scope `{}` is not a field of aggregate `{}`'s state",
                            i.name.name, i.scope.name, a.name
                        ),
                    );
                    None
                }
            }
        });
        let check = self.wasm_ref(&i.check);
        if !ok {
            return None;
        }
        Some(ContextInvariant {
            name: i.name.name.clone(),
            docs: i.docs.clone(),
            aggregate: i.on.name.clone(),
            projection: ProjectionRef {
                context: target_ctx.to_string(),
                name: i.projection.name.name.clone(),
            },
            scope: scope?,
            check,
        })
    }

    fn enum_decl(&mut self, e: &ast::EnumDecl, scope: Scope<'_>) -> EnumType {
        let mut variants: Vec<EnumVariant> = Vec::new();
        for v in &e.variants {
            if variants.iter().any(|x| x.name == v.name.name) {
                self.diag(
                    "S008",
                    v.name.span,
                    format!(
                        "duplicate variant `{}` in enum `{}`",
                        v.name.name, e.name.name
                    ),
                );
                continue;
            }
            let payload = v.payload.as_ref().map(|fields| self.fields(fields, scope));
            variants.push(EnumVariant {
                name: v.name.name.clone(),
                docs: v.docs.clone(),
                payload,
            });
        }
        EnumType {
            name: e.name.name.clone(),
            docs: e.docs.clone(),
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
                let default = f
                    .default
                    .as_ref()
                    .and_then(|d| self.default_value(&f.name.name, d, &ty));
                out.push(Field {
                    name: f.name.name.clone(),
                    ty,
                    docs: f.docs.clone(),
                    default,
                });
            }
        }
        out
    }

    /// The canonical JSON of a field default (S043, S045), or `None` with a
    /// diagnostic.
    fn default_value(&mut self, field: &str, d: &ast::Literal, ty: &Type) -> Option<Value> {
        let span = d.span();
        match ty {
            Type::Scalar(sc) => {
                let candidate = match d {
                    ast::Literal::Number(text, _) => {
                        if *sc == Scalar::Decimal {
                            Value::String(text.clone())
                        } else if let Ok(i) = text.parse::<i64>() {
                            Value::from(i)
                        } else if let Ok(u) = text.parse::<u64>() {
                            Value::from(u)
                        } else {
                            Value::String(text.clone())
                        }
                    }
                    ast::Literal::Str(s) => Value::String(s.value.clone()),
                    ast::Literal::Bool(b, _) => Value::Bool(*b),
                    ast::Literal::Variant(v) => {
                        self.diag(
                            "S045",
                            span,
                            format!(
                                "default for `{field}: {sc}` is invalid: `{}` names a variant, but the field is not an enum",
                                v.name
                            ),
                        );
                        return None;
                    }
                };
                match crate::validate::check_scalar(*sc, &candidate, "$") {
                    Ok(k) => Some(k.to_value()),
                    Err(e) => {
                        self.diag(
                            "S045",
                            span,
                            format!("default for `{field}: {sc}` is invalid: {e}"),
                        );
                        None
                    }
                }
            }
            Type::Enum(r) => {
                let ast::Literal::Variant(v) = d else {
                    self.diag(
                        "S045",
                        span,
                        format!(
                            "default for `{field}: {r}` must be one of its variants, written bare"
                        ),
                    );
                    return None;
                };
                let variants =
                    self.index
                        .contexts
                        .get(&r.context)
                        .and_then(|c| match &r.aggregate {
                            None => c.enums.get(&r.name),
                            Some(agg) => c.aggregates.get(agg).and_then(|a| a.enums.get(&r.name)),
                        });
                match variants.and_then(|vs| vs.iter().find(|(n, _)| *n == v.name)) {
                    None => {
                        let names: Vec<&str> = variants
                            .map(|vs| vs.iter().map(|(n, _)| n.as_str()).collect())
                            .unwrap_or_default();
                        self.diag(
                            "S045",
                            span,
                            format!(
                                "default `{}` is not a variant of {r} (variants: {})",
                                v.name,
                                names.join(", ")
                            ),
                        );
                        None
                    }
                    Some((_, true)) => {
                        self.diag(
                            "S045",
                            span,
                            format!(
                                "default `{}` carries a payload; only a unit variant can be a default",
                                v.name
                            ),
                        );
                        None
                    }
                    Some((name, false)) => Some(Value::String(name.clone())),
                }
            }
            other => {
                self.diag(
                    "S043",
                    span,
                    format!(
                        "field `{field}` of type `{other}` cannot have a default; defaults apply to required scalar and enum fields"
                    ),
                );
                None
            }
        }
    }

    /// S044: a key or id field carries no default (its value is the
    /// record's identity, never implied).
    fn no_default_on_key(&mut self, f: &ast::Field, what: &str) -> bool {
        match &f.default {
            Some(d) => {
                self.diag(
                    "S044",
                    d.span(),
                    format!("{what} `{}` cannot have a default", f.name.name),
                );
                true
            }
            None => false,
        }
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
        self.no_default_on_key(&a.key, "aggregate key");
        let key = Field {
            name: a.key.name.name.clone(),
            ty: key_ty.unwrap_or(Type::Scalar(Scalar::String)),
            docs: a.key.docs.clone(),
            default: None,
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
                            docs: v.docs.clone(),
                            fields,
                            rules: Vec::new(),
                        },
                    );
                }
                ast::LocalItem::Enum(e) => {
                    let scope = Scope {
                        ctx: ctx_name,
                        agg: Some(agg_name),
                        place: Place::LocalEnum { agg: agg_name },
                    };
                    let en = self.enum_decl(e, scope);
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
                    let mut id_field = e.id.clone();
                    if self.no_default_on_key(&e.id, "entity id") {
                        id_field.default = None;
                    }
                    all.push(id_field);
                    all.extend(e.fields.iter().cloned());
                    let fields = self.fields(&all, scope);
                    let id = fields
                        .iter()
                        .find(|f| f.name == e.id.name.name)
                        .cloned()
                        .unwrap_or(Field {
                            name: e.id.name.name.clone(),
                            ty: Type::Scalar(Scalar::String),
                            docs: e.id.docs.clone(),
                            default: None,
                        });
                    entities.insert(
                        e.name.name.clone(),
                        Entity {
                            name: e.name.name.clone(),
                            docs: e.docs.clone(),
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
                    docs: c.docs.clone(),
                    fields,
                    requires: Vec::new(),
                    handler,
                },
            );
        }

        // Invariants and guards are lowered by `resolve_guards`, once every
        // type exists.
        let invariants = IndexMap::new();

        Aggregate {
            name: agg_name.to_string(),
            docs: a.docs.clone(),
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
            invariants,
        }
    }

    fn process(&mut self, ctx_name: &str, p: &ast::ProcessDecl) -> Process {
        let scope = Scope {
            ctx: ctx_name,
            agg: None,
            place: Place::Table,
        };
        let key_ty = self.ty(&p.key.ty, scope);
        let key_scalar = match &key_ty {
            Some(Type::Scalar(
                sc @ (Scalar::Uuid | Scalar::String | Scalar::Int | Scalar::Uint),
            )) => Some(*sc),
            Some(t) => {
                self.diag(
                    "S036",
                    p.key.ty.span,
                    format!(
                        "process key `{}` must be uuid, string, int or uint, not {t}",
                        p.key.name.name
                    ),
                );
                None
            }
            None => None,
        };
        self.no_default_on_key(&p.key, "process key");
        let key = Field {
            name: p.key.name.name.clone(),
            ty: Type::Scalar(key_scalar.unwrap_or(Scalar::String)),
            docs: p.key.docs.clone(),
            default: None,
        };

        let mut from: Vec<ProcessSource> = Vec::new();
        for src in &p.from {
            let e = &src.event;
            let target_ctx = e.qualifier.as_ref().map_or(ctx_name, |q| q.name.as_str());
            let known = match self.index.contexts.get(target_ctx) {
                None => {
                    self.diag("S037", e.span, format!("unknown context `{target_ctx}`"));
                    false
                }
                Some(ci) if !ci.events.contains(&e.name.name) => {
                    self.diag(
                        "S037",
                        e.span,
                        format!("unknown event `{}` in context `{target_ctx}`", e.name.name),
                    );
                    false
                }
                Some(_) => true,
            };
            if !known {
                continue;
            }
            let family = EventFamilyRef {
                context: target_ctx.to_string(),
                name: e.name.name.clone(),
            };
            if from.iter().any(|s| s.family == family) {
                continue;
            }
            let by = src
                .by
                .as_ref()
                .map_or(p.key.name.name.clone(), |b| b.name.clone());
            // Every version of the family must carry the correlating field
            // with the key's type; checked once all contexts are lowered.
            if let Some(key) = key_scalar {
                self.process_checks.push(ProcessCheck {
                    process: p.name.name.clone(),
                    context: target_ctx.to_string(),
                    event: e.name.name.clone(),
                    by: by.clone(),
                    key,
                    span: src.span,
                });
            }
            from.push(ProcessSource { family, by });
        }

        let state = self.fields(&p.state, scope);
        let react = self.wasm_ref(&p.react);
        let snapshot_every = match &p.snapshot_every {
            Some(lit) => self
                .int_in_range(lit, u64::from(u32::MAX), "snapshot every")
                .map_or(0, |v| v as u32),
            None => 0,
        };
        let mut timers: Vec<String> = Vec::new();
        for t in &p.timers {
            if timers.contains(&t.name) {
                self.diag(
                    "S056",
                    t.span,
                    format!("duplicate timer `{}` in process `{}`", t.name, p.name.name),
                );
                continue;
            }
            timers.push(t.name.clone());
        }

        Process {
            name: p.name.name.clone(),
            docs: p.docs.clone(),
            key,
            from,
            state,
            react,
            snapshot_every,
            timers,
        }
    }

    /// Remembers every value's rules for lowering after all types exist.
    fn collect_rules(&mut self, file: &ast::File) {
        let mut seen_ctx = HashSet::new();
        for ctx in &file.contexts {
            if !seen_ctx.insert(ctx.name.name.clone()) {
                continue;
            }
            let mut seen_values = HashSet::new();
            for item in &ctx.items {
                match item {
                    ast::Item::Value(v) if !v.rules.is_empty() => {
                        if seen_values.insert(v.name.name.clone()) {
                            self.pending_rules.push(PendingRules {
                                ctx: ctx.name.name.clone(),
                                agg: None,
                                value: v.name.name.clone(),
                                rules: v.rules.clone(),
                            });
                        }
                    }
                    ast::Item::Aggregate(a) => {
                        let mut seen_local = HashSet::new();
                        for li in &a.items {
                            if let ast::LocalItem::Value(v) = li
                                && !v.rules.is_empty()
                                && seen_local.insert(v.name.name.clone())
                            {
                                self.pending_rules.push(PendingRules {
                                    ctx: ctx.name.name.clone(),
                                    agg: Some(a.name.name.clone()),
                                    value: v.name.name.clone(),
                                    rules: v.rules.clone(),
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// Remembers every event version's upcast clause (or its absence) for
    /// resolution once every version and type is known.
    fn collect_upcasts(&mut self, file: &ast::File) {
        let mut seen_ctx = HashSet::new();
        for ctx in &file.contexts {
            if !seen_ctx.insert(ctx.name.name.clone()) {
                continue;
            }
            let mut seen: HashSet<(String, u64)> = HashSet::new();
            for item in &ctx.items {
                let ast::Item::Event(e) = item else {
                    continue;
                };
                if !seen.insert((e.name.name.clone(), e.version.value)) {
                    continue;
                }
                self.pending_upcasts.push(PendingUpcast {
                    ctx: ctx.name.name.clone(),
                    name: e.name.name.clone(),
                    version: e.version.value,
                    decl: e.upcast.clone(),
                    name_span: e.name.span,
                });
            }
        }
    }

    /// Resolves every upcast: S048–S052.
    fn resolve_upcasts(&mut self, schema: &mut Schema) {
        let pending = std::mem::take(&mut self.pending_upcasts);
        let mut lowered: Vec<(String, String, u16, Upcast)> = Vec::new();
        for p in &pending {
            let Some(family) = schema
                .contexts
                .get(&p.ctx)
                .and_then(|c| c.events.get(&p.name))
            else {
                continue;
            };
            if p.version > u64::from(u16::MAX) {
                continue; // S029 already reported
            }
            let version = p.version as u16;
            let Some(target) = family.versions.get(&version) else {
                continue;
            };
            let predecessor = family
                .versions
                .range(..version)
                .next_back()
                .map(|(v, t)| (*v, t));
            let label = format!("{} v{version}", p.name);
            match (&p.decl, predecessor) {
                (None, None) => {}
                (Some(d), None) => {
                    self.diag(
                        "S049",
                        d.span,
                        format!(
                            "event `{label}` is the first version of `{}` and cannot upcast",
                            p.name
                        ),
                    );
                }
                (None, Some((pv, prev))) => {
                    if implicit_upcast_ok(&prev.fields, &target.fields) {
                        lowered.push((
                            p.ctx.clone(),
                            p.name.clone(),
                            version,
                            Upcast {
                                from: pv,
                                how: UpcastHow::Declarative(DeclarativeUpcast::default()),
                            },
                        ));
                    } else {
                        self.diag(
                            "S052",
                            p.name_span,
                            format!(
                                "event `{label}` has no `upcast from v{pv}`; every version after the first declares how to produce it from its predecessor (a version that only adds optional or defaulted fields needs none)"
                            ),
                        );
                    }
                }
                (Some(d), Some((pv, prev))) => {
                    let Some(from) =
                        self.int_in_range(&d.from, u64::from(u16::MAX), "upcast version")
                    else {
                        continue;
                    };
                    let from = from as u16;
                    if !family.versions.contains_key(&from) {
                        let versions: Vec<String> =
                            family.versions.keys().map(|v| format!("v{v}")).collect();
                        self.diag(
                            "S048",
                            d.from.span,
                            format!(
                                "event `{label}` upcasts from `v{from}`, which is not a version of `{}` (versions: {})",
                                p.name,
                                versions.join(", ")
                            ),
                        );
                        continue;
                    }
                    if from != pv {
                        self.diag(
                            "S049",
                            d.from.span,
                            format!(
                                "event `{label}` must upcast from its predecessor `v{pv}`, not `v{from}`"
                            ),
                        );
                        continue;
                    }
                    let how = match &d.how {
                        ast::UpcastHow::Wasm(w) => UpcastHow::Wasm(self.wasm_ref(w)),
                        ast::UpcastHow::Ops(ops) => {
                            let sides = UpcastSides {
                                label: &label,
                                from: pv,
                                source: &prev.fields,
                                target: &target.fields,
                            };
                            let Some(up) = self.declarative_upcast(schema, &sides, ops, d.span)
                            else {
                                continue;
                            };
                            UpcastHow::Declarative(up)
                        }
                    };
                    lowered.push((p.ctx.clone(), p.name.clone(), version, Upcast { from, how }));
                }
            }
        }
        for (ctx, name, version, up) in lowered {
            if let Some(t) = schema
                .contexts
                .get_mut(&ctx)
                .and_then(|c| c.events.get_mut(&name))
                .and_then(|f| f.versions.get_mut(&version))
            {
                t.upcast = Some(up);
            }
        }
    }

    /// Checks and lowers the ops of a declarative upcast (S050, S051).
    fn declarative_upcast(
        &mut self,
        schema: &Schema,
        sides: &UpcastSides<'_>,
        ops: &[ast::UpcastOp],
        decl_span: Span,
    ) -> Option<DeclarativeUpcast> {
        let UpcastSides {
            label,
            from,
            source,
            target,
        } = *sides;
        let whole = upcast_span(ops, decl_span);
        let mut ok = true;
        let mut rename: Vec<(String, String)> = Vec::new();
        let mut set: Vec<(String, Value)> = Vec::new();
        let in_source = |n: &str| source.iter().any(|f| f.name == n);
        let target_field = |n: &str| target.iter().find(|f| f.name == n);
        for op in ops {
            match op {
                ast::UpcastOp::Rename {
                    from: old,
                    to,
                    span,
                } => {
                    if !in_source(&old.name) {
                        self.diag(
                            "S050",
                            old.span,
                            format!(
                                "upcast: `rename {}`: `{}` is not a field of `v{from}`",
                                old.name, old.name
                            ),
                        );
                        ok = false;
                    }
                    if target_field(&to.name).is_none() {
                        self.diag(
                            "S050",
                            to.span,
                            format!(
                                "upcast: `rename {} as {}`: `{}` is not a field of `{label}`",
                                old.name, to.name, to.name
                            ),
                        );
                        ok = false;
                    }
                    if rename.iter().any(|(o, _)| *o == old.name) {
                        self.diag(
                            "S050",
                            *span,
                            format!("upcast: `{}` is renamed twice", old.name),
                        );
                        ok = false;
                    }
                    if rename.iter().any(|(_, n)| *n == to.name)
                        || set.iter().any(|(f, _)| *f == to.name)
                    {
                        self.diag(
                            "S050",
                            *span,
                            format!("upcast: field `{}` is both set and renamed into", to.name),
                        );
                        ok = false;
                    }
                    rename.push((old.name.clone(), to.name.clone()));
                }
                ast::UpcastOp::Set { field, value, span } => {
                    let Some(t) = target_field(&field.name) else {
                        self.diag(
                            "S050",
                            field.span,
                            format!(
                                "upcast: `set` names `{}`, which is not a field of `{label}`",
                                field.name
                            ),
                        );
                        ok = false;
                        continue;
                    };
                    if set.iter().any(|(f, _)| *f == field.name)
                        || rename.iter().any(|(_, n)| *n == field.name)
                    {
                        self.diag(
                            "S050",
                            *span,
                            format!(
                                "upcast: field `{}` is both set and renamed into",
                                field.name
                            ),
                        );
                        ok = false;
                        continue;
                    }
                    let json = lower_upcast_value(schema, value, &t.ty);
                    if let Err(errs) = schema.validate_value(&t.ty, &json) {
                        let msg: Vec<String> = errs.iter().map(ToString::to_string).collect();
                        self.diag(
                            "S051",
                            value.span(),
                            format!("upcast: set `{}`: {}", field.name, msg.join("; ")),
                        );
                        ok = false;
                        continue;
                    }
                    set.push((field.name.clone(), json));
                }
            }
        }
        if !ok {
            return None;
        }
        // Every target field must be produced by something of its type.
        for t in target {
            if set.iter().any(|(f, _)| *f == t.name) {
                continue;
            }
            let source_name = rename
                .iter()
                .find(|(_, n)| *n == t.name)
                .map(|(o, _)| o.as_str())
                .or_else(|| {
                    (!rename.iter().any(|(o, _)| *o == t.name) && in_source(&t.name))
                        .then_some(t.name.as_str())
                });
            match source_name.and_then(|n| source.iter().find(|f| f.name == n)) {
                Some(sf) if sf.ty == t.ty => {}
                Some(sf) if sf.name == t.name => {
                    self.diag(
                        "S051",
                        whole,
                        format!(
                            "upcast: field `{}` is {} in `v{from}` but {} in `{label}`; set it or rename another field into it",
                            t.name, sf.ty, t.ty
                        ),
                    );
                    ok = false;
                }
                Some(sf) => {
                    self.diag(
                        "S051",
                        whole,
                        format!(
                            "upcast: `rename {} as {}`: `{}` is {}, `{}` is {}",
                            sf.name, t.name, sf.name, sf.ty, t.name, t.ty
                        ),
                    );
                    ok = false;
                }
                None if t.default.is_some() || t.ty.is_optional() => {}
                None => {
                    self.diag(
                        "S051",
                        whole,
                        format!(
                            "upcast: `{label}` requires `{}`, which `v{from}` lacks; set it or rename a v{from} field into it",
                            t.name
                        ),
                    );
                    ok = false;
                }
            }
        }
        ok.then_some(DeclarativeUpcast { set, rename })
    }

    /// Remembers every aggregate's invariants and command guards.
    fn collect_guards(&mut self, file: &ast::File) {
        let mut seen_ctx = HashSet::new();
        for ctx in &file.contexts {
            if !seen_ctx.insert(ctx.name.name.clone()) {
                continue;
            }
            let mut seen_agg = HashSet::new();
            for item in &ctx.items {
                let ast::Item::Aggregate(a) = item else {
                    continue;
                };
                if !seen_agg.insert(a.name.name.clone()) {
                    continue;
                }
                let mut seen_cmd = HashSet::new();
                let commands = a
                    .commands
                    .iter()
                    .filter(|c| seen_cmd.insert(c.name.name.clone()))
                    .map(|c| (c.name.name.clone(), c.requires.clone()))
                    .collect();
                self.pending_guards.push(PendingGuards {
                    ctx: ctx.name.name.clone(),
                    agg: a.name.name.clone(),
                    invariants: a.invariants.clone(),
                    commands,
                });
            }
        }
    }

    /// Lowers every aggregate's invariants (S031) and command guards
    /// (S053–S055) against the finished schema.
    fn resolve_guards(&mut self, schema: &mut Schema) {
        let pending = std::mem::take(&mut self.pending_guards);
        let mut lowered = Vec::new();
        for p in &pending {
            let Some(agg) = schema
                .contexts
                .get(&p.ctx)
                .and_then(|c| c.aggregates.get(&p.agg))
            else {
                continue;
            };
            let state = agg.state.clone();
            let mut invariants: IndexMap<String, StateInvariant> = IndexMap::new();
            for inv in &p.invariants {
                if invariants.contains_key(&inv.name.name) {
                    self.diag(
                        "S031",
                        inv.name.span,
                        format!(
                            "duplicate invariant `{}` in aggregate `{}`",
                            inv.name.name, p.agg
                        ),
                    );
                    continue;
                }
                let check = match &inv.check {
                    ast::InvariantCheckSyntax::Wasm(w) => InvariantCheck::Wasm(self.wasm_ref(w)),
                    ast::InvariantCheckSyntax::Expr(e) => {
                        let Some(expr) = self.lower_expr(schema, &ExprScope::Record(&state), e)
                        else {
                            continue;
                        };
                        InvariantCheck::Expr {
                            expr,
                            text: crate::fmt::expr_str(e),
                        }
                    }
                };
                invariants.insert(
                    inv.name.name.clone(),
                    StateInvariant {
                        name: inv.name.name.clone(),
                        docs: inv.docs.clone(),
                        check,
                    },
                );
            }
            let mut commands: Vec<(String, Vec<Guard>)> = Vec::new();
            for (cmd_name, rules) in &p.commands {
                let Some(cmd) = agg.commands.get(cmd_name) else {
                    continue;
                };
                let cmd_fields = cmd.fields.clone();
                let roots: [(&str, &[Field]); 2] = [("state", &state), ("command", &cmd_fields)];
                let scope = ExprScope::Rooted(&roots);
                let mut guards: Vec<Guard> = Vec::new();
                for r in rules {
                    if guards.iter().any(|g| g.name == r.name.name) {
                        self.diag(
                            "S055",
                            r.name.span,
                            format!("duplicate guard `{}` in command `{cmd_name}`", r.name.name),
                        );
                        continue;
                    }
                    if let Some(expr) = self.lower_expr(schema, &scope, &r.expr) {
                        guards.push(Guard {
                            name: r.name.name.clone(),
                            docs: r.docs.clone(),
                            expr,
                            text: crate::fmt::expr_str(&r.expr),
                        });
                    }
                }
                commands.push((cmd_name.clone(), guards));
            }
            lowered.push((p.ctx.clone(), p.agg.clone(), invariants, commands));
        }
        for (ctx, agg_name, invariants, commands) in lowered {
            let Some(agg) = schema
                .contexts
                .get_mut(&ctx)
                .and_then(|c| c.aggregates.get_mut(&agg_name))
            else {
                continue;
            };
            agg.invariants = invariants;
            for (name, guards) in commands {
                if let Some(cmd) = agg.commands.get_mut(&name) {
                    cmd.requires = guards;
                }
            }
        }
    }

    /// Lowers every value's rules against the finished schema and stores them.
    fn resolve_rules(&mut self, schema: &mut Schema) {
        let pending = std::mem::take(&mut self.pending_rules);
        let mut lowered: Vec<(String, Option<String>, String, Vec<Rule>)> = Vec::new();
        for p in &pending {
            let Some(vt) = (match &p.agg {
                None => schema
                    .contexts
                    .get(&p.ctx)
                    .and_then(|c| c.values.get(&p.value)),
                Some(agg) => schema
                    .contexts
                    .get(&p.ctx)
                    .and_then(|c| c.aggregates.get(agg))
                    .and_then(|a| a.values.get(&p.value)),
            }) else {
                continue; // a duplicate or an undeclared name already reported
            };
            let fields = vt.fields.clone();
            let mut rules: Vec<Rule> = Vec::new();
            for r in &p.rules {
                if rules.iter().any(|x| x.name == r.name.name) {
                    self.diag(
                        "S042",
                        r.name.span,
                        format!("duplicate rule `{}` in value `{}`", r.name.name, p.value),
                    );
                    continue;
                }
                if let Some(expr) = self.lower_expr(schema, &ExprScope::Record(&fields), &r.expr) {
                    rules.push(Rule {
                        name: r.name.name.clone(),
                        docs: r.docs.clone(),
                        expr,
                    });
                }
            }
            lowered.push((p.ctx.clone(), p.agg.clone(), p.value.clone(), rules));
        }
        for (ctx, agg, value, rules) in lowered {
            let Some(c) = schema.contexts.get_mut(&ctx) else {
                continue;
            };
            let slot = match agg {
                None => c.values.get_mut(&value),
                Some(a) => c
                    .aggregates
                    .get_mut(&a)
                    .and_then(|a| a.values.get_mut(&value)),
            };
            if let Some(vt) = slot {
                vt.rules = rules;
            }
        }
    }

    fn lower_expr(
        &mut self,
        schema: &Schema,
        scope: &ExprScope<'_>,
        e: &ast::Expr,
    ) -> Option<RuleExpr> {
        match e {
            ast::Expr::Or(a, b) => {
                let a = self.lower_expr(schema, scope, a);
                let b = self.lower_expr(schema, scope, b);
                Some(RuleExpr::Or(Box::new(a?), Box::new(b?)))
            }
            ast::Expr::And(a, b) => {
                let a = self.lower_expr(schema, scope, a);
                let b = self.lower_expr(schema, scope, b);
                Some(RuleExpr::And(Box::new(a?), Box::new(b?)))
            }
            ast::Expr::Not(inner) => Some(RuleExpr::Not(Box::new(
                self.lower_expr(schema, scope, inner)?,
            ))),
            ast::Expr::Exists { root, span } => match scope {
                ExprScope::Rooted(_) if root.name == "state" => Some(RuleExpr::Exists {
                    segments: vec![root.name.clone()],
                }),
                ExprScope::Rooted(_) => {
                    self.diag(
                        "S054",
                        *span,
                        format!(
                            "`exists` applies to `state` in a `requires` guard, not to `{}`",
                            root.name
                        ),
                    );
                    None
                }
                ExprScope::Record(_) => {
                    self.diag(
                        "S054",
                        *span,
                        "`exists` applies only to `state` in a command's `requires` guard",
                    );
                    None
                }
            },
            ast::Expr::Cmp { lhs, op, rhs, span } => {
                // A bare name beside an enum field is one of its variants.
                let l_enum = self.peek_enum(schema, scope, lhs);
                let r_enum = self.peek_enum(schema, scope, rhs);
                let l = self.lower_term(schema, scope, lhs, r_enum.as_ref());
                let r = self.lower_term(schema, scope, rhs, l_enum.as_ref());
                let (l, r) = (l?, r?);
                let (lk, rk) = (term_kind(&l), term_kind(&r));
                if lk != rk {
                    self.diag(
                        "S040",
                        *span,
                        format!("cannot compare {} with {}", kind_name(lk), kind_name(rk)),
                    );
                    return None;
                }
                let op = match op {
                    ast::CmpOp::Lt => RuleOp::Lt,
                    ast::CmpOp::Le => RuleOp::Le,
                    ast::CmpOp::Gt => RuleOp::Gt,
                    ast::CmpOp::Ge => RuleOp::Ge,
                    ast::CmpOp::Eq => RuleOp::Eq,
                    ast::CmpOp::Ne => RuleOp::Ne,
                };
                if !matches!(op, RuleOp::Eq | RuleOp::Ne) && lk != OperandKind::Number {
                    self.diag(
                        "S040",
                        *span,
                        format!("`{}` needs numbers, not {}", cmp_str(op), kind_name(lk)),
                    );
                    return None;
                }
                Some(RuleExpr::Cmp { lhs: l, op, rhs: r })
            }
            ast::Expr::Matches {
                path,
                pattern,
                span,
            } => {
                let (rp, _) = self.lower_path(schema, scope, path)?;
                if rp.kind != OperandKind::Text {
                    self.diag(
                        "S040",
                        *span,
                        format!("`matches` needs a string field, not {}", kind_name(rp.kind)),
                    );
                    return None;
                }
                match regex::Regex::new(&pattern.value) {
                    Ok(re) => Some(RuleExpr::Matches {
                        path: rp,
                        pattern: Pattern(re),
                    }),
                    Err(e) => {
                        self.diag(
                            "S041",
                            pattern.span,
                            format!("invalid regular expression: {e}"),
                        );
                        None
                    }
                }
            }
            ast::Expr::In { path, items, span } => {
                let (rp, enum_ref) = self.lower_path(schema, scope, path)?;
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    let t = match item {
                        ast::Literal::Variant(v) => {
                            self.variant_term(schema, v, enum_ref.as_ref(), &path_text(path))?
                        }
                        other => self.lower_literal(other)?,
                    };
                    if term_kind(&t) != rp.kind {
                        self.diag(
                            "S040",
                            *span,
                            format!(
                                "`in` list holds {}, but the field is {}",
                                kind_name(term_kind(&t)),
                                kind_name(rp.kind)
                            ),
                        );
                        return None;
                    }
                    out.push(t);
                }
                Some(RuleExpr::In {
                    path: rp,
                    items: out,
                })
            }
        }
    }

    /// The enum a term's path ends at, when it is a path to an enum field;
    /// reports nothing.
    fn peek_enum(
        &mut self,
        schema: &Schema,
        scope: &ExprScope<'_>,
        t: &ast::Term,
    ) -> Option<TypeRef> {
        let ast::Term::Path(p) = t else {
            return None;
        };
        let (_, _, _, enum_ref) = self.walk_path(schema, scope, p, true)?;
        enum_ref
    }

    /// A bare name standing for a variant of `enum_ref`; `peer` names the
    /// operand it is compared with (S053).
    fn variant_term(
        &mut self,
        schema: &Schema,
        v: &ast::Ident,
        enum_ref: Option<&TypeRef>,
        peer: &str,
    ) -> Option<RuleTerm> {
        let Some(r) = enum_ref else {
            self.diag(
                "S053",
                v.span,
                format!("`{}` names a variant, but {peer} is not an enum", v.name),
            );
            return None;
        };
        let en = schema.enum_type(r)?;
        if en.variant(&v.name).is_some() {
            Some(RuleTerm::Text(v.name.clone()))
        } else {
            self.diag(
                "S053",
                v.span,
                format!(
                    "`{}` is not a variant of enum {r} (variants: {})",
                    v.name,
                    en.variant_names().join(", ")
                ),
            );
            None
        }
    }

    /// Lowers a term; `peer_enum` is the enum the other side of a
    /// comparison ends at, which makes a bare name that is no field a
    /// variant of it.
    fn lower_term(
        &mut self,
        schema: &Schema,
        scope: &ExprScope<'_>,
        t: &ast::Term,
        peer_enum: Option<&TypeRef>,
    ) -> Option<RuleTerm> {
        match t {
            ast::Term::Lit(ast::Literal::Variant(v)) => {
                self.variant_term(schema, v, peer_enum, "the other operand")
            }
            ast::Term::Lit(l) => self.lower_literal(l),
            ast::Term::Path(p) => {
                let bare = p.segments.len() == 1 && !self.names_a_field(scope, &p.segments[0].name);
                let looks_like_variant = bare
                    && p.segments[0]
                        .name
                        .starts_with(|c: char| c.is_ascii_uppercase());
                if bare && (peer_enum.is_some() || looks_like_variant) {
                    return self.variant_term(
                        schema,
                        &p.segments[0],
                        peer_enum,
                        "the other operand",
                    );
                }
                Some(RuleTerm::Field(self.lower_path(schema, scope, p)?.0))
            }
            ast::Term::Len(p, span) => {
                let (kind, optional, segments, _) = self.walk_path(schema, scope, p, false)?;
                match kind {
                    PathKind::Collection | PathKind::Operand(OperandKind::Text) => {
                        Some(RuleTerm::Len { segments, optional })
                    }
                    _ => {
                        self.diag(
                            "S040",
                            *span,
                            "`len()` needs a string, list, set or map field".to_string(),
                        );
                        None
                    }
                }
            }
        }
    }

    /// Whether a single name is a field (record scope) or a root (rooted scope).
    fn names_a_field(&self, scope: &ExprScope<'_>, name: &str) -> bool {
        match scope {
            ExprScope::Record(fields) => fields.iter().any(|f| f.name == name),
            ExprScope::Rooted(roots) => roots.iter().any(|(r, _)| *r == name),
        }
    }

    fn lower_literal(&mut self, l: &ast::Literal) -> Option<RuleTerm> {
        match l {
            ast::Literal::Number(text, span) => match text.parse::<rust_decimal::Decimal>() {
                Ok(d) => Some(RuleTerm::Number(d)),
                Err(e) => {
                    self.diag("S040", *span, format!("bad number `{text}`: {e}"));
                    None
                }
            },
            ast::Literal::Str(s) => Some(RuleTerm::Text(s.value.clone())),
            ast::Literal::Bool(b, _) => Some(RuleTerm::Bool(*b)),
            // Variants are checked against their enum by `variant_term`.
            ast::Literal::Variant(v) => Some(RuleTerm::Text(v.name.clone())),
        }
    }

    fn lower_path(
        &mut self,
        schema: &Schema,
        scope: &ExprScope<'_>,
        p: &ast::FieldPath,
    ) -> Option<(RulePath, Option<TypeRef>)> {
        let (kind, optional, segments, enum_ref) = self.walk_path(schema, scope, p, false)?;
        match kind {
            PathKind::Operand(kind) => Some((
                RulePath {
                    segments,
                    kind,
                    optional,
                },
                enum_ref,
            )),
            PathKind::Collection => {
                self.diag(
                    "S040",
                    p.span,
                    "a collection field can only be used through `len()`".to_string(),
                );
                None
            }
            PathKind::Record => {
                self.diag(
                    "S040",
                    p.span,
                    "a value or entity field cannot be compared as a whole; name one of its fields"
                        .to_string(),
                );
                None
            }
        }
    }

    /// Walks `p` from the scope's fields (or from one of its roots),
    /// descending through nested values; returns the path's kind, whether
    /// any step is optional, the segments, and the enum it ends at. `quiet`
    /// suppresses the diagnostics (a probe).
    fn walk_path(
        &mut self,
        schema: &Schema,
        scope: &ExprScope<'_>,
        p: &ast::FieldPath,
        quiet: bool,
    ) -> Option<(PathKind, bool, Vec<String>, Option<TypeRef>)> {
        let mut segments = Vec::with_capacity(p.segments.len());
        let (mut current, rest): (Vec<Field>, &[ast::Ident]) = match scope {
            ExprScope::Record(fields) => (fields.to_vec(), &p.segments),
            ExprScope::Rooted(roots) => {
                let first = &p.segments[0];
                let Some((_, fields)) = roots.iter().find(|(r, _)| *r == first.name) else {
                    if !quiet {
                        let names: Vec<String> =
                            roots.iter().map(|(r, _)| format!("`{r}.`")).collect();
                        self.diag(
                            "S054",
                            first.span,
                            format!(
                                "a `requires` path starts with {}, not `{}`",
                                names.join(" or "),
                                first.name
                            ),
                        );
                    }
                    return None;
                };
                if p.segments.len() == 1 {
                    if !quiet {
                        let hint = if first.name == "state" {
                            ", or write `state exists`"
                        } else {
                            ""
                        };
                        self.diag(
                            "S054",
                            p.span,
                            format!(
                                "`{}` alone cannot be compared; name one of its fields{hint}",
                                first.name
                            ),
                        );
                    }
                    return None;
                }
                segments.push(first.name.clone());
                (fields.to_vec(), &p.segments[1..])
            }
        };
        let mut optional = false;
        let last = rest.len() - 1;
        for (i, seg) in rest.iter().enumerate() {
            let Some(field) = current.iter().find(|f| f.name == seg.name) else {
                if !quiet {
                    self.diag(
                        "S039",
                        seg.span,
                        format!("rule path names no field `{}`", seg.name),
                    );
                }
                return None;
            };
            segments.push(seg.name.clone());
            let mut ty = &field.ty;
            if let Type::Optional(inner) = ty {
                optional = true;
                ty = inner;
            }
            if i == last {
                let (kind, enum_ref) = match ty {
                    Type::Scalar(Scalar::Int | Scalar::Uint | Scalar::Decimal) => {
                        (PathKind::Operand(OperandKind::Number), None)
                    }
                    Type::Scalar(Scalar::Bool) => (PathKind::Operand(OperandKind::Bool), None),
                    Type::Scalar(_) => (PathKind::Operand(OperandKind::Text), None),
                    Type::Enum(r) => (PathKind::Operand(OperandKind::Text), Some(r.clone())),
                    Type::List(_) | Type::Set(_) | Type::Map(_, _) => (PathKind::Collection, None),
                    Type::Value(_) | Type::Entity(_) => (PathKind::Record, None),
                    Type::Optional(_) => unreachable!("unwrapped above"),
                };
                return Some((kind, optional, segments, enum_ref));
            }
            match ty {
                Type::Value(r) => match schema.value_type(r) {
                    Some(vt) => current = vt.fields.clone(),
                    None => {
                        if !quiet {
                            self.diag("S039", seg.span, format!("rule path: unknown value {r}"));
                        }
                        return None;
                    }
                },
                other => {
                    if !quiet {
                        self.diag(
                            "S039",
                            seg.span,
                            format!(
                                "rule path cannot descend into `{}` of type {other}; only nested values can be entered",
                                seg.name
                            ),
                        );
                    }
                    return None;
                }
            }
        }
        unreachable!("a path has at least one segment")
    }

    /// S038 for every recorded process source, against the lowered schema.
    fn check_processes(&mut self, schema: &Schema) {
        let checks = std::mem::take(&mut self.process_checks);
        for c in checks {
            let Some(family) = schema.event_family(&c.context, &c.event) else {
                continue; // S037 already reported
            };
            for (version, ty) in &family.versions {
                match ty.fields.iter().find(|f| f.name == c.by) {
                    Some(f) if f.ty == Type::Scalar(c.key) => {}
                    Some(f) => self.diag(
                        "S038",
                        c.span,
                        format!(
                            "event `{}.{} v{version}` field `{}` has type {}, but process `{}` keys by {}",
                            c.context, c.event, c.by, f.ty, c.process, c.key
                        ),
                    ),
                    None => self.diag(
                        "S038",
                        c.span,
                        format!(
                            "event `{}.{} v{version}` has no field `{}` to correlate process `{}` by",
                            c.context, c.event, c.by, c.process
                        ),
                    ),
                }
            }
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
        let snapshot_every = match &p.snapshot_every {
            Some(lit) => self
                .int_in_range(lit, u64::from(u32::MAX), "snapshot every")
                .map_or(0, |v| v as u32),
            None => 0,
        };
        Projection {
            name: p.name.name.clone(),
            docs: p.docs.clone(),
            from,
            fold,
            snapshot_every,
            tables,
        }
    }

    fn table(&mut self, ctx_name: &str, t: &ast::TableDecl) -> Table {
        let scope = Scope {
            ctx: ctx_name,
            agg: None,
            place: Place::Table,
        };
        let mut all: Vec<ast::Field> = Vec::with_capacity(t.fields.len());
        for tf in &t.fields {
            let mut f = tf.field.clone();
            if tf.key && self.no_default_on_key(&tf.field, "table key") {
                f.default = None;
            }
            all.push(f);
        }
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
            docs: t.docs.clone(),
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
                Place::ContextEnum => {
                    self.diag(
                        "S015",
                        span,
                        format!(
                            "a context-level enum payload may not contain the entity `{}`; declare the enum inside aggregate `{owner}`",
                            tr.name
                        ),
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
                Place::Entity { agg }
                | Place::State { agg }
                | Place::Command { agg }
                | Place::LocalEnum { agg } => agg == owner,
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
            Place::ContextValue | Place::ContextEnum | Place::Table => false,
            Place::LocalValue { agg }
            | Place::LocalEnum { agg }
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
            for e in ctx.enums.values().filter(|e| e.has_payloads()) {
                nodes.push((
                    TypeRef::new(&ctx.name, None, &e.name),
                    enum_edges(schema, e),
                ));
            }
            for agg in ctx.aggregates.values() {
                for v in agg.values.values() {
                    nodes.push((
                        TypeRef::new(&ctx.name, Some(agg.name.clone()), &v.name),
                        record_edges(schema, &v.fields),
                    ));
                }
                for e in agg.enums.values().filter(|e| e.has_payloads()) {
                    nodes.push((
                        TypeRef::new(&ctx.name, Some(agg.name.clone()), &e.name),
                        enum_edges(schema, e),
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

/// The values, entities and payload-carrying enums referenced by a
/// record's fields (a unit-only enum is a leaf and has no node).
fn record_edges(schema: &Schema, fields: &[Field]) -> Vec<TypeRef> {
    let mut out = Vec::new();
    for f in fields {
        for r in f.ty.refs() {
            if schema.enum_type(r).is_some_and(|e| !e.has_payloads()) {
                continue;
            }
            if !out.contains(r) {
                out.push(r.clone());
            }
        }
    }
    out
}

fn enum_edges(schema: &Schema, e: &EnumType) -> Vec<TypeRef> {
    let mut out = Vec::new();
    for v in &e.variants {
        if let Some(fields) = &v.payload {
            for r in record_edges(schema, fields) {
                if !out.contains(&r) {
                    out.push(r);
                }
            }
        }
    }
    out
}

/// The variants of an enum declaration, for the index: name and whether
/// it carries a payload.
fn variant_list(e: &ast::EnumDecl) -> VariantList {
    e.variants
        .iter()
        .map(|v| (v.name.name.clone(), v.payload.is_some()))
        .collect()
}

/// A version that only adds optional or defaulted fields, and carries every
/// field of its predecessor unchanged, needs no written upcast.
fn implicit_upcast_ok(source: &[Field], target: &[Field]) -> bool {
    target
        .iter()
        .all(|t| match source.iter().find(|s| s.name == t.name) {
            Some(s) => s.ty == t.ty,
            None => t.default.is_some() || t.ty.is_optional(),
        })
}

/// The span the ops of an upcast cover (for a diagnostic about the whole),
/// or the clause's span when it has none.
fn upcast_span(ops: &[ast::UpcastOp], clause: Span) -> Span {
    match (ops.first(), ops.last()) {
        (Some(a), Some(b)) => a.span().join(b.span()),
        _ => clause,
    }
}

/// A `set` value as JSON, shaped by the field's type where the syntax is
/// ambiguous (a number for a decimal is a string in JSON); anything that
/// does not line up is lowered as written and refused by validation.
fn lower_upcast_value(schema: &Schema, v: &ast::UpcastValue, ty: &Type) -> Value {
    match v {
        ast::UpcastValue::Null(_) => Value::Null,
        ast::UpcastValue::Lit(l) => match l {
            ast::Literal::Number(text, _) => {
                if matches!(ty.required(), Type::Scalar(Scalar::Decimal)) {
                    Value::String(text.clone())
                } else if let Ok(i) = text.parse::<i64>() {
                    Value::from(i)
                } else if let Ok(u) = text.parse::<u64>() {
                    Value::from(u)
                } else {
                    Value::String(text.clone())
                }
            }
            ast::Literal::Str(s) => Value::String(s.value.clone()),
            ast::Literal::Bool(b, _) => Value::Bool(*b),
            ast::Literal::Variant(i) => Value::String(i.name.clone()),
        },
        ast::UpcastValue::List(items, _) => {
            let elem: Option<Type> = match ty.required() {
                Type::List(t) => Some((**t).clone()),
                Type::Set(sc) => Some(Type::Scalar(*sc)),
                _ => None,
            };
            Value::Array(
                items
                    .iter()
                    .map(|i| match &elem {
                        Some(t) => lower_upcast_value(schema, i, t),
                        None => lower_upcast_value(schema, i, &Type::Scalar(Scalar::String)),
                    })
                    .collect(),
            )
        }
        ast::UpcastValue::Object(entries, _) => {
            let fields: Option<Vec<Field>> = match ty.required() {
                Type::Value(r) => schema.value_type(r).map(|v| v.fields.clone()),
                Type::Entity(r) => schema.entity(r).map(|e| e.fields.clone()),
                _ => None,
            };
            let map_value: Option<Type> = match ty.required() {
                Type::Map(_, t) => Some((**t).clone()),
                _ => None,
            };
            let mut out = serde_json::Map::new();
            for (k, val) in entries {
                let field_ty = fields
                    .as_ref()
                    .and_then(|fs| fs.iter().find(|f| f.name == k.name).map(|f| f.ty.clone()))
                    .or_else(|| map_value.clone())
                    .unwrap_or(Type::Scalar(Scalar::String));
                out.insert(k.name.clone(), lower_upcast_value(schema, val, &field_ty));
            }
            Value::Object(out)
        }
    }
}

/// A path as written, for messages.
fn path_text(p: &ast::FieldPath) -> String {
    p.segments
        .iter()
        .map(|s| s.name.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

fn term_kind(t: &RuleTerm) -> OperandKind {
    match t {
        RuleTerm::Number(_) | RuleTerm::Len { .. } => OperandKind::Number,
        RuleTerm::Text(_) => OperandKind::Text,
        RuleTerm::Bool(_) => OperandKind::Bool,
        RuleTerm::Field(p) => p.kind,
    }
}

fn kind_name(k: OperandKind) -> &'static str {
    match k {
        OperandKind::Number => "a number",
        OperandKind::Text => "text",
        OperandKind::Bool => "a boolean",
    }
}

fn cmp_str(op: RuleOp) -> &'static str {
    match op {
        RuleOp::Lt => "<",
        RuleOp::Le => "<=",
        RuleOp::Gt => ">",
        RuleOp::Ge => ">=",
        RuleOp::Eq => "==",
        RuleOp::Ne => "!=",
    }
}
