//! What changed between two compiled schemas, and what the daemon must do
//! about it.
//!
//! The principle: data already in the log must still validate against the
//! new definition, else the change is [`Compatibility::Breaking`]; derived
//! data (read models, snapshots) is rebuilt
//! ([`Compatibility::NeedsRebuild`]) and removed things are cleaned up. Facts
//! about the log ([`Facts`]) decide whether a removal breaks anything;
//! offline, [`AssumeData`] assumes the log holds everything the old schema
//! could have written.
//!
//! The diff is model-based and destructures every model struct exhaustively,
//! so a new model field fails to compile here until it is classified.
//! Doc comments, declaration order and anything else that leaves the model
//! unchanged are not changes.

use std::collections::BTreeSet;
use std::fmt;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::ast::Layer;
use crate::model::*;
use crate::types::Type;

/// How a change bears on data the daemon already holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compatibility {
    /// Nothing stored is affected.
    Compatible,
    /// Derived data must be rebuilt from the log; the `Action` says what.
    NeedsRebuild,
    /// Stored data would no longer fit the schema; refused without force.
    Breaking,
}

impl fmt::Display for Compatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Compatibility::Compatible => "compatible",
            Compatibility::NeedsRebuild => "rebuild",
            Compatibility::Breaking => "breaking",
        })
    }
}

/// What the daemon does when it adopts the new schema.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Action {
    None,
    /// Drop the projection's checkpoint, tables and snapshots; it refills
    /// from the start of the log.
    RebuildProjection {
        context: String,
        name: String,
    },
    /// A projection that no longer exists: drop everything it owned.
    DropProjection {
        context: String,
        name: String,
        tables: Vec<String>,
    },
    /// One table of a projection that still exists.
    DropTable {
        context: String,
        projection: String,
        table: String,
    },
    /// The aggregate's state shape changed: its instance snapshots are
    /// stale and are dropped; streams replay from their events.
    ClearAggregateSnapshots {
        context: String,
        name: String,
    },
    /// An aggregate that no longer exists: drop its snapshots and locks.
    DropAggregate {
        context: String,
        name: String,
    },
}

impl Action {
    /// The layer whose node carries the action out. `None` has no layer and
    /// is reported as the domain's.
    pub fn layer(&self) -> Layer {
        match self {
            Action::None => Layer::Domain,
            Action::RebuildProjection { .. }
            | Action::DropProjection { .. }
            | Action::DropTable { .. }
            | Action::ClearAggregateSnapshots { .. }
            | Action::DropAggregate { .. } => Layer::Derivation,
        }
    }
}

/// The kind of change, for machines; `Change::description` is for people.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    ContextAdded,
    ContextRemoved,
    ValueAdded,
    ValueRemoved,
    EnumAdded,
    EnumRemoved,
    VariantAdded,
    VariantRemoved,
    FieldAdded,
    FieldRemoved,
    FieldTypeChanged,
    FieldDefaultChanged,
    RulesChanged,
    EventFamilyAdded,
    EventFamilyRemoved,
    EventVersionAdded,
    EventVersionRemoved,
    UpcastChanged,
    EntityAdded,
    EntityRemoved,
    EntityIdChanged,
    AggregateAdded,
    AggregateRemoved,
    AggregateKeyChanged,
    AggregateStreamChanged,
    AggregateEventsChanged,
    StateAdded,
    StateRemoved,
    AggregateStateChanged,
    WasmChanged,
    SnapshotEveryChanged,
    ProjectionAdded,
    ProjectionRemoved,
    ProjectionSourcesChanged,
    TableAdded,
    TableRemoved,
    TableKeyChanged,
    ColumnAdded,
    ColumnRemoved,
    ColumnTypeChanged,
}

/// One classified change.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    pub kind: ChangeKind,
    /// Where, as `Context.Thing.part` (an event version as `Ctx.E@v2`).
    pub path: String,
    pub description: String,
    pub compatibility: Compatibility,
    pub action: Action,
}

/// Every change between two schemas, ordered by path then kind.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaDiff {
    pub changes: Vec<Change>,
}

impl SchemaDiff {
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn has_breaking(&self) -> bool {
        self.worst() == Some(Compatibility::Breaking)
    }

    /// The most severe compatibility among the changes.
    pub fn worst(&self) -> Option<Compatibility> {
        self.changes.iter().map(|c| c.compatibility).max()
    }

    /// The actions to take, each once, in a stable order.
    pub fn actions(&self) -> Vec<Action> {
        let set: BTreeSet<&Action> = self
            .changes
            .iter()
            .map(|c| &c.action)
            .filter(|a| **a != Action::None)
            .collect();
        set.into_iter().cloned().collect()
    }

    /// The actions one layer's node carries out, each once, in a stable
    /// order.
    pub fn actions_for(&self, layer: Layer) -> Vec<Action> {
        self.actions()
            .into_iter()
            .filter(|a| a.layer() == layer)
            .collect()
    }

    /// `N change(s): a breaking, b rebuild, c compatible`.
    pub fn summary(&self) -> String {
        if self.changes.is_empty() {
            return "no changes".to_string();
        }
        let count = |c: Compatibility| self.changes.iter().filter(|x| x.compatibility == c).count();
        format!(
            "{} change(s): {} breaking, {} rebuild, {} compatible",
            self.changes.len(),
            count(Compatibility::Breaking),
            count(Compatibility::NeedsRebuild),
            count(Compatibility::Compatible)
        )
    }

    pub fn breaking(&self) -> impl Iterator<Item = &Change> {
        self.changes
            .iter()
            .filter(|c| c.compatibility == Compatibility::Breaking)
    }
}

impl fmt::Display for SchemaDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for c in &self.changes {
            writeln!(f, "[{}] {}: {}", c.compatibility, c.path, c.description)?;
        }
        write!(f, "{}", self.summary())
    }
}

/// What the log holds, for the removals whose cost depends on it.
pub trait Facts {
    /// Whether the log holds an event of `family`, of `version` when given.
    fn has_events(&self, family: &EventFamilyRef, version: Option<u16>) -> bool;
    /// Whether the log holds a stream of the aggregate.
    fn has_streams(&self, context: &str, aggregate: &str) -> bool;
}

/// Assumes the log holds everything the old schema could have written.
pub struct AssumeData;

impl Facts for AssumeData {
    fn has_events(&self, _: &EventFamilyRef, _: Option<u16>) -> bool {
        true
    }
    fn has_streams(&self, _: &str, _: &str) -> bool {
        true
    }
}

/// The diff from `old` to `new` derivation schemas, assuming the log
/// holds data of everything in `old`.
pub fn diff(old: &DerivationSchema, new: &DerivationSchema) -> SchemaDiff {
    diff_derivation(old, new, &AssumeData)
}

/// The diff from `old` to `new` derivation schemas given what the log
/// actually holds.
pub fn diff_with(old: &DerivationSchema, new: &DerivationSchema, facts: &dyn Facts) -> SchemaDiff {
    diff_derivation(old, new, facts)
}

/// The domain layer's changes: contexts, types, events and aggregate
/// identities. What the database checks.
pub fn diff_domain(old: &DomainSchema, new: &DomainSchema, facts: &dyn Facts) -> SchemaDiff {
    let mut d = Differ {
        facts,
        changes: Vec::new(),
    };
    d.domain(old, new);
    d.finish()
}

/// The derivation layer's changes: the domain's plus aggregate states and
/// projections. What a derivation node checks.
pub fn diff_derivation(
    old: &DerivationSchema,
    new: &DerivationSchema,
    facts: &dyn Facts,
) -> SchemaDiff {
    let mut d = Differ {
        facts,
        changes: Vec::new(),
    };
    d.domain(old, new);
    d.derivation(old, new);
    d.finish()
}

fn empty_context(name: &str) -> Context {
    Context {
        name: name.to_string(),
        docs: Vec::new(),
        values: IndexMap::new(),
        enums: IndexMap::new(),
        events: IndexMap::new(),
        aggregates: IndexMap::new(),
    }
}

struct Differ<'a> {
    facts: &'a dyn Facts,
    changes: Vec<Change>,
}

impl Differ<'_> {
    fn finish(mut self) -> SchemaDiff {
        self.changes
            .sort_by(|a, b| (&a.path, a.kind).cmp(&(&b.path, b.kind)));
        SchemaDiff {
            changes: self.changes,
        }
    }

    fn domain(&mut self, old: &DomainSchema, new: &DomainSchema) {
        // `docs` are text and `dir` (private) is where the schema was loaded
        // from; neither is part of the model.
        let old_contexts = &old.contexts;
        let new_contexts = &new.contexts;
        for (name, o) in old_contexts {
            match new_contexts.get(name) {
                Some(n) => self.context(o, n),
                None => {
                    self.push(
                        ChangeKind::ContextRemoved,
                        name.clone(),
                        format!("context `{name}` removed"),
                        Compatibility::Compatible,
                        Action::None,
                    );
                    self.context(o, &empty_context(name));
                }
            }
        }
        for (name, n) in new_contexts {
            if !old_contexts.contains_key(name) {
                self.push(
                    ChangeKind::ContextAdded,
                    name.clone(),
                    format!("context `{name}` added"),
                    Compatibility::Compatible,
                    Action::None,
                );
                self.context(&empty_context(name), n);
            }
        }
    }

    fn derivation(&mut self, old: &DerivationSchema, new: &DerivationSchema) {
        let DerivationSchema {
            domain: _,
            states: o_states,
            projections: o_projs,
            ..
        } = old;
        let DerivationSchema {
            domain: _,
            states: n_states,
            projections: n_projs,
            ..
        } = new;
        for (agg, os) in o_states {
            match n_states.get(agg) {
                Some(ns) => self.state(os, ns),
                None => self.push(
                    ChangeKind::StateRemoved,
                    format!("{agg}.state"),
                    format!("state of `{agg}` removed; its instance snapshots are dropped"),
                    Compatibility::Compatible,
                    Action::ClearAggregateSnapshots {
                        context: agg.context.clone(),
                        name: agg.name.clone(),
                    },
                ),
            }
        }
        for agg in n_states.keys() {
            if !o_states.contains_key(agg) {
                self.note(
                    ChangeKind::StateAdded,
                    format!("{agg}.state"),
                    format!("state of `{agg}` added; streams fold from their events"),
                );
            }
        }
        for (key, op) in o_projs {
            match n_projs.get(key) {
                Some(np) => self.projection(&key.context, op, np),
                None => self.push(
                    ChangeKind::ProjectionRemoved,
                    key.to_string(),
                    format!("projection `{}` removed; its tables are dropped", key.name),
                    Compatibility::Compatible,
                    Action::DropProjection {
                        context: key.context.clone(),
                        name: key.name.clone(),
                        tables: op.tables.keys().cloned().collect(),
                    },
                ),
            }
        }
        for key in n_projs.keys() {
            if !o_projs.contains_key(key) {
                self.note(
                    ChangeKind::ProjectionAdded,
                    key.to_string(),
                    format!(
                        "projection `{}` added; it fills from the start of the log",
                        key.name
                    ),
                );
            }
        }
    }

    fn push(
        &mut self,
        kind: ChangeKind,
        path: String,
        description: String,
        compatibility: Compatibility,
        action: Action,
    ) {
        self.changes.push(Change {
            kind,
            path,
            description,
            compatibility,
            action,
        });
    }

    fn note(&mut self, kind: ChangeKind, path: String, description: String) {
        self.push(
            kind,
            path,
            description,
            Compatibility::Compatible,
            Action::None,
        );
    }

    fn context(&mut self, o: &Context, n: &Context) {
        let Context {
            name: ctx,
            docs: _,
            values: o_values,
            enums: o_enums,
            events: o_events,
            aggregates: o_aggs,
        } = o;
        let Context {
            name: _,
            docs: _,
            values: n_values,
            enums: n_enums,
            events: n_events,
            aggregates: n_aggs,
        } = n;
        self.values(ctx, o_values, n_values);
        self.enums(ctx, o_enums, n_enums);
        self.events(ctx, o_events, n_events);
        for (name, oa) in o_aggs {
            let path = format!("{ctx}.{name}");
            match n_aggs.get(name) {
                Some(na) => self.aggregate(ctx, oa, na),
                None => {
                    let has = self.facts.has_streams(ctx, name);
                    self.push(
                        ChangeKind::AggregateRemoved,
                        path,
                        if has {
                            format!("aggregate `{name}` removed, but the log holds its streams")
                        } else {
                            format!("aggregate `{name}` removed (no streams in the log)")
                        },
                        if has {
                            Compatibility::Breaking
                        } else {
                            Compatibility::Compatible
                        },
                        Action::DropAggregate {
                            context: ctx.clone(),
                            name: name.clone(),
                        },
                    );
                }
            }
        }
        for name in n_aggs.keys() {
            if !o_aggs.contains_key(name) {
                self.note(
                    ChangeKind::AggregateAdded,
                    format!("{ctx}.{name}"),
                    format!("aggregate `{name}` added"),
                );
            }
        }
    }

    fn values(
        &mut self,
        prefix: &str,
        o: &IndexMap<String, ValueType>,
        n: &IndexMap<String, ValueType>,
    ) {
        for (name, ov) in o {
            let path = format!("{prefix}.{name}");
            match n.get(name) {
                Some(nv) => self.value(&path, ov, nv),
                None => self.note(
                    ChangeKind::ValueRemoved,
                    path,
                    format!("value `{name}` removed"),
                ),
            }
        }
        for name in n.keys() {
            if !o.contains_key(name) {
                self.note(
                    ChangeKind::ValueAdded,
                    format!("{prefix}.{name}"),
                    format!("value `{name}` added"),
                );
            }
        }
    }

    fn value(&mut self, path: &str, o: &ValueType, n: &ValueType) {
        let ValueType {
            name: _,
            docs: _,
            fields: of,
            rules: or,
        } = o;
        let ValueType {
            name: _,
            docs: _,
            fields: nf,
            rules: nr,
        } = n;
        self.fields(path, of, nf);
        if or != nr {
            self.note(
                ChangeKind::RulesChanged,
                path.to_string(),
                "rules changed; they apply to new writes only".to_string(),
            );
        }
    }

    fn enums(
        &mut self,
        prefix: &str,
        o: &IndexMap<String, EnumType>,
        n: &IndexMap<String, EnumType>,
    ) {
        for (name, oe) in o {
            let path = format!("{prefix}.{name}");
            match n.get(name) {
                Some(ne) => self.enum_type(&path, oe, ne),
                None => self.note(
                    ChangeKind::EnumRemoved,
                    path,
                    format!("enum `{name}` removed"),
                ),
            }
        }
        for name in n.keys() {
            if !o.contains_key(name) {
                self.note(
                    ChangeKind::EnumAdded,
                    format!("{prefix}.{name}"),
                    format!("enum `{name}` added"),
                );
            }
        }
    }

    fn enum_type(&mut self, path: &str, o: &EnumType, n: &EnumType) {
        let EnumType {
            name: _,
            docs: _,
            variants: ov,
        } = o;
        let EnumType {
            name: _,
            docs: _,
            variants: nv,
        } = n;
        for v in ov {
            let EnumVariant {
                name,
                docs: _,
                payload: op,
            } = v;
            let vpath = format!("{path}.{name}");
            match nv.iter().find(|x| x.name == *name) {
                None => self.push(
                    ChangeKind::VariantRemoved,
                    vpath,
                    format!("variant `{name}` removed; stored records may hold it"),
                    Compatibility::Breaking,
                    Action::None,
                ),
                Some(x) => match (op, &x.payload) {
                    (None, None) => {}
                    (Some(of), Some(nf)) => self.fields(&vpath, of, nf),
                    (None, Some(nf)) => {
                        // A unit variant now carries a payload: stored
                        // `"Name"` strings no longer fit, unless every
                        // payload field is optional or defaulted.
                        let empty = Vec::new();
                        self.fields(&vpath, &empty, nf);
                    }
                    (Some(_), None) => self.push(
                        ChangeKind::FieldRemoved,
                        vpath,
                        format!("variant `{name}` lost its payload; stored records carry one"),
                        Compatibility::Breaking,
                        Action::None,
                    ),
                },
            }
        }
        for v in nv {
            if !ov.iter().any(|x| x.name == v.name) {
                self.note(
                    ChangeKind::VariantAdded,
                    format!("{path}.{}", v.name),
                    format!("variant `{}` added", v.name),
                );
            }
        }
    }

    /// Field-by-field comparison of a stored record type (events, values,
    /// entities, enum payloads: in the log for good).
    fn fields(&mut self, path: &str, o: &[Field], n: &[Field]) {
        for of in o {
            let Field {
                name,
                ty: oty,
                docs: _,
                default: od,
            } = of;
            let fpath = format!("{path}.{name}");
            match n.iter().find(|f| f.name == *name) {
                None => {
                    let (compat, desc) = if oty.is_optional() {
                        (
                            Compatibility::Compatible,
                            format!(
                                "optional field `{name}` removed; stored records may still carry it"
                            ),
                        )
                    } else {
                        (
                            Compatibility::Breaking,
                            format!("required field `{name}` removed; stored records carry it"),
                        )
                    };
                    self.push(ChangeKind::FieldRemoved, fpath, desc, compat, Action::None);
                }
                Some(nf) => {
                    let Field {
                        name: _,
                        ty: nty,
                        docs: _,
                        default: nd,
                    } = nf;
                    if oty != nty {
                        let widened = matches!(nty, Type::Optional(inner) if **inner == *oty);
                        let (compat, desc) = if widened {
                            (
                                Compatibility::Compatible,
                                format!("field `{name}` became optional ({oty} to {nty})"),
                            )
                        } else {
                            (
                                Compatibility::Breaking,
                                format!(
                                    "field `{name}` changed from {oty} to {nty}; stored records hold the old type"
                                ),
                            )
                        };
                        self.push(
                            ChangeKind::FieldTypeChanged,
                            fpath,
                            desc,
                            compat,
                            Action::None,
                        );
                    } else if od != nd {
                        self.note(
                            ChangeKind::FieldDefaultChanged,
                            fpath,
                            format!(
                                "default of `{name}` changed from {} to {}",
                                show_default(od),
                                show_default(nd)
                            ),
                        );
                    }
                }
            }
        }
        for nf in n {
            if o.iter().any(|f| f.name == nf.name) {
                continue;
            }
            let name = &nf.name;
            let fpath = format!("{path}.{name}");
            let (compat, desc) = if nf.ty.is_optional() || nf.default.is_some() {
                (
                    Compatibility::Compatible,
                    format!(
                        "field `{name}` added ({})",
                        if nf.ty.is_optional() {
                            "optional"
                        } else {
                            "with a default"
                        }
                    ),
                )
            } else {
                (
                    Compatibility::Breaking,
                    format!(
                        "required field `{name}` added without a default; stored records lack it"
                    ),
                )
            };
            self.push(ChangeKind::FieldAdded, fpath, desc, compat, Action::None);
        }
    }

    fn events(
        &mut self,
        ctx: &str,
        o: &IndexMap<String, EventFamily>,
        n: &IndexMap<String, EventFamily>,
    ) {
        for (name, of) in o {
            let path = format!("{ctx}.{name}");
            match n.get(name) {
                Some(nf) => self.event_family(&path, of, nf),
                None => {
                    let fam = of.as_ref();
                    let has = self.facts.has_events(&fam, None);
                    self.push(
                        ChangeKind::EventFamilyRemoved,
                        path,
                        if has {
                            format!("event `{name}` removed, but the log holds its events")
                        } else {
                            format!("event `{name}` removed (none in the log)")
                        },
                        if has {
                            Compatibility::Breaking
                        } else {
                            Compatibility::Compatible
                        },
                        Action::None,
                    );
                }
            }
        }
        for name in n.keys() {
            if !o.contains_key(name) {
                self.note(
                    ChangeKind::EventFamilyAdded,
                    format!("{ctx}.{name}"),
                    format!("event `{name}` added"),
                );
            }
        }
    }

    fn event_family(&mut self, path: &str, o: &EventFamily, n: &EventFamily) {
        let EventFamily {
            context: _,
            name: _,
            versions: ov,
        } = o;
        let EventFamily {
            context: _,
            name: _,
            versions: nv,
        } = n;
        let fam = o.as_ref();
        for (version, ot) in ov {
            let vpath = format!("{path}@v{version}");
            match nv.get(version) {
                Some(nt) => self.event_type(&vpath, &fam, ot, nt),
                None => {
                    let has = self.facts.has_events(&fam, Some(*version));
                    self.push(
                        ChangeKind::EventVersionRemoved,
                        vpath,
                        if has {
                            format!("version {version} removed, but the log holds events of it")
                        } else {
                            format!("version {version} removed (none in the log)")
                        },
                        if has {
                            Compatibility::Breaking
                        } else {
                            Compatibility::Compatible
                        },
                        Action::None,
                    );
                }
            }
        }
        for (version, nt) in nv {
            if ov.contains_key(version) {
                continue;
            }
            let desc = match &nt.upcast {
                Some(u) => format!(
                    "version {version} added, upcast from v{}; consumers see it",
                    u.from
                ),
                None if *version > o.latest().id.version => format!(
                    "version {version} added without an upcast; the schema would not compile"
                ),
                None => format!("version {version} added"),
            };
            self.note(
                ChangeKind::EventVersionAdded,
                format!("{path}@v{version}"),
                desc,
            );
        }
    }

    fn event_type(&mut self, path: &str, fam: &EventFamilyRef, o: &EventType, n: &EventType) {
        let EventType {
            id: _,
            docs: _,
            fields: of,
            upcast: ou,
        } = o;
        let EventType {
            id: _,
            docs: _,
            fields: nf,
            upcast: nu,
        } = n;
        self.fields(path, of, nf);
        if ou != nu {
            let from = ou.as_ref().or(nu.as_ref()).map(|u| u.from);
            let has = from.is_some_and(|v| self.facts.has_events(fam, Some(v)));
            self.push(
                ChangeKind::UpcastChanged,
                path.to_string(),
                if has {
                    format!(
                        "upcast changed, but the log holds v{} events; history would be reinterpreted",
                        from.unwrap_or_default()
                    )
                } else {
                    "upcast changed (no events of the source version in the log)".to_string()
                },
                if has {
                    Compatibility::Breaking
                } else {
                    Compatibility::Compatible
                },
                Action::None,
            );
        }
    }

    fn aggregate(&mut self, ctx: &str, o: &Aggregate, n: &Aggregate) {
        let Aggregate {
            name,
            docs: _,
            key: ok,
            stream: os,
            values: ov,
            enums: oe,
            entities: oen,
            events: oev,
        } = o;
        let Aggregate {
            name: _,
            docs: _,
            key: nk,
            stream: ns,
            values: nv,
            enums: ne,
            entities: nen,
            events: nev,
        } = n;
        let path = format!("{ctx}.{name}");
        if ok.name != nk.name || ok.ty != nk.ty {
            self.push(
                ChangeKind::AggregateKeyChanged,
                format!("{path}.key"),
                format!(
                    "key changed from `{}: {}` to `{}: {}`; streams are keyed by it",
                    ok.name, ok.ty, nk.name, nk.ty
                ),
                Compatibility::Breaking,
                Action::None,
            );
        }
        if os != ns {
            self.push(
                ChangeKind::AggregateStreamChanged,
                format!("{path}.stream"),
                format!("stream changed from `{os}` to `{ns}`; existing streams keep the old name"),
                Compatibility::Breaking,
                Action::None,
            );
        }
        self.values(&path, ov, nv);
        self.enums(&path, oe, ne);
        for (ename, oent) in oen {
            let epath = format!("{path}.{ename}");
            match nen.get(ename) {
                Some(nent) => {
                    let Entity {
                        name: _,
                        docs: _,
                        id: oid,
                        fields: of,
                    } = oent;
                    let Entity {
                        name: _,
                        docs: _,
                        id: nid,
                        fields: nf,
                    } = nent;
                    if oid.name != nid.name || oid.ty != nid.ty {
                        self.push(
                            ChangeKind::EntityIdChanged,
                            format!("{epath}.id"),
                            format!(
                                "id changed from `{}: {}` to `{}: {}`",
                                oid.name, oid.ty, nid.name, nid.ty
                            ),
                            Compatibility::Breaking,
                            Action::None,
                        );
                    }
                    // The id is also the first field; it is reported above.
                    let of: Vec<Field> =
                        of.iter().filter(|f| f.name != oid.name).cloned().collect();
                    let nf: Vec<Field> =
                        nf.iter().filter(|f| f.name != nid.name).cloned().collect();
                    self.fields(&epath, &of, &nf);
                }
                None => self.note(
                    ChangeKind::EntityRemoved,
                    epath,
                    format!("entity `{ename}` removed"),
                ),
            }
        }
        for ename in nen.keys() {
            if !oen.contains_key(ename) {
                self.note(
                    ChangeKind::EntityAdded,
                    format!("{path}.{ename}"),
                    format!("entity `{ename}` added"),
                );
            }
        }
        for fam in oev {
            if !nev.contains(fam) {
                let has = self.facts.has_events(fam, None);
                self.push(
                    ChangeKind::AggregateEventsChanged,
                    format!("{path}.events"),
                    if has {
                        format!(
                            "no longer lists `{fam}`, but the log holds its events in these streams"
                        )
                    } else {
                        format!("no longer lists `{fam}` (none in the log)")
                    },
                    if has {
                        Compatibility::Breaking
                    } else {
                        Compatibility::Compatible
                    },
                    Action::None,
                );
            }
        }
        for fam in nev {
            if !oev.contains(fam) {
                self.note(
                    ChangeKind::AggregateEventsChanged,
                    format!("{path}.events"),
                    format!("now lists `{fam}`"),
                );
            }
        }
    }

    fn state(&mut self, o: &AggregateState, n: &AggregateState) {
        let AggregateState {
            aggregate,
            docs: _,
            fields: ost,
            evolve: oevo,
            snapshot_every: osn,
        } = o;
        let AggregateState {
            aggregate: _,
            docs: _,
            fields: nst,
            evolve: nevo,
            snapshot_every: nsn,
        } = n;
        let path = aggregate.to_string();
        if ost != nst {
            self.push(
                ChangeKind::AggregateStateChanged,
                format!("{path}.state"),
                "state shape changed; instance snapshots are dropped and streams replay"
                    .to_string(),
                Compatibility::NeedsRebuild,
                Action::ClearAggregateSnapshots {
                    context: aggregate.context.clone(),
                    name: aggregate.name.clone(),
                },
            );
        }
        self.wasm(&format!("{path}.evolve"), oevo, nevo);
        if osn != nsn {
            self.note(
                ChangeKind::SnapshotEveryChanged,
                format!("{path}.snapshot"),
                format!("snapshot interval changed from {osn} to {nsn}"),
            );
        }
    }

    fn wasm(&mut self, path: &str, o: &WasmRef, n: &WasmRef) {
        let WasmRef {
            module: om,
            export: oe,
        } = o;
        let WasmRef {
            module: nm,
            export: ne,
        } = n;
        if om != nm || oe != ne {
            self.note(
                ChangeKind::WasmChanged,
                path.to_string(),
                format!(
                    "wasm changed from {} to {}",
                    show_wasm(om, oe.as_deref()),
                    show_wasm(nm, ne.as_deref())
                ),
            );
        }
    }

    fn projection(&mut self, ctx: &str, o: &Projection, n: &Projection) {
        let Projection {
            context: _,
            name,
            docs: _,
            from: of,
            fold: ofold,
            snapshot_every: osn,
            tables: ot,
        } = o;
        let Projection {
            context: _,
            name: _,
            docs: _,
            from: nf,
            fold: nfold,
            snapshot_every: nsn,
            tables: nt,
        } = n;
        let path = format!("{ctx}.{name}");
        let rebuild = Action::RebuildProjection {
            context: ctx.to_string(),
            name: name.clone(),
        };
        if of != nf {
            self.push(
                ChangeKind::ProjectionSourcesChanged,
                format!("{path}.from"),
                "sources changed; the projection rebuilds from the start of the log".to_string(),
                Compatibility::NeedsRebuild,
                rebuild.clone(),
            );
        }
        self.wasm(&format!("{path}.fold"), ofold, nfold);
        if osn != nsn {
            self.note(
                ChangeKind::SnapshotEveryChanged,
                format!("{path}.snapshot"),
                format!("snapshot interval changed from {osn} to {nsn}"),
            );
        }
        for (tname, otab) in ot {
            let tpath = format!("{path}.{tname}");
            match nt.get(tname) {
                Some(ntab) => self.table(&tpath, &rebuild, otab, ntab),
                None => self.push(
                    ChangeKind::TableRemoved,
                    tpath,
                    format!("table `{tname}` removed; it is dropped"),
                    Compatibility::Compatible,
                    Action::DropTable {
                        context: ctx.to_string(),
                        projection: name.clone(),
                        table: tname.clone(),
                    },
                ),
            }
        }
        for tname in nt.keys() {
            if !ot.contains_key(tname) {
                self.push(
                    ChangeKind::TableAdded,
                    format!("{path}.{tname}"),
                    format!("table `{tname}` added; the projection rebuilds to fill it"),
                    Compatibility::NeedsRebuild,
                    rebuild.clone(),
                );
            }
        }
    }

    fn table(&mut self, path: &str, rebuild: &Action, o: &Table, n: &Table) {
        let Table {
            name: _,
            docs: _,
            keys: ok,
            columns: oc,
        } = o;
        let Table {
            name: _,
            docs: _,
            keys: nk,
            columns: nc,
        } = n;
        let same_keys = ok.len() == nk.len()
            && ok
                .iter()
                .zip(nk)
                .all(|(a, b)| a.name == b.name && a.ty == b.ty);
        if !same_keys {
            self.push(
                ChangeKind::TableKeyChanged,
                format!("{path}.key"),
                "key columns changed; the projection rebuilds".to_string(),
                Compatibility::NeedsRebuild,
                rebuild.clone(),
            );
        }
        for col in oc {
            let cpath = format!("{path}.{}", col.name);
            match nc.iter().find(|c| c.name == col.name) {
                None => self.push(
                    ChangeKind::ColumnRemoved,
                    cpath,
                    format!("column `{}` removed; the projection rebuilds", col.name),
                    Compatibility::NeedsRebuild,
                    rebuild.clone(),
                ),
                Some(ncol) => {
                    let Field {
                        name: _,
                        ty: oty,
                        docs: _,
                        default: od,
                    } = col;
                    let Field {
                        name: _,
                        ty: nty,
                        docs: _,
                        default: nd,
                    } = ncol;
                    if oty != nty {
                        self.push(
                            ChangeKind::ColumnTypeChanged,
                            cpath,
                            format!(
                                "column `{}` changed from {oty} to {nty}; the projection rebuilds",
                                col.name
                            ),
                            Compatibility::NeedsRebuild,
                            rebuild.clone(),
                        );
                    } else if od != nd {
                        self.note(
                            ChangeKind::FieldDefaultChanged,
                            cpath,
                            format!(
                                "default of `{}` changed from {} to {}",
                                col.name,
                                show_default(od),
                                show_default(nd)
                            ),
                        );
                    }
                }
            }
        }
        for col in nc {
            if oc.iter().any(|c| c.name == col.name) {
                continue;
            }
            let cpath = format!("{path}.{}", col.name);
            if col.ty.is_optional() || col.default.is_some() {
                self.note(
                    ChangeKind::ColumnAdded,
                    cpath,
                    format!(
                        "column `{}` added ({})",
                        col.name,
                        if col.ty.is_optional() {
                            "optional"
                        } else {
                            "with a default"
                        }
                    ),
                );
            } else {
                self.push(
                    ChangeKind::ColumnAdded,
                    cpath,
                    format!(
                        "required column `{}` added; the projection rebuilds to fill it",
                        col.name
                    ),
                    Compatibility::NeedsRebuild,
                    rebuild.clone(),
                );
            }
        }
    }
}

fn show_default(d: &Option<serde_json::Value>) -> String {
    match d {
        Some(v) => v.to_string(),
        None => "none".to_string(),
    }
}

fn show_wasm(module: &str, export: Option<&str>) -> String {
    match export {
        Some(e) => format!("{module:?} export {e:?}"),
        None => format!("{module:?}"),
    }
}
