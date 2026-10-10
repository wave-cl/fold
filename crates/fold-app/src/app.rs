//! An application's registrations: the commands and state invariants of
//! each aggregate it serves, its projection-driven invariants, and its
//! process managers. Every typed closure is wrapped once into one over
//! JSON, so the runtime is untyped and the application is not.

use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use indexmap::IndexMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::{
    CmdCtx, Emit, Fail, InvCtx, PendingEvent, ProcCtx, Reaction, Rejected, Rows, Trigger,
};

/// Why a call into the application did not produce a result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallFail {
    /// A business decision: `FAILED_PRECONDITION` with the code.
    Rejected(Rejected),
    /// The request's payload does not fit the command's type:
    /// `INVALID_ARGUMENT`.
    Payload(String),
    /// The state the derivation node holds does not fit the application's
    /// type: `INTERNAL`.
    State(String),
    /// The application reported a defect: `INTERNAL`.
    Error(String),
    /// The application panicked: `INTERNAL`.
    Panic(String),
}

impl From<Fail> for CallFail {
    fn from(f: Fail) -> Self {
        match f {
            Fail::Rejected(r) => CallFail::Rejected(r),
            Fail::Error(e) => CallFail::Error(e),
        }
    }
}

impl std::fmt::Display for CallFail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallFail::Rejected(r) => write!(f, "rejected {}: {}", r.code, r.message),
            CallFail::Payload(e) => write!(f, "payload: {e}"),
            CallFail::State(e) => write!(f, "state: {e}"),
            CallFail::Error(e) => write!(f, "{e}"),
            CallFail::Panic(e) => write!(f, "panicked: {e}"),
        }
    }
}

/// Runs `f`, turning a panic into a [`CallFail::Panic`] naming `what`.
fn guarded<T>(what: &str, f: impl FnOnce() -> Result<T, CallFail>) -> Result<T, CallFail> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(payload) => {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "(no message)".to_string());
            Err(CallFail::Panic(format!("{what} panicked: {msg}")))
        }
    }
}

fn state_from<S: DeserializeOwned>(
    aggregate: &str,
    state: Option<Value>,
) -> Result<Option<S>, CallFail> {
    match state {
        None => Ok(None),
        Some(v) => serde_json::from_value(v).map(Some).map_err(|e| {
            CallFail::State(format!(
                "state of {aggregate} does not fit the application's type: {e}"
            ))
        }),
    }
}

pub(crate) type Handler =
    Arc<dyn Fn(&CmdCtx, Option<Value>, Value) -> Result<Vec<Emit>, CallFail> + Send + Sync>;
pub(crate) type StateCheck =
    Arc<dyn Fn(&InvCtx, &Value, &[PendingEvent]) -> Result<(), CallFail> + Send + Sync>;
pub(crate) type ContextCheck =
    Arc<dyn Fn(&InvCtx, &dyn Rows, &Value, &[PendingEvent]) -> Result<(), CallFail> + Send + Sync>;
pub(crate) type React = Arc<
    dyn Fn(&ProcCtx, Option<Value>, &Trigger) -> Result<Reaction<Value>, CallFail> + Send + Sync,
>;

/// One command of an aggregate.
#[derive(Clone)]
pub struct CommandDef {
    pub name: String,
    pub(crate) handler: Handler,
}

/// One state invariant of an aggregate, checked against the state every
/// command (and guarded append) would leave behind.
#[derive(Clone)]
pub struct StateInvariantDef {
    pub name: String,
    pub(crate) check: StateCheck,
}

/// The commands and state invariants registered for one aggregate.
#[derive(Clone)]
pub struct AggregateDef {
    /// `Context.Aggregate`.
    pub name: String,
    pub commands: IndexMap<String, CommandDef>,
    pub invariants: IndexMap<String, StateInvariantDef>,
}

/// Registers the commands and state invariants of one aggregate whose
/// state the application reads as `S`.
pub struct AggregateBuilder<S> {
    def: AggregateDef,
    _state: PhantomData<fn() -> S>,
}

impl<S> AggregateBuilder<S>
where
    S: DeserializeOwned + Send + Sync + 'static,
{
    /// A command: the request's payload is deserialized into `C`, the
    /// state the derivation node holds into `S`.
    pub fn command<C>(
        mut self,
        name: &str,
        handler: impl Fn(&CmdCtx, Option<S>, C) -> Result<Vec<Emit>, Fail> + Send + Sync + 'static,
    ) -> Self
    where
        C: DeserializeOwned + 'static,
    {
        let aggregate = self.def.name.clone();
        let what = format!("{aggregate}.{name}");
        let wrapped: Handler = Arc::new(move |cx, state, payload| {
            let state = state_from::<S>(&aggregate, state)?;
            let command: C = serde_json::from_value(payload)
                .map_err(|e| CallFail::Payload(format!("command {what}: {e}")))?;
            guarded(&format!("handler of {what}"), || {
                handler(cx, state, command).map_err(CallFail::from)
            })
        });
        self.def.commands.insert(
            name.to_string(),
            CommandDef {
                name: name.to_string(),
                handler: wrapped,
            },
        );
        self
    }

    /// A state invariant, checked against the candidate state after every
    /// command and guarded append on this aggregate.
    pub fn invariant(
        mut self,
        name: &str,
        check: impl Fn(&InvCtx, &S, &[PendingEvent]) -> Result<(), Rejected> + Send + Sync + 'static,
    ) -> Self {
        let aggregate = self.def.name.clone();
        let what = format!("{aggregate}.{name}");
        let wrapped: StateCheck = Arc::new(move |cx, state, events| {
            let state: S =
                state_from::<S>(&aggregate, Some(state.clone()))?.expect("Some in, Some out");
            guarded(&format!("invariant {what}"), || {
                check(cx, &state, events).map_err(CallFail::Rejected)
            })
        });
        self.def.invariants.insert(
            name.to_string(),
            StateInvariantDef {
                name: name.to_string(),
                check: wrapped,
            },
        );
        self
    }
}

/// A projection-driven invariant: when a command on `on` would commit, the
/// node locks the scope value, catches the projection up to the head and
/// runs the check over its rows.
#[derive(Clone)]
pub struct ContextInvariant {
    /// `Context.Name`.
    pub name: String,
    /// `Context.Aggregate` whose commands trigger the check.
    pub on: String,
    /// `Context.Projection` the check reads.
    pub projection: String,
    /// The field of the aggregate's state the check is serialized by.
    pub scope: String,
    pub(crate) check: Option<ContextCheck>,
}

impl ContextInvariant {
    pub fn new(name: &str) -> Self {
        ContextInvariant {
            name: name.to_string(),
            on: String::new(),
            projection: String::new(),
            scope: String::new(),
            check: None,
        }
    }

    pub fn on(mut self, aggregate: &str) -> Self {
        self.on = aggregate.to_string();
        self
    }

    pub fn projection(mut self, projection: &str) -> Self {
        self.projection = projection.to_string();
        self
    }

    pub fn scope(mut self, field: &str) -> Self {
        self.scope = field.to_string();
        self
    }

    /// The check, reading the candidate state as `S` and the projection's
    /// rows through [`Rows`].
    pub fn check<S>(
        mut self,
        check: impl Fn(&InvCtx, &dyn Rows, &S, &[PendingEvent]) -> Result<(), Fail>
        + Send
        + Sync
        + 'static,
    ) -> Self
    where
        S: DeserializeOwned + Send + Sync + 'static,
    {
        let what = self.name.clone();
        let on = self.on.clone();
        let wrapped: ContextCheck = Arc::new(move |cx, rows, state, events| {
            let state: S = state_from::<S>(&on, Some(state.clone()))?.expect("Some in, Some out");
            guarded(&format!("invariant {what}"), || {
                check(cx, rows, &state, events).map_err(CallFail::from)
            })
        });
        self.check = Some(wrapped);
        self
    }
}

/// A process manager: reacts to the events it lists, keeps state per
/// correlation key, issues commands and sets timers.
#[derive(Clone)]
pub struct Process {
    /// `Context.Process`.
    pub name: String,
    /// The correlation key's name: the field of the state and, by default,
    /// of every source event.
    pub key: String,
    /// `(Context.Event, field carrying the key)`.
    pub sources: Vec<(String, String)>,
    /// The timer names a reaction may set.
    pub timers: Vec<String>,
    /// Snapshot every this many positions; `0` = never.
    pub snapshot_every: u32,
    pub(crate) react: Option<React>,
}

impl Process {
    pub fn new(name: &str) -> Self {
        Process {
            name: name.to_string(),
            key: String::new(),
            sources: Vec::new(),
            timers: Vec::new(),
            snapshot_every: 0,
            react: None,
        }
    }

    pub fn key(mut self, field: &str) -> Self {
        self.key = field.to_string();
        self
    }

    /// A source event whose field named like the key correlates it.
    pub fn from(mut self, family: &str) -> Self {
        let by = self.key.clone();
        self.sources.push((family.to_string(), by));
        self
    }

    /// A source event correlated by `by`.
    pub fn from_by(mut self, family: &str, by: &str) -> Self {
        self.sources.push((family.to_string(), by.to_string()));
        self
    }

    pub fn timers<I, T>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.timers = names.into_iter().map(Into::into).collect();
        self
    }

    pub fn snapshot_every(mut self, every: u32) -> Self {
        self.snapshot_every = every;
        self
    }

    /// The reaction, reading and writing the instance's state as `S`.
    pub fn react<S>(
        mut self,
        react: impl Fn(&ProcCtx, Option<S>, &Trigger) -> Result<Reaction<S>, String>
        + Send
        + Sync
        + 'static,
    ) -> Self
    where
        S: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let what = self.name.clone();
        let wrapped: React = Arc::new(move |cx, state, trigger| {
            let state = state_from::<S>(&what, state)?;
            let reaction = guarded(&format!("reaction of {what}"), || {
                react(cx, state, trigger).map_err(CallFail::Error)
            })?;
            reaction
                .into_json()
                .map_err(|e| CallFail::State(format!("state of {what} is not serializable: {e}")))
        });
        self.react = Some(wrapped);
        self
    }
}

/// Everything an application registers.
#[derive(Clone, Default)]
pub struct App {
    pub aggregates: IndexMap<String, AggregateDef>,
    pub invariants: IndexMap<String, ContextInvariant>,
    pub processes: IndexMap<String, Process>,
}

impl App {
    pub fn new() -> App {
        App::default()
    }

    /// The commands and state invariants of `name` (`Context.Aggregate`),
    /// whose state the application reads as `S`. Registering an aggregate
    /// twice adds to the first registration.
    pub fn aggregate<S>(
        mut self,
        name: &str,
        f: impl FnOnce(AggregateBuilder<S>) -> AggregateBuilder<S>,
    ) -> App
    where
        S: DeserializeOwned + Send + Sync + 'static,
    {
        let def = self
            .aggregates
            .shift_remove(name)
            .unwrap_or_else(|| AggregateDef {
                name: name.to_string(),
                commands: IndexMap::new(),
                invariants: IndexMap::new(),
            });
        let built = f(AggregateBuilder {
            def,
            _state: PhantomData,
        });
        self.aggregates.insert(name.to_string(), built.def);
        self
    }

    pub fn invariant(mut self, inv: ContextInvariant) -> App {
        self.invariants.insert(inv.name.clone(), inv);
        self
    }

    pub fn process(mut self, p: Process) -> App {
        self.processes.insert(p.name.clone(), p);
        self
    }

    /// What the application registered, as data: stored beside the
    /// process managers' tables so a changed registration is noticed, and
    /// served by `AppAdmin.GetSchema`.
    pub fn manifest(&self) -> Manifest {
        let mut aggregates: Vec<AggregateManifest> = self
            .aggregates
            .values()
            .map(|a| AggregateManifest {
                name: a.name.clone(),
                commands: a.commands.keys().cloned().collect(),
                invariants: a.invariants.keys().cloned().collect(),
            })
            .collect();
        aggregates.sort_by(|a, b| a.name.cmp(&b.name));
        let mut invariants: Vec<ContextInvariantManifest> = self
            .invariants
            .values()
            .map(|i| ContextInvariantManifest {
                name: i.name.clone(),
                on: i.on.clone(),
                projection: i.projection.clone(),
                scope: i.scope.clone(),
            })
            .collect();
        invariants.sort_by(|a, b| a.name.cmp(&b.name));
        let mut processes: Vec<ProcessManifest> = self
            .processes
            .values()
            .map(|p| ProcessManifest {
                name: p.name.clone(),
                key: p.key.clone(),
                from: p.sources.clone(),
                timers: p.timers.clone(),
                snapshot_every: p.snapshot_every,
            })
            .collect();
        processes.sort_by(|a, b| a.name.cmp(&b.name));
        Manifest {
            aggregates,
            invariants,
            processes,
        }
    }
}

/// The registrations as data (see [`App::manifest`]).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Manifest {
    #[serde(default)]
    pub aggregates: Vec<AggregateManifest>,
    #[serde(default)]
    pub invariants: Vec<ContextInvariantManifest>,
    #[serde(default)]
    pub processes: Vec<ProcessManifest>,
}

impl Manifest {
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("a manifest is plain data")
    }

    pub fn process(&self, name: &str) -> Option<&ProcessManifest> {
        self.processes.iter().find(|p| p.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateManifest {
    pub name: String,
    pub commands: Vec<String>,
    pub invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextInvariantManifest {
    pub name: String,
    pub on: String,
    pub projection: String,
    pub scope: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessManifest {
    pub name: String,
    pub key: String,
    pub from: Vec<(String, String)>,
    pub timers: Vec<String>,
    pub snapshot_every: u32,
}

impl ProcessManifest {
    /// What a rebuild of the process's tables must start over for: the
    /// key, the sources and the timers; not how often it snapshots.
    pub fn derivation_changed(&self, other: &ProcessManifest) -> bool {
        self.key != other.key || self.from != other.from
    }

    /// A hash of the definition, recorded in the process's snapshot files
    /// in place of a guest module's hash: a snapshot taken under another
    /// definition does not restore without `force`.
    pub fn fingerprint(&self) -> [u8; 32] {
        use sha2::Digest;
        let mut h = sha2::Sha256::new();
        h.update(serde_json::to_vec(self).expect("plain data"));
        h.finalize().into()
    }
}
