//! What an application's handlers, invariant checks and reactions receive
//! and return. Events reach them as [`Event`] (the guest SDK's type, since
//! the derivation node's guests see the same shape); everything else is
//! the application layer's own.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use fold_guest::Event;

/// An event a handler emits: `Context.Event` (the latest version) with its
/// payload, and metadata the request's replaces when absent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Emit {
    /// `Context.Event` or `Context.Event@vN` (no version: the latest).
    #[serde(rename = "type")]
    pub r#type: String,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

impl Emit {
    pub fn event(r#type: impl Into<String>, payload: Value) -> Self {
        Emit {
            r#type: r#type.into(),
            payload,
            metadata: Value::Null,
        }
    }

    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// A business rejection of a command, by a handler or an invariant: it
/// reaches the client as `FAILED_PRECONDITION` with the code in the
/// `fold-rejection-code` header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rejected {
    /// A stable, machine-readable code such as `ALREADY_PLACED`.
    pub code: String,
    pub message: String,
}

impl Rejected {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Rejected {
            code: code.into(),
            message: message.into(),
        }
    }
}

/// Why a handler emitted nothing, or a check did not pass.
///
/// A [`Rejected`] is a business decision and reaches the client as
/// `FAILED_PRECONDITION` with its code. An `Error` is a defect of the
/// application and reaches the client as `INTERNAL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fail {
    Rejected(Rejected),
    Error(String),
}

impl From<Rejected> for Fail {
    fn from(r: Rejected) -> Self {
        Fail::Rejected(r)
    }
}

impl From<String> for Fail {
    fn from(e: String) -> Self {
        Fail::Error(e)
    }
}

impl From<&str> for Fail {
    fn from(e: &str) -> Self {
        Fail::Error(e.to_string())
    }
}

/// An event a command is about to append: like [`Event`] but without a
/// global position, which is only assigned once the invariants have passed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingEvent {
    /// `Context.Event@vN`.
    #[serde(rename = "type")]
    pub r#type: String,
    /// The stream version this event will have.
    pub version: u64,
    pub payload: Value,
    #[serde(default)]
    pub metadata: Value,
}

impl PendingEvent {
    pub fn family(&self) -> &str {
        self.r#type.split('@').next().unwrap_or(&self.r#type)
    }

    pub fn is(&self, family: &str) -> bool {
        self.family() == family
    }
}

/// What a command handler knows about the call besides the state and the
/// command.
#[derive(Debug, Clone, PartialEq)]
pub struct CmdCtx {
    /// `Context.Aggregate`.
    pub aggregate: String,
    pub stream: String,
    /// The aggregate key, as rendered into the stream id. Handlers put it
    /// in the events they emit.
    pub key: Value,
    /// The stream's current version, `None` for a new stream.
    pub version: Option<u64>,
    /// The node's wall clock at the call, RFC 3339.
    pub now: String,
}

/// What an invariant check knows besides the candidate state and events.
#[derive(Debug, Clone, PartialEq)]
pub struct InvCtx {
    /// `Context.Aggregate.Name` for a state invariant, `Context.Name` for a
    /// context invariant.
    pub invariant: String,
    /// `Context.Aggregate` whose command is being checked.
    pub aggregate: String,
    pub stream: String,
    pub key: Value,
    /// The stream version after the pending events.
    pub version: u64,
    /// For a context invariant: the projection the check may read.
    pub projection: Option<String>,
    /// For a context invariant: the value of the scope field.
    pub scope: Option<Value>,
}

/// What a process manager knows about the call besides state and trigger.
#[derive(Debug, Clone, PartialEq)]
pub struct ProcCtx {
    /// `Context.Process`.
    pub process: String,
    /// The correlation key of this instance.
    pub key: Value,
    /// The node's wall clock at the call, RFC 3339.
    pub now: String,
}

/// A command a process manager asks the node to execute.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IssuedCommand {
    /// `Context.Aggregate.Command`.
    pub command: String,
    /// The target aggregate instance's stream id.
    pub stream: String,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

impl IssuedCommand {
    pub fn new(command: impl Into<String>, stream: impl Into<String>, payload: Value) -> Self {
        IssuedCommand {
            command: command.into(),
            stream: stream.into(),
            payload,
            metadata: Value::Null,
        }
    }

    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// What woke a process manager instance.
#[derive(Debug, Clone, PartialEq)]
pub enum Trigger {
    /// An event the process listed among its sources.
    Event(Event),
    /// A command this instance issued earlier was refused by a handler or
    /// an invariant. Defects (`INTERNAL`) are retried by the node instead.
    Rejected {
        command: IssuedCommand,
        rejected: Rejected,
    },
    /// A timer this instance set came due. The primary's application node
    /// appends a `Fold.TimerFired` event when it does; every node (and a
    /// rebuild) reacts to that event, so a timer fires once for everyone.
    Timer {
        name: String,
        /// When it was due, RFC 3339.
        due_at: String,
        /// When the node fired it, RFC 3339.
        fired_at: String,
    },
}

/// A timer a reaction sets: one per (instance, name); setting it again
/// moves it. Exactly one of `after_ms` and `at`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetTimer {
    /// A name the process registered under `timers`.
    pub name: String,
    /// Milliseconds after the moment the trigger was recorded (an event's
    /// recording, or the firing of the timer being reacted to), so a replay
    /// derives the same deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_ms: Option<u64>,
    /// An absolute RFC 3339 time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
}

impl SetTimer {
    pub fn after(name: impl Into<String>, ms: u64) -> Self {
        SetTimer {
            name: name.into(),
            after_ms: Some(ms),
            at: None,
        }
    }

    pub fn at(name: impl Into<String>, rfc3339: impl Into<String>) -> Self {
        SetTimer {
            name: name.into(),
            after_ms: None,
            at: Some(rfc3339.into()),
        }
    }
}

/// A process manager's answer: the state to keep (`None` ends the
/// instance), the commands to issue in order, and the timers to set or
/// cancel.
#[derive(Debug, Clone, PartialEq)]
pub struct Reaction<S = Value> {
    pub state: Option<S>,
    pub commands: Vec<IssuedCommand>,
    /// Timers to set (or move), applied after `cancel_timers`.
    pub timers: Vec<SetTimer>,
    /// Timers to cancel, by name; a name with no pending timer is a no-op.
    pub cancel_timers: Vec<String>,
}

impl<S> Default for Reaction<S> {
    fn default() -> Self {
        Reaction {
            state: None,
            commands: Vec::new(),
            timers: Vec::new(),
            cancel_timers: Vec::new(),
        }
    }
}

impl<S> Reaction<S> {
    /// Keep `state`, issue nothing yet.
    pub fn keep(state: S) -> Self {
        Reaction {
            state: Some(state),
            ..Reaction::default()
        }
    }

    /// End the instance: its state and its pending timers are removed.
    pub fn end() -> Self {
        Reaction::default()
    }

    /// Ignore the trigger: `state` stays as it was.
    pub fn unchanged(state: Option<S>) -> Self {
        Reaction {
            state,
            ..Reaction::default()
        }
    }

    pub fn issue(mut self, command: IssuedCommand) -> Self {
        self.commands.push(command);
        self
    }

    pub fn set_timer(mut self, timer: SetTimer) -> Self {
        self.timers.push(timer);
        self
    }

    pub fn cancel_timer(mut self, name: impl Into<String>) -> Self {
        self.cancel_timers.push(name.into());
        self
    }

    /// The same reaction with its state as JSON.
    pub fn into_json(self) -> Result<Reaction<Value>, serde_json::Error>
    where
        S: Serialize,
    {
        Ok(Reaction {
            state: self.state.map(serde_json::to_value).transpose()?,
            commands: self.commands,
            timers: self.timers,
            cancel_timers: self.cancel_timers,
        })
    }
}

/// Rows of the projection a context invariant reads: the node has caught
/// the projection up to the log's head and locked the scope before the
/// check runs.
pub trait Rows: Send + Sync {
    /// The row of `table` at `key` (a JSON object of the key fields), if any.
    fn get(&self, table: &str, key: &Value) -> Result<Option<Value>, String>;
}
