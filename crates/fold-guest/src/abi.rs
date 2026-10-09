//! The JSON documents exchanged with the host. Field names are the wire
//! contract; the host (`fold-wasm`) has the mirror image of these types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::mutation::Mutation;

/// One recorded event as the host presents it to a guest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// The stream the event belongs to, e.g. `order-…`.
    pub stream: String,
    /// `Context.Event@vN`.
    #[serde(rename = "type")]
    pub r#type: String,
    /// Position within the stream, 0-based.
    pub version: u64,
    /// Position within the log, 0-based.
    pub position: u64,
    pub payload: Value,
    #[serde(default)]
    pub metadata: Value,
}

impl Event {
    /// `Context.Event` without the version suffix.
    pub fn family(&self) -> &str {
        self.r#type.split('@').next().unwrap_or(&self.r#type)
    }

    /// True when the event is `family` at any version.
    pub fn is(&self, family: &str) -> bool {
        self.family() == family
    }
}

/// A command as the host presents it to a handler.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Command {
    /// `Context.Aggregate.Command`.
    #[serde(rename = "type")]
    pub r#type: String,
    pub payload: Value,
}

impl Command {
    /// The command's own name, after the last dot.
    pub fn name(&self) -> &str {
        self.r#type.rsplit('.').next().unwrap_or(&self.r#type)
    }
}

/// An event a command handler asks the host to append.
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

/// A business rejection of a command.
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

/// What an invariant check knows besides the candidate state and events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<String>,
    /// For a context invariant: the value of the scope field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckInput {
    pub abi: i32,
    #[serde(flatten)]
    pub ctx: InvCtx,
    /// The state the aggregate would have after the pending events.
    pub state: Value,
    pub events: Vec<PendingEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CheckOutput {
    Ok { ok: bool },
    Violation { violation: Rejected },
    Err { error: String },
}

/// A command a process manager asks the daemon to execute.
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
}

/// What woke a process manager instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    /// An event the process declared in `from`.
    Event(Event),
    /// A command this instance issued earlier was refused by a handler or an
    /// invariant. Defects (`INTERNAL`) are retried by the daemon instead.
    Rejected {
        command: IssuedCommand,
        rejected: Rejected,
    },
    /// A timer this instance set came due. The primary appends a
    /// `Fold.TimerFired` event when it does; every member (and a rebuild)
    /// reacts to that event, so a timer fires once for everyone.
    Timer {
        name: String,
        /// When it was due, RFC 3339.
        due_at: String,
        /// When the daemon fired it, RFC 3339.
        fired_at: String,
    },
}

/// A timer a reaction sets: one per (instance, name); setting it again
/// moves it. Exactly one of `after_ms` and `at`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetTimer {
    /// A name the process declares under `timers`.
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

/// What a process manager knows about the call besides state and trigger.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcCtx {
    /// `Context.Process`.
    pub process: String,
    /// The correlation key of this instance.
    pub key: Value,
    /// The host's wall clock at the call, RFC 3339.
    #[serde(default)]
    pub now: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessInput {
    pub abi: i32,
    #[serde(flatten)]
    pub ctx: ProcCtx,
    /// `None` when this is the instance's first trigger, or after it ended.
    pub state: Option<Value>,
    pub trigger: Trigger,
}

/// A process manager's answer: the state to keep (`None` ends the instance)
/// and the commands to issue, in order.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reaction {
    pub state: Option<Value>,
    #[serde(default)]
    pub commands: Vec<IssuedCommand>,
    /// Timers to set (or move), applied after `cancel_timers`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timers: Vec<SetTimer>,
    /// Timers to cancel, by name; a name with no pending timer is a no-op.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cancel_timers: Vec<String>,
}

impl Reaction {
    /// Keep `state`, issue nothing yet.
    pub fn keep(state: Value) -> Self {
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
    pub fn unchanged(state: Option<Value>) -> Self {
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProcessOutput {
    // `Err` first: a reaction's fields all have defaults, so it would also
    // accept an error document if it were tried first.
    Err { error: String },
    Ok(Reaction),
}

/// A recorded event of an older version, as the host hands it to an
/// upcaster.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpcastEvent {
    /// `Context.Event@v<from>`.
    #[serde(rename = "type")]
    pub r#type: String,
    pub from_version: u16,
    pub to_version: u16,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpcastInput {
    pub abi: i32,
    pub event: UpcastEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UpcastOutput {
    Ok { payload: Value },
    Err { error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionInput {
    pub abi: i32,
    pub projection: String,
    pub event: Event,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProjectionOutput {
    Ok { mutations: Vec<Mutation> },
    Err { error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolveInput {
    pub abi: i32,
    /// `Context.Aggregate`.
    pub aggregate: String,
    pub stream: String,
    /// The aggregate key, as rendered into the stream id.
    #[serde(default)]
    pub key: Value,
    pub version: Option<u64>,
    pub state: Option<Value>,
    pub event: Event,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EvolveOutput {
    Ok { state: Value },
    Err { error: String },
}

/// What a command handler knows about the call besides state and command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CmdCtx {
    /// `Context.Aggregate`.
    pub aggregate: String,
    pub stream: String,
    /// The aggregate key, as rendered into the stream id. Handlers put it in
    /// the events they emit.
    pub key: Value,
    /// The stream's current version, `None` for a new stream.
    pub version: Option<u64>,
    /// The host's wall clock at the call, RFC 3339. Guests have no clock.
    pub now: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandInput {
    pub abi: i32,
    /// `Context.Aggregate`.
    pub aggregate: String,
    pub stream: String,
    /// The aggregate key, as rendered into the stream id. Handlers put it in
    /// the events they emit.
    #[serde(default)]
    pub key: Value,
    /// The stream's current version, `None` for a new stream.
    pub version: Option<u64>,
    pub state: Option<Value>,
    /// The host's wall clock at the call, RFC 3339. Guests have no clock.
    #[serde(default)]
    pub now: String,
    pub command: Command,
}

impl CommandInput {
    /// Splits the document into the handler's three arguments.
    pub fn into_parts(self) -> (CmdCtx, Option<Value>, Command) {
        (
            CmdCtx {
                aggregate: self.aggregate,
                stream: self.stream,
                key: self.key,
                version: self.version,
                now: self.now,
            },
            self.state,
            self.command,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CommandOutput {
    Ok { events: Vec<Emit> },
    Rejected { rejected: Rejected },
    Err { error: String },
}
