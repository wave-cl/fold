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
