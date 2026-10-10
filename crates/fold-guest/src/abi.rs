//! The JSON documents exchanged with the host for the derivation layer's
//! roles (projection steps, evolves, upcasters). Field names are the wire
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
