pub mod aggregate;
pub mod append;
pub mod backup;
pub mod exec;
pub mod health;
pub mod init;
pub mod log;
pub mod process;
pub mod projection;
pub mod promote;
pub mod query;
pub mod schema;

use anyhow::Context;
use serde_json::Value;

/// Parses a JSON argument, naming the flag in the error.
pub fn json_arg(flag: &str, text: &str) -> anyhow::Result<Value> {
    serde_json::from_str(text).with_context(|| format!("{flag} is not valid JSON"))
}

pub fn json_arg_bytes(flag: &str, text: &str) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(&json_arg(flag, text)?)?)
}
