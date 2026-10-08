use clap::Args as ClapArgs;
use fold_proto::v1::{AppendRequest, ExpectedVersion, NewEvent, expected_version};
use serde_json::json;

use crate::client;
use crate::output::Format;

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// The stream id, e.g. order-<uuid>
    pub stream: String,
    /// "Context.Event" or "Context.Event@vN"
    pub event_type: String,
    /// The event payload as JSON.
    #[arg(short = 'd', long = "data")]
    pub data: String,
    /// Event metadata as JSON.
    #[arg(long)]
    pub meta: Option<String>,
    /// Expected stream version: a number, "none" (stream must not exist),
    /// "exists", or "any" (default).
    #[arg(long, default_value = "any")]
    pub expect: String,
    /// Fencing token: the epoch from `fold health`.
    #[arg(long, value_name = "EPOCH")]
    pub fencing_token: Option<u64>,
}

pub fn parse_expect(s: &str) -> anyhow::Result<ExpectedVersion> {
    let kind = match s {
        "any" => expected_version::Kind::Any(true),
        "none" => expected_version::Kind::NoStream(true),
        "exists" => expected_version::Kind::StreamExists(true),
        n => expected_version::Kind::Exact(n.parse::<u64>().map_err(|_| {
            anyhow::anyhow!("--expect must be a version number, any, none or exists")
        })?),
    };
    Ok(ExpectedVersion { kind: Some(kind) })
}

pub async fn run(args: Args, addr: &str, format: Format) -> anyhow::Result<()> {
    let payload = super::json_arg_bytes("--data", &args.data)?;
    let metadata = match &args.meta {
        Some(m) => super::json_arg_bytes("--meta", m)?,
        None => Vec::new(),
    };
    let expected = parse_expect(&args.expect)?;
    let mut c = client::command(addr).await?;
    let resp = c
        .append(AppendRequest {
            stream_id: args.stream,
            expected: Some(expected),
            events: vec![NewEvent {
                r#type: args.event_type,
                payload,
                content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                metadata,
            }],
            fencing_token: args.fencing_token,
        })
        .await?
        .into_inner();
    match format {
        Format::Json => println!(
            "{}",
            json!({ "first_position": resp.first_position, "last_position": resp.last_position, "version": resp.version, "token": resp.token })
        ),
        Format::Human => println!(
            "appended at position {}, stream now at version {}, token {}",
            resp.last_position, resp.version, resp.token
        ),
    }
    Ok(())
}
