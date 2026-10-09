use clap::Args as ClapArgs;
use fold_proto::common::v1::{ExpectedVersion, NewEvent, expected_version};
use serde_json::json;

use crate::client::{self, Addrs};
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
    /// Append on the database directly (Log.Append), past the application
    /// node's invariants: the escape hatch for migrations. The database
    /// still validates the event against the domain.
    #[arg(long)]
    pub unguarded: bool,
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

/// What either append answers with.
struct Appended {
    first_position: u64,
    last_position: u64,
    version: u64,
    token: String,
}

pub async fn run(
    args: Args,
    addrs: &Addrs,
    format: Format,
    session: &mut Option<crate::session::Session>,
) -> anyhow::Result<()> {
    let payload = super::json_arg_bytes("--data", &args.data)?;
    let metadata = match &args.meta {
        Some(m) => super::json_arg_bytes("--meta", m)?,
        None => Vec::new(),
    };
    let expected = parse_expect(&args.expect)?;
    let events = vec![NewEvent {
        r#type: args.event_type,
        payload,
        content_type: fold_proto::CONTENT_TYPE_JSON.into(),
        metadata,
    }];
    let resp = if args.unguarded {
        if format == Format::Human {
            eprintln!(
                "fold: appending on the database directly; the aggregate's invariants are not checked"
            );
        }
        let r = client::log(addrs)
            .await?
            .append(fold_proto::database::v1::AppendRequest {
                stream_id: args.stream,
                expected: Some(expected),
                events,
                fencing_token: args.fencing_token,
                idempotency_key: Vec::new(),
            })
            .await?
            .into_inner();
        Appended {
            first_position: r.first_position,
            last_position: r.last_position,
            version: r.version,
            token: r.token,
        }
    } else {
        let r = client::command(addrs)
            .await?
            .append(fold_proto::application::v1::AppendRequest {
                stream_id: args.stream,
                expected: Some(expected),
                events,
                fencing_token: args.fencing_token,
            })
            .await?
            .into_inner();
        Appended {
            first_position: r.first_position,
            last_position: r.last_position,
            version: r.version,
            token: r.token,
        }
    };
    if let Some(s) = session {
        s.advance(&resp.token)?;
    }
    match format {
        Format::Json => println!(
            "{}",
            json!({ "first_position": resp.first_position, "last_position": resp.last_position, "version": resp.version, "token": resp.token, "unguarded": args.unguarded })
        ),
        Format::Human => println!(
            "appended at position {}, stream now at version {}, token {}",
            resp.last_position, resp.version, resp.token
        ),
    }
    Ok(())
}
