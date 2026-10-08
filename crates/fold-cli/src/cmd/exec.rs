use clap::Args as ClapArgs;
use fold_proto::v1::ExecuteRequest;
use serde_json::json;

use crate::client;
use crate::output::{self, Format};

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// "Context.Aggregate.Command", e.g. Orders.Order.PlaceOrder
    pub command: String,
    /// The aggregate instance's stream id, e.g. order-<uuid>
    pub stream: String,
    /// The command payload as JSON.
    #[arg(short = 'd', long = "data", default_value = "{}")]
    pub data: String,
    /// Metadata attached to every emitted event, as JSON.
    #[arg(long)]
    pub meta: Option<String>,
    /// Fencing token: the epoch from `fold health`. Refused if stale; a
    /// newer one fences the daemon it reaches.
    #[arg(long, value_name = "EPOCH")]
    pub fencing_token: Option<u64>,
}

pub async fn run(args: Args, addr: &str, format: Format) -> anyhow::Result<()> {
    let payload = super::json_arg_bytes("--data", &args.data)?;
    let metadata = match &args.meta {
        Some(m) => super::json_arg_bytes("--meta", m)?,
        None => Vec::new(),
    };
    let mut c = client::command(addr).await?;
    let resp = c
        .execute(ExecuteRequest {
            command: args.command,
            stream_id: args.stream,
            payload,
            content_type: fold_proto::CONTENT_TYPE_JSON.into(),
            metadata,
            fencing_token: args.fencing_token,
        })
        .await?
        .into_inner();
    match format {
        Format::Json => println!(
            "{}",
            json!({
                "events": resp.events.iter().map(output::event_json).collect::<Vec<_>>(),
                "first_position": if resp.events.is_empty() { None } else { Some(resp.first_position) },
                "last_position": resp.last_position,
                "version": resp.version,
                "token": resp.token,
            })
        ),
        Format::Human => {
            if resp.events.is_empty() {
                println!(
                    "ok, no events emitted (last position {}, token {})",
                    resp.last_position, resp.token
                );
            } else {
                for e in &resp.events {
                    output::print_event(format, e);
                }
                println!(
                    "ok: {} event(s), stream at version {}, last position {}, token {}",
                    resp.events.len(),
                    resp.version
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "-".into()),
                    resp.last_position,
                    resp.token
                );
            }
        }
    }
    Ok(())
}
