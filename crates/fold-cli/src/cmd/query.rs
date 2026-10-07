use clap::Subcommand;
use fold_proto::v1::{GetRequest, ScanRequest};
use serde_json::json;
use tokio_stream::StreamExt;

use crate::client;
use crate::output::{self, Format};

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Fetch one row by key (Query.Get).
    Get {
        /// "Context.Projection"
        projection: String,
        table: String,
        /// JSON object of the table's key fields, e.g. '{"customer_id":"..."}'
        key: String,
        /// Wait until the projection has applied this position (read-your-writes).
        #[arg(long)]
        after: Option<u64>,
        /// How long to wait for --after, in milliseconds.
        #[arg(long)]
        wait: Option<u32>,
    },
    /// Stream rows, optionally by key prefix (Query.Scan).
    Scan {
        projection: String,
        table: String,
        /// JSON object of the leading key fields.
        #[arg(long)]
        prefix: Option<String>,
        #[arg(long, default_value_t = 100)]
        limit: u32,
        #[arg(long)]
        after: Option<u64>,
        #[arg(long)]
        wait: Option<u32>,
    },
}

pub async fn run(cmd: Cmd, addr: &str, format: Format) -> anyhow::Result<()> {
    let mut q = client::query(addr).await?;
    match cmd {
        Cmd::Get {
            projection,
            table,
            key,
            after,
            wait,
        } => {
            let resp = q
                .get(GetRequest {
                    projection,
                    table,
                    key: super::json_arg_bytes("key", &key)?,
                    min_position: after,
                    wait_ms: wait,
                })
                .await?
                .into_inner();
            match (format, resp.found, resp.row) {
                (Format::Json, found, row) => println!(
                    "{}",
                    json!({
                        "found": found,
                        "checkpoint": resp.checkpoint,
                        "key": row.as_ref().map(|r| serde_json::from_slice::<serde_json::Value>(&r.key).ok()),
                        "row": row.as_ref().map(|r| serde_json::from_slice::<serde_json::Value>(&r.row).ok()),
                    })
                ),
                (Format::Human, true, Some(row)) => output::print_row(format, &row),
                (Format::Human, _, _) => {
                    println!("not found");
                    std::process::exit(1);
                }
            }
        }
        Cmd::Scan {
            projection,
            table,
            prefix,
            limit,
            after,
            wait,
        } => {
            let key_prefix = match prefix {
                Some(p) => super::json_arg_bytes("--prefix", &p)?,
                None => Vec::new(),
            };
            let mut rows = q
                .scan(ScanRequest {
                    projection,
                    table,
                    key_prefix,
                    limit,
                    min_position: after,
                    wait_ms: wait,
                })
                .await?
                .into_inner();
            let mut n = 0usize;
            while let Some(row) = rows.next().await {
                output::print_row(format, &row?);
                n += 1;
            }
            if format == Format::Human {
                println!("{n} row(s)");
            }
        }
    }
    Ok(())
}
