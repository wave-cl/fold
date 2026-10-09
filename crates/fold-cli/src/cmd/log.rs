use clap::Subcommand;
use fold_proto::application::v1::GetProcessRequest;
use fold_proto::database::v1::{
    HealthRequest, ReadAllRequest, ReadStreamRequest, SubscribeAllRequest, log_item,
};
use fold_proto::derivation::v1::GetAggregateRequest;
use tokio_stream::StreamExt;

use crate::client::{self, Addrs};
use crate::output::{self, Format};

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Events of one stream (database: Log.ReadStream).
    Read {
        stream: String,
        #[arg(long, default_value_t = 0)]
        from: u64,
        /// Maximum events; 0 = all.
        #[arg(long, default_value_t = 0)]
        max: u32,
        #[arg(long)]
        backward: bool,
    },
    /// Every event from a global position (database: Log.ReadAll).
    All {
        #[arg(long, default_value_t = 0)]
        from: u64,
        #[arg(long, default_value_t = 0)]
        max: u32,
    },
    /// Follow the log live from a position; Ctrl-C to stop (database: Log.SubscribeAll).
    Tail {
        #[arg(long)]
        from: Option<u64>,
    },
    /// Current state of one aggregate instance (derivation: Aggregate.GetAggregate).
    Aggregate { stream: String },
    /// Current state of one process manager instance (application: AppAdmin.GetProcess).
    Process {
        /// "Context.Process"
        process: String,
        /// The correlation key as JSON, e.g. '"a0000000-…"'
        key: String,
    },
}

pub async fn run(cmd: Cmd, addrs: &Addrs, format: Format) -> anyhow::Result<()> {
    match cmd {
        Cmd::Read {
            stream,
            from,
            max,
            backward,
        } => {
            let mut events = client::log(addrs)
                .await?
                .read_stream(ReadStreamRequest {
                    stream_id: stream,
                    from_version: from,
                    max,
                    backward,
                })
                .await?
                .into_inner();
            while let Some(e) = events.next().await {
                output::print_event(format, &e?);
            }
        }
        Cmd::All { from, max } => {
            let mut events = client::log(addrs)
                .await?
                .read_all(ReadAllRequest {
                    from_position: from,
                    max,
                })
                .await?
                .into_inner();
            while let Some(e) = events.next().await {
                output::print_event(format, &e?);
            }
        }
        Cmd::Tail { from } => {
            // Default to the live edge: ask Health for the head.
            let from = match from {
                Some(p) => p,
                None => {
                    client::cluster(addrs)
                        .await?
                        .health(HealthRequest {})
                        .await?
                        .into_inner()
                        .head
                }
            };
            let mut items = client::log(addrs)
                .await?
                .subscribe_all(SubscribeAllRequest {
                    from_position: from,
                    last_event_id: String::new(),
                })
                .await?
                .into_inner();
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break,
                    next = items.next() => match next {
                        Some(item) => match item?.item {
                            Some(log_item::Item::Event(e)) => output::print_event(format, &e),
                            // The log's status, first and on every role or
                            // epoch change; events are what a tail shows.
                            Some(log_item::Item::Status(_)) | None => {}
                        },
                        None => break,
                    },
                }
            }
        }
        Cmd::Process { process, key } => {
            let resp = client::app_admin(addrs)
                .await?
                .get_process(GetProcessRequest {
                    process,
                    key: super::json_arg_bytes("key", &key)?,
                })
                .await?
                .into_inner();
            if !resp.found {
                if format == Format::Json {
                    println!(r#"{{"found":false}}"#);
                } else {
                    println!("no such process instance");
                }
                std::process::exit(1);
            }
            let state: serde_json::Value = serde_json::from_slice(&resp.state)?;
            if format == Format::Json {
                println!("{}", serde_json::json!({ "found": true, "state": state }));
            } else {
                println!("{}", serde_json::to_string_pretty(&state)?);
            }
        }
        Cmd::Aggregate { stream } => {
            let resp = client::aggregates(addrs)
                .await?
                .get_aggregate(GetAggregateRequest { stream_id: stream })
                .await?
                .into_inner();
            if !resp.found {
                if format == Format::Json {
                    println!(r#"{{"found":false}}"#);
                } else {
                    println!("no such aggregate instance (empty stream)");
                }
                std::process::exit(1);
            }
            output::print_aggregate(format, &resp);
        }
    }
    Ok(())
}
