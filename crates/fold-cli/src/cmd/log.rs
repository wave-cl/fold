use clap::Subcommand;
use fold_proto::v1::{GetAggregateRequest, ReadAllRequest, ReadStreamRequest, SubscribeAllRequest};
use tokio_stream::StreamExt;

use crate::client;
use crate::output::{self, Format};

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Events of one stream (Log.ReadStream).
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
    /// Every event from a global position (Log.ReadAll).
    All {
        #[arg(long, default_value_t = 0)]
        from: u64,
        #[arg(long, default_value_t = 0)]
        max: u32,
    },
    /// Follow the log live from a position; Ctrl-C to stop (Log.SubscribeAll).
    Tail {
        #[arg(long)]
        from: Option<u64>,
    },
    /// Current state of one aggregate instance (Log.GetAggregate).
    Aggregate { stream: String },
}

pub async fn run(cmd: Cmd, addr: &str, format: Format) -> anyhow::Result<()> {
    let mut l = client::log(addr).await?;
    match cmd {
        Cmd::Read {
            stream,
            from,
            max,
            backward,
        } => {
            let mut events = l
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
            let mut events = l
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
                    client::admin(addr)
                        .await?
                        .health(fold_proto::v1::HealthRequest {})
                        .await?
                        .into_inner()
                        .head
                }
            };
            let mut events = l
                .subscribe_all(SubscribeAllRequest {
                    from_position: from,
                })
                .await?
                .into_inner();
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break,
                    next = events.next() => match next {
                        Some(e) => output::print_event(format, &e?),
                        None => break,
                    },
                }
            }
        }
        Cmd::Aggregate { stream } => {
            let resp = l
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
