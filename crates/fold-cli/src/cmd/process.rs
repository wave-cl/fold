use clap::Subcommand;
use fold_proto::v1::ListProcessesRequest;
use serde_json::{Value, json};

use crate::client;
use crate::output::{Format, state_name};

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Every process manager with its state, checkpoint and outbox (Admin.ListProcesses).
    List,
}

pub async fn run(cmd: Cmd, addr: &str, format: Format) -> anyhow::Result<()> {
    match cmd {
        Cmd::List => {
            let resp = client::admin(addr)
                .await?
                .list_processes(ListProcessesRequest {})
                .await?
                .into_inner();
            match format {
                Format::Json => {
                    for p in &resp.processes {
                        println!(
                            "{}",
                            json!({
                                "name": p.name,
                                "state": state_name(p.state),
                                "checkpoint": p.checkpoint,
                                "head": p.head,
                                "pending_commands": p.pending_commands,
                                "dispatched": p.dispatched,
                                "rejected": p.rejected,
                                "error": if p.error.is_empty() { Value::Null } else { Value::String(p.error.clone()) },
                            })
                        );
                    }
                }
                Format::Human => {
                    let w = resp
                        .processes
                        .iter()
                        .map(|p| p.name.len())
                        .max()
                        .unwrap_or(4)
                        .max(4);
                    println!(
                        "{:<w$}  {:<11}  {:>10}  {:>8}  {:>7}  {:>10}  {:>8}",
                        "NAME", "STATE", "CHECKPOINT", "HEAD", "PENDING", "DISPATCHED", "REJECTED"
                    );
                    for p in &resp.processes {
                        let cp = p
                            .checkpoint
                            .map(|c| c.to_string())
                            .unwrap_or_else(|| "-".into());
                        println!(
                            "{:<w$}  {:<11}  {:>10}  {:>8}  {:>7}  {:>10}  {:>8}",
                            p.name,
                            state_name(p.state),
                            cp,
                            p.head,
                            p.pending_commands,
                            p.dispatched,
                            p.rejected
                        );
                        if !p.error.is_empty() {
                            println!("{:<w$}  error: {}", "", p.error);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
