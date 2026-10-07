use anyhow::Context as _;
use clap::Subcommand;
use fold_proto::v1::GetSchemaRequest;
use serde_json::json;

use crate::client;
use crate::output::Format;

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Parse and validate a schema file (offline).
    Check {
        /// Path to a .fold file.
        file: std::path::PathBuf,
    },
    /// Print the schema the running daemon loaded (Admin.GetSchema).
    Show,
}

pub async fn run(cmd: Cmd, addr: &str, format: Format) -> anyhow::Result<()> {
    match cmd {
        Cmd::Check { file } => check(&file, format),
        Cmd::Show => {
            let s = client::admin(addr)
                .await?
                .get_schema(GetSchemaRequest {})
                .await?
                .into_inner();
            match format {
                Format::Json => println!("{}", json!({ "path": s.path, "source": s.source })),
                Format::Human => {
                    println!("// {}", s.path);
                    print!("{}", s.source);
                }
            }
            Ok(())
        }
    }
}

fn check(file: &std::path::Path, format: Format) -> anyhow::Result<()> {
    let source =
        std::fs::read_to_string(file).with_context(|| format!("cannot read {}", file.display()))?;
    match fold_schema::compile(&source) {
        Err(diagnostics) => {
            if format == Format::Json {
                println!(
                    "{}",
                    json!({ "ok": false, "diagnostics": diagnostics.to_string() })
                );
            } else {
                eprintln!("{diagnostics}");
            }
            std::process::exit(1);
        }
        Ok(schema) => {
            if format == Format::Json {
                let contexts: Vec<serde_json::Value> = schema
                    .contexts
                    .values()
                    .map(|c| {
                        json!({
                            "name": c.name,
                            "events": c.events.keys().collect::<Vec<_>>(),
                            "aggregates": c.aggregates.keys().collect::<Vec<_>>(),
                            "projections": c.projections.keys().collect::<Vec<_>>(),
                        })
                    })
                    .collect();
                println!("{}", json!({ "ok": true, "contexts": contexts }));
            } else {
                println!("ok: {}", file.display());
                for c in schema.contexts.values() {
                    println!("context {}", c.name);
                    for (name, fam) in &c.events {
                        let versions: Vec<String> =
                            fam.versions.keys().map(|v| format!("v{v}")).collect();
                        println!("  event      {name} ({})", versions.join(", "));
                    }
                    for (name, agg) in &c.aggregates {
                        println!(
                            "  aggregate  {name}  stream {}  {} command(s)  {} entity(ies)",
                            agg.stream,
                            agg.commands.len(),
                            agg.entities.len()
                        );
                    }
                    for (name, agg) in &c.aggregates {
                        for inv in agg.invariants.keys() {
                            println!("  invariant  {name}.{inv}  (state)");
                        }
                    }
                    for (name, inv) in &c.invariants {
                        println!(
                            "  invariant  {name}  on {}  projection {}  scope {}",
                            inv.aggregate, inv.projection, inv.scope.name
                        );
                    }
                    for (name, p) in &c.processes {
                        println!(
                            "  process    {name}  key {}  from {}",
                            p.key.name,
                            p.from
                                .iter()
                                .map(|s| format!("{} by {}", s.family, s.by))
                                .collect::<Vec<_>>()
                                .join(", ")
                        );
                    }
                    for (name, p) in &c.projections {
                        println!(
                            "  projection {name}  from {}  tables {}",
                            p.from
                                .iter()
                                .map(|r| format!("{}.{}", r.context, r.name))
                                .collect::<Vec<_>>()
                                .join(", "),
                            p.tables.keys().cloned().collect::<Vec<_>>().join(", ")
                        );
                    }
                }
            }
            Ok(())
        }
    }
}
