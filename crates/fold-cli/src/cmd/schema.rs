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
    /// Rewrite schema files in canonical layout, keeping every comment
    /// (offline). Only the named files are formatted.
    Fmt {
        /// Paths to .fold files.
        #[arg(required = true)]
        files: Vec<std::path::PathBuf>,
        /// Report files that would change and exit 1 instead of writing.
        #[arg(long)]
        check: bool,
    },
}

pub async fn run(cmd: Cmd, addr: &str, format: Format) -> anyhow::Result<()> {
    match cmd {
        Cmd::Check { file } => check(&file, format),
        Cmd::Fmt { files, check } => fmt(&files, check, format),
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

fn fmt(files: &[std::path::PathBuf], check_only: bool, format: Format) -> anyhow::Result<()> {
    let mut would_change = false;
    for file in files {
        let source = std::fs::read_to_string(file)
            .with_context(|| format!("cannot read {}", file.display()))?;
        let formatted = match fold_schema::fmt::format_source(&source) {
            Ok(f) => f,
            Err(e) => {
                let diagnostics = fold_schema::Diagnostics::from_parse_error(&source, &e);
                if format == Format::Json {
                    println!(
                        "{}",
                        json!({ "file": file, "ok": false, "diagnostics": diagnostics.to_string() })
                    );
                } else {
                    eprintln!("{}: {diagnostics}", file.display());
                }
                std::process::exit(1);
            }
        };
        let changed = formatted != source;
        if check_only {
            would_change |= changed;
            match format {
                Format::Json => println!(
                    "{}",
                    json!({ "file": file, "ok": true, "formatted": !changed })
                ),
                Format::Human if changed => println!("would reformat {}", file.display()),
                Format::Human => {}
            }
            continue;
        }
        if changed {
            // Write beside the file, then rename, so a crash leaves the
            // original intact.
            let tmp = file.with_extension("fold.tmp");
            std::fs::write(&tmp, &formatted)
                .with_context(|| format!("cannot write {}", tmp.display()))?;
            std::fs::rename(&tmp, file)
                .with_context(|| format!("cannot replace {}", file.display()))?;
        }
        match format {
            Format::Json => println!(
                "{}",
                json!({ "file": file, "ok": true, "formatted": true, "changed": changed })
            ),
            Format::Human => println!(
                "{} {}",
                if changed { "formatted" } else { "unchanged" },
                file.display()
            ),
        }
    }
    if would_change {
        std::process::exit(1);
    }
    Ok(())
}

/// The first doc line of a declaration, for a listing.
fn doc_note(docs: &[String]) -> String {
    docs.first()
        .filter(|d| !d.trim().is_empty())
        .map(|d| format!("  -- {}", d.trim()))
        .unwrap_or_default()
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
                            "docs": c.docs.join("\n"),
                            "events": c.events.keys().collect::<Vec<_>>(),
                            "aggregates": c.aggregates.keys().collect::<Vec<_>>(),
                            "projections": c.projections.keys().collect::<Vec<_>>(),
                        })
                    })
                    .collect();
                println!(
                    "{}",
                    json!({ "ok": true, "docs": schema.docs.join("\n"), "contexts": contexts })
                );
            } else {
                println!("ok: {}", file.display());
                for d in &schema.docs {
                    println!("//! {d}");
                }
                for c in schema.contexts.values() {
                    println!("context {}{}", c.name, doc_note(&c.docs));
                    for (name, fam) in &c.events {
                        let versions: Vec<String> =
                            fam.versions.keys().map(|v| format!("v{v}")).collect();
                        println!(
                            "  event      {name} ({}){}",
                            versions.join(", "),
                            doc_note(&fam.latest().docs)
                        );
                    }
                    for (name, agg) in &c.aggregates {
                        println!(
                            "  aggregate  {name}  stream {}  {} command(s)  {} entity(ies){}",
                            agg.stream,
                            agg.commands.len(),
                            agg.entities.len(),
                            doc_note(&agg.docs)
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
                            "  projection {name}  from {}  tables {}{}",
                            p.from
                                .iter()
                                .map(|r| format!("{}.{}", r.context, r.name))
                                .collect::<Vec<_>>()
                                .join(", "),
                            p.tables.keys().cloned().collect::<Vec<_>>().join(", "),
                            doc_note(&p.docs)
                        );
                    }
                }
            }
            Ok(())
        }
    }
}
