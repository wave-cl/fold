use anyhow::Context as _;
use clap::Subcommand;
use fold_proto::v1::GetSchemaRequest;
use serde_json::json;

use crate::client;
use crate::output::Format;

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Parse and validate a schema file and everything it imports (offline).
    Check {
        /// Path to the root .fold file.
        file: std::path::PathBuf,
    },
    /// Print the schema the running daemon loaded (Admin.GetSchema).
    Show,
    /// Classify every change from one schema to another (offline): compatible,
    /// needs a rebuild, or breaking (exit 1). Either file may be a root
    /// `.fold` with imports or a bundle as `schema show` prints it.
    Diff {
        old: std::path::PathBuf,
        new: std::path::PathBuf,
    },
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
        Cmd::Diff { old, new } => diff(&old, &new, format),
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

/// A root `.fold` file (with imports) or a stored bundle.
fn load_schema_or_bundle(path: &std::path::Path) -> anyhow::Result<fold_schema::Schema> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    let sources = if text.starts_with(fold_schema::source::BUNDLE_MARKER) {
        fold_schema::Sources::from_bundle(&text)
    } else {
        fold_schema::Sources::load(path)?
    };
    sources
        .compile()
        .map_err(|d| anyhow::anyhow!("{}: {} error(s)\n{d}", path.display(), d.len()))
}

fn diff(old: &std::path::Path, new: &std::path::Path, format: Format) -> anyhow::Result<()> {
    let (old_schema, new_schema) = (load_schema_or_bundle(old)?, load_schema_or_bundle(new)?);
    let diff = fold_schema::diff(&old_schema, &new_schema);
    match format {
        Format::Json => println!(
            "{}",
            json!({
                "breaking": diff.has_breaking(),
                "summary": diff.summary(),
                "changes": diff.changes,
                "actions": diff.actions(),
            })
        ),
        Format::Human => println!("{diff}"),
    }
    if diff.has_breaking() {
        std::process::exit(1);
    }
    Ok(())
}

fn check(file: &std::path::Path, format: Format) -> anyhow::Result<()> {
    let sources = fold_schema::Sources::load(file)?;
    let files: Vec<&str> = sources.files().iter().map(|f| f.path.as_str()).collect();
    match sources.compile() {
        Err(diagnostics) => {
            if format == Format::Json {
                println!(
                    "{}",
                    json!({ "ok": false, "files": files, "diagnostics": diagnostics.to_string() })
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
                    json!({ "ok": true, "files": files, "docs": schema.docs.join("\n"), "contexts": contexts })
                );
            } else {
                if files.len() > 1 {
                    println!("files: {}", files.join(", "));
                }
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
                        for (iname, inv) in &agg.invariants {
                            match &inv.check {
                                fold_schema::InvariantCheck::Wasm(_) => {
                                    println!("  invariant  {name}.{iname}  (state, wasm)")
                                }
                                fold_schema::InvariantCheck::Expr { text, .. } => {
                                    println!("  invariant  {name}.{iname}  (state) {text}")
                                }
                            }
                        }
                        for (cname, cmd) in &agg.commands {
                            for g in &cmd.requires {
                                println!("  requires   {name}.{cname}.{}  {}", g.name, g.text);
                            }
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
