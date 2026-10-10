use anyhow::Context as _;
use clap::Subcommand;
use fold_proto::common::v1::GetSchemaRequest;
use serde_json::json;

use crate::client::{self, Addrs};
use crate::output::Format;

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Parse and validate a schema file and everything it imports (offline).
    Check {
        /// Path to the root .fold file.
        file: std::path::PathBuf,
    },
    /// Print the bundle a running node loaded (GetSchema on that layer's
    /// admin service).
    Show {
        /// Which node to ask: the application node holds every layer, the
        /// derivation node its own and the domain, the database the domain.
        #[arg(long, value_enum, default_value_t = ShowLayer::Application)]
        layer: ShowLayer,
    },
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

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShowLayer {
    Domain,
    Derivation,
    Application,
}

pub async fn run(cmd: Cmd, addrs: &Addrs, format: Format) -> anyhow::Result<()> {
    match cmd {
        Cmd::Check { file } => check(&file, format),
        Cmd::Fmt { files, check } => fmt(&files, check, format),
        Cmd::Diff { old, new } => diff(&old, &new, format),
        Cmd::Show { layer } => {
            let s = match layer {
                ShowLayer::Domain => client::schema(addrs)
                    .await?
                    .get_schema(GetSchemaRequest {})
                    .await?
                    .into_inner(),
                ShowLayer::Derivation => client::derive_admin(addrs)
                    .await?
                    .get_schema(GetSchemaRequest {})
                    .await?
                    .into_inner(),
                ShowLayer::Application => client::app_admin(addrs)
                    .await?
                    .get_schema(GetSchemaRequest {})
                    .await?
                    .into_inner(),
            };
            match format {
                Format::Json => println!(
                    "{}",
                    json!({ "path": s.path, "layer": s.layer, "sha256": s.sha256, "source": s.source })
                ),
                Format::Human => {
                    println!("// {} ({} layer, sha256 {})", s.path, s.layer, s.sha256);
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

/// A root `.fold` file (with imports) or a stored bundle, compiled to the
/// schema of its layer.
fn load_schema_or_bundle(path: &std::path::Path) -> anyhow::Result<fold_schema::Compiled> {
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
    use fold_schema::{AssumeData, Compiled};
    let (old_schema, new_schema) = (load_schema_or_bundle(old)?, load_schema_or_bundle(new)?);
    // Two roots of one layer compare that layer and the ones below it.
    let diff = match (&old_schema, &new_schema) {
        (Compiled::Domain(o), Compiled::Domain(n)) => fold_schema::diff_domain(o, n, &AssumeData),
        (Compiled::Derivation(o), Compiled::Derivation(n)) => {
            fold_schema::diff_derivation(o, n, &AssumeData)
        }
        (o, n) => anyhow::bail!(
            "the files are of different layers: {} is `layer {}`, {} is `layer {}`",
            old.display(),
            o.layer(),
            new.display(),
            n.layer()
        ),
    };
    match format {
        Format::Json => println!(
            "{}",
            json!({
                "layer": old_schema.layer().to_string(),
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
        Ok(compiled) => {
            let layer = compiled.layer();
            let domain = compiled.domain();
            let derivation = compiled.derivation();
            let docs = match &compiled {
                fold_schema::Compiled::Domain(d) => &d.docs,
                fold_schema::Compiled::Derivation(d) => &d.docs,
            };
            if format == Format::Json {
                let contexts: Vec<serde_json::Value> = domain
                    .contexts
                    .values()
                    .map(|c| {
                        json!({
                            "name": c.name,
                            "docs": c.docs.join("\n"),
                            "events": c.events.keys().collect::<Vec<_>>(),
                            "aggregates": c.aggregates.keys().collect::<Vec<_>>(),
                        })
                    })
                    .collect();
                let states: Vec<String> = derivation
                    .map(|d| d.states.keys().map(ToString::to_string).collect())
                    .unwrap_or_default();
                let projections: Vec<String> = derivation
                    .map(|d| d.projections.keys().map(ToString::to_string).collect())
                    .unwrap_or_default();
                println!(
                    "{}",
                    json!({
                        "ok": true,
                        "layer": layer.to_string(),
                        "files": files,
                        "docs": docs.join("\n"),
                        "contexts": contexts,
                        "states": states,
                        "projections": projections,
                    })
                );
                return Ok(());
            }
            if files.len() > 1 {
                println!("files: {}", files.join(", "));
            }
            println!("ok: {}", file.display());
            println!("layer {layer}");
            for d in docs {
                println!("//! {d}");
            }
            for c in domain.contexts.values() {
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
                        "  aggregate  {name}  stream {}  {} event(s)  {} entity(ies){}",
                        agg.stream,
                        agg.events.len(),
                        agg.entities.len(),
                        doc_note(&agg.docs)
                    );
                }
            }
            if let Some(d) = derivation {
                for st in d.states.values() {
                    println!(
                        "state       {}  {} field(s)  snapshot every {}{}",
                        st.aggregate,
                        st.fields.len(),
                        st.snapshot_every,
                        doc_note(&st.docs)
                    );
                }
                for p in d.projections() {
                    println!(
                        "projection  {}.{}  from {}  tables {}{}",
                        p.context,
                        p.name,
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
            Ok(())
        }
    }
}
