use anyhow::Context as _;
use clap::Args as ClapArgs;
use serde_json::json;

use crate::output::Format;

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Directory to create the log in.
    pub dir: std::path::PathBuf,
    /// The schema file to load into it.
    #[arg(long)]
    pub schema: std::path::PathBuf,
}

/// Validates the schema (and its imports), creates the log directory and
/// records the schema bundle in it, and writes `foldd.toml` so
/// `foldd -c <dir>/foldd.toml` runs.
pub fn run(args: Args, format: Format) -> anyhow::Result<()> {
    let sources = fold_schema::Sources::load(&args.schema)
        .with_context(|| format!("cannot read {}", args.schema.display()))?;
    let schema = sources.compile().map_err(|d| anyhow::anyhow!("{d}"))?;
    let source = sources.bundle();

    let data_dir = args.dir.join("data");
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("cannot create {}", data_dir.display()))?;
    let log = fold_core::Log::create(
        &data_dir,
        foldd_log_name(),
        fold_core::OpenOptions::default(),
    )
    .with_context(|| format!("cannot create a log in {}", data_dir.display()))?;
    log.set_schema_source(&source)?;
    drop(log);

    let schema_abs = std::fs::canonicalize(&args.schema)?;
    let config = format!(
        "# written by `fold init`\ndata_dir = {:?}\nschema = {:?}\nlisten = \"127.0.0.1:4141\"\n",
        data_dir.display().to_string(),
        schema_abs.display().to_string()
    );
    let config_path = args.dir.join("foldd.toml");
    std::fs::write(&config_path, config)?;

    let (contexts, aggregates, projections) = (
        schema.contexts.len(),
        schema.aggregates().count(),
        schema.projections().count(),
    );
    match format {
        Format::Json => println!(
            "{}",
            json!({ "data_dir": data_dir, "config": config_path, "contexts": contexts, "aggregates": aggregates, "projections": projections })
        ),
        Format::Human => {
            println!("created log in {}", data_dir.display());
            println!(
                "schema: {contexts} context(s), {aggregates} aggregate(s), {projections} projection(s)"
            );
            println!(
                "start the daemon with:\n  foldd -c {}",
                config_path.display()
            );
        }
    }
    Ok(())
}

fn foldd_log_name() -> &'static str {
    "default"
}
