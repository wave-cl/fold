use clap::Args as ClapArgs;

use crate::output::Format;

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Directory to create the log in.
    pub dir: std::path::PathBuf,
    /// The schema file to load into it.
    #[arg(long)]
    pub schema: std::path::PathBuf,
}

// Offline initialisation lands with the fold-core and fold-schema crates.
pub fn run(args: Args, _format: Format) -> anyhow::Result<()> {
    anyhow::bail!(
        "init is not wired up yet (asked for {} with {})",
        args.dir.display(),
        args.schema.display()
    )
}
