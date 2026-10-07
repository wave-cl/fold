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

// Offline checking lands with the fold-schema crate.
fn check(file: &std::path::Path, _format: Format) -> anyhow::Result<()> {
    anyhow::bail!(
        "schema check is not wired up yet (asked for {})",
        file.display()
    )
}
