//! Compile a schema file and print its diagnostics or a summary.
//!
//! `cargo run -p fold-schema --example check -- examples/orders/schema.fold`

use std::process::ExitCode;

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: check <schema.fold>");
        return ExitCode::from(2);
    };
    match fold_schema::Schema::from_file(&path) {
        Ok(schema) => {
            for (ctx, agg) in schema.aggregates() {
                println!(
                    "aggregate {}.{}: key {}: {}, stream {}, {} event(s), {} command(s)",
                    ctx.name,
                    agg.name,
                    agg.key.name,
                    agg.key.ty,
                    agg.stream,
                    agg.events.len(),
                    agg.commands.len()
                );
            }
            for (ctx, proj) in schema.projections() {
                println!(
                    "projection {}.{}: from {}, {} table(s)",
                    ctx.name,
                    proj.name,
                    proj.from
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    proj.tables.len()
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
