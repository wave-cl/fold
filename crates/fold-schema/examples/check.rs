//! Compile a schema file and print its diagnostics or a summary.
//!
//! `cargo run -p fold-schema --example check -- examples/orders/derive.fold`
//!
//! The file may be of any layer; the summary covers the layers it reaches.

use std::process::ExitCode;

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: check <file.fold>");
        return ExitCode::from(2);
    };
    let sources = match fold_schema::Sources::load(&path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    match sources.compile() {
        Ok(compiled) => {
            println!("layer {}", compiled.layer());
            for (ctx, agg) in compiled.domain().aggregates() {
                let aggregate = fold_schema::AggRef::new(&ctx.name, &agg.name);
                let commands = compiled
                    .application()
                    .and_then(|a| a.commands_of(&aggregate))
                    .map_or(0, |c| c.commands.len());
                println!(
                    "aggregate {aggregate}: key {}: {}, stream {}, {} event(s), {} command(s)",
                    agg.key.name,
                    agg.key.ty,
                    agg.stream,
                    agg.events.len(),
                    commands
                );
            }
            if let Some(derivation) = compiled.derivation() {
                for proj in derivation.projections() {
                    println!(
                        "projection {}.{}: from {}, {} table(s)",
                        proj.context,
                        proj.name,
                        proj.from
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", "),
                        proj.tables.len()
                    );
                }
            }
            ExitCode::SUCCESS
        }
        Err(d) => {
            eprintln!("{d}");
            ExitCode::FAILURE
        }
    }
}
