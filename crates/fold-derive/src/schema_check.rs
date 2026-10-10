//! The derivation schema this store's tables were derived under, against
//! the one the node starts with: rebuilds and drops follow the diff.

use fold_schema::{Action, DerivationSchema, Sources};
use fold_store::DerivedStore;

/// Compares the stored text with the new one; applies the derivation
/// layer's actions and stores the new text. Returns Health's note when the
/// text changed. A stored text that no longer compiles is an error unless
/// `force`.
pub fn check(
    store: &DerivedStore,
    new_text: &str,
    new: &DerivationSchema,
    force: bool,
) -> anyhow::Result<Option<String>> {
    let Some(stored) = store.schema_source()? else {
        store.set_schema_source(new_text)?;
        return Ok(None);
    };
    if stored == new_text {
        return Ok(None);
    }
    let old = match Sources::from_bundle(&stored).compile_derivation() {
        Ok(s) => s,
        Err(d) => {
            if force {
                store.set_schema_source(new_text)?;
                return Ok(Some(format!(
                    "forced: the stored schema no longer compiles ({} error(s)); nothing was rebuilt",
                    d.len()
                )));
            }
            anyhow::bail!(
                "the schema this store was derived under no longer compiles under this version of fold ({} error(s)):\n{d}\nstart with --force-schema to replace it without a compatibility check",
                d.len()
            );
        }
    };
    let diff = fold_schema::diff_derivation(&old, new, &fold_schema::AssumeData);
    for action in diff.actions_for(fold_schema::Layer::Derivation) {
        tracing::info!(?action, "schema change");
        match &action {
            Action::RebuildProjection { context, name } => {
                let full = format!("{context}.{name}");
                let mut tables: Vec<String> = Vec::new();
                for schema in [&*old, new] {
                    if let Some(p) = schema.projection(context, name) {
                        tables.extend(p.tables.keys().cloned());
                    }
                }
                tables.sort();
                tables.dedup();
                let refs: Vec<&str> = tables.iter().map(String::as_str).collect();
                store.reset(&full, &refs)?;
            }
            Action::DropProjection {
                context,
                name,
                tables,
            } => {
                let full = format!("{context}.{name}");
                let refs: Vec<&str> = tables.iter().map(String::as_str).collect();
                store.reset(&full, &refs)?;
                store.drop_tables(&full, &refs)?;
            }
            Action::DropTable {
                context,
                projection,
                table,
            } => {
                store.drop_tables(&format!("{context}.{projection}"), &[table.as_str()])?;
            }
            Action::ClearAggregateSnapshots { context, name }
            | Action::DropAggregate { context, name } => {
                store.snapshots().clear(&format!("{context}.{name}"))?;
            }
            Action::None => {}
        }
    }
    store.set_schema_source(new_text)?;
    let note = if diff.is_empty() {
        "textual change only: stored the new text".to_string()
    } else {
        format!("applied: {}", diff.summary())
    };
    Ok(Some(note))
}
