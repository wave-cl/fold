//! The application schema this store's process tables were derived under,
//! against the one the node starts with: process rebuilds and drops follow
//! the diff.

use fold_schema::{Action, ApplicationSchema, Sources};
use fold_store::DerivedStore;

pub fn check(
    store: &DerivedStore,
    new_text: &str,
    new: &ApplicationSchema,
    force: bool,
) -> anyhow::Result<Option<String>> {
    let Some(stored) = store.schema_source()? else {
        store.set_schema_source(new_text)?;
        return Ok(None);
    };
    if stored == new_text {
        return Ok(None);
    }
    let old = match Sources::from_bundle(&stored).compile_application() {
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
    let diff = fold_schema::diff_application(&old, new, &fold_schema::AssumeData);
    for action in diff.actions_for(fold_schema::Layer::Application) {
        tracing::info!(?action, "schema change");
        match &action {
            Action::RebuildProcess { context, name } | Action::DropProcess { context, name } => {
                store.reset(&format!("{context}.{name}"), &crate::process::TABLES)?;
            }
            Action::DropTimer {
                context,
                process,
                timer,
            } => {
                let name = format!("{context}.{process}");
                let rows =
                    store
                        .snapshot()?
                        .scan(&name, crate::process::TIMERS_TABLE, &[], 1 << 20)?;
                let mut doomed = Vec::new();
                for (key, bytes) in rows {
                    if let Ok(row) = serde_json::from_slice::<crate::process::TimerRow>(&bytes)
                        && row.name == *timer
                    {
                        doomed.push((crate::process::TIMERS_TABLE.to_string(), key));
                    }
                }
                if !doomed.is_empty()
                    && let Some(cp) = store.checkpoint(&name)?
                {
                    store.commit(&name, cp, vec![], doomed)?;
                }
            }
            _ => {}
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
