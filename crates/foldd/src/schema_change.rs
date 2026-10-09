//! What happens at start when the schema file differs from the one the log
//! stored: the two are diffed against what the log holds, a breaking
//! change is refused (unless forced), and the actions of a compatible one
//! are applied before any runner starts.

use fold_core::{GlobalPosition, Log};
use fold_schema::{Action, EventFamilyRef, Facts, Schema, SchemaDiff, Sources};

/// What the log holds, read from its indexes.
pub struct LogFacts<'a> {
    pub log: &'a Log,
    /// The schema the log was written under (its stream templates say which
    /// streams are an aggregate's).
    pub old: &'a Schema,
}

impl Facts for LogFacts<'_> {
    fn has_events(&self, family: &EventFamilyRef, version: Option<u16>) -> bool {
        let name = family.to_string();
        let mut from = GlobalPosition(0);
        loop {
            let page = match self.log.read_by_type(&name, from, 256) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(%family, error = %e, "schema check: cannot read events; assuming some exist");
                    return true;
                }
            };
            if page.is_empty() {
                return false;
            }
            match version {
                None => return true,
                Some(v) => {
                    if page.iter().any(|e| e.event_type.version == v) {
                        return true;
                    }
                }
            }
            from = GlobalPosition(page.last().expect("non-empty").position.0 + 1);
        }
    }

    fn has_streams(&self, context: &str, aggregate: &str) -> bool {
        let Some(agg) = self
            .old
            .contexts
            .get(context)
            .and_then(|c| c.aggregates.get(aggregate))
        else {
            return false;
        };
        match self.log.stream_ids() {
            Ok(ids) => ids.iter().any(|s| agg.stream.matches(s).is_some()),
            Err(e) => {
                tracing::warn!(%context, %aggregate, error = %e, "schema check: cannot list streams; assuming some exist");
                true
            }
        }
    }
}

/// The result of a check that did not refuse.
pub struct Outcome {
    /// The schema the log was written under, when it still compiles.
    pub old: Option<Schema>,
    pub diff: SchemaDiff,
    /// One line for Health and the log.
    pub note: String,
}

/// Compares the stored schema text with the new one. `None` when the texts
/// are identical. A stored text that no longer compiles, or a breaking
/// diff, is an error unless `force`.
pub fn check(
    log: &Log,
    stored: &str,
    new_text: &str,
    new: &Schema,
    force: bool,
) -> anyhow::Result<Option<Outcome>> {
    if stored == new_text {
        return Ok(None);
    }
    let old = match Sources::from_bundle(stored).compile() {
        Ok(s) => s,
        Err(d) => {
            if force {
                return Ok(Some(Outcome {
                    old: None,
                    diff: SchemaDiff::default(),
                    note: format!(
                        "forced: the stored schema no longer compiles ({} error(s)); nothing was rebuilt",
                        d.len()
                    ),
                }));
            }
            anyhow::bail!(
                "the schema stored in the log no longer compiles under this version of fold ({} error(s)):\n{d}\nstart with --force-schema to replace it without a compatibility check",
                d.len()
            );
        }
    };
    let facts = LogFacts { log, old: &old };
    let diff = fold_schema::diff_with(&old, new, &facts);
    if diff.has_breaking() && !force {
        anyhow::bail!(
            "the schema changed in a way that breaks data in the log:\n{diff}\nstart with --force-schema to apply it anyway",
        );
    }
    let note = if diff.is_empty() {
        "textual change only: stored the new text".to_string()
    } else if diff.has_breaking() {
        format!("forced over a breaking change: {}", diff.summary())
    } else {
        format!("applied: {}", diff.summary())
    };
    Ok(Some(Outcome {
        old: Some(old),
        diff,
        note,
    }))
}

/// Carries out the diff's actions on the log's derived data, then stores
/// the new text.
pub fn apply(log: &Log, outcome: &Outcome, new: &Schema, new_text: &str) -> anyhow::Result<()> {
    let models = log.read_models();
    for action in outcome.diff.actions() {
        tracing::info!(?action, "schema change");
        match &action {
            Action::None => {}
            Action::RebuildProjection { context, name } => {
                let full = format!("{context}.{name}");
                let mut tables: Vec<String> = Vec::new();
                for schema in [outcome.old.as_ref(), Some(new)].into_iter().flatten() {
                    if let Some(p) = schema
                        .contexts
                        .get(context)
                        .and_then(|c| c.projections.get(name))
                    {
                        tables.extend(p.tables.keys().cloned());
                    }
                }
                tables.sort();
                tables.dedup();
                let refs: Vec<&str> = tables.iter().map(String::as_str).collect();
                models.reset(&full, &refs)?;
            }
            Action::RebuildProcess { context, name } | Action::DropProcess { context, name } => {
                models.reset(&format!("{context}.{name}"), &crate::process::TABLES)?;
            }
            Action::DropProjection {
                context,
                name,
                tables,
            } => {
                let refs: Vec<&str> = tables.iter().map(String::as_str).collect();
                models.reset(&format!("{context}.{name}"), &refs)?;
            }
            Action::DropTable {
                context,
                projection,
                table,
            } => {
                models.drop_tables(&format!("{context}.{projection}"), &[table.as_str()])?;
            }
            Action::ClearAggregateSnapshots { context, name }
            | Action::DropAggregate { context, name } => {
                log.snapshots().clear(&format!("{context}.{name}"))?;
            }
            Action::DropTimer {
                context,
                process,
                timer,
            } => {
                let full = format!("{context}.{process}");
                let rows = models.snapshot()?.scan(
                    &full,
                    crate::process::TIMERS_TABLE,
                    &[],
                    usize::MAX,
                )?;
                let doomed: Vec<(String, Vec<u8>)> = rows
                    .into_iter()
                    .filter(|(_, v)| {
                        serde_json::from_slice::<crate::process::TimerRow>(v)
                            .is_ok_and(|r| r.name == *timer)
                    })
                    .map(|(k, _)| (crate::process::TIMERS_TABLE.to_string(), k))
                    .collect();
                if !doomed.is_empty() {
                    models.delete_rows(&full, &doomed)?;
                }
            }
        }
    }
    log.set_schema_source(new_text)?;
    Ok(())
}
