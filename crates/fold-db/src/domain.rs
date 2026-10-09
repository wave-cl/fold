//! The domain schema the log is written under: loading it from the root
//! file (of any layer; the domain is what the database keeps), and the
//! start-up check of the file against the text stored in the log.

use std::path::Path;
use std::sync::Arc;

use fold_core::{GlobalPosition, Log};
use fold_schema::{DomainSchema, EventFamilyRef, Facts, SchemaDiff, Sources};

/// A loaded domain: the model, its bundle (what the log stores) and the
/// bundle's sha256.
pub struct Loaded {
    pub domain: Arc<DomainSchema>,
    pub source: String,
    pub sha256: String,
}

/// Loads the domain of the schema rooted at `path`. A derivation or
/// application root works too: its domain is taken, and its bundle stored,
/// so the composite daemon and a standalone database agree on the text.
pub fn load(path: &Path) -> anyhow::Result<Loaded> {
    let sources = Sources::load(path)
        .map_err(|e| anyhow::anyhow!("cannot read schema {}: {e}", path.display()))?;
    let source = sources.bundle();
    let domain = sources.compile_domain().map_err(|d| {
        anyhow::anyhow!(
            "schema {} is invalid: {} error(s)\n{d}",
            path.display(),
            d.len()
        )
    })?;
    let sha256 = sha256_hex(&source);
    Ok(Loaded {
        domain,
        source,
        sha256,
    })
}

pub fn sha256_hex(text: &str) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(text.as_bytes());
    crate::codec::hex(&h.finalize())
}

/// What the log holds, for the removals whose cost depends on it.
pub struct LogFacts<'a> {
    pub log: &'a Log,
    pub old: &'a DomainSchema,
}

const PAGE: usize = 256;

impl Facts for LogFacts<'_> {
    fn has_events(&self, family: &EventFamilyRef, version: Option<u16>) -> bool {
        let name = family.to_string();
        let mut from = GlobalPosition(0);
        loop {
            let page = match self.log.read_by_type(&name, from, PAGE) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(%family, error = %e, "schema check: cannot read the log; assuming events exist");
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
        let Some(agg) = self.old.aggregate(context, aggregate) else {
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
    pub diff: SchemaDiff,
    /// One line for Health and the log.
    pub note: String,
}

/// Compares the stored schema text with the new one at the domain layer.
/// `None` when the texts are identical. A stored text that no longer
/// compiles, or a breaking domain diff, is an error unless `force`. Changes
/// in the upper layers of a stored bundle are not the database's concern.
pub fn check(
    log: &Log,
    stored: &str,
    new_text: &str,
    new: &DomainSchema,
    force: bool,
) -> anyhow::Result<Option<Outcome>> {
    if stored == new_text {
        return Ok(None);
    }
    let old = match Sources::from_bundle(stored).compile_domain() {
        Ok(s) => s,
        Err(d) => {
            if force {
                return Ok(Some(Outcome {
                    diff: SchemaDiff::default(),
                    note: format!(
                        "forced: the stored schema no longer compiles ({} error(s))",
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
    let diff = fold_schema::diff_domain(&old, new, &facts);
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
    Ok(Some(Outcome { diff, note }))
}
