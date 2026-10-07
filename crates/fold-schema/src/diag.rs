//! Diagnostics produced by [`crate::compile`] and the crate's top-level error.

use std::fmt;
use std::path::PathBuf;

use crate::parser::ParseError;
use crate::span::{Span, line_col, source_line};

/// One problem found in a schema. `code` is `P001` for a syntax error and
/// `S001`.. for a resolution rule (see `resolve.rs` for the list).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostic {
    pub code: &'static str,
    pub span: Span,
    pub message: String,
}

/// Every diagnostic of one compilation, with the source kept so they render
/// as `line:col: code: message` followed by the offending line.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Diagnostics {
    pub diagnostics: Vec<Diagnostic>,
    pub source: String,
}

impl Diagnostics {
    pub fn new(source: &str, mut diagnostics: Vec<Diagnostic>) -> Self {
        diagnostics.sort_by(|a, b| (a.span.start, a.code).cmp(&(b.span.start, b.code)));
        Diagnostics {
            diagnostics,
            source: source.to_string(),
        }
    }

    pub fn from_parse_error(source: &str, err: &ParseError) -> Self {
        Diagnostics::new(
            source,
            vec![Diagnostic {
                code: "P001",
                span: err.span,
                message: err.to_string(),
            }],
        )
    }

    pub fn iter(&self) -> impl Iterator<Item = &Diagnostic> {
        self.diagnostics.iter()
    }

    pub fn len(&self) -> usize {
        self.diagnostics.len()
    }

    pub fn is_empty(&self) -> bool {
        self.diagnostics.is_empty()
    }

    /// All codes, in rendering order.
    pub fn codes(&self) -> Vec<&'static str> {
        self.diagnostics.iter().map(|d| d.code).collect()
    }
}

impl std::ops::Deref for Diagnostics {
    type Target = [Diagnostic];

    fn deref(&self) -> &Self::Target {
        &self.diagnostics
    }
}

impl<'a> IntoIterator for &'a Diagnostics {
    type Item = &'a Diagnostic;
    type IntoIter = std::slice::Iter<'a, Diagnostic>;

    fn into_iter(self) -> Self::IntoIter {
        self.diagnostics.iter()
    }
}

impl fmt::Display for Diagnostics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, d) in self.diagnostics.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            let (line, col) = line_col(&self.source, d.span.start);
            writeln!(f, "{line}:{col}: {}: {}", d.code, d.message)?;
            let text = source_line(&self.source, d.span.start);
            writeln!(f, "  | {text}")?;
            let width = (d.span.end.saturating_sub(d.span.start)).max(1);
            // Clamp the marker to the line so a multi-line span does not run on.
            let width = width.min(text.len().saturating_sub(col - 1).max(1));
            write!(f, "  | {}{}", " ".repeat(col - 1), "^".repeat(width))?;
        }
        Ok(())
    }
}

impl std::error::Error for Diagnostics {}

/// The error of [`crate::Schema::from_file`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cannot read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{path}: schema has {} error(s)\n{diagnostics}", diagnostics.len())]
    Compile {
        path: PathBuf,
        diagnostics: Diagnostics,
    },
}
