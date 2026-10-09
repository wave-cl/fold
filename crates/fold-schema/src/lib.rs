//! `fold-schema`: the domain schema language of fold.
//!
//! A `.fold` file declares bounded contexts with events, values, enums,
//! aggregates (with their entities, local types, state and commands) and
//! projections (with their tables). This crate parses it ([`parse`]),
//! resolves it into a [`Schema`] with every rule checked ([`compile`]),
//! validates and canonicalizes JSON payloads against the resolved types,
//! applies typed column operations to read-model rows ([`rows`]) and prints
//! a syntax tree back to canonical source ([`fmt::format`]).

pub mod ast;
pub mod diag;
pub mod fmt;
pub mod lexer;
pub mod model;
pub mod parser;
pub mod resolve;
pub mod rows;
pub mod source;
pub mod span;
pub mod template;
pub mod types;
pub mod upcast;
pub mod validate;

pub use diag::{Diagnostic, Diagnostics, Error, Section};
pub use model::{
    Aggregate, Command, Context, ContextInvariant, DeclarativeUpcast, Entity, EnumType,
    EnumVariant, EventFamily, EventFamilyRef, EventRefError, EventType, EventTypeId, Field, Guard,
    InvariantCheck, OperandKind, Pattern, Process, ProcessSource, Projection, ProjectionRef, Rule,
    RuleExpr, RuleOp, RulePath, RuleTerm, Schema, StateInvariant, Table, Upcast, UpcastHow,
    ValueType, WasmRef, parse_event_ref,
};
pub use parser::ParseError;
pub use rows::{ColumnOp, RowError, TruncateFrom};
pub use source::{FsLoader, Loader, MapLoader, SourceFile, Sources};
pub use span::Span;
pub use template::{StreamTemplate, TemplateError};
pub use types::{Scalar, Type, TypeRef};
pub use validate::{ScalarKey, ValidationError, rules};

/// Parse `src` into a syntax tree without resolving names.
pub fn parse(src: &str) -> Result<ast::File, ParseError> {
    parser::parse(src)
}

/// Parse and resolve `src`. A syntax error is reported as the single
/// diagnostic `P001`; resolution reports every rule violation at once.
pub fn compile(src: &str) -> Result<Schema, Diagnostics> {
    Sources::single(src).compile()
}
