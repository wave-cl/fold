//! `fold-schema`: the domain schema language of fold.
//!
//! A schema is layered: a `layer domain` file declares bounded contexts
//! with events, values, enums and aggregates (their key, stream, entities
//! and local types); a `layer derivation` file imports it and adds aggregate
//! states and projections; a `layer application` file imports that and adds
//! commands, invariants and processes. This crate parses each ([`parse`]),
//! resolves them into a [`DomainSchema`], [`DerivationSchema`] or
//! [`ApplicationSchema`] with every rule checked ([`compile`]),
//! validates and canonicalizes JSON payloads against the resolved types,
//! applies typed column operations to read-model rows ([`rows`]) and prints
//! a syntax tree back to canonical source ([`fmt::format`]).

pub mod ast;
pub mod diag;
pub mod diff;
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

pub use ast::Layer;
pub use diag::{Diagnostic, Diagnostics, Error, Section};
pub use diff::{
    Action, AssumeData, Change, ChangeKind, Compatibility, Facts, SchemaDiff, diff,
    diff_application, diff_derivation, diff_domain, diff_with,
};
pub use model::{
    AggRef, Aggregate, AggregateCommands, AggregateState, ApplicationSchema, Command, Compiled,
    Context, ContextInvariant, DeclarativeUpcast, DerivationSchema, DomainSchema, Entity, EnumType,
    EnumVariant, EventFamily, EventFamilyRef, EventRefError, EventType, EventTypeId, Field, Guard,
    InvariantCheck, OperandKind, Pattern, Process, ProcessSource, Projection, ProjectionRef,
    RESERVED_CONTEXT, Rule, RuleExpr, RuleOp, RulePath, RuleTerm, Schema, StateInvariant,
    TIMER_FIRED_EVENT, Table, Upcast, UpcastHow, ValueType, WasmRef, parse_event_ref,
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

/// Parse and resolve `src` as an application schema: either one
/// `layer application` file or a bundle (`// ---- file: path` sections,
/// see [`Sources::from_bundle`]) whose root is one. A syntax error is
/// reported as the single diagnostic `P001`; resolution reports every rule
/// violation at once.
pub fn compile(src: &str) -> Result<Schema, Diagnostics> {
    Sources::from_bundle(src)
        .compile_application()
        .map(std::sync::Arc::unwrap_or_clone)
}

/// Parse and resolve `src` (one file or a bundle) at whatever layer its
/// root declares.
pub fn compile_any(src: &str) -> Result<Compiled, Diagnostics> {
    Sources::from_bundle(src).compile()
}
