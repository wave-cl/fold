//! The syntax tree produced by [`crate::parse`]. Every node carries the span of
//! the source it came from; [`File::strip_spans`] zeroes them so two trees can be
//! compared structurally (the formatter round-trip test does this).

use crate::span::Span;
use crate::types::Scalar;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ident {
    pub name: String,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StrLit {
    pub value: String,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntLit {
    pub value: u64,
    pub span: Span,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct File {
    pub contexts: Vec<Context>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Context {
    pub name: Ident,
    pub items: Vec<Item>,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Item {
    Value(ValueDecl),
    Enum(EnumDecl),
    Event(EventDecl),
    Aggregate(Box<AggregateDecl>),
    Projection(ProjectionDecl),
    Invariant(InvariantDecl),
    Process(ProcessDecl),
}

/// A process manager: reacts to events, keeps state per correlation key,
/// issues commands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessDecl {
    pub name: Ident,
    /// The correlation key: its name is looked up in each event unless the
    /// source says `by`.
    pub key: Field,
    pub from: Vec<ProcessSource>,
    pub state: Vec<Field>,
    pub react: WasmRef,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessSource {
    pub event: EventRef,
    /// The event field carrying the correlation key, when not named like it.
    pub by: Option<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueDecl {
    pub name: Ident,
    pub fields: Vec<Field>,
    /// Rules every instance must satisfy, checked wherever one is created.
    pub rules: Vec<RuleDecl>,
    pub span: Span,
}

/// `Name: expr` inside a value's `rules { ... }`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuleDecl {
    pub name: Ident,
    pub expr: Expr,
    pub span: Span,
}

/// A rule expression. Precedence, lowest first: `or`, `and`, `not`,
/// comparison; parentheses group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expr {
    Or(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Cmp {
        lhs: Term,
        op: CmpOp,
        rhs: Term,
        span: Span,
    },
    /// `path matches "regex"`.
    Matches {
        path: FieldPath,
        pattern: StrLit,
        span: Span,
    },
    /// `path in [lit, lit, ...]`.
    In {
        path: FieldPath,
        items: Vec<Literal>,
        span: Span,
    },
}

impl Expr {
    pub fn span(&self) -> Span {
        match self {
            Expr::Or(a, b) | Expr::And(a, b) => a.span().join(b.span()),
            Expr::Not(e) => e.span(),
            Expr::Cmp { span, .. } | Expr::Matches { span, .. } | Expr::In { span, .. } => *span,
        }
    }

    fn strip_spans(&mut self) {
        match self {
            Expr::Or(a, b) | Expr::And(a, b) => {
                a.strip_spans();
                b.strip_spans();
            }
            Expr::Not(e) => e.strip_spans(),
            Expr::Cmp { lhs, rhs, span, .. } => {
                lhs.strip_spans();
                rhs.strip_spans();
                *span = Span::default();
            }
            Expr::Matches {
                path,
                pattern,
                span,
            } => {
                path.strip_spans();
                pattern.strip();
                *span = Span::default();
            }
            Expr::In { path, items, span } => {
                path.strip_spans();
                for i in items {
                    i.strip_spans();
                }
                *span = Span::default();
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
}

impl CmpOp {
    pub fn as_str(self) -> &'static str {
        match self {
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
            CmpOp::Eq => "==",
            CmpOp::Ne => "!=",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Term {
    Lit(Literal),
    Path(FieldPath),
    /// `len(path)`: characters of a string, elements of a collection.
    Len(FieldPath, Span),
}

impl Term {
    pub fn span(&self) -> Span {
        match self {
            Term::Lit(l) => l.span(),
            Term::Path(p) => p.span,
            Term::Len(_, s) => *s,
        }
    }

    fn strip_spans(&mut self) {
        match self {
            Term::Lit(l) => l.strip_spans(),
            Term::Path(p) => p.strip_spans(),
            Term::Len(p, s) => {
                p.strip_spans();
                *s = Span::default();
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Literal {
    /// Integer or decimal text, with an optional leading `-`.
    Number(String, Span),
    Str(StrLit),
    Bool(bool, Span),
}

impl Literal {
    pub fn span(&self) -> Span {
        match self {
            Literal::Number(_, s) | Literal::Bool(_, s) => *s,
            Literal::Str(l) => l.span,
        }
    }

    fn strip_spans(&mut self) {
        match self {
            Literal::Number(_, s) | Literal::Bool(_, s) => *s = Span::default(),
            Literal::Str(l) => l.strip(),
        }
    }
}

/// `a.b.c`: a field, descending through nested values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldPath {
    pub segments: Vec<Ident>,
    pub span: Span,
}

impl FieldPath {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        for s in &mut self.segments {
            s.strip();
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnumDecl {
    pub name: Ident,
    pub variants: Vec<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventDecl {
    pub name: Ident,
    /// The `vN` token; `value` is `N`.
    pub version: IntLit,
    pub fields: Vec<Field>,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: Ident,
    pub ty: Type,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Type {
    pub base: BaseType,
    pub optional: bool,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BaseType {
    Scalar(Scalar),
    Ref(TypeRefSyntax),
    /// Both `[T]` and `list<T>`.
    List(Box<Type>),
    Set(Scalar),
    Map(Scalar, Box<Type>),
}

/// `Name` or `Qualifier.Name` as written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeRefSyntax {
    pub qualifier: Option<Ident>,
    pub name: Ident,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateDecl {
    pub name: Ident,
    pub key: Field,
    pub stream: StrLit,
    pub items: Vec<LocalItem>,
    pub events: Vec<EventRef>,
    pub state: Vec<Field>,
    pub evolve: WasmRef,
    pub snapshot_every: Option<IntLit>,
    pub commands: Vec<CommandDecl>,
    pub invariants: Vec<InvariantRef>,
    pub span: Span,
}

/// `Name -> wasm "..."` inside an aggregate's `invariants` list: a rule
/// checked against the state a command would produce.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvariantRef {
    pub name: Ident,
    pub check: WasmRef,
    pub span: Span,
}

/// A context-level invariant: a rule over a projection's read model that
/// every command on aggregate `on` must respect, serialized per `scope`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvariantDecl {
    pub name: Ident,
    pub on: Ident,
    /// `Name` or `Context.Name` naming a projection.
    pub projection: EventRef,
    /// A field of the aggregate's state.
    pub scope: Ident,
    pub check: WasmRef,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalItem {
    Value(ValueDecl),
    Enum(EnumDecl),
    Entity(EntityDecl),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityDecl {
    pub name: Ident,
    /// The `id` field.
    pub id: Field,
    /// The fields after the id.
    pub fields: Vec<Field>,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandDecl {
    pub name: Ident,
    pub fields: Vec<Field>,
    pub handler: WasmRef,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WasmRef {
    pub module: StrLit,
    pub export: Option<StrLit>,
    pub span: Span,
}

/// `Name` or `Context.Name` naming an event family.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRef {
    pub qualifier: Option<Ident>,
    pub name: Ident,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionDecl {
    pub name: Ident,
    pub from: Vec<EventRef>,
    pub fold: WasmRef,
    pub tables: Vec<TableDecl>,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableDecl {
    pub name: Ident,
    pub fields: Vec<TableField>,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableField {
    pub key: bool,
    pub field: Field,
}

// ---------------------------------------------------------------------------
// strip_spans

impl File {
    /// Zero every span in the tree, for structural comparison.
    pub fn strip_spans(mut self) -> Self {
        for c in &mut self.contexts {
            c.strip_spans();
        }
        self
    }
}

impl Ident {
    fn strip(&mut self) {
        self.span = Span::default();
    }
}

impl StrLit {
    fn strip(&mut self) {
        self.span = Span::default();
    }
}

impl IntLit {
    fn strip(&mut self) {
        self.span = Span::default();
    }
}

impl Context {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        for item in &mut self.items {
            match item {
                Item::Value(v) => v.strip_spans(),
                Item::Enum(e) => e.strip_spans(),
                Item::Event(e) => e.strip_spans(),
                Item::Aggregate(a) => a.strip_spans(),
                Item::Projection(p) => p.strip_spans(),
                Item::Invariant(i) => i.strip_spans(),
                Item::Process(p) => p.strip_spans(),
            }
        }
    }
}

fn strip_fields(fields: &mut [Field]) {
    for f in fields {
        f.strip_spans();
    }
}

impl ValueDecl {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        strip_fields(&mut self.fields);
        for r in &mut self.rules {
            r.span = Span::default();
            r.name.strip();
            r.expr.strip_spans();
        }
    }
}

impl EnumDecl {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        for v in &mut self.variants {
            v.strip();
        }
    }
}

impl EventDecl {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        self.version.strip();
        strip_fields(&mut self.fields);
    }
}

impl Field {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        self.ty.strip_spans();
    }
}

impl Type {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        match &mut self.base {
            BaseType::Scalar(_) | BaseType::Set(_) => {}
            BaseType::Ref(r) => {
                r.name.strip();
                if let Some(q) = &mut r.qualifier {
                    q.strip();
                }
            }
            BaseType::List(t) | BaseType::Map(_, t) => t.strip_spans(),
        }
    }
}

impl AggregateDecl {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        self.key.strip_spans();
        self.stream.strip();
        for item in &mut self.items {
            match item {
                LocalItem::Value(v) => v.strip_spans(),
                LocalItem::Enum(e) => e.strip_spans(),
                LocalItem::Entity(e) => e.strip_spans(),
            }
        }
        for e in &mut self.events {
            e.strip_spans();
        }
        strip_fields(&mut self.state);
        self.evolve.strip_spans();
        if let Some(s) = &mut self.snapshot_every {
            s.strip();
        }
        for i in &mut self.invariants {
            i.strip_spans();
        }
        for c in &mut self.commands {
            c.strip_spans();
        }
    }
}

impl EntityDecl {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        self.id.strip_spans();
        strip_fields(&mut self.fields);
    }
}

impl ProcessDecl {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        self.key.strip_spans();
        for s in &mut self.from {
            s.span = Span::default();
            s.event.strip_spans();
            if let Some(b) = &mut s.by {
                b.strip();
            }
        }
        strip_fields(&mut self.state);
        self.react.strip_spans();
    }
}

impl InvariantRef {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        self.check.strip_spans();
    }
}

impl InvariantDecl {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        self.on.strip();
        self.projection.strip_spans();
        self.scope.strip();
        self.check.strip_spans();
    }
}

impl CommandDecl {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        strip_fields(&mut self.fields);
        self.handler.strip_spans();
    }
}

impl WasmRef {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.module.strip();
        if let Some(e) = &mut self.export {
            e.strip();
        }
    }
}

impl EventRef {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        if let Some(q) = &mut self.qualifier {
            q.strip();
        }
    }
}

impl ProjectionDecl {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        for e in &mut self.from {
            e.strip_spans();
        }
        self.fold.strip_spans();
        for t in &mut self.tables {
            t.strip_spans();
        }
    }
}

impl TableDecl {
    fn strip_spans(&mut self) {
        self.span = Span::default();
        self.name.strip();
        for f in &mut self.fields {
            f.field.strip_spans();
        }
    }
}
