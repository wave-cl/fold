//! The syntax tree produced by [`crate::parse`]. Every node carries the span of
//! the source it came from; [`File::strip_spans`] zeroes them so two trees can be
//! compared structurally (the formatter round-trip test does this). Doc
//! comments (`///`) are part of the tree; ordinary comments are not (the
//! formatter keeps those by position, see [`crate::fmt::format_with`]).

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
    /// `//!` lines at the top of the file.
    pub docs: Vec<String>,
    /// `import "path"` lines, before the contexts.
    pub imports: Vec<Import>,
    pub contexts: Vec<Context>,
}

/// `import "relative/path.fold"`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Import {
    pub path: StrLit,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Context {
    /// `///` lines written before it.
    pub docs: Vec<String>,
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
    /// `///` lines written before it.
    pub docs: Vec<String>,
    pub name: Ident,
    /// The correlation key: its name is looked up in each event unless the
    /// source says `by`.
    pub key: Field,
    pub from: Vec<ProcessSource>,
    pub state: Vec<Field>,
    pub react: WasmRef,
    /// `snapshot every N`: snapshot the instances and outbox every N
    /// positions. Absent = never.
    pub snapshot_every: Option<IntLit>,
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
    /// `///` lines written before it.
    pub docs: Vec<String>,
    pub name: Ident,
    pub fields: Vec<Field>,
    /// Rules every instance must satisfy, checked wherever one is created.
    pub rules: Vec<RuleDecl>,
    pub span: Span,
}

/// `Name: expr` inside a value's `rules { ... }`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuleDecl {
    /// `///` lines written before it.
    pub docs: Vec<String>,
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
    /// `state exists`: whether a `requires` guard's root is present.
    Exists {
        root: Ident,
        span: Span,
    },
}

impl Expr {
    pub fn span(&self) -> Span {
        match self {
            Expr::Or(a, b) | Expr::And(a, b) => a.span().join(b.span()),
            Expr::Not(e) => e.span(),
            Expr::Cmp { span, .. }
            | Expr::Matches { span, .. }
            | Expr::In { span, .. }
            | Expr::Exists { span, .. } => *span,
        }
    }

    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        match self {
            Expr::Or(a, b) | Expr::And(a, b) => {
                a.map_spans(f);
                b.map_spans(f);
            }
            Expr::Not(e) => e.map_spans(f),
            Expr::Cmp { lhs, rhs, span, .. } => {
                lhs.map_spans(f);
                rhs.map_spans(f);
                *span = f(*span);
            }
            Expr::Matches {
                path,
                pattern,
                span,
            } => {
                path.map_spans(f);
                pattern.map_spans(f);
                *span = f(*span);
            }
            Expr::In { path, items, span } => {
                path.map_spans(f);
                for i in items {
                    i.map_spans(f);
                }
                *span = f(*span);
            }
            Expr::Exists { root, span } => {
                root.map_spans(f);
                *span = f(*span);
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

    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        match self {
            Term::Lit(l) => l.map_spans(f),
            Term::Path(p) => p.map_spans(f),
            Term::Len(p, s) => {
                p.map_spans(f);
                *s = f(*s);
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
    /// A bare identifier where a literal is expected: an enum variant.
    Variant(Ident),
}

impl Literal {
    pub fn span(&self) -> Span {
        match self {
            Literal::Number(_, s) | Literal::Bool(_, s) => *s,
            Literal::Str(l) => l.span,
            Literal::Variant(i) => i.span,
        }
    }

    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        match self {
            Literal::Number(_, s) | Literal::Bool(_, s) => *s = f(*s),
            Literal::Str(l) => l.map_spans(f),
            Literal::Variant(i) => i.map_spans(f),
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
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        for s in &mut self.segments {
            s.map_spans(f);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnumDecl {
    /// `///` lines written before it.
    pub docs: Vec<String>,
    pub name: Ident,
    pub variants: Vec<Variant>,
    pub span: Span,
}

/// `Name` or `Name { fields }` inside an enum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Variant {
    pub docs: Vec<String>,
    pub name: Ident,
    /// The payload's fields; `None` for a unit variant.
    pub payload: Option<Vec<Field>>,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventDecl {
    /// `///` lines written before it.
    pub docs: Vec<String>,
    pub name: Ident,
    /// The `vN` token; `value` is `N`.
    pub version: IntLit,
    pub fields: Vec<Field>,
    /// `upcast from vM { ... }` or `upcast from vM wasm "..."`: how to
    /// produce this version from the previous one.
    pub upcast: Option<UpcastDecl>,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpcastDecl {
    /// The `vM` token.
    pub from: IntLit,
    pub how: UpcastHow,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpcastHow {
    /// `{ set f: v, rename a as b, ... }`.
    Ops(Vec<UpcastOp>),
    Wasm(WasmRef),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpcastOp {
    Set {
        field: Ident,
        value: UpcastValue,
        span: Span,
    },
    Rename {
        from: Ident,
        to: Ident,
        span: Span,
    },
}

impl UpcastOp {
    pub fn span(&self) -> Span {
        match self {
            UpcastOp::Set { span, .. } | UpcastOp::Rename { span, .. } => *span,
        }
    }
}

/// A JSON-like literal in an upcast `set`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpcastValue {
    Lit(Literal),
    Null(Span),
    List(Vec<UpcastValue>, Span),
    Object(Vec<(Ident, UpcastValue)>, Span),
}

impl UpcastValue {
    pub fn span(&self) -> Span {
        match self {
            UpcastValue::Lit(l) => l.span(),
            UpcastValue::Null(s) | UpcastValue::List(_, s) | UpcastValue::Object(_, s) => *s,
        }
    }

    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        match self {
            UpcastValue::Lit(l) => l.map_spans(f),
            UpcastValue::Null(s) => *s = f(*s),
            UpcastValue::List(items, s) => {
                *s = f(*s);
                for i in items {
                    i.map_spans(f);
                }
            }
            UpcastValue::Object(entries, s) => {
                *s = f(*s);
                for (k, v) in entries {
                    k.map_spans(f);
                    v.map_spans(f);
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    /// `///` lines written before it.
    pub docs: Vec<String>,
    pub name: Ident,
    pub ty: Type,
    /// `= literal`: the value a record gets when the field is absent.
    pub default: Option<Literal>,
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
    /// `///` lines written before it.
    pub docs: Vec<String>,
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

/// `Name -> wasm "..."` or `Name: expr` inside an aggregate's `invariants`
/// list: a rule checked against the state a command would produce.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvariantRef {
    /// `///` lines written before it.
    pub docs: Vec<String>,
    pub name: Ident,
    pub check: InvariantCheckSyntax,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvariantCheckSyntax {
    Wasm(WasmRef),
    /// An expression over the state's fields.
    Expr(Expr),
}

/// A context-level invariant: a rule over a projection's read model that
/// every command on aggregate `on` must respect, serialized per `scope`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvariantDecl {
    /// `///` lines written before it.
    pub docs: Vec<String>,
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
    Entity(Box<EntityDecl>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityDecl {
    /// `///` lines written before it.
    pub docs: Vec<String>,
    pub name: Ident,
    /// The `id` field.
    pub id: Field,
    /// The fields after the id.
    pub fields: Vec<Field>,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandDecl {
    /// `///` lines written before it.
    pub docs: Vec<String>,
    pub name: Ident,
    pub fields: Vec<Field>,
    /// `requires { Name: expr, ... }` (a bare `requires expr` is the one
    /// guard named `Requires`), over `state.` and `command.`.
    pub requires: Vec<RuleDecl>,
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
    /// `///` lines written before it.
    pub docs: Vec<String>,
    pub name: Ident,
    pub from: Vec<EventRef>,
    pub fold: WasmRef,
    /// `snapshot every N`: write a read-model snapshot every N applied
    /// positions. Absent = never.
    pub snapshot_every: Option<IntLit>,
    pub tables: Vec<TableDecl>,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableDecl {
    /// `///` lines written before it.
    pub docs: Vec<String>,
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
// span visitors

impl File {
    /// Zero every span in the tree, for structural comparison.
    pub fn strip_spans(mut self) -> Self {
        self.map_spans(&|_| Span::default());
        self
    }

    /// Move every span forward by `delta` (a file placed inside a bundle).
    pub fn shift_spans(mut self, delta: usize) -> Self {
        self.map_spans(&|s| Span::new(s.start + delta, s.end + delta));
        self
    }

    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        for i in &mut self.imports {
            i.span = f(i.span);
            i.path.map_spans(f);
        }
        for c in &mut self.contexts {
            c.map_spans(f);
        }
    }

    /// Prefix every wasm path with `dir` (an imported file's directory,
    /// root-relative, without a trailing slash). Paths the resolver would
    /// reject (S024) are left as written so the diagnostic names them.
    pub fn rebase_wasm(mut self, dir: &str) -> Self {
        if dir.is_empty() {
            return self;
        }
        let rebase = |w: &mut WasmRef| {
            let p = &w.module.value;
            let clean = !p.is_empty()
                && !p.starts_with(['/', '\\'])
                && !p.contains(':')
                && !p.split(['/', '\\']).any(|seg| seg == "..");
            if clean {
                w.module.value = format!("{dir}/{p}");
            }
        };
        for c in &mut self.contexts {
            for item in &mut c.items {
                match item {
                    Item::Value(_) | Item::Enum(_) => {}
                    Item::Event(e) => {
                        if let Some(u) = &mut e.upcast
                            && let UpcastHow::Wasm(w) = &mut u.how
                        {
                            rebase(w);
                        }
                    }
                    Item::Aggregate(a) => {
                        rebase(&mut a.evolve);
                        for cmd in &mut a.commands {
                            rebase(&mut cmd.handler);
                        }
                        for inv in &mut a.invariants {
                            if let InvariantCheckSyntax::Wasm(w) = &mut inv.check {
                                rebase(w);
                            }
                        }
                    }
                    Item::Projection(p) => rebase(&mut p.fold),
                    Item::Invariant(i) => rebase(&mut i.check),
                    Item::Process(p) => rebase(&mut p.react),
                }
            }
        }
        self
    }
}

impl Ident {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
    }
}

impl StrLit {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
    }
}

impl IntLit {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
    }
}

impl Context {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        for item in &mut self.items {
            match item {
                Item::Value(v) => v.map_spans(f),
                Item::Enum(e) => e.map_spans(f),
                Item::Event(e) => e.map_spans(f),
                Item::Aggregate(a) => a.map_spans(f),
                Item::Projection(p) => p.map_spans(f),
                Item::Invariant(i) => i.map_spans(f),
                Item::Process(p) => p.map_spans(f),
            }
        }
    }
}

fn map_fields(fields: &mut [Field], f: &dyn Fn(Span) -> Span) {
    for field in fields {
        field.map_spans(f);
    }
}

impl ValueDecl {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        map_fields(&mut self.fields, f);
        for r in &mut self.rules {
            r.span = f(r.span);
            r.name.map_spans(f);
            r.expr.map_spans(f);
        }
    }
}

impl EnumDecl {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        for v in &mut self.variants {
            v.span = f(v.span);
            v.name.map_spans(f);
            if let Some(fields) = &mut v.payload {
                map_fields(fields, f);
            }
        }
    }
}

impl EventDecl {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        self.version.map_spans(f);
        map_fields(&mut self.fields, f);
        if let Some(u) = &mut self.upcast {
            u.span = f(u.span);
            u.from.map_spans(f);
            match &mut u.how {
                UpcastHow::Wasm(w) => w.map_spans(f),
                UpcastHow::Ops(ops) => {
                    for op in ops {
                        match op {
                            UpcastOp::Set { field, value, span } => {
                                field.map_spans(f);
                                value.map_spans(f);
                                *span = f(*span);
                            }
                            UpcastOp::Rename { from, to, span } => {
                                from.map_spans(f);
                                to.map_spans(f);
                                *span = f(*span);
                            }
                        }
                    }
                }
            }
        }
    }
}

impl Field {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        self.ty.map_spans(f);
        if let Some(d) = &mut self.default {
            d.map_spans(f);
        }
    }
}

impl Type {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        match &mut self.base {
            BaseType::Scalar(_) | BaseType::Set(_) => {}
            BaseType::Ref(r) => {
                r.name.map_spans(f);
                if let Some(q) = &mut r.qualifier {
                    q.map_spans(f);
                }
            }
            BaseType::List(t) | BaseType::Map(_, t) => t.map_spans(f),
        }
    }
}

impl AggregateDecl {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        self.key.map_spans(f);
        self.stream.map_spans(f);
        for item in &mut self.items {
            match item {
                LocalItem::Value(v) => v.map_spans(f),
                LocalItem::Enum(e) => e.map_spans(f),
                LocalItem::Entity(e) => e.map_spans(f),
            }
        }
        for e in &mut self.events {
            e.map_spans(f);
        }
        map_fields(&mut self.state, f);
        self.evolve.map_spans(f);
        if let Some(s) = &mut self.snapshot_every {
            s.map_spans(f);
        }
        for i in &mut self.invariants {
            i.map_spans(f);
        }
        for c in &mut self.commands {
            c.map_spans(f);
        }
    }
}

impl EntityDecl {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        self.id.map_spans(f);
        map_fields(&mut self.fields, f);
    }
}

impl ProcessDecl {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        self.key.map_spans(f);
        for s in &mut self.from {
            s.span = f(s.span);
            s.event.map_spans(f);
            if let Some(b) = &mut s.by {
                b.map_spans(f);
            }
        }
        map_fields(&mut self.state, f);
        self.react.map_spans(f);
        if let Some(n) = &mut self.snapshot_every {
            n.map_spans(f);
        }
    }
}

impl InvariantRef {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        match &mut self.check {
            InvariantCheckSyntax::Wasm(w) => w.map_spans(f),
            InvariantCheckSyntax::Expr(e) => e.map_spans(f),
        }
    }
}

impl InvariantDecl {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        self.on.map_spans(f);
        self.projection.map_spans(f);
        self.scope.map_spans(f);
        self.check.map_spans(f);
    }
}

impl CommandDecl {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        map_fields(&mut self.fields, f);
        for r in &mut self.requires {
            r.span = f(r.span);
            r.name.map_spans(f);
            r.expr.map_spans(f);
        }
        self.handler.map_spans(f);
    }
}

impl WasmRef {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.module.map_spans(f);
        if let Some(e) = &mut self.export {
            e.map_spans(f);
        }
    }
}

impl EventRef {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        if let Some(q) = &mut self.qualifier {
            q.map_spans(f);
        }
    }
}

impl ProjectionDecl {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        if let Some(n) = &mut self.snapshot_every {
            n.map_spans(f);
        }
        for e in &mut self.from {
            e.map_spans(f);
        }
        self.fold.map_spans(f);
        for t in &mut self.tables {
            t.map_spans(f);
        }
    }
}

impl TableDecl {
    fn map_spans(&mut self, f: &dyn Fn(Span) -> Span) {
        self.span = f(self.span);
        self.name.map_spans(f);
        for tf in &mut self.fields {
            tf.field.map_spans(f);
        }
    }
}
