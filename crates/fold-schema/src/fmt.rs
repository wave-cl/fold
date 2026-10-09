//! Canonical printer for the syntax tree: `parse(format(ast))` is `ast`
//! modulo spans (the formatter round-trip test proves it). Doc comments are
//! part of the tree and print with their nodes; ordinary comments are kept
//! by position: [`format_with`] puts each one back right before the next
//! thing printed after it in the source, so [`format_source`] loses nothing.

use std::fmt::Write;

use crate::ast::*;
use crate::lexer::Comment;
use crate::parser::{ParseError, parse_with_comments};
use crate::span::Span;

/// Render a file in canonical layout: two-space indentation, one field per
/// line with trailing commas, `[T]` for lists, a blank line between items.
pub fn format(file: &File) -> String {
    format_with(file, &[])
}

/// Like [`format`], keeping `comments` (as [`crate::parser::parse_with_comments`]
/// returned them for the same source the tree came from). A comment on its
/// own line prints on its own line before the next node; a comment that
/// followed something on its line trails the last printed line.
pub fn format_with(file: &File, comments: &[Comment]) -> String {
    let mut p = Printer {
        out: String::new(),
        comments,
        next: 0,
    };
    p.file(file);
    p.out
}

/// Parse and reprint `src`, comments included.
pub fn format_source(src: &str) -> Result<String, ParseError> {
    let (file, comments) = parse_with_comments(src)?;
    Ok(format_with(&file, &comments))
}

struct Printer<'a> {
    out: String,
    comments: &'a [Comment],
    next: usize,
}

impl Printer<'_> {
    fn indent(&mut self, depth: usize) {
        for _ in 0..depth {
            self.out.push_str("  ");
        }
    }

    /// Emits every pending comment that starts before `pos`.
    fn flush_before(&mut self, pos: usize, depth: usize) {
        while let Some(c) = self.comments.get(self.next) {
            if c.span.start >= pos {
                break;
            }
            let c = c.clone();
            self.next += 1;
            self.emit(&c, depth);
        }
    }

    fn emit(&mut self, c: &Comment, depth: usize) {
        let at_line_start = self.out.is_empty() || self.out.ends_with('\n');
        // A trailing comment attaches to the last printed line, if there is
        // one and it is not blank (a blank line is layout, not a line).
        let last_line_has_text = self.out.ends_with('\n') && !self.out.ends_with("\n\n");
        if !c.own_line && last_line_has_text {
            self.out.pop();
            self.out.push_str("  ");
            self.out.push_str(&c.text);
            self.out.push('\n');
            return;
        }
        if !at_line_start {
            self.out.push('\n');
        }
        self.indent(depth);
        self.out.push_str(&c.text);
        self.out.push('\n');
    }

    /// Whether a pending comment starts inside `span`.
    fn has_comment_in(&self, span: Span) -> bool {
        self.comments[self.next..]
            .iter()
            .any(|c| c.span.start >= span.start && c.span.start < span.end)
    }

    fn docs(&mut self, docs: &[String], depth: usize) {
        for d in docs {
            self.indent(depth);
            self.out.push_str("///");
            if !d.is_empty() {
                self.out.push(' ');
                self.out.push_str(d);
            }
            self.out.push('\n');
        }
    }

    fn file(&mut self, file: &File) {
        for d in &file.docs {
            self.out.push_str("//!");
            if !d.is_empty() {
                self.out.push(' ');
                self.out.push_str(d);
            }
            self.out.push('\n');
        }
        if !file.docs.is_empty() && !file.contexts.is_empty() {
            self.out.push('\n');
        }
        for (i, ctx) in file.contexts.iter().enumerate() {
            if i > 0 {
                self.out.push('\n');
            }
            self.context(ctx);
        }
        self.flush_before(usize::MAX, 0);
    }

    fn context(&mut self, ctx: &Context) {
        self.flush_before(ctx.span.start, 0);
        self.docs(&ctx.docs, 0);
        let _ = writeln!(self.out, "context {} {{", ctx.name.name);
        for (i, item) in ctx.items.iter().enumerate() {
            if i > 0 {
                self.out.push('\n');
            }
            match item {
                Item::Value(v) => self.value(v, 1),
                Item::Enum(e) => self.enum_decl(e, 1),
                Item::Event(e) => self.event(e, 1),
                Item::Aggregate(a) => self.aggregate(a, 1),
                Item::Projection(p) => self.projection(p, 1),
                Item::Invariant(i) => self.invariant(i, 1),
                Item::Process(p) => self.process(p, 1),
            }
        }
        self.flush_before(close_of(ctx.span), 1);
        self.out.push_str("}\n");
    }

    fn field_line(&mut self, f: &Field, depth: usize, key: bool) {
        self.flush_before(f.span.start, depth);
        self.docs(&f.docs, depth);
        self.indent(depth);
        if key {
            self.out.push_str("key ");
        }
        let _ = writeln!(self.out, "{},", field_str(f));
    }

    /// `{ fields }` with one field per line; `close` is where the closing
    /// brace was, for comments that sat before it.
    fn fields_block(&mut self, fields: &[Field], depth: usize, close: Option<usize>) {
        if fields.is_empty() && !close.is_some_and(|c| self.has_comment_in(Span::new(0, c))) {
            self.out.push_str("{}\n");
            return;
        }
        self.out.push_str("{\n");
        for f in fields {
            self.field_line(f, depth + 1, false);
        }
        if let Some(c) = close {
            self.flush_before(c, depth + 1);
        }
        self.indent(depth);
        self.out.push_str("}\n");
    }

    fn value(&mut self, v: &ValueDecl, depth: usize) {
        self.flush_before(v.span.start, depth);
        self.docs(&v.docs, depth);
        self.indent(depth);
        let _ = write!(self.out, "value {} ", v.name.name);
        if v.rules.is_empty() {
            self.fields_block(&v.fields, depth, Some(close_of(v.span)));
            return;
        }
        self.fields_block(&v.fields, depth, None);
        self.out.pop();
        self.out.push_str(" rules {\n");
        for r in &v.rules {
            self.flush_before(r.span.start, depth + 1);
            self.docs(&r.docs, depth + 1);
            self.indent(depth + 1);
            let _ = writeln!(self.out, "{}: {},", r.name.name, expr_str(&r.expr));
        }
        self.flush_before(close_of(v.span), depth + 1);
        self.indent(depth);
        self.out.push_str("}\n");
    }

    fn enum_decl(&mut self, e: &EnumDecl, depth: usize) {
        self.flush_before(e.span.start, depth);
        self.docs(&e.docs, depth);
        self.indent(depth);
        let plain = e
            .variants
            .iter()
            .all(|v| v.docs.is_empty() && v.payload.is_none());
        if plain && !self.has_comment_in(e.span) {
            let variants: Vec<&str> = e.variants.iter().map(|v| v.name.name.as_str()).collect();
            let _ = writeln!(
                self.out,
                "enum {} {{ {} }}",
                e.name.name,
                variants.join(", ")
            );
            return;
        }
        let _ = writeln!(self.out, "enum {} {{", e.name.name);
        for v in &e.variants {
            self.flush_before(v.span.start, depth + 1);
            self.docs(&v.docs, depth + 1);
            self.indent(depth + 1);
            self.out.push_str(&v.name.name);
            if let Some(fields) = &v.payload {
                self.out.push(' ');
                self.command_fields(fields, depth + 1);
            }
            self.out.push_str(",\n");
        }
        self.flush_before(close_of(e.span), depth + 1);
        self.indent(depth);
        self.out.push_str("}\n");
    }

    fn event(&mut self, e: &EventDecl, depth: usize) {
        self.flush_before(e.span.start, depth);
        self.docs(&e.docs, depth);
        self.indent(depth);
        let _ = write!(self.out, "event {} v{} ", e.name.name, e.version.value);
        self.fields_block(&e.fields, depth, Some(close_of(e.span)));
    }

    fn wasm_ref(&mut self, w: &WasmRef) {
        let _ = write!(self.out, "wasm {}", string_lit(&w.module.value));
        if let Some(e) = &w.export {
            let _ = write!(self.out, " export {}", string_lit(&e.value));
        }
    }

    /// `keyword a, b, c` on one line, or one ref per line when a comment
    /// lies among them (before the last one starts: a comment inside or
    /// after the last ref trails the line either way, which keeps the
    /// choice stable across reformats).
    fn event_refs_line(&mut self, keyword: &str, refs: &[EventRef], depth: usize) {
        let first = refs.first().map_or(0, |r| r.span.start);
        self.flush_before(first, depth);
        let spread = refs.len() > 1
            && self.has_comment_in(Span::new(
                first,
                refs.last().map_or(first, |r| r.span.start),
            ));
        self.indent(depth);
        if !spread {
            let _ = writeln!(self.out, "{keyword} {}", event_refs(refs));
            return;
        }
        let _ = write!(self.out, "{keyword} ");
        for (i, r) in refs.iter().enumerate() {
            if i > 0 {
                self.flush_before(r.span.start, depth + 1);
                self.indent(depth + 1);
            }
            self.out.push_str(&event_ref_str(r));
            if i + 1 < refs.len() {
                self.out.push(',');
            }
            self.out.push('\n');
        }
    }

    fn aggregate(&mut self, a: &AggregateDecl, depth: usize) {
        self.flush_before(a.span.start, depth);
        self.docs(&a.docs, depth);
        self.indent(depth);
        let _ = writeln!(self.out, "aggregate {} {{", a.name.name);
        self.flush_before(a.key.span.start, depth + 1);
        self.indent(depth + 1);
        let _ = writeln!(self.out, "key {}", field_str(&a.key));
        self.flush_before(a.stream.span.start, depth + 1);
        self.indent(depth + 1);
        let _ = writeln!(self.out, "stream {}", string_lit(&a.stream.value));
        if !a.items.is_empty() {
            self.out.push('\n');
        }
        for item in &a.items {
            match item {
                LocalItem::Value(v) => self.value(v, depth + 1),
                LocalItem::Enum(e) => self.enum_decl(e, depth + 1),
                LocalItem::Entity(e) => self.entity(e, depth + 1),
            }
        }
        self.out.push('\n');
        self.event_refs_line("events", &a.events, depth + 1);
        let state_at = a
            .state
            .first()
            .map_or(a.evolve.span.start, |f| f.span.start);
        self.flush_before(state_at, depth + 1);
        self.indent(depth + 1);
        self.out.push_str("state ");
        self.fields_block(&a.state, depth + 1, None);
        self.flush_before(a.evolve.span.start, depth + 1);
        self.indent(depth + 1);
        self.out.push_str("evolve ");
        self.wasm_ref(&a.evolve);
        self.out.push('\n');
        if let Some(s) = &a.snapshot_every {
            self.flush_before(s.span.start, depth + 1);
            self.indent(depth + 1);
            let _ = writeln!(self.out, "snapshot every {}", s.value);
        }
        if let Some(first) = a.commands.first() {
            self.flush_before(first.span.start, depth + 1);
            self.indent(depth + 1);
            self.out.push_str("commands\n");
            for (i, c) in a.commands.iter().enumerate() {
                self.flush_before(c.span.start, depth + 2);
                self.docs(&c.docs, depth + 2);
                self.indent(depth + 2);
                let _ = write!(self.out, "{} ", c.name.name);
                self.command_fields(&c.fields, depth + 2);
                self.out.push_str(" -> ");
                self.wasm_ref(&c.handler);
                if i + 1 < a.commands.len() {
                    self.out.push(',');
                }
                self.out.push('\n');
            }
        }
        if let Some(first) = a.invariants.first() {
            self.flush_before(first.span.start, depth + 1);
            self.indent(depth + 1);
            self.out.push_str("invariants\n");
            for (i, inv) in a.invariants.iter().enumerate() {
                self.flush_before(inv.span.start, depth + 2);
                self.docs(&inv.docs, depth + 2);
                self.indent(depth + 2);
                let _ = write!(self.out, "{} -> ", inv.name.name);
                self.wasm_ref(&inv.check);
                if i + 1 < a.invariants.len() {
                    self.out.push(',');
                }
                self.out.push('\n');
            }
        }
        self.flush_before(close_of(a.span), depth + 1);
        self.indent(depth);
        self.out.push_str("}\n");
    }

    /// Command fields are printed inline when short and undocumented, as a
    /// block otherwise.
    fn command_fields(&mut self, fields: &[Field], depth: usize) {
        if fields.is_empty() {
            self.out.push_str("{}");
            return;
        }
        // A comment before the last field starts forces the block form; one
        // inside or after the last field trails the command line either way.
        let span = Span::new(
            fields[0].span.start,
            fields.last().map_or(0, |f| f.span.start),
        );
        let inline: Vec<String> = fields.iter().map(field_str).collect();
        let joined = inline.join(", ");
        let documented = fields.iter().any(|f| !f.docs.is_empty());
        if joined.len() <= 60 && !documented && !self.has_comment_in(span) {
            let _ = write!(self.out, "{{ {joined} }}");
            return;
        }
        self.out.push_str("{\n");
        for f in fields {
            self.field_line(f, depth + 1, false);
        }
        self.indent(depth);
        self.out.push('}');
    }

    fn entity(&mut self, e: &EntityDecl, depth: usize) {
        self.flush_before(e.span.start, depth);
        self.docs(&e.docs, depth);
        self.indent(depth);
        let _ = writeln!(self.out, "entity {} {{", e.name.name);
        self.flush_before(e.id.span.start, depth + 1);
        self.docs(&e.id.docs, depth + 1);
        self.indent(depth + 1);
        let _ = writeln!(self.out, "id {},", field_str(&e.id));
        for f in &e.fields {
            self.field_line(f, depth + 1, false);
        }
        self.flush_before(close_of(e.span), depth + 1);
        self.indent(depth);
        self.out.push_str("}\n");
    }

    fn process(&mut self, p: &ProcessDecl, depth: usize) {
        self.flush_before(p.span.start, depth);
        self.docs(&p.docs, depth);
        self.indent(depth);
        let _ = writeln!(self.out, "process {} {{", p.name.name);
        self.flush_before(p.key.span.start, depth + 1);
        self.indent(depth + 1);
        let _ = writeln!(self.out, "key {}", field_str(&p.key));
        let first = p.from.first().map_or(0, |s| s.span.start);
        self.flush_before(first, depth + 1);
        let last_start = p.from.last().map_or(first, |s| s.span.start);
        let spread = p.from.len() > 1 && self.has_comment_in(Span::new(first, last_start));
        self.indent(depth + 1);
        if spread {
            self.out.push_str("from ");
            for (i, s) in p.from.iter().enumerate() {
                if i > 0 {
                    self.flush_before(s.span.start, depth + 2);
                    self.indent(depth + 2);
                }
                self.out.push_str(&source_str(s));
                if i + 1 < p.from.len() {
                    self.out.push(',');
                }
                self.out.push('\n');
            }
        } else {
            let sources: Vec<String> = p.from.iter().map(source_str).collect();
            let _ = writeln!(self.out, "from {}", sources.join(", "));
        }
        let state_at = p.state.first().map_or(p.react.span.start, |f| f.span.start);
        self.flush_before(state_at, depth + 1);
        self.indent(depth + 1);
        self.out.push_str("state ");
        self.fields_block(&p.state, depth + 1, None);
        self.flush_before(p.react.span.start, depth + 1);
        self.indent(depth + 1);
        self.out.push_str("react ");
        self.wasm_ref(&p.react);
        self.out.push('\n');
        if let Some(n) = &p.snapshot_every {
            self.flush_before(n.span.start, depth + 1);
            self.indent(depth + 1);
            let _ = writeln!(self.out, "snapshot every {}", n.value);
        }
        self.flush_before(close_of(p.span), depth + 1);
        self.indent(depth);
        self.out.push_str("}\n");
    }

    fn invariant(&mut self, i: &InvariantDecl, depth: usize) {
        self.flush_before(i.span.start, depth);
        self.docs(&i.docs, depth);
        self.indent(depth);
        let _ = writeln!(self.out, "invariant {} {{", i.name.name);
        self.flush_before(i.on.span.start, depth + 1);
        self.indent(depth + 1);
        let _ = writeln!(self.out, "on {}", i.on.name);
        self.flush_before(i.projection.span.start, depth + 1);
        self.indent(depth + 1);
        let _ = writeln!(self.out, "projection {}", event_ref_str(&i.projection));
        self.flush_before(i.scope.span.start, depth + 1);
        self.indent(depth + 1);
        let _ = writeln!(self.out, "scope {}", i.scope.name);
        self.flush_before(i.check.span.start, depth + 1);
        self.indent(depth + 1);
        self.out.push_str("check ");
        self.wasm_ref(&i.check);
        self.out.push('\n');
        self.flush_before(close_of(i.span), depth + 1);
        self.indent(depth);
        self.out.push_str("}\n");
    }

    fn projection(&mut self, p: &ProjectionDecl, depth: usize) {
        self.flush_before(p.span.start, depth);
        self.docs(&p.docs, depth);
        self.indent(depth);
        let _ = writeln!(self.out, "projection {} {{", p.name.name);
        self.event_refs_line("from", &p.from, depth + 1);
        self.flush_before(p.fold.span.start, depth + 1);
        self.indent(depth + 1);
        self.out.push_str("fold ");
        self.wasm_ref(&p.fold);
        self.out.push('\n');
        if let Some(n) = &p.snapshot_every {
            self.flush_before(n.span.start, depth + 1);
            self.indent(depth + 1);
            let _ = writeln!(self.out, "snapshot every {}", n.value);
        }
        for t in &p.tables {
            self.flush_before(t.span.start, depth + 1);
            self.docs(&t.docs, depth + 1);
            self.indent(depth + 1);
            let _ = writeln!(self.out, "table {} {{", t.name.name);
            for f in &t.fields {
                self.field_line(&f.field, depth + 2, f.key);
            }
            self.flush_before(close_of(t.span), depth + 2);
            self.indent(depth + 1);
            self.out.push_str("}\n");
        }
        self.flush_before(close_of(p.span), depth + 1);
        self.indent(depth);
        self.out.push_str("}\n");
    }
}

/// The position of a block's closing brace: comments before it belong
/// inside the block.
fn close_of(span: Span) -> usize {
    span.end.saturating_sub(1)
}

/// Binding strength: higher binds tighter.
fn prec(e: &Expr) -> u8 {
    match e {
        Expr::Or(..) => 1,
        Expr::And(..) => 2,
        Expr::Not(..) => 3,
        _ => 4,
    }
}

/// Prints `e` as a child of an operator with precedence `parent`, adding
/// parentheses when the parse would otherwise regroup it. `right` marks a
/// right operand, where equal precedence also needs parentheses.
fn child_str(e: &Expr, parent: u8, right: bool) -> String {
    let s = expr_str(e);
    let p = prec(e);
    if p < parent || (right && p == parent && parent < 3) {
        format!("({s})")
    } else {
        s
    }
}

pub fn expr_str(e: &Expr) -> String {
    match e {
        Expr::Or(a, b) => format!("{} or {}", child_str(a, 1, false), child_str(b, 1, true)),
        Expr::And(a, b) => format!("{} and {}", child_str(a, 2, false), child_str(b, 2, true)),
        Expr::Not(inner) => format!("not {}", child_str(inner, 3, false)),
        Expr::Cmp { lhs, op, rhs, .. } => {
            format!("{} {} {}", term_str(lhs), op.as_str(), term_str(rhs))
        }
        Expr::Matches { path, pattern, .. } => {
            format!("{} matches {}", path_str(path), string_lit(&pattern.value))
        }
        Expr::In { path, items, .. } => format!(
            "{} in [{}]",
            path_str(path),
            items.iter().map(literal_str).collect::<Vec<_>>().join(", ")
        ),
    }
}

fn term_str(t: &Term) -> String {
    match t {
        Term::Lit(l) => literal_str(l),
        Term::Path(p) => path_str(p),
        Term::Len(p, _) => format!("len({})", path_str(p)),
    }
}

fn literal_str(l: &Literal) -> String {
    match l {
        Literal::Number(text, _) => text.clone(),
        Literal::Str(s) => string_lit(&s.value),
        Literal::Bool(b, _) => b.to_string(),
        Literal::Variant(i) => i.name.clone(),
    }
}

/// `name: T` with its default, if any.
fn field_str(f: &Field) -> String {
    match &f.default {
        Some(d) => format!("{}: {} = {}", f.name.name, type_str(&f.ty), literal_str(d)),
        None => format!("{}: {}", f.name.name, type_str(&f.ty)),
    }
}

fn path_str(p: &FieldPath) -> String {
    p.segments
        .iter()
        .map(|s| s.name.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

/// A type as source text.
pub fn type_str(t: &Type) -> String {
    let mut s = match &t.base {
        BaseType::Scalar(sc) => sc.name().to_string(),
        BaseType::Ref(r) => match &r.qualifier {
            Some(q) => format!("{}.{}", q.name, r.name.name),
            None => r.name.name.clone(),
        },
        BaseType::List(inner) => format!("[{}]", type_str(inner)),
        BaseType::Set(sc) => format!("set<{}>", sc.name()),
        BaseType::Map(k, v) => format!("map<{}, {}>", k.name(), type_str(v)),
    };
    if t.optional {
        s.push('?');
    }
    s
}

/// A string literal with the escapes the lexer understands.
pub fn string_lit(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\0' => out.push_str("\\0"),
            c if c.is_control() => {
                let _ = write!(out, "\\u{{{:x}}}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn event_ref_str(r: &EventRef) -> String {
    match &r.qualifier {
        Some(q) => format!("{}.{}", q.name, r.name.name),
        None => r.name.name.clone(),
    }
}

fn event_refs(refs: &[EventRef]) -> String {
    refs.iter()
        .map(event_ref_str)
        .collect::<Vec<_>>()
        .join(", ")
}

fn source_str(s: &ProcessSource) -> String {
    let e = event_ref_str(&s.event);
    match &s.by {
        Some(b) => format!("{e} by {}", b.name),
        None => e,
    }
}
