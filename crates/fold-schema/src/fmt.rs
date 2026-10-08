//! Canonical printer for the syntax tree: `parse(format(ast))` is `ast`
//! modulo spans (the formatter round-trip test proves it).

use std::fmt::Write;

use crate::ast::*;

/// Render a file in canonical layout: two-space indentation, one field per
/// line with trailing commas, `[T]` for lists, a blank line between items.
pub fn format(file: &File) -> String {
    let mut out = String::new();
    for (i, ctx) in file.contexts.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        context(&mut out, ctx);
    }
    out
}

fn indent(out: &mut String, depth: usize) {
    for _ in 0..depth {
        out.push_str("  ");
    }
}

fn context(out: &mut String, ctx: &Context) {
    let _ = writeln!(out, "context {} {{", ctx.name.name);
    for (i, item) in ctx.items.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        match item {
            Item::Value(v) => value(out, v, 1),
            Item::Enum(e) => enum_decl(out, e, 1),
            Item::Event(e) => event(out, e, 1),
            Item::Aggregate(a) => aggregate(out, a, 1),
            Item::Projection(p) => projection(out, p, 1),
            Item::Invariant(i) => invariant(out, i, 1),
            Item::Process(p) => process(out, p, 1),
        }
    }
    out.push_str("}\n");
}

fn fields_block(out: &mut String, fields: &[Field], depth: usize) {
    if fields.is_empty() {
        out.push_str("{}\n");
        return;
    }
    out.push_str("{\n");
    for f in fields {
        indent(out, depth + 1);
        let _ = writeln!(out, "{}: {},", f.name.name, type_str(&f.ty));
    }
    indent(out, depth);
    out.push_str("}\n");
}

fn value(out: &mut String, v: &ValueDecl, depth: usize) {
    indent(out, depth);
    let _ = write!(out, "value {} ", v.name.name);
    if v.rules.is_empty() {
        fields_block(out, &v.fields, depth);
        return;
    }
    let mut block = String::new();
    fields_block(&mut block, &v.fields, depth);
    out.push_str(block.trim_end_matches('\n'));
    out.push_str(" rules {\n");
    for r in &v.rules {
        indent(out, depth + 1);
        let _ = writeln!(out, "{}: {},", r.name.name, expr_str(&r.expr));
    }
    indent(out, depth);
    out.push_str("}\n");
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
    }
}

fn path_str(p: &FieldPath) -> String {
    p.segments
        .iter()
        .map(|s| s.name.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

fn enum_decl(out: &mut String, e: &EnumDecl, depth: usize) {
    indent(out, depth);
    let variants: Vec<&str> = e.variants.iter().map(|v| v.name.as_str()).collect();
    let _ = writeln!(out, "enum {} {{ {} }}", e.name.name, variants.join(", "));
}

fn event(out: &mut String, e: &EventDecl, depth: usize) {
    indent(out, depth);
    let _ = write!(out, "event {} v{} ", e.name.name, e.version.value);
    fields_block(out, &e.fields, depth);
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

fn wasm_ref(out: &mut String, w: &WasmRef) {
    let _ = write!(out, "wasm {}", string_lit(&w.module.value));
    if let Some(e) = &w.export {
        let _ = write!(out, " export {}", string_lit(&e.value));
    }
}

fn event_refs(refs: &[EventRef]) -> String {
    refs.iter()
        .map(|r| match &r.qualifier {
            Some(q) => format!("{}.{}", q.name, r.name.name),
            None => r.name.name.clone(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn aggregate(out: &mut String, a: &AggregateDecl, depth: usize) {
    indent(out, depth);
    let _ = writeln!(out, "aggregate {} {{", a.name.name);
    indent(out, depth + 1);
    let _ = writeln!(out, "key {}: {}", a.key.name.name, type_str(&a.key.ty));
    indent(out, depth + 1);
    let _ = writeln!(out, "stream {}", string_lit(&a.stream.value));
    if !a.items.is_empty() {
        out.push('\n');
    }
    for item in &a.items {
        match item {
            LocalItem::Value(v) => value(out, v, depth + 1),
            LocalItem::Enum(e) => enum_decl(out, e, depth + 1),
            LocalItem::Entity(e) => entity(out, e, depth + 1),
        }
    }
    out.push('\n');
    indent(out, depth + 1);
    let _ = writeln!(out, "events {}", event_refs(&a.events));
    indent(out, depth + 1);
    out.push_str("state ");
    fields_block(out, &a.state, depth + 1);
    indent(out, depth + 1);
    out.push_str("evolve ");
    wasm_ref(out, &a.evolve);
    out.push('\n');
    if let Some(s) = &a.snapshot_every {
        indent(out, depth + 1);
        let _ = writeln!(out, "snapshot every {}", s.value);
    }
    if !a.commands.is_empty() {
        indent(out, depth + 1);
        out.push_str("commands\n");
        for (i, c) in a.commands.iter().enumerate() {
            indent(out, depth + 2);
            let _ = write!(out, "{} ", c.name.name);
            command_fields(out, &c.fields, depth + 2);
            out.push_str(" -> ");
            wasm_ref(out, &c.handler);
            if i + 1 < a.commands.len() {
                out.push(',');
            }
            out.push('\n');
        }
    }
    if !a.invariants.is_empty() {
        indent(out, depth + 1);
        out.push_str("invariants\n");
        for (i, inv) in a.invariants.iter().enumerate() {
            indent(out, depth + 2);
            let _ = write!(out, "{} -> ", inv.name.name);
            wasm_ref(out, &inv.check);
            if i + 1 < a.invariants.len() {
                out.push(',');
            }
            out.push('\n');
        }
    }
    indent(out, depth);
    out.push_str("}\n");
}

fn process(out: &mut String, p: &ProcessDecl, depth: usize) {
    indent(out, depth);
    let _ = writeln!(out, "process {} {{", p.name.name);
    indent(out, depth + 1);
    let _ = writeln!(out, "key {}: {}", p.key.name.name, type_str(&p.key.ty));
    indent(out, depth + 1);
    let sources: Vec<String> = p
        .from
        .iter()
        .map(|s| {
            let e = event_refs(std::slice::from_ref(&s.event));
            match &s.by {
                Some(b) => format!("{e} by {}", b.name),
                None => e,
            }
        })
        .collect();
    let _ = writeln!(out, "from {}", sources.join(", "));
    indent(out, depth + 1);
    out.push_str("state ");
    fields_block(out, &p.state, depth + 1);
    indent(out, depth + 1);
    out.push_str("react ");
    wasm_ref(out, &p.react);
    out.push('\n');
    indent(out, depth);
    out.push_str("}\n");
}

fn invariant(out: &mut String, i: &InvariantDecl, depth: usize) {
    indent(out, depth);
    let _ = writeln!(out, "invariant {} {{", i.name.name);
    indent(out, depth + 1);
    let _ = writeln!(out, "on {}", i.on.name);
    indent(out, depth + 1);
    let _ = writeln!(
        out,
        "projection {}",
        event_refs(std::slice::from_ref(&i.projection))
    );
    indent(out, depth + 1);
    let _ = writeln!(out, "scope {}", i.scope.name);
    indent(out, depth + 1);
    out.push_str("check ");
    wasm_ref(out, &i.check);
    out.push('\n');
    indent(out, depth);
    out.push_str("}\n");
}

/// Command fields are printed inline when short, as a block otherwise.
fn command_fields(out: &mut String, fields: &[Field], depth: usize) {
    if fields.is_empty() {
        out.push_str("{}");
        return;
    }
    let inline: Vec<String> = fields
        .iter()
        .map(|f| format!("{}: {}", f.name.name, type_str(&f.ty)))
        .collect();
    let joined = inline.join(", ");
    if joined.len() <= 60 {
        let _ = write!(out, "{{ {joined} }}");
    } else {
        out.push_str("{\n");
        for f in fields {
            indent(out, depth + 1);
            let _ = writeln!(out, "{}: {},", f.name.name, type_str(&f.ty));
        }
        indent(out, depth);
        out.push('}');
    }
}

fn entity(out: &mut String, e: &EntityDecl, depth: usize) {
    indent(out, depth);
    let _ = writeln!(out, "entity {} {{", e.name.name);
    indent(out, depth + 1);
    let _ = writeln!(out, "id {}: {},", e.id.name.name, type_str(&e.id.ty));
    for f in &e.fields {
        indent(out, depth + 1);
        let _ = writeln!(out, "{}: {},", f.name.name, type_str(&f.ty));
    }
    indent(out, depth);
    out.push_str("}\n");
}

fn projection(out: &mut String, p: &ProjectionDecl, depth: usize) {
    indent(out, depth);
    let _ = writeln!(out, "projection {} {{", p.name.name);
    indent(out, depth + 1);
    let _ = writeln!(out, "from {}", event_refs(&p.from));
    indent(out, depth + 1);
    out.push_str("fold ");
    wasm_ref(out, &p.fold);
    out.push('\n');
    for t in &p.tables {
        indent(out, depth + 1);
        let _ = writeln!(out, "table {} {{", t.name.name);
        for f in &t.fields {
            indent(out, depth + 2);
            if f.key {
                out.push_str("key ");
            }
            let _ = writeln!(out, "{}: {},", f.field.name.name, type_str(&f.field.ty));
        }
        indent(out, depth + 1);
        out.push_str("}\n");
    }
    indent(out, depth);
    out.push_str("}\n");
}
