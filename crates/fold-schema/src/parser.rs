//! Recursive-descent parser for `.fold` files (no precedence; one token of
//! lookahead; the error names what was expected and what was found).

use std::fmt;

use crate::ast::*;
use crate::lexer::{Comment, LexError, Token, TokenKind, lex_with_comments};
use crate::span::Span;
use crate::types::Scalar;

/// A syntax error: the token at `span` was `found` where one of `expected` was
/// required.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseError {
    pub span: Span,
    pub expected: Vec<&'static str>,
    pub found: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "expected ")?;
        match self.expected.as_slice() {
            [] => write!(f, "nothing")?,
            [one] => write!(f, "{one}")?,
            [init @ .., last] => {
                write!(f, "{}", init.join(", "))?;
                write!(f, " or {last}")?;
            }
        }
        write!(f, ", found {}", self.found)
    }
}

impl std::error::Error for ParseError {}

impl From<LexError> for ParseError {
    fn from(e: LexError) -> Self {
        let span = e.span();
        match e {
            LexError::UnterminatedString { .. } => ParseError {
                span,
                expected: vec!["a closing `\"`"],
                found: "end of the string literal".to_string(),
            },
            LexError::BadEscape { escape, .. } => ParseError {
                span,
                expected: vec![
                    "an escape sequence (`\\\\`, `\\\"`, `\\n`, `\\t`, `\\r`, `\\0`, `\\u{..}`)",
                ],
                found: format!("`{escape}`"),
            },
            LexError::UnterminatedComment { .. } => ParseError {
                span,
                expected: vec!["a closing `*/`"],
                found: "end of input".to_string(),
            },
            LexError::UnexpectedChar { ch, .. } => ParseError {
                span,
                expected: vec!["a token"],
                found: format!("`{ch}`"),
            },
            LexError::IntegerTooLarge { .. } => ParseError {
                span,
                expected: vec!["an integer that fits in 64 bits"],
                found: "a larger integer".to_string(),
            },
        }
    }
}

/// Parse a whole schema file.
pub fn parse(src: &str) -> Result<File, ParseError> {
    parse_with_comments(src).map(|(file, _)| file)
}

/// Parse a whole schema file, also returning its ordinary comments for the
/// formatter.
pub fn parse_with_comments(src: &str) -> Result<(File, Vec<Comment>), ParseError> {
    let (toks, comments) = lex_with_comments(src)?;
    let mut p = Parser { toks, pos: 0 };
    let file = p.file()?;
    Ok((file, comments))
}

struct Parser {
    toks: Vec<Token>,
    pos: usize,
}

type PResult<T> = Result<T, ParseError>;

impl Parser {
    fn peek(&self) -> &Token {
        &self.toks[self.pos.min(self.toks.len() - 1)]
    }

    fn peek_kind(&self) -> &TokenKind {
        &self.peek().kind
    }

    fn peek_ident(&self) -> Option<&str> {
        match self.peek_kind() {
            TokenKind::Ident(name) => Some(name),
            _ => None,
        }
    }

    fn peek_at(&self, n: usize) -> &TokenKind {
        &self.toks[(self.pos + n).min(self.toks.len() - 1)].kind
    }

    fn bump(&mut self) -> Token {
        let tok = self.peek().clone();
        if self.pos < self.toks.len() - 1 {
            self.pos += 1;
        }
        tok
    }

    fn error<T>(&self, expected: Vec<&'static str>) -> PResult<T> {
        let tok = self.peek();
        Err(ParseError {
            span: tok.span,
            expected,
            found: tok.kind.describe(),
        })
    }

    fn at_punct(&self, kind: &TokenKind) -> bool {
        self.peek_kind() == kind
    }

    fn eat_punct(&mut self, kind: &TokenKind) -> bool {
        if self.at_punct(kind) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_punct(&mut self, kind: TokenKind, label: &'static str) -> PResult<Span> {
        if self.at_punct(&kind) {
            Ok(self.bump().span)
        } else {
            self.error(vec![label])
        }
    }

    fn at_keyword(&self, kw: &str) -> bool {
        self.peek_ident() == Some(kw)
    }

    fn expect_keyword(&mut self, kw: &str, label: &'static str) -> PResult<Span> {
        if self.at_keyword(kw) {
            Ok(self.bump().span)
        } else {
            self.error(vec![label])
        }
    }

    fn expect_ident(&mut self, label: &'static str) -> PResult<Ident> {
        match self.peek_kind() {
            TokenKind::Ident(_) => {
                let tok = self.bump();
                let TokenKind::Ident(name) = tok.kind else {
                    unreachable!()
                };
                Ok(Ident {
                    name,
                    span: tok.span,
                })
            }
            _ => self.error(vec![label]),
        }
    }

    fn expect_string(&mut self, label: &'static str) -> PResult<StrLit> {
        match self.peek_kind() {
            TokenKind::Str(_) => {
                let tok = self.bump();
                let TokenKind::Str(value) = tok.kind else {
                    unreachable!()
                };
                Ok(StrLit {
                    value,
                    span: tok.span,
                })
            }
            _ => self.error(vec![label]),
        }
    }

    fn expect_int(&mut self, label: &'static str) -> PResult<IntLit> {
        match self.peek_kind() {
            TokenKind::Int(_) => {
                let tok = self.bump();
                let TokenKind::Int(value) = tok.kind else {
                    unreachable!()
                };
                Ok(IntLit {
                    value,
                    span: tok.span,
                })
            }
            _ => self.error(vec![label]),
        }
    }

    /// Consecutive `///` lines; the declaration they document follows.
    fn docs(&mut self) -> Vec<String> {
        let mut out = Vec::new();
        while let TokenKind::Doc(text) = self.peek_kind() {
            out.push(text.clone());
            self.bump();
        }
        out
    }

    /// Fails at `at` (where doc comments started) when the docs were read
    /// but nothing that can carry them followed, so the error names the
    /// doc comment.
    fn error_at<T>(&mut self, at: usize, expected: Vec<&'static str>) -> PResult<T> {
        self.pos = at;
        self.error(expected)
    }

    // -- productions --------------------------------------------------------

    fn file(&mut self) -> PResult<File> {
        let mut docs = Vec::new();
        while let TokenKind::InnerDoc(text) = self.peek_kind() {
            docs.push(text.clone());
            self.bump();
        }
        let layer = self.layer_decl()?;
        let mut imports = Vec::new();
        let mut contexts = Vec::new();
        let mut items = Vec::new();
        // Every declaration parses in every file (S058 names the misplaced
        // ones); the expected list on an error names the file's own layer.
        let decls: &[&str] = match layer.layer {
            Layer::Domain => &["`context`"],
            Layer::Derivation => &["`state`", "`projection`"],
            Layer::Application => &["`commands`", "`invariants`", "`invariant`", "`process`"],
        };
        loop {
            let at = self.pos;
            let item_docs = self.docs();
            let nothing_yet = contexts.is_empty() && items.is_empty();
            if self.at_keyword("import") && nothing_yet {
                if !item_docs.is_empty() {
                    return self.error_at(at, decls.to_vec());
                }
                let start = self.bump().span;
                let path = self.expect_string("a file path")?;
                let span = start.join(path.span);
                imports.push(Import { path, span });
                continue;
            }
            match self.peek_ident() {
                Some("context") => contexts.push(self.context(item_docs)?),
                Some("state") => items.push(LayerItem::State(self.state_decl(item_docs)?)),
                Some("projection") => {
                    let start = self.bump().span;
                    let path = self.ctx_path("a projection name")?;
                    let decl = self.projection_decl(item_docs, start, path.name.clone())?;
                    items.push(LayerItem::Projection(path, decl));
                }
                Some("commands") => items.push(LayerItem::Commands(self.commands_decl(item_docs)?)),
                Some("invariants") => {
                    items.push(LayerItem::Invariants(self.invariants_decl(item_docs)?))
                }
                Some("invariant") => {
                    let start = self.bump().span;
                    let path = self.ctx_path("an invariant name")?;
                    let decl = self.invariant_decl(item_docs, start, path.name.clone())?;
                    items.push(LayerItem::Invariant(path, decl));
                }
                Some("process") => {
                    let start = self.bump().span;
                    let path = self.ctx_path("a process name")?;
                    let decl = self.process_decl(item_docs, start, path.name.clone())?;
                    items.push(LayerItem::Process(path, decl));
                }
                _ if item_docs.is_empty() && matches!(self.peek_kind(), TokenKind::Eof) => {
                    return Ok(File {
                        docs,
                        layer,
                        imports,
                        contexts,
                        items,
                    });
                }
                _ => {
                    let mut expected = decls.to_vec();
                    if nothing_yet && imports.is_empty() {
                        expected.insert(0, "`import`");
                    }
                    expected.push("end of input");
                    return self.error_at(at, expected);
                }
            }
        }
    }

    /// `layer domain | derivation | application`, first in every file.
    fn layer_decl(&mut self) -> PResult<LayerDecl> {
        let at = self.pos;
        if !self.at_keyword("layer") {
            return self.error_at(
                at,
                vec![
                    "`layer domain`",
                    "`layer derivation`",
                    "`layer application`",
                ],
            );
        }
        let start = self.bump().span;
        let name = self.expect_ident("`domain`, `derivation` or `application`")?;
        let Some(layer) = Layer::parse(&name.name) else {
            self.pos -= 1;
            return self.error(vec!["`domain`", "`derivation`", "`application`"]);
        };
        Ok(LayerDecl {
            layer,
            span: start.join(name.span),
        })
    }

    /// `Context.Aggregate`.
    fn agg_path(&mut self) -> PResult<AggPath> {
        let context = self.expect_ident("a context name")?;
        self.expect_punct(TokenKind::Dot, "`.`")?;
        let aggregate = self.expect_ident("an aggregate name")?;
        Ok(AggPath {
            span: context.span.join(aggregate.span),
            context,
            aggregate,
        })
    }

    /// `Context.Name`.
    fn ctx_path(&mut self, what: &'static str) -> PResult<CtxPath> {
        let context = self.expect_ident("a context name")?;
        self.expect_punct(TokenKind::Dot, "`.`")?;
        let name = self.expect_ident(what)?;
        Ok(CtxPath {
            span: context.span.join(name.span),
            context,
            name,
        })
    }

    /// `state Ctx.Agg { fields } evolve wasm ".." [snapshot every N]`.
    fn state_decl(&mut self, docs: Vec<String>) -> PResult<StateDecl> {
        let start = self.expect_keyword("state", "`state`")?;
        let aggregate = self.agg_path()?;
        let (fields, _) = self.field_block()?;
        self.expect_keyword("evolve", "`evolve`")?;
        let evolve = self.wasm_ref()?;
        let mut end = evolve.span;
        let snapshot_every = if self.at_keyword("snapshot") {
            self.bump();
            self.expect_keyword("every", "`every`")?;
            let n = self.expect_int("an integer")?;
            end = n.span;
            Some(n)
        } else {
            None
        };
        Ok(StateDecl {
            docs,
            aggregate,
            fields,
            evolve,
            snapshot_every,
            span: start.join(end),
        })
    }

    /// `commands Ctx.Agg { Name { .. } -> wasm "..", .. }`.
    fn commands_decl(&mut self, docs: Vec<String>) -> PResult<CommandsDecl> {
        let start = self.expect_keyword("commands", "`commands`")?;
        let aggregate = self.agg_path()?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        let mut commands = Vec::new();
        let end = loop {
            let at = self.pos;
            let item_docs = self.docs();
            if item_docs.is_empty() && self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            if !matches!(self.peek_kind(), TokenKind::Ident(_)) {
                return self.error_at(at, vec!["a command name", "`}`"]);
            }
            self.pos = at;
            commands.push(self.command_decl()?);
            if self.eat_punct(&TokenKind::Comma) {
                continue;
            }
            if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            return self.error(vec!["`,`", "`}`"]);
        };
        Ok(CommandsDecl {
            docs,
            aggregate,
            commands,
            span: start.join(end),
        })
    }

    /// `invariants Ctx.Agg { Name -> wasm "..", Name: expr, .. }`.
    fn invariants_decl(&mut self, docs: Vec<String>) -> PResult<InvariantsDecl> {
        let start = self.expect_keyword("invariants", "`invariants`")?;
        let aggregate = self.agg_path()?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        let mut invariants = Vec::new();
        let end = loop {
            let at = self.pos;
            let item_docs = self.docs();
            if item_docs.is_empty() && self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            if !matches!(self.peek_kind(), TokenKind::Ident(_)) {
                return self.error_at(at, vec!["an invariant name", "`}`"]);
            }
            self.pos = at;
            invariants.push(self.invariant_ref()?);
            if self.eat_punct(&TokenKind::Comma) {
                continue;
            }
            if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            return self.error(vec!["`,`", "`}`"]);
        };
        Ok(InvariantsDecl {
            docs,
            aggregate,
            invariants,
            span: start.join(end),
        })
    }

    fn context(&mut self, docs: Vec<String>) -> PResult<Context> {
        let start = self.expect_keyword("context", "`context`")?;
        let name = self.expect_ident("a context name")?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        let mut items = Vec::new();
        let end = loop {
            let at = self.pos;
            let docs = self.docs();
            if docs.is_empty() && self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            match self.peek_ident() {
                Some("value") => items.push(Item::Value(self.value_decl(docs)?)),
                Some("enum") => items.push(Item::Enum(self.enum_decl(docs)?)),
                Some("event") => items.push(Item::Event(self.event_decl(docs)?)),
                Some("aggregate") => {
                    items.push(Item::Aggregate(Box::new(self.aggregate_decl(docs)?)))
                }
                _ => {
                    return self.error_at(
                        at,
                        vec!["`value`", "`enum`", "`event`", "`aggregate`", "`}`"],
                    );
                }
            }
        };
        Ok(Context {
            docs,
            name,
            items,
            span: start.join(end),
        })
    }

    fn value_decl(&mut self, docs: Vec<String>) -> PResult<ValueDecl> {
        let start = self.expect_keyword("value", "`value`")?;
        let name = self.expect_ident("a value name")?;
        let (fields, mut end) = self.field_block()?;
        let mut rules = Vec::new();
        if self.at_keyword("rules") {
            self.bump();
            let (block, block_end) = self.rule_block()?;
            rules = block;
            end = block_end;
        }
        Ok(ValueDecl {
            docs,
            name,
            fields,
            rules,
            span: start.join(end),
        })
    }

    /// `{ Name: expr, ... }`; returns the rules and the closing brace's span.
    fn rule_block(&mut self) -> PResult<(Vec<RuleDecl>, Span)> {
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        let mut rules = Vec::new();
        let end = loop {
            let at = self.pos;
            let rule_docs = self.docs();
            if rule_docs.is_empty() && self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            if !matches!(self.peek_kind(), TokenKind::Ident(_)) {
                return self.error_at(at, vec!["a rule name", "`}`"]);
            }
            rules.push(self.rule_decl(rule_docs)?);
            if self.eat_punct(&TokenKind::Comma) {
                continue;
            }
            if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            return self.error(vec!["`,`", "`}`"]);
        };
        Ok((rules, end))
    }

    fn rule_decl(&mut self, docs: Vec<String>) -> PResult<RuleDecl> {
        let name = self.expect_ident("a rule name")?;
        self.expect_punct(TokenKind::Colon, "`:`")?;
        let expr = self.or_expr()?;
        let span = name.span.join(expr.span());
        Ok(RuleDecl {
            docs,
            name,
            expr,
            span,
        })
    }

    // -- rule expressions ------------------------------------------------

    fn or_expr(&mut self) -> PResult<Expr> {
        let mut lhs = self.and_expr()?;
        while self.at_keyword("or") {
            self.bump();
            let rhs = self.and_expr()?;
            lhs = Expr::Or(Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn and_expr(&mut self) -> PResult<Expr> {
        let mut lhs = self.not_expr()?;
        while self.at_keyword("and") {
            self.bump();
            let rhs = self.not_expr()?;
            lhs = Expr::And(Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn not_expr(&mut self) -> PResult<Expr> {
        if self.at_keyword("not") {
            self.bump();
            let inner = self.not_expr()?;
            return Ok(Expr::Not(Box::new(inner)));
        }
        if self.at_punct(&TokenKind::LParen) {
            self.bump();
            let inner = self.or_expr()?;
            self.expect_punct(TokenKind::RParen, "`)`")?;
            return Ok(inner);
        }
        self.comparison()
    }

    fn comparison(&mut self) -> PResult<Expr> {
        let lhs = self.term()?;
        if self.at_keyword("exists") {
            let root = match lhs {
                Term::Path(p) if p.segments.len() == 1 => p.segments.into_iter().next().unwrap(),
                _ => return self.error(vec!["a single name before `exists`"]),
            };
            let kw = self.bump().span;
            let span = root.span.join(kw);
            return Ok(Expr::Exists { root, span });
        }
        if self.at_keyword("matches") {
            let Term::Path(path) = lhs else {
                return self.error(vec!["a field path before `matches`"]);
            };
            self.bump();
            let pattern = self.expect_string("a regular expression string")?;
            let span = path.span.join(pattern.span);
            return Ok(Expr::Matches {
                path,
                pattern,
                span,
            });
        }
        if self.at_keyword("in") {
            let Term::Path(path) = lhs else {
                return self.error(vec!["a field path before `in`"]);
            };
            self.bump();
            self.expect_punct(TokenKind::LBracket, "`[`")?;
            let mut items = Vec::new();
            let end = loop {
                if self.at_punct(&TokenKind::RBracket) {
                    break self.bump().span;
                }
                items.push(self.literal()?);
                if self.eat_punct(&TokenKind::Comma) {
                    continue;
                }
                if self.at_punct(&TokenKind::RBracket) {
                    break self.bump().span;
                }
                return self.error(vec!["`,`", "`]`"]);
            };
            let span = path.span.join(end);
            return Ok(Expr::In { path, items, span });
        }
        let op = match self.peek_kind() {
            TokenKind::Lt => CmpOp::Lt,
            TokenKind::Le => CmpOp::Le,
            TokenKind::Gt => CmpOp::Gt,
            TokenKind::Ge => CmpOp::Ge,
            TokenKind::EqEq => CmpOp::Eq,
            TokenKind::Ne => CmpOp::Ne,
            _ => {
                return self.error(vec![
                    "`<`",
                    "`<=`",
                    "`>`",
                    "`>=`",
                    "`==`",
                    "`!=`",
                    "`matches`",
                    "`in`",
                ]);
            }
        };
        self.bump();
        let rhs = self.term()?;
        let span = lhs.span().join(rhs.span());
        Ok(Expr::Cmp { lhs, op, rhs, span })
    }

    fn term(&mut self) -> PResult<Term> {
        match self.peek_kind().clone() {
            TokenKind::Int(_) | TokenKind::Dec(_) | TokenKind::Str(_) | TokenKind::Minus => {
                Ok(Term::Lit(self.literal()?))
            }
            TokenKind::Ident(name) if name == "true" || name == "false" => {
                Ok(Term::Lit(self.literal()?))
            }
            TokenKind::Ident(name) if name == "len" && self.peek_at(1) == &TokenKind::LParen => {
                let start = self.bump().span;
                self.bump();
                let path = self.field_path()?;
                let end = self.expect_punct(TokenKind::RParen, "`)`")?;
                Ok(Term::Len(path, start.join(end)))
            }
            TokenKind::Ident(_) => Ok(Term::Path(self.field_path()?)),
            _ => self.error(vec!["a field path", "a literal", "`len(`"]),
        }
    }

    fn literal(&mut self) -> PResult<Literal> {
        let negative = if self.at_punct(&TokenKind::Minus) {
            Some(self.bump().span)
        } else {
            None
        };
        let tok = self.peek().clone();
        match (&tok.kind, negative) {
            (TokenKind::Int(n), neg) => {
                self.bump();
                let text = match neg {
                    Some(_) => format!("-{n}"),
                    None => n.to_string(),
                };
                Ok(Literal::Number(
                    text,
                    neg.unwrap_or(tok.span).join(tok.span),
                ))
            }
            (TokenKind::Dec(d), neg) => {
                self.bump();
                let text = match neg {
                    Some(_) => format!("-{d}"),
                    None => d.clone(),
                };
                Ok(Literal::Number(
                    text,
                    neg.unwrap_or(tok.span).join(tok.span),
                ))
            }
            (TokenKind::Str(_), None) => Ok(Literal::Str(self.expect_string("a string")?)),
            (TokenKind::Ident(b), None) if b == "true" || b == "false" => {
                self.bump();
                Ok(Literal::Bool(b == "true", tok.span))
            }
            (TokenKind::Ident(_), None) => {
                Ok(Literal::Variant(self.expect_ident("a variant name")?))
            }
            _ => self.error(vec![
                "a number",
                "a string",
                "`true`",
                "`false`",
                "a variant name",
            ]),
        }
    }

    fn field_path(&mut self) -> PResult<FieldPath> {
        let first = self.expect_ident("a field name")?;
        let start = first.span;
        let mut end = first.span;
        let mut segments = vec![first];
        while self.at_punct(&TokenKind::Dot) {
            self.bump();
            let next = self.expect_ident("a field name")?;
            end = next.span;
            segments.push(next);
        }
        Ok(FieldPath {
            segments,
            span: start.join(end),
        })
    }

    fn enum_decl(&mut self, docs: Vec<String>) -> PResult<EnumDecl> {
        let start = self.expect_keyword("enum", "`enum`")?;
        let name = self.expect_ident("an enum name")?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        let mut variants = vec![self.variant()?];
        let end = loop {
            if self.eat_punct(&TokenKind::Comma) {
                if self.at_punct(&TokenKind::RBrace) {
                    break self.bump().span;
                }
                variants.push(self.variant()?);
            } else if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            } else {
                return self.error(vec!["`,`", "`}`"]);
            }
        };
        Ok(EnumDecl {
            docs,
            name,
            variants,
            span: start.join(end),
        })
    }

    /// `Name` or `Name { fields }`; a payload has at least one field.
    fn variant(&mut self) -> PResult<Variant> {
        let docs = self.docs();
        let name = self.expect_ident("a variant name")?;
        let mut span = name.span;
        let payload = if self.at_punct(&TokenKind::LBrace) {
            if self.peek_at(1) == &TokenKind::RBrace {
                self.bump();
                return self.error(vec!["a field name"]);
            }
            let (fields, end) = self.field_block()?;
            span = span.join(end);
            Some(fields)
        } else {
            None
        };
        Ok(Variant {
            docs,
            name,
            payload,
            span,
        })
    }

    fn event_decl(&mut self, docs: Vec<String>) -> PResult<EventDecl> {
        let start = self.expect_keyword("event", "`event`")?;
        let name = self.expect_ident("an event name")?;
        let version = self.version()?;
        let (fields, mut end) = self.field_block()?;
        let upcast = if self.at_keyword("upcast") {
            let u = self.upcast_decl()?;
            end = u.span;
            Some(u)
        } else {
            None
        };
        Ok(EventDecl {
            docs,
            name,
            version,
            fields,
            upcast,
            span: start.join(end),
        })
    }

    /// `upcast from vN { ops }` or `upcast from vN wasm "..."`.
    fn upcast_decl(&mut self) -> PResult<UpcastDecl> {
        let start = self.expect_keyword("upcast", "`upcast`")?;
        self.expect_keyword("from", "`from`")?;
        let from = self.version()?;
        if self.at_keyword("wasm") {
            let w = self.wasm_ref()?;
            let span = start.join(w.span);
            return Ok(UpcastDecl {
                from,
                how: UpcastHow::Wasm(w),
                span,
            });
        }
        if !self.at_punct(&TokenKind::LBrace) {
            return self.error(vec!["`{`", "`wasm`"]);
        }
        self.bump();
        let mut ops = Vec::new();
        let end = loop {
            if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            match self.peek_ident() {
                Some("set") => {
                    let kw = self.bump().span;
                    let field = self.expect_ident("a field name")?;
                    self.expect_punct(TokenKind::Colon, "`:`")?;
                    let value = self.upcast_value()?;
                    let span = kw.join(value.span());
                    ops.push(UpcastOp::Set { field, value, span });
                }
                Some("rename") => {
                    let kw = self.bump().span;
                    let from = self.expect_ident("a field name")?;
                    self.expect_keyword("as", "`as`")?;
                    let to = self.expect_ident("a field name")?;
                    let span = kw.join(to.span);
                    ops.push(UpcastOp::Rename { from, to, span });
                }
                _ => return self.error(vec!["`set`", "`rename`", "`}`"]),
            }
            if self.eat_punct(&TokenKind::Comma) {
                continue;
            }
            if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            return self.error(vec!["`,`", "`}`"]);
        };
        Ok(UpcastDecl {
            from,
            how: UpcastHow::Ops(ops),
            span: start.join(end),
        })
    }

    /// A literal, `null`, `[v, ...]` or `{ k: v, ... }`.
    fn upcast_value(&mut self) -> PResult<UpcastValue> {
        match self.peek_kind().clone() {
            TokenKind::Ident(name) if name == "null" => {
                let span = self.bump().span;
                Ok(UpcastValue::Null(span))
            }
            TokenKind::LBracket => {
                let start = self.bump().span;
                let mut items = Vec::new();
                let end = loop {
                    if self.at_punct(&TokenKind::RBracket) {
                        break self.bump().span;
                    }
                    items.push(self.upcast_value()?);
                    if self.eat_punct(&TokenKind::Comma) {
                        continue;
                    }
                    if self.at_punct(&TokenKind::RBracket) {
                        break self.bump().span;
                    }
                    return self.error(vec!["`,`", "`]`"]);
                };
                Ok(UpcastValue::List(items, start.join(end)))
            }
            TokenKind::LBrace => {
                let start = self.bump().span;
                let mut entries = Vec::new();
                let end = loop {
                    if self.at_punct(&TokenKind::RBrace) {
                        break self.bump().span;
                    }
                    let key = self.expect_ident("a field name")?;
                    self.expect_punct(TokenKind::Colon, "`:`")?;
                    let value = self.upcast_value()?;
                    entries.push((key, value));
                    if self.eat_punct(&TokenKind::Comma) {
                        continue;
                    }
                    if self.at_punct(&TokenKind::RBrace) {
                        break self.bump().span;
                    }
                    return self.error(vec!["`,`", "`}`"]);
                };
                Ok(UpcastValue::Object(entries, start.join(end)))
            }
            TokenKind::Int(_)
            | TokenKind::Dec(_)
            | TokenKind::Str(_)
            | TokenKind::Minus
            | TokenKind::Ident(_) => Ok(UpcastValue::Lit(self.literal()?)),
            _ => self.error(vec!["a literal", "a variant name", "`null`", "`[`", "`{`"]),
        }
    }

    /// `vN`, lexed as a single identifier.
    fn version(&mut self) -> PResult<IntLit> {
        if let Some(name) = self.peek_ident()
            && let Some(digits) = name.strip_prefix('v')
            && !digits.is_empty()
            && digits.bytes().all(|b| b.is_ascii_digit())
        {
            let value = digits.parse::<u64>().map_err(|_| ParseError {
                span: self.peek().span,
                expected: vec!["a version that fits in 64 bits"],
                found: self.peek().kind.describe(),
            })?;
            let tok = self.bump();
            return Ok(IntLit {
                value,
                span: tok.span,
            });
        }
        self.error(vec!["a version like `v1`"])
    }

    /// `{ Field, Field, }`; returns the fields and the span of the closing brace.
    fn field_block(&mut self) -> PResult<(Vec<Field>, Span)> {
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        let mut fields = Vec::new();
        let end = loop {
            let at = self.pos;
            let docs = self.docs();
            if docs.is_empty() && self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            if !matches!(self.peek_kind(), TokenKind::Ident(_)) {
                return self.error_at(at, vec!["a field name", "`}`"]);
            }
            fields.push(self.field(docs)?);
            if self.eat_punct(&TokenKind::Comma) {
                continue;
            }
            if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            return self.error(vec!["`,`", "`}`"]);
        };
        Ok((fields, end))
    }

    fn field(&mut self, docs: Vec<String>) -> PResult<Field> {
        let name = self.expect_ident("a field name")?;
        self.expect_punct(TokenKind::Colon, "`:`")?;
        let ty = self.ty()?;
        let mut span = name.span.join(ty.span);
        let default = if self.eat_punct(&TokenKind::Eq) {
            let lit = self.literal()?;
            span = span.join(lit.span());
            Some(lit)
        } else {
            None
        };
        Ok(Field {
            docs,
            name,
            ty,
            default,
            span,
        })
    }

    fn ty(&mut self) -> PResult<Type> {
        let start = self.peek().span;
        let base = match self.peek_kind().clone() {
            TokenKind::LBracket => {
                self.bump();
                let inner = self.ty()?;
                self.expect_punct(TokenKind::RBracket, "`]`")?;
                BaseType::List(Box::new(inner))
            }
            TokenKind::Ident(name) => match name.as_str() {
                "list" if self.peek_at(1) == &TokenKind::Lt => {
                    self.bump();
                    self.bump();
                    let inner = self.ty()?;
                    self.expect_punct(TokenKind::Gt, "`>`")?;
                    BaseType::List(Box::new(inner))
                }
                "set" if self.peek_at(1) == &TokenKind::Lt => {
                    self.bump();
                    self.bump();
                    let sc = self.scalar()?;
                    self.expect_punct(TokenKind::Gt, "`>`")?;
                    BaseType::Set(sc)
                }
                "map" if self.peek_at(1) == &TokenKind::Lt => {
                    self.bump();
                    self.bump();
                    let key = self.scalar()?;
                    self.expect_punct(TokenKind::Comma, "`,`")?;
                    let value = self.ty()?;
                    self.expect_punct(TokenKind::Gt, "`>`")?;
                    BaseType::Map(key, Box::new(value))
                }
                _ => {
                    if let Ok(sc) = name.parse::<Scalar>() {
                        self.bump();
                        BaseType::Scalar(sc)
                    } else {
                        let first = self.expect_ident("a type")?;
                        if self.eat_punct(&TokenKind::Dot) {
                            let second = self.expect_ident("a type name")?;
                            BaseType::Ref(TypeRefSyntax {
                                qualifier: Some(first),
                                name: second,
                            })
                        } else {
                            BaseType::Ref(TypeRefSyntax {
                                qualifier: None,
                                name: first,
                            })
                        }
                    }
                }
            },
            _ => return self.error(vec!["a type"]),
        };
        let mut end = self.toks[self.pos - 1].span;
        let optional = if self.at_punct(&TokenKind::Question) {
            end = self.bump().span;
            true
        } else {
            false
        };
        Ok(Type {
            base,
            optional,
            span: start.join(end),
        })
    }

    fn scalar(&mut self) -> PResult<Scalar> {
        if let Some(name) = self.peek_ident()
            && let Ok(sc) = name.parse::<Scalar>()
        {
            self.bump();
            return Ok(sc);
        }
        self.error(vec!["a scalar type"])
    }

    fn aggregate_decl(&mut self, docs: Vec<String>) -> PResult<AggregateDecl> {
        let start = self.expect_keyword("aggregate", "`aggregate`")?;
        let name = self.expect_ident("an aggregate name")?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        self.expect_keyword("key", "`key`")?;
        let key = self.field(Vec::new())?;
        self.expect_keyword("stream", "`stream`")?;
        let stream = self.expect_string("a stream template string")?;
        let mut items = Vec::new();
        loop {
            let at = self.pos;
            let item_docs = self.docs();
            match self.peek_ident() {
                Some("value") => items.push(LocalItem::Value(self.value_decl(item_docs)?)),
                Some("enum") => items.push(LocalItem::Enum(self.enum_decl(item_docs)?)),
                Some("entity") => {
                    items.push(LocalItem::Entity(Box::new(self.entity_decl(item_docs)?)))
                }
                Some("events") if item_docs.is_empty() => break,
                _ => {
                    return self.error_at(at, vec!["`value`", "`enum`", "`entity`", "`events`"]);
                }
            }
        }
        self.expect_keyword("events", "`events`")?;
        let events = self.event_refs()?;
        let end = self.expect_punct(TokenKind::RBrace, "`}`")?;
        Ok(AggregateDecl {
            docs,
            name,
            key,
            stream,
            items,
            events,
            span: start.join(end),
        })
    }

    fn entity_decl(&mut self, docs: Vec<String>) -> PResult<EntityDecl> {
        let start = self.expect_keyword("entity", "`entity`")?;
        let name = self.expect_ident("an entity name")?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        let id_docs = self.docs();
        self.expect_keyword("id", "`id`")?;
        let id = self.field(id_docs)?;
        let mut fields = Vec::new();
        let end = loop {
            if self.eat_punct(&TokenKind::Comma) {
                let at = self.pos;
                let field_docs = self.docs();
                if field_docs.is_empty() && self.at_punct(&TokenKind::RBrace) {
                    break self.bump().span;
                }
                if !matches!(self.peek_kind(), TokenKind::Ident(_)) {
                    return self.error_at(at, vec!["a field name", "`}`"]);
                }
                fields.push(self.field(field_docs)?);
            } else if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            } else {
                return self.error(vec!["`,`", "`}`"]);
            }
        };
        Ok(EntityDecl {
            docs,
            name,
            id,
            fields,
            span: start.join(end),
        })
    }

    fn event_refs(&mut self) -> PResult<Vec<EventRef>> {
        let mut refs = vec![self.event_ref()?];
        while self.eat_punct(&TokenKind::Comma) {
            refs.push(self.event_ref()?);
        }
        Ok(refs)
    }

    fn event_ref(&mut self) -> PResult<EventRef> {
        let first = self.expect_ident("an event name")?;
        if self.eat_punct(&TokenKind::Dot) {
            let second = self.expect_ident("an event name")?;
            let span = first.span.join(second.span);
            Ok(EventRef {
                qualifier: Some(first),
                name: second,
                span,
            })
        } else {
            let span = first.span;
            Ok(EventRef {
                qualifier: None,
                name: first,
                span,
            })
        }
    }

    fn wasm_ref(&mut self) -> PResult<WasmRef> {
        let start = self.expect_keyword("wasm", "`wasm`")?;
        let module = self.expect_string("a wasm module path string")?;
        let mut end = module.span;
        let export = if self.at_keyword("export") {
            self.bump();
            let e = self.expect_string("an export name string")?;
            end = e.span;
            Some(e)
        } else {
            None
        };
        Ok(WasmRef {
            module,
            export,
            span: start.join(end),
        })
    }

    fn command_decl(&mut self) -> PResult<CommandDecl> {
        let docs = self.docs();
        let name = self.expect_ident("a command name")?;
        let (fields, _) = self.field_block()?;
        let mut requires = Vec::new();
        if self.at_keyword("requires") {
            let kw = self.bump().span;
            if self.at_punct(&TokenKind::LBrace) {
                requires = self.rule_block()?.0;
            } else {
                let expr = self.or_expr()?;
                let span = kw.join(expr.span());
                requires.push(RuleDecl {
                    docs: Vec::new(),
                    name: Ident {
                        name: "Requires".to_string(),
                        span: kw,
                    },
                    expr,
                    span,
                });
            }
        }
        self.expect_punct(TokenKind::Arrow, "`->`")?;
        let handler = self.wasm_ref()?;
        let span = name.span.join(handler.span);
        Ok(CommandDecl {
            docs,
            name,
            fields,
            requires,
            handler,
            span,
        })
    }

    fn invariant_ref(&mut self) -> PResult<InvariantRef> {
        let docs = self.docs();
        let name = self.expect_ident("an invariant name")?;
        let start = name.span;
        let (check, end) = if self.eat_punct(&TokenKind::Arrow) {
            let w = self.wasm_ref()?;
            let end = w.span;
            (InvariantCheckSyntax::Wasm(w), end)
        } else if self.eat_punct(&TokenKind::Colon) {
            let e = self.or_expr()?;
            let end = e.span();
            (InvariantCheckSyntax::Expr(e), end)
        } else {
            return self.error(vec!["`->`", "`:`"]);
        };
        Ok(InvariantRef {
            docs,
            name,
            check,
            span: start.join(end),
        })
    }

    /// The body of `invariant Ctx.Name { .. }`, after the name.
    fn invariant_decl(
        &mut self,
        docs: Vec<String>,
        start: Span,
        name: Ident,
    ) -> PResult<InvariantDecl> {
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        self.expect_keyword("on", "`on`")?;
        let on = self.expect_ident("an aggregate name")?;
        self.expect_keyword("projection", "`projection`")?;
        let projection = self.event_ref()?;
        self.expect_keyword("scope", "`scope`")?;
        let scope = self.expect_ident("a state field name")?;
        self.expect_keyword("check", "`check`")?;
        let check = self.wasm_ref()?;
        let end = self.expect_punct(TokenKind::RBrace, "`}`")?;
        Ok(InvariantDecl {
            docs,
            name,
            on,
            projection,
            scope,
            check,
            span: start.join(end),
        })
    }

    /// The body of `process Ctx.Name { .. }`, after the name.
    fn process_decl(
        &mut self,
        docs: Vec<String>,
        start: Span,
        name: Ident,
    ) -> PResult<ProcessDecl> {
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        self.expect_keyword("key", "`key`")?;
        let key = self.field(Vec::new())?;
        self.expect_keyword("from", "`from`")?;
        let mut from = vec![self.process_source()?];
        while self.eat_punct(&TokenKind::Comma) {
            from.push(self.process_source()?);
        }
        self.expect_keyword("state", "`state`")?;
        let (state, _) = self.field_block()?;
        self.expect_keyword("react", "`react`")?;
        let react = self.wasm_ref()?;
        let snapshot_every = if self.at_keyword("snapshot") {
            self.bump();
            self.expect_keyword("every", "`every`")?;
            Some(self.expect_int("an integer")?)
        } else {
            None
        };
        let mut timers = Vec::new();
        if self.at_keyword("timers") {
            self.bump();
            timers.push(self.expect_ident("a timer name")?);
            while self.eat_punct(&TokenKind::Comma) {
                timers.push(self.expect_ident("a timer name")?);
            }
        }
        let end = self.expect_punct(TokenKind::RBrace, "`}`")?;
        Ok(ProcessDecl {
            docs,
            name,
            key,
            from,
            state,
            react,
            snapshot_every,
            timers,
            span: start.join(end),
        })
    }

    fn process_source(&mut self) -> PResult<ProcessSource> {
        let event = self.event_ref()?;
        let start = event.span;
        let mut end = event.span;
        let by = if self.at_keyword("by") {
            self.bump();
            let ident = self.expect_ident("an event field name")?;
            end = ident.span;
            Some(ident)
        } else {
            None
        };
        Ok(ProcessSource {
            event,
            by,
            span: start.join(end),
        })
    }

    /// The body of `projection Ctx.Name { .. }`, after the name.
    fn projection_decl(
        &mut self,
        docs: Vec<String>,
        start: Span,
        name: Ident,
    ) -> PResult<ProjectionDecl> {
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        self.expect_keyword("from", "`from`")?;
        let from = self.event_refs()?;
        self.expect_keyword("fold", "`fold`")?;
        let fold = self.wasm_ref()?;
        let snapshot_every = if self.at_keyword("snapshot") {
            self.bump();
            self.expect_keyword("every", "`every`")?;
            Some(self.expect_int("an integer")?)
        } else {
            None
        };
        let first_docs = self.docs();
        let mut tables = vec![self.table_decl(first_docs)?];
        let end = loop {
            let at = self.pos;
            let table_docs = self.docs();
            if self.at_keyword("table") {
                tables.push(self.table_decl(table_docs)?);
            } else if table_docs.is_empty() && self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            } else {
                return self.error_at(at, vec!["`table`", "`}`"]);
            }
        };
        Ok(ProjectionDecl {
            docs,
            name,
            from,
            fold,
            snapshot_every,
            tables,
            span: start.join(end),
        })
    }

    fn table_decl(&mut self, docs: Vec<String>) -> PResult<TableDecl> {
        let start = self.expect_keyword("table", "`table`")?;
        let name = self.expect_ident("a table name")?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        let mut fields = Vec::new();
        let end = loop {
            let at = self.pos;
            let field_docs = self.docs();
            if field_docs.is_empty() && self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            if !matches!(self.peek_kind(), TokenKind::Ident(_)) {
                return self.error_at(at, vec!["`key`", "a column name", "`}`"]);
            }
            // `key name: T` marks a key column; `key: T` is a column called key.
            let key = self.at_keyword("key") && self.peek_at(1) != &TokenKind::Colon;
            if key {
                self.bump();
            }
            let field = self.field(field_docs)?;
            fields.push(TableField { key, field });
            if self.eat_punct(&TokenKind::Comma) {
                continue;
            }
            if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            return self.error(vec!["`,`", "`}`"]);
        };
        Ok(TableDecl {
            docs,
            name,
            fields,
            span: start.join(end),
        })
    }
}
