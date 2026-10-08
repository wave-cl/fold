//! Recursive-descent parser for `.fold` files (no precedence; one token of
//! lookahead; the error names what was expected and what was found).

use std::fmt;

use crate::ast::*;
use crate::lexer::{LexError, Token, TokenKind, lex};
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
    let toks = lex(src)?;
    let mut p = Parser { toks, pos: 0 };
    p.file()
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

    // -- productions --------------------------------------------------------

    fn file(&mut self) -> PResult<File> {
        let mut contexts = Vec::new();
        loop {
            if matches!(self.peek_kind(), TokenKind::Eof) {
                return Ok(File { contexts });
            }
            if self.at_keyword("context") {
                contexts.push(self.context()?);
            } else {
                return self.error(vec!["`context`", "end of input"]);
            }
        }
    }

    fn context(&mut self) -> PResult<Context> {
        let start = self.expect_keyword("context", "`context`")?;
        let name = self.expect_ident("a context name")?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        let mut items = Vec::new();
        let end = loop {
            if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            match self.peek_ident() {
                Some("value") => items.push(Item::Value(self.value_decl()?)),
                Some("enum") => items.push(Item::Enum(self.enum_decl()?)),
                Some("event") => items.push(Item::Event(self.event_decl()?)),
                Some("aggregate") => items.push(Item::Aggregate(Box::new(self.aggregate_decl()?))),
                Some("projection") => items.push(Item::Projection(self.projection_decl()?)),
                Some("invariant") => items.push(Item::Invariant(self.invariant_decl()?)),
                Some("process") => items.push(Item::Process(self.process_decl()?)),
                _ => {
                    return self.error(vec![
                        "`value`",
                        "`enum`",
                        "`event`",
                        "`aggregate`",
                        "`projection`",
                        "`invariant`",
                        "`process`",
                        "`}`",
                    ]);
                }
            }
        };
        Ok(Context {
            name,
            items,
            span: start.join(end),
        })
    }

    fn value_decl(&mut self) -> PResult<ValueDecl> {
        let start = self.expect_keyword("value", "`value`")?;
        let name = self.expect_ident("a value name")?;
        let (fields, mut end) = self.field_block()?;
        let mut rules = Vec::new();
        if self.at_keyword("rules") {
            self.bump();
            self.expect_punct(TokenKind::LBrace, "`{`")?;
            loop {
                if self.at_punct(&TokenKind::RBrace) {
                    end = self.bump().span;
                    break;
                }
                rules.push(self.rule_decl()?);
                if self.eat_punct(&TokenKind::Comma) {
                    continue;
                }
                if self.at_punct(&TokenKind::RBrace) {
                    end = self.bump().span;
                    break;
                }
                return self.error(vec!["`,`", "`}`"]);
            }
        }
        Ok(ValueDecl {
            name,
            fields,
            rules,
            span: start.join(end),
        })
    }

    fn rule_decl(&mut self) -> PResult<RuleDecl> {
        let name = self.expect_ident("a rule name")?;
        self.expect_punct(TokenKind::Colon, "`:`")?;
        let expr = self.or_expr()?;
        let span = name.span.join(expr.span());
        Ok(RuleDecl { name, expr, span })
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
            _ => self.error(vec!["a number", "a string", "`true`", "`false`"]),
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

    fn enum_decl(&mut self) -> PResult<EnumDecl> {
        let start = self.expect_keyword("enum", "`enum`")?;
        let name = self.expect_ident("an enum name")?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        let mut variants = vec![self.expect_ident("a variant name")?];
        let end = loop {
            if self.eat_punct(&TokenKind::Comma) {
                if self.at_punct(&TokenKind::RBrace) {
                    break self.bump().span;
                }
                variants.push(self.expect_ident("a variant name")?);
            } else if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            } else {
                return self.error(vec!["`,`", "`}`"]);
            }
        };
        Ok(EnumDecl {
            name,
            variants,
            span: start.join(end),
        })
    }

    fn event_decl(&mut self) -> PResult<EventDecl> {
        let start = self.expect_keyword("event", "`event`")?;
        let name = self.expect_ident("an event name")?;
        let version = self.version()?;
        let (fields, end) = self.field_block()?;
        Ok(EventDecl {
            name,
            version,
            fields,
            span: start.join(end),
        })
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
            if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            if !matches!(self.peek_kind(), TokenKind::Ident(_)) {
                return self.error(vec!["a field name", "`}`"]);
            }
            fields.push(self.field()?);
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

    fn field(&mut self) -> PResult<Field> {
        let name = self.expect_ident("a field name")?;
        self.expect_punct(TokenKind::Colon, "`:`")?;
        let ty = self.ty()?;
        let span = name.span.join(ty.span);
        Ok(Field { name, ty, span })
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

    fn aggregate_decl(&mut self) -> PResult<AggregateDecl> {
        let start = self.expect_keyword("aggregate", "`aggregate`")?;
        let name = self.expect_ident("an aggregate name")?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        self.expect_keyword("key", "`key`")?;
        let key = self.field()?;
        self.expect_keyword("stream", "`stream`")?;
        let stream = self.expect_string("a stream template string")?;
        let mut items = Vec::new();
        loop {
            match self.peek_ident() {
                Some("value") => items.push(LocalItem::Value(self.value_decl()?)),
                Some("enum") => items.push(LocalItem::Enum(self.enum_decl()?)),
                Some("entity") => items.push(LocalItem::Entity(self.entity_decl()?)),
                Some("events") => break,
                _ => return self.error(vec!["`value`", "`enum`", "`entity`", "`events`"]),
            }
        }
        self.expect_keyword("events", "`events`")?;
        let events = self.event_refs()?;
        self.expect_keyword("state", "`state`")?;
        let (state, _) = self.field_block()?;
        self.expect_keyword("evolve", "`evolve`")?;
        let evolve = self.wasm_ref()?;
        let snapshot_every = if self.at_keyword("snapshot") {
            self.bump();
            self.expect_keyword("every", "`every`")?;
            Some(self.expect_int("an integer")?)
        } else {
            None
        };
        let mut commands = Vec::new();
        if self.at_keyword("commands") {
            self.bump();
            commands.push(self.command_decl()?);
            while self.eat_punct(&TokenKind::Comma) {
                if self.at_punct(&TokenKind::RBrace) {
                    break;
                }
                commands.push(self.command_decl()?);
            }
        }
        let mut invariants = Vec::new();
        if self.at_keyword("invariants") {
            self.bump();
            invariants.push(self.invariant_ref()?);
            while self.eat_punct(&TokenKind::Comma) {
                if self.at_punct(&TokenKind::RBrace) {
                    break;
                }
                invariants.push(self.invariant_ref()?);
            }
        }
        let end = if self.at_punct(&TokenKind::RBrace) {
            self.bump().span
        } else {
            let mut expected = vec!["`}`"];
            if snapshot_every.is_none() && commands.is_empty() && invariants.is_empty() {
                expected.insert(0, "`snapshot`");
            }
            if commands.is_empty() && invariants.is_empty() {
                expected.insert(expected.len() - 1, "`commands`");
            }
            if invariants.is_empty() {
                expected.insert(expected.len() - 1, "`invariants`");
            }
            if !commands.is_empty() || !invariants.is_empty() {
                expected.insert(0, "`,`");
            }
            return self.error(expected);
        };
        Ok(AggregateDecl {
            name,
            key,
            stream,
            items,
            events,
            state,
            evolve,
            snapshot_every,
            commands,
            invariants,
            span: start.join(end),
        })
    }

    fn entity_decl(&mut self) -> PResult<EntityDecl> {
        let start = self.expect_keyword("entity", "`entity`")?;
        let name = self.expect_ident("an entity name")?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        self.expect_keyword("id", "`id`")?;
        let id = self.field()?;
        let mut fields = Vec::new();
        let end = loop {
            if self.eat_punct(&TokenKind::Comma) {
                if self.at_punct(&TokenKind::RBrace) {
                    break self.bump().span;
                }
                fields.push(self.field()?);
            } else if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            } else {
                return self.error(vec!["`,`", "`}`"]);
            }
        };
        Ok(EntityDecl {
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
        let name = self.expect_ident("a command name")?;
        let (fields, _) = self.field_block()?;
        self.expect_punct(TokenKind::Arrow, "`->`")?;
        let handler = self.wasm_ref()?;
        let span = name.span.join(handler.span);
        Ok(CommandDecl {
            name,
            fields,
            handler,
            span,
        })
    }

    fn invariant_ref(&mut self) -> PResult<InvariantRef> {
        let name = self.expect_ident("an invariant name")?;
        let start = name.span;
        self.expect_punct(TokenKind::Arrow, "`->`")?;
        let check = self.wasm_ref()?;
        let span = start.join(check.span);
        Ok(InvariantRef { name, check, span })
    }

    fn invariant_decl(&mut self) -> PResult<InvariantDecl> {
        let start = self.expect_keyword("invariant", "`invariant`")?;
        let name = self.expect_ident("an invariant name")?;
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
            name,
            on,
            projection,
            scope,
            check,
            span: start.join(end),
        })
    }

    fn process_decl(&mut self) -> PResult<ProcessDecl> {
        let start = self.expect_keyword("process", "`process`")?;
        let name = self.expect_ident("a process name")?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        self.expect_keyword("key", "`key`")?;
        let key = self.field()?;
        self.expect_keyword("from", "`from`")?;
        let mut from = vec![self.process_source()?];
        while self.eat_punct(&TokenKind::Comma) {
            from.push(self.process_source()?);
        }
        self.expect_keyword("state", "`state`")?;
        let (state, _) = self.field_block()?;
        self.expect_keyword("react", "`react`")?;
        let react = self.wasm_ref()?;
        let end = self.expect_punct(TokenKind::RBrace, "`}`")?;
        Ok(ProcessDecl {
            name,
            key,
            from,
            state,
            react,
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

    fn projection_decl(&mut self) -> PResult<ProjectionDecl> {
        let start = self.expect_keyword("projection", "`projection`")?;
        let name = self.expect_ident("a projection name")?;
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
        let mut tables = vec![self.table_decl()?];
        let end = loop {
            if self.at_keyword("table") {
                tables.push(self.table_decl()?);
            } else if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            } else {
                return self.error(vec!["`table`", "`}`"]);
            }
        };
        Ok(ProjectionDecl {
            name,
            from,
            fold,
            snapshot_every,
            tables,
            span: start.join(end),
        })
    }

    fn table_decl(&mut self) -> PResult<TableDecl> {
        let start = self.expect_keyword("table", "`table`")?;
        let name = self.expect_ident("a table name")?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        let mut fields = Vec::new();
        let end = loop {
            if self.at_punct(&TokenKind::RBrace) {
                break self.bump().span;
            }
            if !matches!(self.peek_kind(), TokenKind::Ident(_)) {
                return self.error(vec!["`key`", "a column name", "`}`"]);
            }
            // `key name: T` marks a key column; `key: T` is a column called key.
            let key = self.at_keyword("key") && self.peek_at(1) != &TokenKind::Colon;
            if key {
                self.bump();
            }
            let field = self.field()?;
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
            name,
            fields,
            span: start.join(end),
        })
    }
}
