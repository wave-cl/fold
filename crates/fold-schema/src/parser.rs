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
                _ => {
                    return self.error(vec![
                        "`value`",
                        "`enum`",
                        "`event`",
                        "`aggregate`",
                        "`projection`",
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
        let (fields, end) = self.field_block()?;
        Ok(ValueDecl {
            name,
            fields,
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
        let end = if self.at_punct(&TokenKind::RBrace) {
            self.bump().span
        } else {
            let mut expected = vec!["`}`"];
            if snapshot_every.is_none() {
                expected.insert(0, "`snapshot`");
            }
            if commands.is_empty() {
                expected.insert(expected.len() - 1, "`commands`");
            } else {
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

    fn projection_decl(&mut self) -> PResult<ProjectionDecl> {
        let start = self.expect_keyword("projection", "`projection`")?;
        let name = self.expect_ident("a projection name")?;
        self.expect_punct(TokenKind::LBrace, "`{`")?;
        self.expect_keyword("from", "`from`")?;
        let from = self.event_refs()?;
        self.expect_keyword("fold", "`fold`")?;
        let fold = self.wasm_ref()?;
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
