//! Tokenizer for `.fold` schema files.
//!
//! Keywords are lexed as identifiers; the parser decides contextually which
//! identifiers are keywords, so a field may be called `key` or `state`.

use crate::span::Span;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenKind {
    Ident(String),
    Int(u64),
    Str(String),
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Lt,
    Gt,
    Colon,
    Comma,
    Dot,
    Question,
    Arrow,
    /// A decimal literal such as `12.50`, kept as text.
    Dec(String),
    LParen,
    RParen,
    Ge,
    Le,
    EqEq,
    Ne,
    Minus,
    Eof,
}

impl TokenKind {
    /// How the token is named in an `expected ..., found ...` message.
    pub fn describe(&self) -> String {
        match self {
            TokenKind::Ident(name) => format!("identifier `{name}`"),
            TokenKind::Int(n) => format!("integer `{n}`"),
            TokenKind::Str(s) => format!("string {s:?}"),
            TokenKind::Dec(s) => format!("number `{s}`"),
            TokenKind::Eof => "end of input".to_string(),
            other => format!("`{}`", other.punct()),
        }
    }

    fn punct(&self) -> &'static str {
        match self {
            TokenKind::LBrace => "{",
            TokenKind::RBrace => "}",
            TokenKind::LBracket => "[",
            TokenKind::RBracket => "]",
            TokenKind::Lt => "<",
            TokenKind::Gt => ">",
            TokenKind::Colon => ":",
            TokenKind::Comma => ",",
            TokenKind::Dot => ".",
            TokenKind::Question => "?",
            TokenKind::Arrow => "->",
            TokenKind::LParen => "(",
            TokenKind::RParen => ")",
            TokenKind::Ge => ">=",
            TokenKind::Le => "<=",
            TokenKind::EqEq => "==",
            TokenKind::Ne => "!=",
            TokenKind::Minus => "-",
            _ => "",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LexError {
    #[error("unterminated string literal")]
    UnterminatedString { span: Span },
    #[error("unknown escape sequence `{escape}`")]
    BadEscape { span: Span, escape: String },
    #[error("unterminated block comment")]
    UnterminatedComment { span: Span },
    #[error("unexpected character `{ch}`")]
    UnexpectedChar { span: Span, ch: char },
    #[error("integer literal out of range")]
    IntegerTooLarge { span: Span },
}

impl LexError {
    pub fn span(&self) -> Span {
        match self {
            LexError::UnterminatedString { span }
            | LexError::BadEscape { span, .. }
            | LexError::UnterminatedComment { span }
            | LexError::UnexpectedChar { span, .. }
            | LexError::IntegerTooLarge { span } => *span,
        }
    }
}

/// Tokenize `src`. The result always ends with an `Eof` token whose span is
/// the empty range at the end of the input.
pub fn lex(src: &str) -> Result<Vec<Token>, LexError> {
    let bytes = src.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let start = i;
                i += 2;
                loop {
                    if i + 1 >= bytes.len() {
                        return Err(LexError::UnterminatedComment {
                            span: Span::new(start, src.len()),
                        });
                    }
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'>' | b'<' | b'=' | b'!' if bytes.get(i + 1) == Some(&b'=') => {
                let kind = match c {
                    b'>' => TokenKind::Ge,
                    b'<' => TokenKind::Le,
                    b'=' => TokenKind::EqEq,
                    _ => TokenKind::Ne,
                };
                toks.push(Token {
                    kind,
                    span: Span::new(i, i + 2),
                });
                i += 2;
            }
            b'{' | b'}' | b'[' | b']' | b'<' | b'>' | b':' | b',' | b'.' | b'?' => {
                let kind = match c {
                    b'{' => TokenKind::LBrace,
                    b'}' => TokenKind::RBrace,
                    b'[' => TokenKind::LBracket,
                    b']' => TokenKind::RBracket,
                    b'<' => TokenKind::Lt,
                    b'>' => TokenKind::Gt,
                    b':' => TokenKind::Colon,
                    b',' => TokenKind::Comma,
                    b'.' => TokenKind::Dot,
                    _ => TokenKind::Question,
                };
                toks.push(Token {
                    kind,
                    span: Span::new(i, i + 1),
                });
                i += 1;
            }
            b'-' if bytes.get(i + 1) == Some(&b'>') => {
                toks.push(Token {
                    kind: TokenKind::Arrow,
                    span: Span::new(i, i + 2),
                });
                i += 2;
            }
            b'(' | b')' | b'-' => {
                let kind = match c {
                    b'(' => TokenKind::LParen,
                    b')' => TokenKind::RParen,
                    _ => TokenKind::Minus,
                };
                toks.push(Token {
                    kind,
                    span: Span::new(i, i + 1),
                });
                i += 1;
            }
            b'"' => {
                let (tok, next) = lex_string(src, i)?;
                toks.push(tok);
                i = next;
            }
            b'0'..=b'9' => {
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                // `12.50` is one decimal literal; `12.` or `.5` are not.
                if bytes.get(i) == Some(&b'.') && bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
                    i += 1;
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                    toks.push(Token {
                        kind: TokenKind::Dec(src[start..i].to_string()),
                        span: Span::new(start, i),
                    });
                    continue;
                }
                let span = Span::new(start, i);
                let value = src[start..i]
                    .parse::<u64>()
                    .map_err(|_| LexError::IntegerTooLarge { span })?;
                toks.push(Token {
                    kind: TokenKind::Int(value),
                    span,
                });
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                toks.push(Token {
                    kind: TokenKind::Ident(src[start..i].to_string()),
                    span: Span::new(start, i),
                });
            }
            _ => {
                let ch = src[i..].chars().next().unwrap_or('\u{FFFD}');
                return Err(LexError::UnexpectedChar {
                    span: Span::new(i, i + ch.len_utf8()),
                    ch,
                });
            }
        }
    }
    toks.push(Token {
        kind: TokenKind::Eof,
        span: Span::new(src.len(), src.len()),
    });
    Ok(toks)
}

/// Lex a string literal starting at the opening quote at byte `start`.
/// Escapes: `\\`, `\"`, `\n`, `\t`, `\r`, `\0`, `\u{XXXX}`.
fn lex_string(src: &str, start: usize) -> Result<(Token, usize), LexError> {
    let mut out = String::new();
    let mut chars = src[start + 1..].char_indices().peekable();
    while let Some((off, ch)) = chars.next() {
        let abs = start + 1 + off;
        match ch {
            '"' => {
                let span = Span::new(start, abs + 1);
                return Ok((
                    Token {
                        kind: TokenKind::Str(out),
                        span,
                    },
                    abs + 1,
                ));
            }
            '\n' => {
                return Err(LexError::UnterminatedString {
                    span: Span::new(start, abs),
                });
            }
            '\\' => {
                let Some((eoff, esc)) = chars.next() else {
                    return Err(LexError::UnterminatedString {
                        span: Span::new(start, src.len()),
                    });
                };
                let esc_start = abs;
                let esc_end = start + 1 + eoff + esc.len_utf8();
                match esc {
                    '\\' => out.push('\\'),
                    '"' => out.push('"'),
                    'n' => out.push('\n'),
                    't' => out.push('\t'),
                    'r' => out.push('\r'),
                    '0' => out.push('\0'),
                    'u' => {
                        // \u{XXXX}
                        if chars.next_if(|(_, c)| *c == '{').is_none() {
                            return Err(LexError::BadEscape {
                                span: Span::new(esc_start, esc_end),
                                escape: "\\u".to_string(),
                            });
                        }
                        let mut hex = String::new();
                        let mut closed = false;
                        let mut end = esc_end + 1;
                        for (hoff, hc) in chars.by_ref() {
                            end = start + 1 + hoff + hc.len_utf8();
                            if hc == '}' {
                                closed = true;
                                break;
                            }
                            hex.push(hc);
                        }
                        let code = u32::from_str_radix(&hex, 16).ok();
                        match (closed, code.and_then(char::from_u32)) {
                            (true, Some(c)) if !hex.is_empty() && hex.len() <= 6 => out.push(c),
                            _ => {
                                return Err(LexError::BadEscape {
                                    span: Span::new(esc_start, end),
                                    escape: format!("\\u{{{hex}{}", if closed { "}" } else { "" }),
                                });
                            }
                        }
                    }
                    other => {
                        return Err(LexError::BadEscape {
                            span: Span::new(esc_start, esc_end),
                            escape: format!("\\{other}"),
                        });
                    }
                }
            }
            c => out.push(c),
        }
    }
    Err(LexError::UnterminatedString {
        span: Span::new(start, src.len()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(src: &str) -> Vec<TokenKind> {
        lex(src).unwrap().into_iter().map(|t| t.kind).collect()
    }

    #[test]
    fn punctuation_and_idents() {
        assert_eq!(
            kinds("a { b: [c]? } -> <d, e> . 42"),
            vec![
                TokenKind::Ident("a".into()),
                TokenKind::LBrace,
                TokenKind::Ident("b".into()),
                TokenKind::Colon,
                TokenKind::LBracket,
                TokenKind::Ident("c".into()),
                TokenKind::RBracket,
                TokenKind::Question,
                TokenKind::RBrace,
                TokenKind::Arrow,
                TokenKind::Lt,
                TokenKind::Ident("d".into()),
                TokenKind::Comma,
                TokenKind::Ident("e".into()),
                TokenKind::Gt,
                TokenKind::Dot,
                TokenKind::Int(42),
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn spans_are_byte_offsets() {
        let toks = lex("ab \"cd\" 7").unwrap();
        assert_eq!(toks[0].span, Span::new(0, 2));
        assert_eq!(toks[1].span, Span::new(3, 7));
        assert_eq!(toks[2].span, Span::new(8, 9));
        assert_eq!(toks[3].span, Span::new(9, 9));
    }

    #[test]
    fn comments_are_skipped() {
        assert_eq!(
            kinds("a // line comment\n /* block\n comment */ b"),
            vec![
                TokenKind::Ident("a".into()),
                TokenKind::Ident("b".into()),
                TokenKind::Eof
            ]
        );
    }

    #[test]
    fn string_escapes() {
        assert_eq!(
            kinds(r#""a\"b\\c\n\t\r\0\u{e9}""#),
            vec![TokenKind::Str("a\"b\\c\n\t\r\0é".into()), TokenKind::Eof]
        );
    }

    #[test]
    fn unterminated_string_has_span() {
        let err = lex("x \"abc").unwrap_err();
        assert_eq!(
            err,
            LexError::UnterminatedString {
                span: Span::new(2, 6)
            }
        );
        // A newline ends the literal too.
        let err = lex("\"abc\ndef\"").unwrap_err();
        assert_eq!(
            err,
            LexError::UnterminatedString {
                span: Span::new(0, 4)
            }
        );
    }

    #[test]
    fn bad_escape_has_span() {
        let err = lex("\"ab\\qcd\"").unwrap_err();
        assert_eq!(
            err,
            LexError::BadEscape {
                span: Span::new(3, 5),
                escape: "\\q".into()
            }
        );
        let err = lex("\"\\u{zz}\"").unwrap_err();
        assert!(matches!(err, LexError::BadEscape { .. }), "{err:?}");
        let err = lex("\"\\u{41\"").unwrap_err();
        assert!(matches!(err, LexError::BadEscape { .. }), "{err:?}");
    }

    #[test]
    fn unterminated_comment() {
        let err = lex("a /* b").unwrap_err();
        assert_eq!(
            err,
            LexError::UnterminatedComment {
                span: Span::new(2, 6)
            }
        );
    }

    #[test]
    fn unexpected_char() {
        let err = lex("a @ b").unwrap_err();
        assert_eq!(
            err,
            LexError::UnexpectedChar {
                span: Span::new(2, 3),
                ch: '@'
            }
        );
        let err = lex("a # b").unwrap_err();
        assert!(matches!(err, LexError::UnexpectedChar { ch: '#', .. }));
    }

    #[test]
    fn rule_operators_and_decimals_lex() {
        let kinds: Vec<TokenKind> = lex("a >= -2.50 and b != (c) <= 7")
            .unwrap()
            .into_iter()
            .map(|t| t.kind)
            .collect();
        assert_eq!(
            kinds,
            [
                TokenKind::Ident("a".into()),
                TokenKind::Ge,
                TokenKind::Minus,
                TokenKind::Dec("2.50".into()),
                TokenKind::Ident("and".into()),
                TokenKind::Ident("b".into()),
                TokenKind::Ne,
                TokenKind::LParen,
                TokenKind::Ident("c".into()),
                TokenKind::RParen,
                TokenKind::Le,
                TokenKind::Int(7),
                TokenKind::Eof,
            ]
        );
        // `12.` is an integer then a dot (as in a qualified name), not a decimal.
        let kinds: Vec<TokenKind> = lex("12.x").unwrap().into_iter().map(|t| t.kind).collect();
        assert_eq!(kinds[0], TokenKind::Int(12));
        assert_eq!(kinds[1], TokenKind::Dot);
    }

    #[test]
    fn integer_too_large() {
        let err = lex("99999999999999999999").unwrap_err();
        assert!(matches!(err, LexError::IntegerTooLarge { .. }));
    }
}
