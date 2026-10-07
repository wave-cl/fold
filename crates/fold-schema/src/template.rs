//! Stream-id templates such as `"order-{order_id}"`: one placeholder, the
//! aggregate key, rendered from and matched back to a typed JSON value.

use std::fmt;

use serde_json::Value;

use crate::types::Scalar;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TemplateError {
    #[error("stream template has no `{{placeholder}}`")]
    NoPlaceholder,
    #[error("stream template has more than one placeholder")]
    MultiplePlaceholders,
    #[error("stream template has an unclosed `{{`")]
    Unclosed,
    #[error("stream template has an unmatched `}}`")]
    UnmatchedClose,
    #[error("stream template has an empty `{{}}`")]
    EmptyPlaceholder,
    #[error("stream template placeholder `{name}` is not a valid identifier")]
    BadPlaceholder { name: String },
    #[error("key is not a valid {expected}: {found}")]
    BadKey { expected: Scalar, found: String },
    #[error("key renders to an empty string")]
    EmptyKey,
}

/// A parsed template: `prefix{placeholder}suffix`. The placeholder is the
/// aggregate's key field; `key_type` is that field's scalar type and governs
/// how [`StreamTemplate::matches`] types the captured key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamTemplate {
    prefix: String,
    placeholder: String,
    suffix: String,
    key_type: Scalar,
}

impl StreamTemplate {
    /// Parse a template. The key type defaults to `string`; the resolver
    /// sets it with [`StreamTemplate::with_key_type`].
    pub fn parse(src: &str) -> Result<Self, TemplateError> {
        let mut prefix = String::new();
        let mut placeholder: Option<String> = None;
        let mut suffix = String::new();
        let mut chars = src.chars();
        while let Some(c) = chars.next() {
            match c {
                '{' => {
                    if placeholder.is_some() {
                        return Err(TemplateError::MultiplePlaceholders);
                    }
                    let mut name = String::new();
                    loop {
                        match chars.next() {
                            None => return Err(TemplateError::Unclosed),
                            Some('}') => break,
                            Some('{') => return Err(TemplateError::Unclosed),
                            Some(ch) => name.push(ch),
                        }
                    }
                    if name.is_empty() {
                        return Err(TemplateError::EmptyPlaceholder);
                    }
                    if !is_ident(&name) {
                        return Err(TemplateError::BadPlaceholder { name });
                    }
                    placeholder = Some(name);
                }
                '}' => return Err(TemplateError::UnmatchedClose),
                c if placeholder.is_none() => prefix.push(c),
                c => suffix.push(c),
            }
        }
        let placeholder = placeholder.ok_or(TemplateError::NoPlaceholder)?;
        Ok(StreamTemplate {
            prefix,
            placeholder,
            suffix,
            key_type: Scalar::String,
        })
    }

    pub fn with_key_type(mut self, key_type: Scalar) -> Self {
        self.key_type = key_type;
        self
    }

    pub fn placeholder(&self) -> &str {
        &self.placeholder
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn suffix(&self) -> &str {
        &self.suffix
    }

    pub fn key_type(&self) -> Scalar {
        self.key_type
    }

    /// Render the stream id for `key`, the aggregate key's JSON value (a
    /// string for `uuid`/`string`, a number for `int`/`uint`).
    pub fn render(&self, key: &Value) -> Result<String, TemplateError> {
        let text = self
            .key_type
            .canonical_key_string(key)
            .map_err(|e| TemplateError::BadKey {
                expected: self.key_type,
                found: e.to_string(),
            })?;
        if text.is_empty() {
            return Err(TemplateError::EmptyKey);
        }
        Ok(format!("{}{}{}", self.prefix, text, self.suffix))
    }

    /// If `stream_id` was rendered by this template, the key it was rendered
    /// from, typed per the key scalar (`"abc"`, `42`, `true`, ...).
    pub fn matches(&self, stream_id: &str) -> Option<Value> {
        let rest = stream_id.strip_prefix(self.prefix.as_str())?;
        let captured = rest.strip_suffix(self.suffix.as_str())?;
        if captured.is_empty() {
            return None;
        }
        let value = match self.key_type {
            Scalar::String => Value::String(captured.to_string()),
            Scalar::Int => Value::from(captured.parse::<i64>().ok()?),
            Scalar::Uint => Value::from(captured.parse::<u64>().ok()?),
            Scalar::Bool => Value::Bool(captured.parse::<bool>().ok()?),
            Scalar::Uuid | Scalar::Decimal | Scalar::Timestamp | Scalar::Bytes => {
                Value::String(captured.to_string())
            }
        };
        // Round-trip through the canonical form so only canonical ids match
        // (`"042"` is not the int 42, an upper-case uuid is not a uuid key).
        let canonical = self.key_type.canonical_key_string(&value).ok()?;
        (canonical == captured).then_some(value)
    }
}

impl fmt::Display for StreamTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{{{}}}{}", self.prefix, self.placeholder, self.suffix)
    }
}

fn is_ident(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}
