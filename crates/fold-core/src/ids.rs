use std::fmt;
use std::ops::Deref;

use uuid::Uuid;

use crate::error::{Error, Result};

/// Dense, 0-based position in the global log. `Log::head()` is the position
/// the next appended event will receive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct GlobalPosition(pub u64);

impl fmt::Display for GlobalPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for GlobalPosition {
    fn from(v: u64) -> Self {
        GlobalPosition(v)
    }
}

/// Dense, 0-based version of an event within its stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StreamVersion(pub u64);

impl fmt::Display for StreamVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for StreamVersion {
    fn from(v: u64) -> Self {
        StreamVersion(v)
    }
}

/// Maximum length of a stream id in bytes.
pub const MAX_STREAM_ID_BYTES: usize = 255;

/// Identifies one stream (one aggregate instance). Non-empty, at most 255
/// bytes of UTF-8, no control characters.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(String);

impl StreamId {
    /// Validates and wraps a stream id.
    pub fn new(s: &str) -> Result<Self> {
        if s.is_empty() {
            return Err(Error::InvalidStreamId("empty".into()));
        }
        if s.len() > MAX_STREAM_ID_BYTES {
            return Err(Error::InvalidStreamId(format!(
                "{} bytes exceeds the maximum of {MAX_STREAM_ID_BYTES}",
                s.len()
            )));
        }
        if let Some(c) = s.chars().find(|c| c.is_control()) {
            return Err(Error::InvalidStreamId(format!(
                "contains control character {:?}",
                c
            )));
        }
        Ok(StreamId(s.to_owned()))
    }

    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for StreamId {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for StreamId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for StreamId {
    type Error = Error;
    fn try_from(s: &str) -> Result<Self> {
        StreamId::new(s)
    }
}

impl TryFrom<String> for StreamId {
    type Error = Error;
    fn try_from(s: String) -> Result<Self> {
        StreamId::new(&s)
    }
}

/// Unique id of an event. The log assigns a UUID v7 when the caller does not
/// supply one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(pub Uuid);

impl EventId {
    /// A fresh, time-ordered id.
    pub fn now() -> Self {
        EventId(Uuid::now_v7())
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for EventId {
    fn from(u: Uuid) -> Self {
        EventId(u)
    }
}

/// Maximum length in bytes of an event type's context or name.
pub const MAX_TYPE_PART_BYTES: usize = u16::MAX as usize;

/// A versioned event type, displayed as `Orders.OrderPlaced@v1`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventType {
    pub context: String,
    pub name: String,
    pub version: u16,
}

impl EventType {
    pub fn new(context: impl Into<String>, name: impl Into<String>, version: u16) -> Self {
        EventType {
            context: context.into(),
            name: name.into(),
            version,
        }
    }

    /// The family `Context.Name` without the version; this is the key
    /// `Log::read_by_type` takes.
    pub fn family(&self) -> String {
        format!("{}.{}", self.context, self.name)
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}@v{}", self.context, self.name, self.version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_id_rules() {
        assert!(StreamId::new("order-1").is_ok());
        assert!(matches!(StreamId::new(""), Err(Error::InvalidStreamId(_))));
        assert!(matches!(
            StreamId::new("a\nb"),
            Err(Error::InvalidStreamId(_))
        ));
        assert!(matches!(
            StreamId::new("a\u{7f}"),
            Err(Error::InvalidStreamId(_))
        ));
        let long = "x".repeat(255);
        assert!(StreamId::new(&long).is_ok());
        let too_long = "x".repeat(256);
        assert!(matches!(
            StreamId::new(&too_long),
            Err(Error::InvalidStreamId(_))
        ));
        // Byte length, not char count: 128 two-byte chars are 256 bytes.
        let wide = "é".repeat(128);
        assert!(matches!(
            StreamId::new(&wide),
            Err(Error::InvalidStreamId(_))
        ));
    }

    #[test]
    fn event_type_display() {
        let t = EventType::new("Orders", "OrderPlaced", 1);
        assert_eq!(t.to_string(), "Orders.OrderPlaced@v1");
        assert_eq!(t.family(), "Orders.OrderPlaced");
    }
}
