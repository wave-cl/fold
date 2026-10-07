//! The event envelope and its fixed-layout body codec.
//!
//! Body layout (all integers big-endian):
//!
//! ```text
//! u8   body format (currently 1)
//! u8   flags            bit 0 LAST_IN_BATCH, bits 1-3 payload encoding
//! u64  position
//! u64  stream_version
//! i64  recorded_at      unix nanoseconds
//! [16] event id         UUID bytes
//! u16  event type version
//! u8   stream_id_len,  bytes
//! u16  context_len,    bytes
//! u16  name_len,       bytes
//! u32  payload_len,    bytes
//! u32  metadata_len,   bytes
//! ```
//!
//! The record framing (`u32 len, u32 crc32, body`) lives in `segment.rs`.

use bytes::Bytes;
use uuid::Uuid;

use crate::ids::{EventId, EventType, GlobalPosition, StreamId, StreamVersion};

/// Set on the last record of every append batch. Recovery without an index
/// truncates to the last record carrying this flag.
pub const FLAG_LAST_IN_BATCH: u8 = 0b0000_0001;
/// Bits 1-3 of `flags` are reserved for the payload encoding tag (0 = JSON).
pub const ENCODING_MASK: u8 = 0b0000_1110;

const BODY_FORMAT: u8 = 1;

/// What a caller hands to `Log::append`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewEvent {
    /// `None` lets the log assign a UUID v7.
    pub id: Option<EventId>,
    pub event_type: EventType,
    pub payload: Bytes,
    pub metadata: Bytes,
}

impl NewEvent {
    pub fn new(event_type: EventType, payload: impl Into<Bytes>) -> Self {
        NewEvent {
            id: None,
            event_type,
            payload: payload.into(),
            metadata: Bytes::new(),
        }
    }

    pub fn with_id(mut self, id: EventId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_metadata(mut self, metadata: impl Into<Bytes>) -> Self {
        self.metadata = metadata.into();
        self
    }
}

/// An event as stored in the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedEvent {
    pub id: EventId,
    pub position: GlobalPosition,
    pub stream_id: StreamId,
    pub stream_version: StreamVersion,
    pub event_type: EventType,
    /// Unix time in nanoseconds when the log recorded the event.
    pub recorded_at: i64,
    pub payload: Bytes,
    pub metadata: Bytes,
    /// Bit 0 [`FLAG_LAST_IN_BATCH`], bits 1-3 [`ENCODING_MASK`].
    pub flags: u8,
}

impl RecordedEvent {
    /// True when this event closed its append batch.
    pub fn is_last_in_batch(&self) -> bool {
        self.flags & FLAG_LAST_IN_BATCH != 0
    }
}

/// Size of the fixed part of a body.
const FIXED: usize = 1 + 1 + 8 + 8 + 8 + 16 + 2 + 1 + 2 + 2 + 4 + 4;

/// Encoded body length of an event with these variable parts.
pub(crate) fn body_len(stream_id: &str, ty: &EventType, payload: usize, metadata: usize) -> usize {
    FIXED + stream_id.len() + ty.context.len() + ty.name.len() + payload + metadata
}

/// Appends the body of `ev` to `out`.
///
/// Panics if a variable-length field exceeds its length prefix; callers
/// validate `StreamId` (≤255 bytes) and the type parts (≤65535 bytes) first.
pub(crate) fn encode_body(ev: &RecordedEvent, out: &mut Vec<u8>) {
    let ty = &ev.event_type;
    assert!(ev.stream_id.len() <= u8::MAX as usize);
    assert!(ty.context.len() <= u16::MAX as usize);
    assert!(ty.name.len() <= u16::MAX as usize);
    assert!(ev.payload.len() <= u32::MAX as usize);
    assert!(ev.metadata.len() <= u32::MAX as usize);

    out.reserve(body_len(
        &ev.stream_id,
        ty,
        ev.payload.len(),
        ev.metadata.len(),
    ));
    out.push(BODY_FORMAT);
    out.push(ev.flags);
    out.extend_from_slice(&ev.position.0.to_be_bytes());
    out.extend_from_slice(&ev.stream_version.0.to_be_bytes());
    out.extend_from_slice(&ev.recorded_at.to_be_bytes());
    out.extend_from_slice(ev.id.0.as_bytes());
    out.extend_from_slice(&ty.version.to_be_bytes());
    out.push(ev.stream_id.len() as u8);
    out.extend_from_slice(ev.stream_id.as_bytes());
    out.extend_from_slice(&(ty.context.len() as u16).to_be_bytes());
    out.extend_from_slice(ty.context.as_bytes());
    out.extend_from_slice(&(ty.name.len() as u16).to_be_bytes());
    out.extend_from_slice(ty.name.as_bytes());
    out.extend_from_slice(&(ev.payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&ev.payload);
    out.extend_from_slice(&(ev.metadata.len() as u32).to_be_bytes());
    out.extend_from_slice(&ev.metadata);
}

/// Why a body did not decode. The segment layer turns this into
/// `Error::Corrupt` (below head) or a truncation point (recovery).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DecodeError {
    Short,
    BadFormat(u8),
    Trailing(usize),
    InvalidUtf8(&'static str),
    InvalidStreamId(String),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Short => write!(f, "body shorter than its length prefixes claim"),
            DecodeError::BadFormat(v) => write!(f, "unknown body format {v}"),
            DecodeError::Trailing(n) => write!(f, "{n} trailing bytes after the body"),
            DecodeError::InvalidUtf8(field) => write!(f, "{field} is not valid UTF-8"),
            DecodeError::InvalidStreamId(e) => write!(f, "stream id rejected: {e}"),
        }
    }
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos.checked_add(n).ok_or(DecodeError::Short)?;
        if end > self.buf.len() {
            return Err(DecodeError::Short);
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn str(&mut self, n: usize, field: &'static str) -> Result<&'a str, DecodeError> {
        std::str::from_utf8(self.take(n)?).map_err(|_| DecodeError::InvalidUtf8(field))
    }
}

/// Decodes one body. The whole slice must be the body.
pub(crate) fn decode_body(body: &[u8]) -> Result<RecordedEvent, DecodeError> {
    let mut c = Cursor { buf: body, pos: 0 };
    let format = c.u8()?;
    if format != BODY_FORMAT {
        return Err(DecodeError::BadFormat(format));
    }
    let flags = c.u8()?;
    let position = GlobalPosition(c.u64()?);
    let stream_version = StreamVersion(c.u64()?);
    let recorded_at = c.i64()?;
    let id = EventId(Uuid::from_bytes(c.take(16)?.try_into().unwrap()));
    let type_version = c.u16()?;
    let sid_len = c.u8()? as usize;
    let stream_id = c.str(sid_len, "stream id")?;
    let ctx_len = c.u16()? as usize;
    let context = c.str(ctx_len, "context")?;
    let name_len = c.u16()? as usize;
    let name = c.str(name_len, "name")?;
    let payload_len = c.u32()? as usize;
    let payload = Bytes::copy_from_slice(c.take(payload_len)?);
    let metadata_len = c.u32()? as usize;
    let metadata = Bytes::copy_from_slice(c.take(metadata_len)?);
    if c.pos != body.len() {
        return Err(DecodeError::Trailing(body.len() - c.pos));
    }
    let stream_id =
        StreamId::new(stream_id).map_err(|e| DecodeError::InvalidStreamId(e.to_string()))?;
    Ok(RecordedEvent {
        id,
        position,
        stream_id,
        stream_version,
        event_type: EventType {
            context: context.to_owned(),
            name: name.to_owned(),
            version: type_version,
        },
        recorded_at,
        payload,
        metadata,
        flags,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn sample(position: u64, flags: u8) -> RecordedEvent {
        RecordedEvent {
            id: EventId(Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef)),
            position: GlobalPosition(position),
            stream_id: StreamId::new("order-42").unwrap(),
            stream_version: StreamVersion(7),
            event_type: EventType::new("Orders", "OrderPlaced", 3),
            recorded_at: -1_234_567_890_123,
            payload: Bytes::from_static(br#"{"a":1}"#),
            metadata: Bytes::from_static(b"meta"),
            flags,
        }
    }

    #[test]
    fn round_trip() {
        let ev = sample(99, FLAG_LAST_IN_BATCH | 0b0100);
        let mut buf = Vec::new();
        encode_body(&ev, &mut buf);
        assert_eq!(
            buf.len(),
            body_len(&ev.stream_id, &ev.event_type, 7, 4),
            "body_len must predict the encoded size"
        );
        let back = decode_body(&buf).unwrap();
        assert_eq!(back, ev);
        assert!(back.is_last_in_batch());
    }

    #[test]
    fn empty_payload_and_metadata() {
        let mut ev = sample(0, 0);
        ev.payload = Bytes::new();
        ev.metadata = Bytes::new();
        let mut buf = Vec::new();
        encode_body(&ev, &mut buf);
        assert_eq!(decode_body(&buf).unwrap(), ev);
    }

    #[test]
    fn short_body_is_detected_at_every_cut() {
        let ev = sample(5, 0);
        let mut buf = Vec::new();
        encode_body(&ev, &mut buf);
        for cut in 0..buf.len() {
            assert_eq!(
                decode_body(&buf[..cut]),
                Err(DecodeError::Short),
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn trailing_bytes_rejected() {
        let ev = sample(5, 0);
        let mut buf = Vec::new();
        encode_body(&ev, &mut buf);
        buf.push(0);
        assert_eq!(decode_body(&buf), Err(DecodeError::Trailing(1)));
    }

    #[test]
    fn bad_format_rejected() {
        let ev = sample(5, 0);
        let mut buf = Vec::new();
        encode_body(&ev, &mut buf);
        buf[0] = 9;
        assert_eq!(decode_body(&buf), Err(DecodeError::BadFormat(9)));
    }

    #[test]
    fn invalid_utf8_rejected() {
        let ev = sample(5, 0);
        let mut buf = Vec::new();
        encode_body(&ev, &mut buf);
        // stream id starts right after the fixed prefix up to and incl. its len byte
        let sid_start = 1 + 1 + 8 + 8 + 8 + 16 + 2 + 1;
        buf[sid_start] = 0xff;
        assert_eq!(
            decode_body(&buf),
            Err(DecodeError::InvalidUtf8("stream id"))
        );
    }
}
