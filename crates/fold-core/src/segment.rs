//! Segment files: a 64-byte header followed by framed records.
//!
//! ```text
//! header (64 bytes)
//!   [0..8)   magic  "FOLDSEG\0"
//!   [8..12)  u32 format (1)
//!   [12..16) u32 crc32 of the header with this field zeroed
//!   [16..24) u64 base position (position of the first record)
//!   [24..40) log uuid
//!   [40..64) zero
//! record
//!   u32 body_len, u32 crc32(body), body
//! ```

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::error::{Error, Result};

pub(crate) const HEADER_LEN: u64 = 64;
pub(crate) const MAGIC: &[u8; 8] = b"FOLDSEG\0";
pub(crate) const FORMAT: u32 = 1;
/// Bytes of framing before each body.
pub(crate) const FRAME_LEN: usize = 8;

/// File name of the segment whose first record has `base`.
pub(crate) fn file_name(base: u64) -> String {
    format!("{base:020}.seg")
}

/// Parses a segment file name back to its base position.
pub(crate) fn parse_file_name(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".seg")?;
    if stem.is_empty() || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentHeader {
    pub base: u64,
    pub log_id: Uuid,
}

impl SegmentHeader {
    pub(crate) fn encode(&self) -> [u8; HEADER_LEN as usize] {
        let mut h = [0u8; HEADER_LEN as usize];
        h[0..8].copy_from_slice(MAGIC);
        h[8..12].copy_from_slice(&FORMAT.to_be_bytes());
        h[16..24].copy_from_slice(&self.base.to_be_bytes());
        h[24..40].copy_from_slice(self.log_id.as_bytes());
        let crc = crc32fast::hash(&h);
        h[12..16].copy_from_slice(&crc.to_be_bytes());
        h
    }

    pub(crate) fn decode(h: &[u8; HEADER_LEN as usize]) -> std::result::Result<Self, String> {
        if &h[0..8] != MAGIC {
            return Err("bad magic".into());
        }
        let format = u32::from_be_bytes(h[8..12].try_into().unwrap());
        if format != FORMAT {
            return Err(format!("unknown segment format {format}"));
        }
        let stored = u32::from_be_bytes(h[12..16].try_into().unwrap());
        let mut zeroed = *h;
        zeroed[12..16].fill(0);
        let actual = crc32fast::hash(&zeroed);
        if stored != actual {
            return Err(format!("header crc {stored:#x} != {actual:#x}"));
        }
        if h[40..].iter().any(|&b| b != 0) {
            return Err("non-zero header padding".into());
        }
        Ok(SegmentHeader {
            base: u64::from_be_bytes(h[16..24].try_into().unwrap()),
            log_id: Uuid::from_bytes(h[24..40].try_into().unwrap()),
        })
    }
}

/// Appends `len, crc, body` to `out`.
pub(crate) fn frame_record(body: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&crc32fast::hash(body).to_be_bytes());
    out.extend_from_slice(body);
}

/// Why a sequential scan stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TailReason {
    /// The file ends exactly on a record boundary.
    Clean,
    /// Fewer than 8 bytes left.
    ShortFrame,
    /// The frame claims more body bytes than the file has.
    ShortBody,
    /// The body's crc does not match.
    BadCrc,
    /// The frame claims a body larger than `max_record_bytes` (or zero).
    BadLength(u32),
}

impl std::fmt::Display for TailReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TailReason::Clean => write!(f, "end of file"),
            TailReason::ShortFrame => write!(f, "short frame"),
            TailReason::ShortBody => write!(f, "short body"),
            TailReason::BadCrc => write!(f, "crc mismatch"),
            TailReason::BadLength(n) => write!(f, "implausible body length {n}"),
        }
    }
}

pub(crate) enum ScanItem {
    Record { offset: u64, body: Vec<u8> },
    Tail { offset: u64, reason: TailReason },
}

/// Reads records sequentially from an offset. Never returns an error for a
/// malformed record: that is reported as a [`ScanItem::Tail`] so recovery can
/// truncate there and ordinary reads can decide for themselves.
pub(crate) struct Scanner {
    reader: BufReader<File>,
    path: PathBuf,
    offset: u64,
    len: u64,
    max_record: usize,
    done: bool,
}

impl Scanner {
    pub(crate) fn open(path: &Path, start: u64, max_record: usize) -> Result<Self> {
        let file = File::open(path).map_err(|e| Error::io(path, "open", e))?;
        let len = file
            .metadata()
            .map_err(|e| Error::io(path, "metadata", e))?
            .len();
        Self::from_file(file, path, start, len, max_record)
    }

    pub(crate) fn from_file(
        file: File,
        path: &Path,
        start: u64,
        len: u64,
        max_record: usize,
    ) -> Result<Self> {
        let mut reader = BufReader::with_capacity(256 * 1024, file);
        reader
            .seek(SeekFrom::Start(start))
            .map_err(|e| Error::io(path, "seek", e))?;
        Ok(Scanner {
            reader,
            path: path.to_owned(),
            offset: start,
            len,
            max_record,
            done: false,
        })
    }

    /// Byte offset the next record would start at.
    pub(crate) fn offset(&self) -> u64 {
        self.offset
    }

    pub(crate) fn next(&mut self) -> Result<ScanItem> {
        if self.done {
            return Ok(ScanItem::Tail {
                offset: self.offset,
                reason: TailReason::Clean,
            });
        }
        let remaining = self.len - self.offset;
        if remaining == 0 {
            self.done = true;
            return Ok(ScanItem::Tail {
                offset: self.offset,
                reason: TailReason::Clean,
            });
        }
        if remaining < FRAME_LEN as u64 {
            self.done = true;
            return Ok(ScanItem::Tail {
                offset: self.offset,
                reason: TailReason::ShortFrame,
            });
        }
        let mut frame = [0u8; FRAME_LEN];
        self.reader
            .read_exact(&mut frame)
            .map_err(|e| Error::io(&self.path, "read", e))?;
        let body_len = u32::from_be_bytes(frame[0..4].try_into().unwrap());
        let crc = u32::from_be_bytes(frame[4..8].try_into().unwrap());
        if body_len == 0 || body_len as usize + FRAME_LEN > self.max_record {
            self.done = true;
            return Ok(ScanItem::Tail {
                offset: self.offset,
                reason: TailReason::BadLength(body_len),
            });
        }
        if remaining - (FRAME_LEN as u64) < u64::from(body_len) {
            self.done = true;
            return Ok(ScanItem::Tail {
                offset: self.offset,
                reason: TailReason::ShortBody,
            });
        }
        let mut body = vec![0u8; body_len as usize];
        self.reader
            .read_exact(&mut body)
            .map_err(|e| Error::io(&self.path, "read", e))?;
        if crc32fast::hash(&body) != crc {
            self.done = true;
            return Ok(ScanItem::Tail {
                offset: self.offset,
                reason: TailReason::BadCrc,
            });
        }
        let offset = self.offset;
        self.offset += (FRAME_LEN + body_len as usize) as u64;
        Ok(ScanItem::Record { offset, body })
    }
}

/// Reads one record at a known offset. Everything wrong is `Corrupt`: the
/// caller only asks for offsets the index vouches for.
pub(crate) fn read_record_at(
    file: &File,
    path: &Path,
    offset: u64,
    max_record: usize,
) -> Result<Vec<u8>> {
    let mut frame = [0u8; FRAME_LEN];
    file.read_exact_at(&mut frame, offset).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::corrupt(path, offset, "record frame past end of file")
        } else {
            Error::io(path, "read", e)
        }
    })?;
    let body_len = u32::from_be_bytes(frame[0..4].try_into().unwrap());
    let crc = u32::from_be_bytes(frame[4..8].try_into().unwrap());
    if body_len == 0 || body_len as usize + FRAME_LEN > max_record {
        return Err(Error::corrupt(
            path,
            offset,
            format!("implausible body length {body_len}"),
        ));
    }
    let mut body = vec![0u8; body_len as usize];
    file.read_exact_at(&mut body, offset + FRAME_LEN as u64)
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::corrupt(path, offset, "record body past end of file")
            } else {
                Error::io(path, "read", e)
            }
        })?;
    if crc32fast::hash(&body) != crc {
        return Err(Error::corrupt(path, offset, "crc mismatch"));
    }
    Ok(body)
}

/// The writer's handle on the current segment.
pub(crate) struct SegmentWriter {
    file: File,
    path: PathBuf,
    base: u64,
    /// Logical end of committed-or-in-flight data; bytes past it are garbage
    /// from a failed append and are overwritten by the next one.
    len: u64,
}

impl SegmentWriter {
    /// Creates a fresh segment with only a header.
    pub(crate) fn create(segments_dir: &Path, base: u64, log_id: Uuid, sync: bool) -> Result<Self> {
        let path = segments_dir.join(file_name(base));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| Error::io(&path, "create", e))?;
        let header = SegmentHeader { base, log_id }.encode();
        file.write_all(&header)
            .map_err(|e| Error::io(&path, "write", e))?;
        if sync {
            file.sync_all().map_err(|e| Error::io(&path, "fsync", e))?;
            sync_dir(segments_dir)?;
        }
        Ok(SegmentWriter {
            file,
            path,
            base,
            len: HEADER_LEN,
        })
    }

    /// Opens an existing, already recovered segment for appending at `len`.
    pub(crate) fn open(path: &Path, base: u64, len: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| Error::io(path, "open", e))?;
        Ok(SegmentWriter {
            file,
            path: path.to_owned(),
            base,
            len,
        })
    }

    pub(crate) fn base(&self) -> u64 {
        self.base
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Writes `buf` at the logical end. Returns the offset it starts at. The
    /// logical end moves only when the caller `advance`s, so a failed commit
    /// leaves the bytes to be overwritten.
    pub(crate) fn write_at_end(&mut self, buf: &[u8]) -> Result<u64> {
        self.file
            .write_all_at(buf, self.len)
            .map_err(|e| Error::io(&self.path, "write", e))?;
        Ok(self.len)
    }

    pub(crate) fn advance(&mut self, bytes: u64) {
        self.len += bytes;
    }

    pub(crate) fn sync_data(&self) -> Result<()> {
        self.file
            .sync_data()
            .map_err(|e| Error::io(&self.path, "fdatasync", e))
    }
}

/// Reads and validates a segment header; `Err(String)` is the reason it is
/// not one, `Ok(None)` means the file is shorter than a header.
pub(crate) fn read_header(
    path: &Path,
) -> Result<std::result::Result<Option<SegmentHeader>, String>> {
    let mut file = File::open(path).map_err(|e| Error::io(path, "open", e))?;
    let mut h = [0u8; HEADER_LEN as usize];
    match file.read_exact(&mut h) {
        Ok(()) => Ok(SegmentHeader::decode(&h).map(Some)),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(Ok(None)),
        Err(e) => Err(Error::io(path, "read", e)),
    }
}

/// Truncates a segment file to `len` and fsyncs it.
pub(crate) fn truncate(path: &Path, len: u64, sync: bool) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|e| Error::io(path, "open", e))?;
    file.set_len(len)
        .map_err(|e| Error::io(path, "truncate", e))?;
    if sync {
        file.sync_all().map_err(|e| Error::io(path, "fsync", e))?;
    }
    Ok(())
}

/// fsyncs a directory so a created or removed entry is durable.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    let d = File::open(dir).map_err(|e| Error::io(dir, "open", e))?;
    d.sync_all().map_err(|e| Error::io(dir, "fsync", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn file_names_are_zero_padded_and_parse_back() {
        assert_eq!(file_name(0), "00000000000000000000.seg");
        assert_eq!(file_name(1234), "00000000000000001234.seg");
        assert_eq!(parse_file_name("00000000000000001234.seg"), Some(1234));
        assert_eq!(parse_file_name("7.seg"), Some(7));
        assert_eq!(parse_file_name("x.seg"), None);
        assert_eq!(parse_file_name(".seg"), None);
        assert_eq!(parse_file_name("1.tmp"), None);
    }

    #[test]
    fn header_round_trip_and_crc() {
        let h = SegmentHeader {
            base: 42,
            log_id: Uuid::from_u128(7),
        };
        let bytes = h.encode();
        assert_eq!(&bytes[0..8], MAGIC);
        assert_eq!(SegmentHeader::decode(&bytes).unwrap(), h);
        let mut flipped = bytes;
        flipped[20] ^= 1;
        assert!(SegmentHeader::decode(&flipped).unwrap_err().contains("crc"));
        let mut magic = bytes;
        magic[0] = b'X';
        assert!(SegmentHeader::decode(&magic).unwrap_err().contains("magic"));
    }

    fn write_segment(dir: &Path, bodies: &[&[u8]]) -> PathBuf {
        let mut w = SegmentWriter::create(dir, 0, Uuid::nil(), false).unwrap();
        let mut buf = Vec::new();
        for b in bodies {
            frame_record(b, &mut buf);
        }
        w.write_at_end(&buf).unwrap();
        w.advance(buf.len() as u64);
        w.path().to_owned()
    }

    fn scan_all(path: &Path) -> (Vec<(u64, Vec<u8>)>, u64, TailReason) {
        let mut s = Scanner::open(path, HEADER_LEN, 1 << 20).unwrap();
        let mut out = Vec::new();
        loop {
            match s.next().unwrap() {
                ScanItem::Record { offset, body } => out.push((offset, body)),
                ScanItem::Tail { offset, reason } => return (out, offset, reason),
            }
        }
    }

    #[test]
    fn framing_round_trips_and_offsets_are_exact() {
        let d = tmp();
        let path = write_segment(d.path(), &[b"alpha", b"be", b"gamma!"]);
        let (recs, end, reason) = scan_all(&path);
        assert_eq!(reason, TailReason::Clean);
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0], (64, b"alpha".to_vec()));
        assert_eq!(recs[1], (64 + 8 + 5, b"be".to_vec()));
        assert_eq!(recs[2], (64 + 8 + 5 + 8 + 2, b"gamma!".to_vec()));
        assert_eq!(end, 64 + 8 + 5 + 8 + 2 + 8 + 6);
        assert_eq!(end, std::fs::metadata(&path).unwrap().len());

        let f = File::open(&path).unwrap();
        assert_eq!(
            read_record_at(&f, &path, recs[1].0, 1 << 20).unwrap(),
            b"be"
        );
    }

    #[test]
    fn crc_flip_stops_the_scan_and_fails_the_random_read() {
        let d = tmp();
        let path = write_segment(d.path(), &[b"alpha", b"beta"]);
        let mut bytes = std::fs::read(&path).unwrap();
        // flip a byte inside the second body
        let second_body = 64 + 8 + 5 + 8;
        bytes[second_body + 1] ^= 0x40;
        std::fs::write(&path, &bytes).unwrap();

        let (recs, end, reason) = scan_all(&path);
        assert_eq!(recs.len(), 1);
        assert_eq!(end, 64 + 8 + 5);
        assert_eq!(reason, TailReason::BadCrc);

        let f = File::open(&path).unwrap();
        let err = read_record_at(&f, &path, 64 + 8 + 5, 1 << 20).unwrap_err();
        assert!(matches!(err, Error::Corrupt { offset: 77, .. }), "{err}");
    }

    #[test]
    fn torn_tail_reasons() {
        let d = tmp();
        let path = write_segment(d.path(), &[b"alpha", b"beta"]);
        let full = std::fs::read(&path).unwrap();
        let second = 64 + 8 + 5;

        // half a body
        std::fs::write(&path, &full[..second + 8 + 2]).unwrap();
        let (recs, end, reason) = scan_all(&path);
        assert_eq!(
            (recs.len(), end, reason),
            (1, second as u64, TailReason::ShortBody)
        );

        // a few frame bytes
        std::fs::write(&path, &full[..second + 3]).unwrap();
        let (recs, end, reason) = scan_all(&path);
        assert_eq!(
            (recs.len(), end, reason),
            (1, second as u64, TailReason::ShortFrame)
        );

        // exactly one record
        std::fs::write(&path, &full[..second]).unwrap();
        let (recs, end, reason) = scan_all(&path);
        assert_eq!(
            (recs.len(), end, reason),
            (1, second as u64, TailReason::Clean)
        );

        // a zeroed frame (pre-allocated space) is a bad length, not a record
        let mut zeroed = full[..second].to_vec();
        zeroed.extend_from_slice(&[0u8; 16]);
        std::fs::write(&path, &zeroed).unwrap();
        let (recs, end, reason) = scan_all(&path);
        assert_eq!(
            (recs.len(), end, reason),
            (1, second as u64, TailReason::BadLength(0))
        );
    }

    #[test]
    fn oversized_length_is_rejected_not_allocated() {
        let d = tmp();
        let path = write_segment(d.path(), &[b"alpha"]);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[64..68].copy_from_slice(&u32::MAX.to_be_bytes());
        std::fs::write(&path, &bytes).unwrap();
        let (recs, _, reason) = scan_all(&path);
        assert_eq!(recs.len(), 0);
        assert_eq!(reason, TailReason::BadLength(u32::MAX));
    }

    #[test]
    fn read_header_distinguishes_short_from_bad() {
        let d = tmp();
        let path = write_segment(d.path(), &[]);
        assert_eq!(read_header(&path).unwrap().unwrap().unwrap().base, 0);
        std::fs::write(&path, b"FOLDSEG\0short").unwrap();
        assert!(read_header(&path).unwrap().unwrap().is_none());
        std::fs::write(&path, [0u8; 64]).unwrap();
        assert!(read_header(&path).unwrap().is_err());
    }
}
