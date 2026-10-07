//! Open-time recovery: validate segments, rebuild a missing index, truncate
//! the torn or unacknowledged tail.
//!
//! Invariants restored by [`recover`]:
//! * every segment file has a valid header whose base matches its name;
//! * the last segment holds only whole, crc-valid records at consecutive
//!   positions, all `< META.head`;
//! * no segment starts past `META.head`;
//! * the index never knows a position the data lacks (otherwise `Corrupt`).

use std::fs;
use std::path::{Path, PathBuf};

use tracing::{info, info_span, warn};

use crate::dir::{self, Identity, Layout};
use crate::error::{Error, Result};
use crate::event::{self, FLAG_LAST_IN_BATCH};
use crate::index::{self, Index, IndexEntry};
use crate::options::{FsyncPolicy, OpenOptions};
use crate::segment::{self, HEADER_LEN, ScanItem, Scanner, TailReason};

/// What `Log::open` needs after recovery.
pub(crate) struct Recovered {
    pub index: Index,
    pub head: u64,
    /// Base and byte length of the segment the writer continues in.
    pub tail_base: u64,
    pub tail_len: u64,
}

/// One good record found by a scan.
struct Found {
    position: u64,
    offset: u64,
    next_offset: u64,
    stream: String,
    version: u64,
    family: String,
    flags: u8,
}

/// Result of scanning a segment for consecutive good records from `base`.
struct Scanned {
    records: Vec<Found>,
    /// Offset of the first byte not covered by a good record.
    end_offset: u64,
    /// Why the scan stopped there.
    reason: String,
}

fn scan_segment(path: &Path, base: u64, max_record: usize) -> Result<Scanned> {
    let mut scanner = Scanner::open(path, HEADER_LEN, max_record)?;
    let mut records = Vec::new();
    let mut expected = base;
    loop {
        match scanner.next()? {
            ScanItem::Record { offset, body } => {
                let ev = match event::decode_body(&body) {
                    Ok(ev) => ev,
                    Err(e) => {
                        return Ok(Scanned {
                            records,
                            end_offset: offset,
                            reason: format!("undecodable body: {e}"),
                        });
                    }
                };
                if ev.position.0 != expected {
                    return Ok(Scanned {
                        records,
                        end_offset: offset,
                        reason: format!(
                            "record carries position {} where {expected} was expected",
                            ev.position.0
                        ),
                    });
                }
                records.push(Found {
                    position: expected,
                    offset,
                    next_offset: scanner.offset(),
                    stream: ev.stream_id.to_string(),
                    version: ev.stream_version.0,
                    family: ev.event_type.family(),
                    flags: ev.flags,
                });
                expected += 1;
            }
            ScanItem::Tail { offset, reason } => {
                return Ok(Scanned {
                    records,
                    end_offset: offset,
                    reason: reason.to_string(),
                });
            }
        }
    }
}

fn file_len(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path)
        .map_err(|e| Error::io(path, "metadata", e))?
        .len())
}

fn remove_segment(path: &Path, segments_dir: &Path, why: &str) -> Result<()> {
    warn!(segment = %path.display(), why, "removing segment");
    fs::remove_file(path).map_err(|e| Error::io(path, "remove", e))?;
    segment::sync_dir(segments_dir)
}

/// Validates headers, dropping a trailing segment whose header never made it
/// to disk. Returns `(base, path)` in numeric order.
fn validated_segments(layout: &Layout, identity: &Identity) -> Result<Vec<(u64, PathBuf)>> {
    let segments_dir = layout.segments_dir();
    let mut segments = dir::list_segments(&segments_dir)?;
    let n = segments.len();
    let mut drop_last = false;
    for (i, (base, path)) in segments.iter().enumerate() {
        match segment::read_header(path)? {
            Ok(Some(h)) => {
                if h.base != *base {
                    return Err(Error::corrupt(
                        path,
                        0,
                        format!("header base {} does not match file name", h.base),
                    ));
                }
                if h.log_id != identity.log_id {
                    return Err(Error::corrupt(
                        path,
                        0,
                        format!(
                            "segment belongs to log {}, not {}",
                            h.log_id, identity.log_id
                        ),
                    ));
                }
            }
            Ok(None) if i + 1 == n => drop_last = true,
            Ok(None) => return Err(Error::corrupt(path, 0, "segment shorter than its header")),
            Err(reason) => return Err(Error::corrupt(path, 0, reason)),
        }
    }
    if drop_last {
        let (_, path) = segments.pop().unwrap();
        remove_segment(&path, &segments_dir, "header never completed")?;
    }
    Ok(segments)
}

/// Runs recovery for `Log::open`.
pub(crate) fn recover(
    layout: &Layout,
    identity: &Identity,
    opts: &OpenOptions,
) -> Result<Recovered> {
    let span = info_span!("fold.recover", log = %layout.root.display());
    let _g = span.enter();

    let segments_dir = layout.segments_dir();
    let mut segments = validated_segments(layout, identity)?;
    let index_path = layout.index_file();

    if segments.is_empty() {
        // Nothing to recover from. Only legitimate if there is no committed
        // data either.
        if index_path.exists() {
            let index = Index::open(&index_path, opts.fsync)?;
            let head = index.head()?;
            if head != 0 {
                return Err(Error::corrupt(
                    &segments_dir,
                    0,
                    format!("index head is {head} but there are no segments"),
                ));
            }
        }
        let w = segment::SegmentWriter::create(&segments_dir, 0, identity.log_id, true)?;
        segments.push((0, w.path().to_owned()));
    }

    let index = if index_path.exists() {
        Index::open(&index_path, opts.fsync)?
    } else {
        rebuild(layout, &segments, opts)?
    };
    let head = index.head()?;

    // Segments entirely past the head were never acknowledged.
    while let Some((base, path)) = segments.last() {
        if *base > head {
            let path = path.clone();
            remove_segment(&path, &segments_dir, "starts past the committed head")?;
            segments.pop();
        } else {
            break;
        }
    }
    let Some((tail_base, tail_path)) = segments.last().cloned() else {
        // Every segment started past head; the index is ahead of the data.
        return Err(Error::corrupt(
            &segments_dir,
            0,
            format!("index head is {head} but no segment starts at or before it"),
        ));
    };

    if opts.verify_all_segments {
        verify_earlier_segments(&segments, opts.max_record_bytes)?;
    }

    let scanned = scan_segment(&tail_path, tail_base, opts.max_record_bytes)?;
    let data_end = tail_base + scanned.records.len() as u64;
    if data_end < head {
        return Err(Error::corrupt(
            &tail_path,
            scanned.end_offset,
            format!(
                "index head is {head} but the data ends at position {data_end} ({})",
                scanned.reason
            ),
        ));
    }
    let truncate_to = if data_end > head {
        // Fsynced but never committed: drop everything from `head` on.
        scanned.records[(head - tail_base) as usize].offset
    } else {
        scanned.end_offset
    };
    let len = file_len(&tail_path)?;
    if truncate_to < len {
        info!(
            segment = %tail_path.display(),
            from = len,
            to = truncate_to,
            dropped_records = data_end.saturating_sub(head),
            reason = %scanned.reason,
            "truncating tail"
        );
        segment::truncate(&tail_path, truncate_to, true)?;
    }

    // The index must agree with the data it points at for the last record.
    if head > tail_base {
        let last = &scanned.records[(head - 1 - tail_base) as usize];
        match index.locate(head - 1)? {
            Some((b, o)) if b == tail_base && o == last.offset => {}
            other => {
                return Err(Error::corrupt(
                    &tail_path,
                    last.offset,
                    format!(
                        "index locates position {} at {other:?}, data has it at ({tail_base}, {})",
                        head - 1,
                        last.offset
                    ),
                ));
            }
        }
    } else if head < tail_base {
        return Err(Error::corrupt(
            &tail_path,
            0,
            format!("segment base {tail_base} is past the index head {head}"),
        ));
    }

    Ok(Recovered {
        index,
        head,
        tail_base,
        tail_len: truncate_to,
    })
}

/// Every segment but the last must be whole and end exactly where the next
/// one begins.
fn verify_earlier_segments(segments: &[(u64, PathBuf)], max_record: usize) -> Result<()> {
    for pair in segments.windows(2) {
        let (base, path) = &pair[0];
        let (next_base, _) = &pair[1];
        let scanned = scan_segment(path, *base, max_record)?;
        let end = base + scanned.records.len() as u64;
        if scanned.reason != TailReason::Clean.to_string() {
            return Err(Error::corrupt(
                path,
                scanned.end_offset,
                format!("non-final segment is not whole: {}", scanned.reason),
            ));
        }
        if end != *next_base {
            return Err(Error::corrupt(
                path,
                scanned.end_offset,
                format!("segment ends at position {end} but the next starts at {next_base}"),
            ));
        }
    }
    Ok(())
}

/// Rebuilds `index.redb` from the segment files. Truncates the last segment
/// to its last `LAST_IN_BATCH` record, since without an index that is the
/// only evidence of an acknowledged append. Writes to a temporary file and
/// renames, so a crash mid-rebuild leaves no half index behind.
fn rebuild(layout: &Layout, segments: &[(u64, PathBuf)], opts: &OpenOptions) -> Result<Index> {
    let segments_dir = layout.segments_dir();
    warn!(log = %layout.root.display(), "index.redb missing; rebuilding from segments");

    let Some((first_base, first_path)) = segments.first() else {
        unreachable!("caller guarantees at least one segment");
    };
    if *first_base != 0 {
        return Err(Error::corrupt(
            first_path,
            0,
            format!("first segment starts at {first_base}, not 0; cannot rebuild"),
        ));
    }

    // Scan everything, requiring every segment but the last to be whole.
    let mut per_segment: Vec<(u64, PathBuf, Scanned)> = Vec::with_capacity(segments.len());
    let mut expected = 0u64;
    for (i, (base, path)) in segments.iter().enumerate() {
        if *base != expected {
            return Err(Error::corrupt(
                path,
                0,
                format!("segment starts at {base} but the previous one ended at {expected}"),
            ));
        }
        let scanned = scan_segment(path, *base, opts.max_record_bytes)?;
        expected = base + scanned.records.len() as u64;
        let last = i + 1 == segments.len();
        if !last && scanned.reason != TailReason::Clean.to_string() {
            return Err(Error::corrupt(
                path,
                scanned.end_offset,
                format!("non-final segment is not whole: {}", scanned.reason),
            ));
        }
        per_segment.push((*base, path.clone(), scanned));
    }

    // Head = one past the last record that closed a batch.
    let head = per_segment
        .iter()
        .rev()
        .find_map(|(_, _, s)| {
            s.records
                .iter()
                .rev()
                .find(|r| r.flags & FLAG_LAST_IN_BATCH != 0)
                .map(|r| r.position + 1)
        })
        .unwrap_or(0);

    // Truncate everything at or past head.
    for (base, path, scanned) in &per_segment {
        let cut = if *base >= head {
            Some(HEADER_LEN)
        } else {
            scanned
                .records
                .iter()
                .find(|r| r.position >= head)
                .map(|r| r.offset)
                .or_else(|| {
                    // all records are below head; still drop any torn tail
                    let end = scanned.records.last().map_or(HEADER_LEN, |r| r.next_offset);
                    Some(end)
                })
        };
        if let Some(cut) = cut {
            let len = file_len(path)?;
            if cut < len {
                info!(segment = %path.display(), from = len, to = cut, "rebuild: truncating");
                segment::truncate(path, cut, true)?;
            }
        }
    }
    // A segment that starts past head can only be an empty shell now; the
    // caller's head check removes it. Keep the one at `head` as the tail.

    let tmp = layout.root.join("index.redb.tmp");
    if tmp.exists() {
        fs::remove_file(&tmp).map_err(|e| Error::io(&tmp, "remove", e))?;
    }
    let index = Index::create(&tmp, FsyncPolicy::Always, 0)?;
    const CHUNK: usize = 8192;
    let mut written = 0u64;
    let mut pending: Vec<&Found> = Vec::with_capacity(CHUNK);
    let flush = |pending: &mut Vec<&Found>, base: u64, up_to: u64| -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let entries: Vec<IndexEntry<'_>> = pending
            .iter()
            .map(|r| IndexEntry {
                position: r.position,
                segment_base: base,
                offset: r.offset,
                stream: &r.stream,
                version: r.version,
                family: &r.family,
            })
            .collect();
        let txn = index.begin_write_durable()?;
        index::write_entries(&txn, &entries, up_to)?;
        txn.commit()?;
        pending.clear();
        Ok(())
    };
    for (base, _, scanned) in &per_segment {
        for r in scanned.records.iter().filter(|r| r.position < head) {
            pending.push(r);
            written = r.position + 1;
            if pending.len() >= CHUNK {
                flush(&mut pending, *base, written)?;
            }
        }
        flush(&mut pending, *base, written)?;
    }
    // Head is written explicitly even when there were no records.
    {
        let txn = index.begin_write_durable()?;
        txn.open_table(index::META)?
            .insert(index::META_HEAD, head)?;
        txn.commit()?;
    }
    drop(index);

    let index_path = layout.index_file();
    fs::rename(&tmp, &index_path).map_err(|e| Error::io(&index_path, "rename", e))?;
    segment::sync_dir(&layout.root)?;
    segment::sync_dir(&segments_dir)?;
    info!(head, segments = per_segment.len(), "index rebuilt");
    Index::open(&index_path, opts.fsync)
}
