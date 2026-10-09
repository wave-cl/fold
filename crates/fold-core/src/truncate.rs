//! Point-in-time truncation of a closed log: the events from a position on
//! are dropped, and so is every index fact that looked past it (stream
//! heads, the type index, idempotency keys, segments). The log's generation
//! is bumped and the cut recorded, so a derivation or application node that
//! built on the longer log knows to drop what it derived past the cut.
//!
//! The cut must fall on a batch boundary: an append is atomic, and a point in
//! time that splits one would leave a batch the log never acknowledged.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

use redb::{ReadableMultimapTable, ReadableTable, ReadableTableMetadata};
use serde::{Deserialize, Serialize};

use crate::dir::{self, Layout, Lock};
use crate::error::{Error, Result};
use crate::event::{self, FLAG_LAST_IN_BATCH};
use crate::ids::GlobalPosition;
use crate::index::{
    EVENT_TYPES, IDEMPOTENCY, IDEMPOTENCY_BY_POS, Index, META, META_HEAD, POSITIONS, STREAM_HEADS,
    STREAMS, record_cut,
};
use crate::log::Log;
use crate::options::OpenOptions;
use crate::segment;

/// Where to cut a log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointInTime {
    /// Keep the events below this position; it must be a batch boundary.
    Position(GlobalPosition),
    /// Keep every batch recorded at or before this instant (unix
    /// nanoseconds). Resolved against the log with [`Log::position_after`].
    Time(i64),
}

impl PointInTime {
    /// The position this cut lands on in `log`.
    pub fn resolve(self, log: &Log) -> Result<GlobalPosition> {
        match self {
            PointInTime::Position(p) => Ok(p),
            PointInTime::Time(at) => log.position_after(at),
        }
    }
}

impl std::fmt::Display for PointInTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PointInTime::Position(p) => write!(f, "position {p}"),
            PointInTime::Time(ns) => write!(f, "time {ns} ns"),
        }
    }
}

impl Log {
    /// The batch boundary after the last batch recorded at or before `at`
    /// (unix nanoseconds): cutting there keeps exactly those batches. A batch
    /// carries one timestamp, and timestamps follow append order, so this is
    /// a binary search over the positions; a clock that stepped backwards
    /// between appends makes the answer only as good as the clock.
    pub fn position_after(&self, at: i64) -> Result<GlobalPosition> {
        let head = self.head().0;
        let recorded_at = |p: u64| -> Result<i64> {
            let ev = self.read_all(GlobalPosition(p), 1)?;
            ev.first()
                .map(|e| e.recorded_at)
                .ok_or_else(|| Error::PositionOutOfRange {
                    position: GlobalPosition(p),
                    head: GlobalPosition(head),
                })
        };
        // First position recorded after `at`, or the head.
        let (mut lo, mut hi) = (0u64, head);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if recorded_at(mid)? > at {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        // Back to the start of its batch.
        let mut p = lo;
        while p > 0 && p < head {
            let prev = self.read_all(GlobalPosition(p - 1), 1)?;
            if prev
                .first()
                .is_some_and(|e| e.flags & FLAG_LAST_IN_BATCH != 0)
            {
                break;
            }
            p -= 1;
        }
        Ok(GlobalPosition(p))
    }
}

/// [`truncate_log`] at a position or a time; a time is resolved against the
/// log first.
pub fn truncate_log_at(dir: &Path, name: &str, at: PointInTime) -> Result<Truncated> {
    let to = match at {
        PointInTime::Position(p) => p,
        PointInTime::Time(_) => {
            let log = Log::open(dir, name, OpenOptions::default())?;
            at.resolve(&log)?
        }
    };
    truncate_log(dir, name, to)
}

/// What a truncation removed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Truncated {
    /// The head before.
    pub from: u64,
    /// The head after: the position the next append receives.
    pub to: u64,
    /// Streams that lost events but keep earlier ones.
    pub streams_cut: u64,
    /// Streams whose every event was past the cut.
    pub streams_removed: u64,
    pub idempotency_keys_dropped: u64,
    pub segments_removed: u64,
    /// The log's generation after the cut (one more than before); a reader
    /// whose derived data was built under an older generation drops what it
    /// derived past `to`.
    pub generation: u64,
}

/// Cuts the closed log `<dir>/<name>` back to `to`: afterwards its head is
/// `to` and it holds exactly the events at positions below it. Refuses a
/// position past the head or inside a batch, and a log that is open. The
/// result is opened once to prove it recovers clean.
pub fn truncate_log(dir: &Path, name: &str, to: GlobalPosition) -> Result<Truncated> {
    let layout = Layout::new(dir, name);
    if !layout.exists() {
        return Err(Error::NotFound {
            path: layout.root.clone(),
        });
    }
    let report = {
        let _lock = Lock::acquire(&layout)?;
        truncate_locked(&layout, to)?
    };
    let log = Log::open(dir, name, OpenOptions::default())?;
    if log.head() != to {
        return Err(Error::corrupt(
            layout.index_file(),
            0,
            format!(
                "truncated to {to} but the log reopened at head {}",
                log.head()
            ),
        ));
    }
    Ok(report)
}

fn truncate_locked(layout: &Layout, to: GlobalPosition) -> Result<Truncated> {
    let opts = OpenOptions::default();
    let index = Index::open(&layout.index_file(), opts.fsync)?;
    let head = index.head()?;
    let mut report = Truncated {
        from: head,
        to: to.0,
        ..Truncated::default()
    };
    if to.0 > head {
        return Err(Error::PositionOutOfRange {
            position: to,
            head: GlobalPosition(head),
        });
    }
    if to.0 == head {
        report.generation = index.generation()?;
        return Ok(report);
    }
    check_batch_boundary(layout, &index, &opts, to, head)?;
    let (cut_base, cut_offset) = index.locate(to.0)?.ok_or_else(|| {
        Error::corrupt(
            layout.index_file(),
            0,
            format!("position {to} is below the head but the index does not locate it"),
        )
    })?;

    // The index first: once it says `head = to`, recovery on open would
    // finish the job on the segments even if this process died here.
    let txn = index.begin_write_durable()?;
    {
        let mut positions = txn.open_table(POSITIONS)?;
        positions.retain(|p, _| p < to.0)?;
    }
    // Streams: drop the versions at or past the cut, recompute the heads.
    let mut new_heads: BTreeMap<String, Option<u64>> = BTreeMap::new();
    {
        let mut streams = txn.open_table(STREAMS)?;
        let mut doomed = Vec::new();
        for r in streams.iter()? {
            let (k, v) = r?;
            if v.value() >= to.0 {
                let (s, version) = k.value();
                doomed.push((s.to_string(), version));
            }
        }
        for (s, version) in &doomed {
            streams.remove((s.as_str(), *version))?;
            new_heads.entry(s.clone()).or_insert(None);
        }
        for (s, head) in new_heads.iter_mut() {
            let last = streams
                .range((s.as_str(), 0)..=(s.as_str(), u64::MAX))?
                .next_back()
                .transpose()?
                .map(|(k, _)| k.value().1);
            *head = last;
        }
    }
    {
        let mut heads = txn.open_table(STREAM_HEADS)?;
        for (s, head) in &new_heads {
            match head {
                Some(v) => {
                    heads.insert(s.as_str(), *v)?;
                    report.streams_cut += 1;
                }
                None => {
                    heads.remove(s.as_str())?;
                    report.streams_removed += 1;
                }
            }
        }
    }
    {
        let mut types = txn.open_multimap_table(EVENT_TYPES)?;
        let mut doomed = Vec::new();
        for r in types.iter()? {
            let (k, vals) = r?;
            for v in vals {
                let p = v?.value();
                if p >= to.0 {
                    doomed.push((k.value().to_string(), p));
                }
            }
        }
        for (family, p) in &doomed {
            types.remove(family.as_str(), *p)?;
        }
    }
    match txn.open_table(IDEMPOTENCY) {
        Ok(mut keys) => {
            let before = keys.len()?;
            keys.retain(|_, p| p < to.0)?;
            report.idempotency_keys_dropped = before - keys.len()?;
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(e) => return Err(e.into()),
    }
    match txn.open_table(IDEMPOTENCY_BY_POS) {
        Ok(mut by_pos) => {
            by_pos.retain(|p, _| p < to.0)?;
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(e) => return Err(e.into()),
    }
    let generation = index.generation()? + 1;
    {
        let mut meta = txn.open_table(META)?;
        meta.insert(META_HEAD, to.0)?;
    }
    record_cut(&txn, generation, to.0)?;
    report.generation = generation;
    txn.commit()?;
    drop(index);

    // Segments: cut the one holding `to`, remove every later one.
    let segments = dir::list_segments(&layout.segments_dir())?;
    for (base, path) in &segments {
        if *base > cut_base {
            std::fs::remove_file(path).map_err(|e| Error::io(path, "remove", e))?;
            report.segments_removed += 1;
        }
    }
    segment::truncate(&layout.segment(cut_base), cut_offset, true)?;

    Ok(report)
}

fn flags_at(layout: &Layout, index: &Index, opts: &OpenOptions, position: u64) -> Result<u8> {
    let (base, offset) = index.locate(position)?.ok_or_else(|| {
        Error::corrupt(
            layout.index_file(),
            0,
            format!("position {position} is below the head but the index does not locate it"),
        )
    })?;
    let path = layout.segment(base);
    let file = File::open(&path).map_err(|e| Error::io(&path, "open", e))?;
    let body = segment::read_record_at(&file, &path, offset, opts.max_record_bytes)?;
    let ev = event::decode_body(&body)
        .map_err(|e| Error::corrupt(&path, offset, format!("record at {position}: {e}")))?;
    Ok(ev.flags)
}

/// `to` must follow a batch's last record (or be 0). Otherwise the error
/// names the batch so the caller can pick either side of it.
fn check_batch_boundary(
    layout: &Layout,
    index: &Index,
    opts: &OpenOptions,
    to: GlobalPosition,
    head: u64,
) -> Result<()> {
    if to.0 == 0 || flags_at(layout, index, opts, to.0 - 1)? & FLAG_LAST_IN_BATCH != 0 {
        return Ok(());
    }
    let mut start = to.0 - 1;
    while start > 0 && flags_at(layout, index, opts, start - 1)? & FLAG_LAST_IN_BATCH == 0 {
        start -= 1;
    }
    let mut end = to.0;
    while end < head && flags_at(layout, index, opts, end)? & FLAG_LAST_IN_BATCH == 0 {
        end += 1;
    }
    Err(Error::InsideBatch {
        position: to,
        batch_start: GlobalPosition(start),
        batch_end: GlobalPosition(end + 1),
    })
}
