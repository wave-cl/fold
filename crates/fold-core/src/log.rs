//! The `Log`: append under a writer mutex, reads through the index,
//! subscriptions over a `watch` channel.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tracing::{error, info, info_span, warn};

use crate::dir::{self, Identity, Layout, Lock};
use crate::error::{Error, Result};
use crate::event::{self, FLAG_LAST_IN_BATCH, NewEvent, RecordedEvent};
use crate::ids::{EventId, GlobalPosition, MAX_TYPE_PART_BYTES, StreamId, StreamVersion};
use crate::index::{Index, IndexEntry};
use crate::options::{FsyncPolicy, OpenOptions};
use crate::readmodel::ReadModelStore;
use crate::recover;
use crate::segment::{self, HEADER_LEN, ScanItem, Scanner, SegmentWriter, TailReason};
use crate::snapshots::SnapshotStore;
use crate::subscribe::Subscription;

/// What the caller asserts about a stream before appending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedVersion {
    /// No check.
    Any,
    /// The stream must not exist yet.
    NoStream,
    /// The stream must already have at least one event.
    StreamExists,
    /// The stream's last version must be exactly this.
    Exact(StreamVersion),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Backward,
}

/// Positions a successful append received.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    pub first: GlobalPosition,
    pub last: GlobalPosition,
    /// Version of the last event appended.
    pub stream_version: StreamVersion,
}

/// One event log. Cheap to clone; every clone shares the same files, index
/// and writer.
#[derive(Clone)]
pub struct Log {
    inner: Arc<Inner>,
}

pub(crate) struct Inner {
    layout: Layout,
    identity: Identity,
    opts: OpenOptions,
    _lock: Lock,
    pub(crate) index: Index,
    writer: Mutex<SegmentWriter>,
    head_tx: watch::Sender<u64>,
    /// Read handles on segment files, by base. Segments are never removed
    /// while the log is open, so entries never go stale.
    files: Mutex<HashMap<u64, Arc<File>>>,
}

impl Inner {
    pub(crate) fn index_path(&self) -> PathBuf {
        self.layout.index_file()
    }
}

impl std::fmt::Debug for Log {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Log")
            .field("path", &self.inner.layout.root)
            .field("head", &self.head())
            .finish()
    }
}

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

impl Log {
    /// Creates a new, empty log at `<dir>/<name>`. Fails with
    /// `AlreadyExists` if a log is already there.
    pub fn create(dir: &Path, name: &str, opts: OpenOptions) -> Result<Log> {
        let layout = Layout::new(dir, name);
        let span = info_span!("fold.open", log = %layout.root.display(), mode = "create");
        let _g = span.enter();
        fs::create_dir_all(&layout.root).map_err(|e| Error::io(&layout.root, "create_dir", e))?;
        let lock = Lock::acquire(&layout)?;
        if layout.exists() {
            return Err(Error::AlreadyExists { path: layout.root });
        }
        let segments_dir = layout.segments_dir();
        fs::create_dir_all(&segments_dir).map_err(|e| Error::io(&segments_dir, "create_dir", e))?;
        let schema_dir = layout.schema_dir();
        fs::create_dir_all(&schema_dir).map_err(|e| Error::io(&schema_dir, "create_dir", e))?;
        let identity = Identity {
            log_id: uuid::Uuid::now_v7(),
            created_at: now_nanos(),
        };
        let writer = SegmentWriter::create(&segments_dir, 0, identity.log_id, true)?;
        let index = Index::create(&layout.index_file(), opts.fsync, 0)?;
        // LOG last: its presence is what `exists()` means, so a crash before
        // this point leaves a directory `create` will happily reuse.
        identity.write(&layout)?;
        segment::sync_dir(&layout.root)?;
        info!(log_id = %identity.log_id, "created");
        Ok(Self::assemble(
            layout, identity, opts, lock, index, writer, 0,
        ))
    }

    /// Opens an existing log, recovering its tail.
    pub fn open(dir: &Path, name: &str, opts: OpenOptions) -> Result<Log> {
        let layout = Layout::new(dir, name);
        let span = info_span!("fold.open", log = %layout.root.display(), mode = "open");
        let _g = span.enter();
        if !layout.exists() {
            return Err(Error::NotFound { path: layout.root });
        }
        let lock = Lock::acquire(&layout)?;
        let identity = Identity::read(&layout)?;
        let recovered = recover::recover(&layout, &identity, &opts)?;
        let writer = SegmentWriter::open(
            &layout.segment(recovered.tail_base),
            recovered.tail_base,
            recovered.tail_len,
        )?;
        info!(log_id = %identity.log_id, head = recovered.head, "opened");
        Ok(Self::assemble(
            layout,
            identity,
            opts,
            lock,
            recovered.index,
            writer,
            recovered.head,
        ))
    }

    pub fn open_or_create(dir: &Path, name: &str, opts: OpenOptions) -> Result<Log> {
        if Layout::new(dir, name).exists() {
            Self::open(dir, name, opts)
        } else {
            match Self::create(dir, name, opts.clone()) {
                Err(Error::AlreadyExists { .. }) => Self::open(dir, name, opts),
                other => other,
            }
        }
    }

    fn assemble(
        layout: Layout,
        identity: Identity,
        opts: OpenOptions,
        lock: Lock,
        index: Index,
        writer: SegmentWriter,
        head: u64,
    ) -> Log {
        let (head_tx, _rx) = watch::channel(head);
        Log {
            inner: Arc::new(Inner {
                layout,
                identity,
                opts,
                _lock: lock,
                index,
                writer: Mutex::new(writer),
                head_tx,
                files: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The log's directory.
    pub fn path(&self) -> &Path {
        &self.inner.layout.root
    }

    /// The log's identity, written into every segment header.
    pub fn log_id(&self) -> uuid::Uuid {
        self.inner.identity.log_id
    }

    /// The position the next appended event will receive.
    pub fn head(&self) -> GlobalPosition {
        GlobalPosition(*self.inner.head_tx.borrow())
    }

    pub fn subscribe(&self) -> Subscription {
        Subscription::new(self.inner.head_tx.subscribe())
    }

    pub fn read_models(&self) -> ReadModelStore {
        ReadModelStore::new(self.inner.clone())
    }

    pub fn snapshots(&self) -> SnapshotStore {
        SnapshotStore::new(self.inner.clone())
    }

    /// `schema/current.fold`, verbatim, if set.
    pub fn schema_source(&self) -> Result<Option<String>> {
        dir::read_schema(&self.inner.layout)
    }

    /// Stores `schema/current.fold` verbatim.
    pub fn set_schema_source(&self, text: &str) -> Result<()> {
        dir::write_schema(&self.inner.layout, text)
    }

    /// Makes everything acknowledged so far durable regardless of
    /// `FsyncPolicy`.
    pub fn flush(&self) -> Result<()> {
        let w = self.writer();
        w.sync_data()?;
        self.inner.index.flush()
    }

    fn writer(&self) -> MutexGuard<'_, SegmentWriter> {
        self.inner
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Last version of `stream`, `None` if it has no events.
    pub fn stream_head(&self, stream: &StreamId) -> Result<Option<StreamVersion>> {
        Ok(self.inner.index.stream_head(stream)?.map(StreamVersion))
    }

    /// Appends a batch to `stream` atomically: all events get consecutive
    /// positions and versions, the last one carries `LAST_IN_BATCH`, and
    /// either every event is committed or none is.
    pub fn append(
        &self,
        stream: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent>,
    ) -> Result<AppendResult> {
        let span = info_span!("fold.append", stream = %stream, count = events.len());
        let _g = span.enter();
        let mut w = self.writer();
        let prepared = self.prepare(&w, stream, expected, events)?;
        let offset = w.write_at_end(&prepared.buf)?;
        if self.inner.opts.fsync == FsyncPolicy::Always {
            w.sync_data()?;
        }
        let base = w.base();
        let entries: Vec<IndexEntry<'_>> = prepared
            .records
            .iter()
            .zip(&prepared.offsets)
            .map(|(ev, rel)| IndexEntry {
                position: ev.position.0,
                segment_base: base,
                offset: offset + rel,
                stream: &ev.stream_id,
                version: ev.stream_version.0,
                family: &ev.family,
            })
            .collect();
        let new_head = prepared.new_head;
        // Commit point.
        self.inner.index.commit_batch(&entries, new_head)?;
        w.advance(prepared.buf.len() as u64);
        self.inner.head_tx.send_replace(new_head);

        if w.len() >= self.inner.opts.segment_max_bytes {
            self.roll(&mut w, new_head);
        }

        let first = prepared.records.first().unwrap().position;
        let last = prepared.records.last().unwrap();
        Ok(AppendResult {
            first,
            last: last.position,
            stream_version: last.stream_version,
        })
    }

    /// Writes (and fsyncs) a batch without committing it to the index: the
    /// state a crash between fdatasync and the redb commit leaves behind.
    /// For tests of recovery only; the log must be dropped afterwards.
    #[doc(hidden)]
    pub fn debug_write_without_commit(
        &self,
        stream: &StreamId,
        events: Vec<NewEvent>,
    ) -> Result<()> {
        let mut w = self.writer();
        let prepared = self.prepare(&w, stream, ExpectedVersion::Any, events)?;
        w.write_at_end(&prepared.buf)?;
        w.sync_data()?;
        Ok(())
    }

    fn prepare(
        &self,
        w: &SegmentWriter,
        stream: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent>,
    ) -> Result<Prepared> {
        if events.is_empty() {
            return Err(Error::EmptyBatch);
        }
        for ev in &events {
            let t = &ev.event_type;
            if t.context.len() > MAX_TYPE_PART_BYTES || t.name.len() > MAX_TYPE_PART_BYTES {
                return Err(Error::InvalidEventType(format!(
                    "context or name longer than {MAX_TYPE_PART_BYTES} bytes"
                )));
            }
            if t.context.is_empty() || t.name.is_empty() {
                return Err(Error::InvalidEventType(
                    "context and name must be non-empty".into(),
                ));
            }
            let size = event::body_len(stream, t, ev.payload.len(), ev.metadata.len())
                + segment::FRAME_LEN;
            if size > self.inner.opts.max_record_bytes {
                return Err(Error::RecordTooLarge {
                    size,
                    max: self.inner.opts.max_record_bytes,
                });
            }
        }

        let head = *self.inner.head_tx.borrow();
        let actual = self.inner.index.stream_head(stream)?;
        let ok = match (expected, actual) {
            (ExpectedVersion::Any, _) => true,
            (ExpectedVersion::NoStream, None) => true,
            (ExpectedVersion::NoStream, Some(_)) => false,
            (ExpectedVersion::StreamExists, Some(_)) => true,
            (ExpectedVersion::StreamExists, None) => false,
            (ExpectedVersion::Exact(v), Some(a)) => v.0 == a,
            (ExpectedVersion::Exact(_), None) => false,
        };
        if !ok {
            return Err(Error::WrongExpectedVersion {
                stream: stream.to_string(),
                expected,
                actual: actual.map(StreamVersion),
            });
        }
        let first_version = actual.map_or(0, |v| v + 1);
        let now = now_nanos();
        let n = events.len();
        let mut records = Vec::with_capacity(n);
        let mut offsets = Vec::with_capacity(n);
        let mut buf = Vec::new();
        let mut body = Vec::new();
        for (i, ev) in events.into_iter().enumerate() {
            let flags = if i + 1 == n { FLAG_LAST_IN_BATCH } else { 0 };
            let family = ev.event_type.family();
            let rec = RecordedEvent {
                id: ev.id.unwrap_or_else(EventId::now),
                position: GlobalPosition(head + i as u64),
                stream_id: stream.clone(),
                stream_version: StreamVersion(first_version + i as u64),
                event_type: ev.event_type,
                recorded_at: now,
                payload: ev.payload,
                metadata: ev.metadata,
                flags,
            };
            body.clear();
            event::encode_body(&rec, &mut body);
            offsets.push(buf.len() as u64);
            segment::frame_record(&body, &mut buf);
            records.push(PreparedRecord {
                position: rec.position,
                stream_version: rec.stream_version,
                stream_id: rec.stream_id,
                family,
            });
        }
        debug_assert!(w.len() >= HEADER_LEN);
        Ok(Prepared {
            buf,
            offsets,
            records,
            new_head: head + n as u64,
        })
    }

    fn roll(&self, w: &mut MutexGuard<'_, SegmentWriter>, base: u64) {
        let span = info_span!(
            "fold.segment.roll",
            from = w.base(),
            to = base,
            bytes = w.len()
        );
        let _g = span.enter();
        let sync = self.inner.opts.fsync == FsyncPolicy::Always;
        match SegmentWriter::create(
            &self.inner.layout.segments_dir(),
            base,
            self.inner.identity.log_id,
            sync,
        ) {
            Ok(next) => {
                **w = next;
                info!("rolled");
            }
            Err(e) => {
                // The batch is committed; keep writing into the old segment.
                error!(error = %e, "segment roll failed; continuing in the current segment");
            }
        }
    }

    fn file(&self, base: u64) -> Result<Arc<File>> {
        let mut files = self.inner.files.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(f) = files.get(&base) {
            return Ok(f.clone());
        }
        let path = self.inner.layout.segment(base);
        let f = Arc::new(File::open(&path).map_err(|e| Error::io(&path, "open", e))?);
        files.insert(base, f.clone());
        Ok(f)
    }

    fn segment_path(&self, base: u64) -> PathBuf {
        self.inner.layout.segment(base)
    }

    fn locate(&self, position: u64) -> Result<(u64, u64)> {
        self.inner.index.locate(position)?.ok_or_else(|| {
            Error::corrupt(
                self.inner.layout.index_file(),
                0,
                format!("position {position} below head is missing from the index"),
            )
        })
    }

    fn read_at(&self, base: u64, offset: u64, expect_position: u64) -> Result<RecordedEvent> {
        let path = self.segment_path(base);
        let file = self.file(base)?;
        let body = segment::read_record_at(&file, &path, offset, self.inner.opts.max_record_bytes)?;
        let ev =
            event::decode_body(&body).map_err(|e| Error::corrupt(&path, offset, e.to_string()))?;
        if ev.position.0 != expect_position {
            return Err(Error::corrupt(
                &path,
                offset,
                format!(
                    "index points position {expect_position} at a record carrying {}",
                    ev.position.0
                ),
            ));
        }
        Ok(ev)
    }

    fn read_position(&self, position: u64) -> Result<RecordedEvent> {
        let (base, offset) = self.locate(position)?;
        self.read_at(base, offset, position)
    }

    /// Events of `stream` from version `from` in `direction`, at most
    /// `limit`. Backward reads start at `from` and go down to version 0;
    /// pass `StreamVersion(u64::MAX)` to start at the newest.
    pub fn read_stream(
        &self,
        stream: &StreamId,
        from: StreamVersion,
        direction: Direction,
        limit: usize,
    ) -> Result<Vec<RecordedEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let forward = direction == Direction::Forward;
        let found = self
            .inner
            .index
            .stream_positions(stream, from.0, forward, limit)?;
        let mut out = Vec::with_capacity(found.len());
        for (version, position) in found {
            let ev = self.read_position(position)?;
            if ev.stream_version.0 != version || ev.stream_id != *stream {
                return Err(Error::corrupt(
                    self.segment_path(self.locate(position)?.0),
                    0,
                    format!(
                        "STREAMS maps ({stream}, {version}) to position {position}, which holds ({}, {})",
                        ev.stream_id, ev.stream_version
                    ),
                ));
            }
            out.push(ev);
        }
        Ok(out)
    }

    /// Events at positions `from..`, in order, at most `limit`. `from ==
    /// head` yields nothing; `from > head` is `PositionOutOfRange`.
    pub fn read_all(&self, from: GlobalPosition, limit: usize) -> Result<Vec<RecordedEvent>> {
        let head = *self.inner.head_tx.borrow();
        if from.0 > head {
            return Err(Error::PositionOutOfRange {
                position: from,
                head: GlobalPosition(head),
            });
        }
        if limit == 0 || from.0 == head {
            return Ok(Vec::new());
        }
        let want = limit.min((head - from.0) as usize);
        let mut out = Vec::with_capacity(want.min(4096));
        let mut expected = from.0;
        let (mut base, mut offset) = self.locate(expected)?;
        let max_record = self.inner.opts.max_record_bytes;
        'segments: loop {
            let path = self.segment_path(base);
            let file = self.file(base)?;
            let len = file
                .metadata()
                .map_err(|e| Error::io(&path, "metadata", e))?
                .len();
            let dup = file.try_clone().map_err(|e| Error::io(&path, "dup", e))?;
            let mut scanner = Scanner::from_file(dup, &path, offset, len, max_record)?;
            loop {
                match scanner.next()? {
                    ScanItem::Record {
                        offset: rec_off,
                        body,
                    } => {
                        let ev = event::decode_body(&body)
                            .map_err(|e| Error::corrupt(&path, rec_off, e.to_string()))?;
                        if ev.position.0 != expected {
                            return Err(Error::corrupt(
                                &path,
                                rec_off,
                                format!(
                                    "expected position {expected}, record carries {}",
                                    ev.position.0
                                ),
                            ));
                        }
                        out.push(ev);
                        expected += 1;
                        if out.len() >= want {
                            break 'segments;
                        }
                    }
                    ScanItem::Tail {
                        offset: tail_off,
                        reason,
                    } => {
                        if expected >= head {
                            break 'segments;
                        }
                        if reason == TailReason::Clean {
                            // Next segment; the index says where.
                            let (b, o) = self.locate(expected)?;
                            if b == base {
                                return Err(Error::corrupt(
                                    &path,
                                    tail_off,
                                    format!(
                                        "segment ends before position {expected} that the index places in it"
                                    ),
                                ));
                            }
                            base = b;
                            offset = o;
                            continue 'segments;
                        }
                        return Err(Error::corrupt(
                            &path,
                            tail_off,
                            format!("{reason} before position {expected} (head {head})"),
                        ));
                    }
                }
            }
        }
        Ok(out)
    }

    /// Events at positions `..=from`, newest first, at most `limit`. `from`
    /// is clamped to the last position, so `GlobalPosition(u64::MAX)` reads
    /// from the end.
    pub fn read_all_backward(
        &self,
        from: GlobalPosition,
        limit: usize,
    ) -> Result<Vec<RecordedEvent>> {
        let head = *self.inner.head_tx.borrow();
        if head == 0 || limit == 0 {
            return Ok(Vec::new());
        }
        let start = from.0.min(head - 1);
        let mut out = Vec::new();
        let mut p = start;
        loop {
            out.push(self.read_position(p)?);
            if out.len() >= limit || p == 0 {
                break;
            }
            p -= 1;
        }
        Ok(out)
    }

    /// Events whose type family (`Context.Name`, any version) is `family`,
    /// at positions `>= from`, in log order, at most `limit`.
    pub fn read_by_type(
        &self,
        family: &str,
        from: GlobalPosition,
        limit: usize,
    ) -> Result<Vec<RecordedEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let positions = self.inner.index.type_positions(family, from.0, limit)?;
        let mut out = Vec::with_capacity(positions.len());
        for p in positions {
            let ev = self.read_position(p)?;
            if ev.event_type.family() != family {
                return Err(Error::corrupt(
                    self.inner.layout.index_file(),
                    0,
                    format!(
                        "EVENT_TYPES lists position {p} under {family}, which holds {}",
                        ev.event_type
                    ),
                ));
            }
            out.push(ev);
        }
        Ok(out)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.opts.fsync == FsyncPolicy::Never {
            // Best effort: make Durability::None commits durable on a clean
            // close so a reopen sees them.
            if let Ok(w) = self.writer.lock() {
                let _ = w.sync_data();
            }
            if let Err(e) = self.index.flush() {
                warn!(error = %e, "flush on close failed");
            }
        }
    }
}

struct PreparedRecord {
    position: GlobalPosition,
    stream_version: StreamVersion,
    stream_id: StreamId,
    family: String,
}

struct Prepared {
    /// All framed records, back to back.
    buf: Vec<u8>,
    /// Offset of each record within `buf`.
    offsets: Vec<u64>,
    records: Vec<PreparedRecord>,
    new_head: u64,
}
