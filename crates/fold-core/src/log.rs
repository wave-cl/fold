//! The `Log`: append under a writer mutex, reads through the index,
//! subscriptions over a `watch` channel.

use std::collections::HashMap;
use std::fs::{self, File};
use std::os::unix::fs::FileExt;
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

    pub(crate) fn layout(&self) -> &Layout {
        &self.layout
    }

    pub(crate) fn identity(&self) -> &Identity {
        &self.identity
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
        Self::create_identified(dir, name, opts, uuid::Uuid::now_v7())
    }

    /// Creates a new, empty log carrying another log's identity: the start
    /// of a replica, which receives that log's records as they are.
    pub fn create_with_id(
        dir: &Path,
        name: &str,
        opts: OpenOptions,
        log_id: uuid::Uuid,
    ) -> Result<Log> {
        Self::create_identified(dir, name, opts, log_id)
    }

    fn create_identified(
        dir: &Path,
        name: &str,
        opts: OpenOptions,
        log_id: uuid::Uuid,
    ) -> Result<Log> {
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
            log_id,
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

    /// Writes a consistent backup of the whole log to `archive`: the index
    /// as of one transaction, then every segment, the identity, the schema
    /// and the snapshot files. See [`crate::backup`].
    pub fn backup_to(&self, archive: &Path) -> Result<crate::backup::BackupMeta> {
        crate::backup::write(&self.inner, archive)
    }

    /// Writes an incremental backup holding the records from `since` up to
    /// the current head, the idempotency keys registered in that range and
    /// the schema. Restore it with [`crate::backup::apply`] onto a log that
    /// is exactly at `since`.
    pub fn backup_incremental(
        &self,
        archive: &Path,
        since: GlobalPosition,
    ) -> Result<crate::backup::BackupMeta> {
        crate::backup::write_incremental(self, archive, since.0)
    }

    /// The raw frames of positions `from..to`, copied from the segments.
    pub(crate) fn frames_between(&self, from: u64, to: u64) -> Result<Vec<u8>> {
        let head = self.head().0;
        if from > to || to > head {
            return Err(Error::PositionOutOfRange {
                position: GlobalPosition(to),
                head: GlobalPosition(head),
            });
        }
        let mut out = Vec::new();
        if from == to {
            return Ok(out);
        }
        let segments = crate::dir::list_segments(&self.inner.layout.segments_dir())?;
        let bases: Vec<u64> = segments.iter().map(|(b, _)| *b).collect();
        for (i, base) in bases.iter().enumerate() {
            let next_base = bases.get(i + 1).copied().unwrap_or(u64::MAX);
            let first = from.max(*base);
            let last_excl = to.min(next_base);
            if first >= last_excl {
                continue;
            }
            let (b1, start) = self.locate(first)?;
            let (b2, last_off) = self.locate(last_excl - 1)?;
            if b1 != *base || b2 != *base {
                return Err(Error::corrupt(
                    self.inner.layout.index_file(),
                    0,
                    format!("positions {first}..{last_excl} are not all in segment {base}"),
                ));
            }
            let path = self.segment_path(*base);
            let file = self.file(*base)?;
            let last_body =
                segment::read_record_at(&file, &path, last_off, self.inner.opts.max_record_bytes)?;
            let end = last_off + (segment::FRAME_LEN + last_body.len()) as u64;
            let mut buf = vec![0u8; (end - start) as usize];
            file.read_exact_at(&mut buf, start)
                .map_err(|e| Error::io(&path, "read", e))?;
            out.extend_from_slice(&buf);
        }
        Ok(out)
    }

    /// Appends ready-made frames (an incremental backup's records), which
    /// must start exactly at the head and end on a batch boundary. Ids,
    /// timestamps and versions are kept as recorded.
    pub(crate) fn import_frames(&self, frames: &[u8]) -> Result<GlobalPosition> {
        let tmp = self.inner.layout.root.join("import.tmp");
        fs::write(&tmp, frames).map_err(|e| Error::io(&tmp, "write", e))?;
        let result = self.import_frames_from(&tmp, frames);
        let _ = fs::remove_file(&tmp);
        result
    }

    fn import_frames_from(&self, tmp: &Path, frames: &[u8]) -> Result<GlobalPosition> {
        let file = File::open(tmp).map_err(|e| Error::io(tmp, "open", e))?;
        let mut scanner = segment::Scanner::from_file(
            file,
            tmp,
            0,
            frames.len() as u64,
            self.inner.opts.max_record_bytes,
        )?;
        let mut w = self.writer();
        let mut head = *self.inner.head_tx.borrow();
        let mut batch: Vec<u8> = Vec::new();
        let mut records: Vec<(u64, u64, RecordedEvent)> = Vec::new(); // (offset in batch, len, event)
        loop {
            match scanner.next()? {
                segment::ScanItem::Tail { reason, .. } => {
                    if reason != segment::TailReason::Clean {
                        return Err(Error::corrupt(
                            tmp,
                            scanner.offset(),
                            format!("incremental records: {reason}"),
                        ));
                    }
                    break;
                }
                segment::ScanItem::Record { offset, body } => {
                    let ev = event::decode_body(&body)
                        .map_err(|e| Error::corrupt(tmp, offset, e.to_string()))?;
                    let expected = head + records.len() as u64;
                    if ev.position.0 != expected {
                        return Err(Error::corrupt(
                            tmp,
                            offset,
                            format!(
                                "incremental record carries position {}, expected {expected}",
                                ev.position.0
                            ),
                        ));
                    }
                    let frame_len = segment::FRAME_LEN + body.len();
                    let rel = batch.len() as u64;
                    batch.extend_from_slice(&frames[offset as usize..offset as usize + frame_len]);
                    let last = ev.is_last_in_batch();
                    records.push((rel, frame_len as u64, ev));
                    if last {
                        let base = w.base();
                        let at = w.write_at_end(&batch)?;
                        if self.inner.opts.fsync == FsyncPolicy::Always {
                            w.sync_data()?;
                        }
                        let families: Vec<String> = records
                            .iter()
                            .map(|(_, _, e)| e.event_type.family())
                            .collect();
                        let entries: Vec<IndexEntry<'_>> = records
                            .iter()
                            .zip(&families)
                            .map(|((rel, _, e), family)| IndexEntry {
                                position: e.position.0,
                                segment_base: base,
                                offset: at + rel,
                                stream: &e.stream_id,
                                version: e.stream_version.0,
                                family,
                            })
                            .collect();
                        let new_head = head + records.len() as u64;
                        self.inner.index.commit_batch(&entries, new_head, None)?;
                        w.advance(batch.len() as u64);
                        self.inner.head_tx.send_replace(new_head);
                        head = new_head;
                        if w.len() >= self.inner.opts.segment_max_bytes {
                            self.roll(&mut w, new_head);
                        }
                        batch.clear();
                        records.clear();
                    }
                }
            }
        }
        if !records.is_empty() {
            return Err(Error::corrupt(
                tmp,
                scanner.offset(),
                "incremental records end inside a batch",
            ));
        }
        Ok(GlobalPosition(head))
    }

    pub(crate) fn inner(&self) -> &Inner {
        &self.inner
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
    /// Every stream that has at least one event, in key order. Linear in
    /// the number of streams; for rebuilds, not for hot paths.
    pub fn stream_ids(&self) -> Result<Vec<StreamId>> {
        self.inner
            .index
            .stream_ids()?
            .into_iter()
            .map(|s| StreamId::new(&s))
            .collect()
    }

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
        self.append_inner(stream, expected, events, None)
    }

    /// Like [`Log::append`], but refuses with [`Error::DuplicateKey`] if
    /// `key` was used by an earlier append. The key is recorded in the same
    /// transaction as the events, so a retry after a crash either finds the
    /// events committed (and the key refused) or neither.
    pub fn append_idempotent(
        &self,
        stream: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent>,
        key: &[u8],
    ) -> Result<AppendResult> {
        self.append_inner(stream, expected, events, Some(key))
    }

    /// Where an idempotency key was first used, if it was.
    pub fn idempotency_position(&self, key: &[u8]) -> Result<Option<GlobalPosition>> {
        Ok(self
            .inner
            .index
            .idempotency_position(key)?
            .map(GlobalPosition))
    }

    fn append_inner(
        &self,
        stream: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent>,
        idempotency_key: Option<&[u8]>,
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
        self.inner
            .index
            .commit_batch(&entries, new_head, idempotency_key)?;
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
            // A scanner seeks, so it needs a handle of its own: a `try_clone`
            // of the cached handle would share its file offset with every
            // other concurrent scan and they would read past each other.
            let file = File::open(&path).map_err(|e| Error::io(&path, "open", e))?;
            let len = file
                .metadata()
                .map_err(|e| Error::io(&path, "metadata", e))?
                .len();
            let mut scanner = Scanner::from_file(file, &path, offset, len, max_record)?;
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
