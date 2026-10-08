//! Whole-log backups: one archive holding the index (dumped from a single
//! read transaction, which fixes the head), every segment file, the LOG
//! identity, the schema and the snapshot files. Restoring writes a fresh
//! log directory that [`Log::open`] recovers like any other: records past
//! the archived head, if a segment was copied while the writer appended,
//! are truncated on open.
//!
//! ```text
//! "FOLDBKUP" | u32 header_len | header JSON (BackupMeta + schema)
//! entries: u8 kind | u16 name_len | name | u64 len | bytes
//!   kind 1 = file (path relative to the log dir), kind 2 = index table dump
//! u8 0xFF | u32 crc32 over everything after the header
//! ```

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::dir::{Layout, list_segments, read_schema};
use crate::error::{Error, Result};
use crate::index::{Index, TableDump};
use crate::log::Inner;
use crate::options::FsyncPolicy;

const MAGIC: &[u8; 8] = b"FOLDBKUP";
const KIND_FILE: u8 = 1;
const KIND_TABLE: u8 = 2;
const KIND_END: u8 = 0xFF;
const FORMAT: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupMeta {
    pub format: u32,
    pub log_id: Uuid,
    /// Next position at the time: every position below it is in the backup.
    pub head: u64,
    pub created_at_unix_nanos: i64,
    /// The schema text stored with the log, if any.
    pub schema: Option<String>,
    /// Filled on completion: entries written and archive size.
    #[serde(default)]
    pub files: u64,
    #[serde(default)]
    pub bytes: u64,
}

struct Crc<W: Write> {
    inner: W,
    hasher: crc32fast::Hasher,
}

impl<W: Write> Write for Crc<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn io<'a>(path: &'a Path, op: &'static str) -> impl FnOnce(std::io::Error) -> Error + 'a {
    move |e| Error::io(path, op, e)
}

fn entry_header<W: Write>(w: &mut W, archive: &Path, kind: u8, name: &str, len: u64) -> Result<()> {
    w.write_all(&[kind]).map_err(io(archive, "write"))?;
    w.write_all(&(name.len() as u16).to_be_bytes())
        .map_err(io(archive, "write"))?;
    w.write_all(name.as_bytes()).map_err(io(archive, "write"))?;
    w.write_all(&len.to_be_bytes())
        .map_err(io(archive, "write"))?;
    Ok(())
}

fn copy_file<W: Write>(w: &mut W, archive: &Path, src: &Path, name: &str) -> Result<u64> {
    let mut f = File::open(src).map_err(io(src, "open"))?;
    let len = f.metadata().map_err(io(src, "metadata"))?.len();
    entry_header(w, archive, KIND_FILE, name, len)?;
    // The file may grow while we copy (a segment being appended to); copy
    // exactly the length recorded in the entry header.
    let mut remaining = len;
    let mut buf = vec![0u8; 1 << 16];
    while remaining > 0 {
        let want = buf.len().min(remaining as usize);
        let n = f.read(&mut buf[..want]).map_err(io(src, "read"))?;
        if n == 0 {
            return Err(Error::corrupt(
                src,
                len - remaining,
                "file shrank during backup",
            ));
        }
        w.write_all(&buf[..n]).map_err(io(archive, "write"))?;
        remaining -= n as u64;
    }
    Ok(len)
}

fn walk(dir: &Path, rel: &Path, out: &mut Vec<(PathBuf, String)>) -> Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(Error::io(dir, "read_dir", e)),
    };
    let mut list: Vec<_> = entries
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(io(dir, "read_dir"))?;
    list.sort_by_key(|e| e.file_name());
    for entry in list {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let rel = rel.join(&name);
        if path.is_dir() {
            walk(&path, &rel, out)?;
        } else if path.is_file() {
            out.push((path, rel.to_string_lossy().into_owned()));
        }
    }
    Ok(())
}

pub(crate) fn write(inner: &Inner, archive: &Path) -> Result<BackupMeta> {
    let layout = inner.layout();
    // 1. The index first: its read transaction fixes the head.
    let (head, tables) = inner.index.dump()?;
    let mut meta = BackupMeta {
        format: FORMAT,
        log_id: inner.identity().log_id,
        head,
        created_at_unix_nanos: now_nanos(),
        schema: read_schema(layout)?,
        files: 0,
        bytes: 0,
    };

    if let Some(parent) = archive.parent() {
        fs::create_dir_all(parent).map_err(io(parent, "create_dir"))?;
    }
    let tmp = archive.with_extension("fbak.tmp");
    let file = File::create(&tmp).map_err(io(&tmp, "create"))?;
    let mut w = BufWriter::new(file);
    let header = serde_json::to_vec(&meta).expect("meta serializes");
    w.write_all(MAGIC).map_err(io(&tmp, "write"))?;
    w.write_all(&(header.len() as u32).to_be_bytes())
        .map_err(io(&tmp, "write"))?;
    w.write_all(&header).map_err(io(&tmp, "write"))?;
    let mut w = Crc {
        inner: w,
        hasher: crc32fast::Hasher::new(),
    };

    // 2. Index tables.
    let mut files = 0u64;
    for t in &tables {
        entry_header(&mut w, &tmp, KIND_TABLE, &t.name, t.entries.len() as u64)?;
        w.write_all(&t.entries).map_err(io(&tmp, "write"))?;
        files += 1;
    }
    // 3. Files: identity, schema, segments (whole files; recovery trims
    // anything past the archived head), snapshot files.
    let mut list: Vec<(PathBuf, String)> = vec![(layout.log_file(), "LOG".into())];
    if layout.schema_file().is_file() {
        list.push((layout.schema_file(), "schema/current.fold".into()));
    }
    for (base, path) in list_segments(&layout.segments_dir())? {
        list.push((path, format!("segments/{base:020}.seg")));
    }
    walk(
        &layout.root.join("snapshots"),
        Path::new("snapshots"),
        &mut list,
    )?;
    for (path, name) in &list {
        copy_file(&mut w, &tmp, path, name)?;
        files += 1;
    }
    w.write_all(&[KIND_END]).map_err(io(&tmp, "write"))?;
    let sum = w.hasher.clone().finalize();
    let mut w = w.inner;
    w.write_all(&sum.to_be_bytes()).map_err(io(&tmp, "write"))?;
    w.flush().map_err(io(&tmp, "flush"))?;
    let file = w
        .into_inner()
        .map_err(|e| Error::io(&tmp, "flush", e.into_error()))?;
    file.sync_all().map_err(io(&tmp, "fsync"))?;
    fs::rename(&tmp, archive).map_err(io(archive, "rename"))?;
    meta.files = files;
    meta.bytes = fs::metadata(archive)
        .map_err(io(archive, "metadata"))?
        .len();
    Ok(meta)
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

fn read_header(r: &mut impl Read, archive: &Path) -> Result<BackupMeta> {
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic).map_err(io(archive, "read"))?;
    if &magic != MAGIC {
        return Err(Error::corrupt(archive, 0, "not a fold backup (bad magic)"));
    }
    let mut len = [0u8; 4];
    r.read_exact(&mut len).map_err(io(archive, "read"))?;
    let len = u32::from_be_bytes(len) as usize;
    if len > 64 << 20 {
        return Err(Error::corrupt(archive, 8, "backup header too large"));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).map_err(io(archive, "read"))?;
    let meta: BackupMeta = serde_json::from_slice(&buf)
        .map_err(|e| Error::corrupt(archive, 12, format!("backup header: {e}")))?;
    if meta.format != FORMAT {
        return Err(Error::corrupt(
            archive,
            12,
            format!("backup format {} is not {FORMAT}", meta.format),
        ));
    }
    Ok(meta)
}

/// Reads an archive's header without verifying its contents.
pub fn inspect(archive: &Path) -> Result<BackupMeta> {
    let f = File::open(archive).map_err(io(archive, "open"))?;
    let mut r = BufReader::new(f);
    let mut meta = read_header(&mut r, archive)?;
    meta.bytes = fs::metadata(archive)
        .map_err(io(archive, "metadata"))?
        .len();
    Ok(meta)
}

/// Restores `archive` as the log `<dir>/<name>`. Refuses to overwrite an
/// existing log. A checksum failure removes everything written.
pub fn restore(archive: &Path, dir: &Path, name: &str) -> Result<BackupMeta> {
    let layout = Layout::new(dir, name);
    if layout.root.exists() {
        return Err(Error::AlreadyExists { path: layout.root });
    }
    match restore_inner(archive, &layout) {
        Ok(meta) => Ok(meta),
        Err(e) => {
            let _ = fs::remove_dir_all(&layout.root);
            Err(e)
        }
    }
}

fn restore_inner(archive: &Path, layout: &Layout) -> Result<BackupMeta> {
    let f = File::open(archive).map_err(io(archive, "open"))?;
    let mut r = BufReader::new(f);
    let mut meta = read_header(&mut r, archive)?;
    fs::create_dir_all(&layout.root).map_err(io(&layout.root, "create_dir"))?;
    fs::create_dir_all(layout.segments_dir()).map_err(io(&layout.segments_dir(), "create_dir"))?;
    fs::create_dir_all(layout.schema_dir()).map_err(io(&layout.schema_dir(), "create_dir"))?;

    let mut hasher = crc32fast::Hasher::new();
    let mut tables: Vec<TableDump> = Vec::new();
    let mut files = 0u64;
    let mut buf = vec![0u8; 1 << 16];
    loop {
        let mut kind = [0u8; 1];
        r.read_exact(&mut kind).map_err(io(archive, "read"))?;
        hasher.update(&kind);
        if kind[0] == KIND_END {
            break;
        }
        let mut nl = [0u8; 2];
        r.read_exact(&mut nl).map_err(io(archive, "read"))?;
        hasher.update(&nl);
        let mut name = vec![0u8; u16::from_be_bytes(nl) as usize];
        r.read_exact(&mut name).map_err(io(archive, "read"))?;
        hasher.update(&name);
        let name = String::from_utf8(name)
            .map_err(|_| Error::corrupt(archive, 0, "entry name is not UTF-8"))?;
        let mut lb = [0u8; 8];
        r.read_exact(&mut lb).map_err(io(archive, "read"))?;
        hasher.update(&lb);
        let len = u64::from_be_bytes(lb);
        match kind[0] {
            KIND_TABLE => {
                let mut entries = vec![0u8; len as usize];
                r.read_exact(&mut entries).map_err(io(archive, "read"))?;
                hasher.update(&entries);
                tables.push(TableDump { name, entries });
            }
            KIND_FILE => {
                if name.contains("..") || name.starts_with('/') {
                    return Err(Error::corrupt(
                        archive,
                        0,
                        format!("unsafe entry path {name}"),
                    ));
                }
                let path = layout.root.join(&name);
                if let Some(p) = path.parent() {
                    fs::create_dir_all(p).map_err(io(p, "create_dir"))?;
                }
                let mut out = BufWriter::new(File::create(&path).map_err(io(&path, "create"))?);
                let mut remaining = len;
                while remaining > 0 {
                    let want = buf.len().min(remaining as usize);
                    r.read_exact(&mut buf[..want])
                        .map_err(io(archive, "read"))?;
                    hasher.update(&buf[..want]);
                    out.write_all(&buf[..want]).map_err(io(&path, "write"))?;
                    remaining -= want as u64;
                }
                out.flush().map_err(io(&path, "flush"))?;
                out.into_inner()
                    .map_err(|e| Error::io(&path, "flush", e.into_error()))?
                    .sync_all()
                    .map_err(io(&path, "fsync"))?;
            }
            other => {
                return Err(Error::corrupt(
                    archive,
                    0,
                    format!("unknown entry kind {other}"),
                ));
            }
        }
        files += 1;
    }
    let mut sum = [0u8; 4];
    r.read_exact(&mut sum).map_err(io(archive, "read"))?;
    if u32::from_be_bytes(sum) != hasher.finalize() {
        return Err(Error::corrupt(
            archive,
            0,
            "checksum mismatch; the archive is damaged",
        ));
    }
    // The index last: a damaged archive leaves no half-built log behind.
    Index::load(
        &layout.index_file(),
        FsyncPolicy::Always,
        meta.head,
        &tables,
    )?;
    crate::segment::sync_dir(&layout.root)?;
    crate::segment::sync_dir(&layout.segments_dir())?;
    meta.files = files;
    meta.bytes = fs::metadata(archive)
        .map_err(io(archive, "metadata"))?
        .len();
    Ok(meta)
}
