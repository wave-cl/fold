//! Projection snapshot files: every row of a projection's tables at one
//! checkpoint, written from a single read transaction so the rows and the
//! checkpoint agree. A rebuild restores the rows, sets the checkpoint, and
//! replays only the events after it.
//!
//! Layout under `<log dir>/snapshots/<Context.Projection>/<checkpoint>.fsnap`:
//!
//! ```text
//! "FOLDPSNP" | u32 header_len | header JSON (SnapshotMeta without id/bytes)
//! records: u16 table_index | u32 key_len | key | u32 row_len | row ...
//! u16 0xFFFF | u32 crc32 over everything after the header
//! ```

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use fold_core::ReadModelSnapshot;
use serde::{Deserialize, Serialize};

const MAGIC: &[u8; 8] = b"FOLDPSNP";
const END: u16 = 0xFFFF;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotMeta {
    /// File stem, unique per projection: the checkpoint, zero padded.
    #[serde(default)]
    pub id: String,
    pub projection: String,
    /// Last position the rows include.
    pub checkpoint: u64,
    pub tables: Vec<String>,
    pub rows: u64,
    #[serde(default)]
    pub bytes: u64,
    pub created_at_unix_nanos: i64,
    /// Hex SHA-256 of the fold module the rows were produced by.
    pub module_hash: String,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("io {op} {path}: {source}")]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("log: {0}")]
    Core(#[from] fold_core::Error),
    #[error("{path}: not a fold projection snapshot ({reason})")]
    Format { path: PathBuf, reason: String },
    #[error("{path}: checksum mismatch; the file is damaged")]
    Checksum { path: PathBuf },
    #[error("snapshot is of projection {found}, not {wanted}")]
    WrongProjection { found: String, wanted: String },
    #[error("snapshot table {0} is not declared by the projection")]
    UnknownTable(String),
    #[error("projection has applied nothing yet; there is nothing to snapshot")]
    Empty,
    #[error("no snapshot {id} for projection {projection}")]
    NotFound { projection: String, id: String },
}

fn io<'a>(op: &'static str, path: &'a Path) -> impl FnOnce(std::io::Error) -> SnapshotError + 'a {
    move |source| SnapshotError::Io {
        op,
        path: path.to_path_buf(),
        source,
    }
}

pub fn dir_for(log_dir: &Path, projection: &str) -> PathBuf {
    log_dir.join("snapshots").join(projection)
}

fn id_for(checkpoint: u64) -> String {
    format!("{checkpoint:020}")
}

/// Writes a snapshot of `projection`'s `tables` as `snap` sees them.
pub fn write(
    log_dir: &Path,
    projection: &str,
    tables: &[String],
    module_hash: [u8; 32],
    snap: &ReadModelSnapshot,
) -> Result<SnapshotMeta, SnapshotError> {
    let next = snap.checkpoint(projection)?.ok_or(SnapshotError::Empty)?;
    let checkpoint = next.0.checked_sub(1).ok_or(SnapshotError::Empty)?;
    let dir = dir_for(log_dir, projection);
    fs::create_dir_all(&dir).map_err(io("create_dir", &dir))?;
    let id = id_for(checkpoint);
    let final_path = dir.join(format!("{id}.fsnap"));
    let tmp_path = dir.join(format!("{id}.fsnap.tmp"));

    let mut meta = SnapshotMeta {
        id: id.clone(),
        projection: projection.to_string(),
        checkpoint,
        tables: tables.to_vec(),
        rows: 0,
        bytes: 0,
        created_at_unix_nanos: jiff::Timestamp::now().as_nanosecond() as i64,
        module_hash: hex(&module_hash),
    };

    // Count first so the header is complete; the tables are small relative
    // to a replay, and a second pass keeps the format single-header.
    let mut rows = 0u64;
    for t in tables {
        rows += snap.scan(projection, t, &[], usize::MAX)?.len() as u64;
    }
    meta.rows = rows;

    let file = File::create(&tmp_path).map_err(io("create", &tmp_path))?;
    let mut w = BufWriter::new(file);
    let header = serde_json::to_vec(&meta).expect("meta serializes");
    w.write_all(MAGIC).map_err(io("write", &tmp_path))?;
    w.write_all(&(header.len() as u32).to_be_bytes())
        .map_err(io("write", &tmp_path))?;
    w.write_all(&header).map_err(io("write", &tmp_path))?;
    let mut crc = crc32fast::Hasher::new();
    let mut body = |w: &mut BufWriter<File>, bytes: &[u8]| -> Result<(), SnapshotError> {
        crc.update(bytes);
        w.write_all(bytes).map_err(io("write", &tmp_path))
    };
    for (i, t) in tables.iter().enumerate() {
        for (key, row) in snap.scan(projection, t, &[], usize::MAX)? {
            body(&mut w, &(i as u16).to_be_bytes())?;
            body(&mut w, &(key.len() as u32).to_be_bytes())?;
            body(&mut w, &key)?;
            body(&mut w, &(row.len() as u32).to_be_bytes())?;
            body(&mut w, &row)?;
        }
    }
    body(&mut w, &END.to_be_bytes())?;
    let sum = crc.finalize();
    w.write_all(&sum.to_be_bytes())
        .map_err(io("write", &tmp_path))?;
    w.flush().map_err(io("flush", &tmp_path))?;
    let file = w.into_inner().map_err(|e| SnapshotError::Io {
        op: "flush",
        path: tmp_path.clone(),
        source: e.into_error(),
    })?;
    file.sync_all().map_err(io("fsync", &tmp_path))?;
    fs::rename(&tmp_path, &final_path).map_err(io("rename", &final_path))?;
    meta.bytes = fs::metadata(&final_path)
        .map_err(io("metadata", &final_path))?
        .len();
    Ok(meta)
}

/// Reads a snapshot's header only.
pub fn read_meta(path: &Path) -> Result<SnapshotMeta, SnapshotError> {
    let file = File::open(path).map_err(io("open", path))?;
    let mut r = BufReader::new(file);
    let (mut meta, _) = header(&mut r, path)?;
    meta.bytes = fs::metadata(path).map_err(io("metadata", path))?.len();
    Ok(meta)
}

fn header(r: &mut impl Read, path: &Path) -> Result<(SnapshotMeta, usize), SnapshotError> {
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic).map_err(io("read", path))?;
    if &magic != MAGIC {
        return Err(SnapshotError::Format {
            path: path.to_path_buf(),
            reason: "bad magic".into(),
        });
    }
    let mut len = [0u8; 4];
    r.read_exact(&mut len).map_err(io("read", path))?;
    let len = u32::from_be_bytes(len) as usize;
    if len > 1 << 20 {
        return Err(SnapshotError::Format {
            path: path.to_path_buf(),
            reason: "header too large".into(),
        });
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).map_err(io("read", path))?;
    let mut meta: SnapshotMeta =
        serde_json::from_slice(&buf).map_err(|e| SnapshotError::Format {
            path: path.to_path_buf(),
            reason: format!("header: {e}"),
        })?;
    meta.id = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_string();
    Ok((meta, 12 + len))
}

/// Reads a whole snapshot: its meta and every `(table, key, row)`.
/// `(table, key, row)` as stored.
pub type Row = (String, Vec<u8>, Vec<u8>);

pub fn read(path: &Path) -> Result<(SnapshotMeta, Vec<Row>), SnapshotError> {
    let file = File::open(path).map_err(io("open", path))?;
    let mut r = BufReader::new(file);
    let (mut meta, _) = header(&mut r, path)?;
    let mut body = Vec::new();
    r.read_to_end(&mut body).map_err(io("read", path))?;
    if body.len() < 6 {
        return Err(SnapshotError::Format {
            path: path.to_path_buf(),
            reason: "truncated".into(),
        });
    }
    let (records, trailer) = body.split_at(body.len() - 4);
    let expected = u32::from_be_bytes(trailer.try_into().expect("4 bytes"));
    if crc32fast::hash(records) != expected {
        return Err(SnapshotError::Checksum {
            path: path.to_path_buf(),
        });
    }
    let mut rows = Vec::with_capacity(meta.rows as usize);
    let mut at = 0usize;
    let take = |at: &mut usize, n: usize| -> Result<&[u8], SnapshotError> {
        let end = *at + n;
        let s = records.get(*at..end).ok_or_else(|| SnapshotError::Format {
            path: path.to_path_buf(),
            reason: "truncated record".into(),
        })?;
        *at = end;
        Ok(s)
    };
    loop {
        let idx = u16::from_be_bytes(take(&mut at, 2)?.try_into().expect("2"));
        if idx == END {
            break;
        }
        let table = meta
            .tables
            .get(idx as usize)
            .ok_or_else(|| SnapshotError::Format {
                path: path.to_path_buf(),
                reason: format!("table index {idx} out of range"),
            })?
            .clone();
        let klen = u32::from_be_bytes(take(&mut at, 4)?.try_into().expect("4")) as usize;
        let key = take(&mut at, klen)?.to_vec();
        let rlen = u32::from_be_bytes(take(&mut at, 4)?.try_into().expect("4")) as usize;
        let row = take(&mut at, rlen)?.to_vec();
        rows.push((table, key, row));
    }
    meta.bytes = fs::metadata(path).map_err(io("metadata", path))?.len();
    Ok((meta, rows))
}

/// Every snapshot of `projection`, newest checkpoint first.
pub fn list(log_dir: &Path, projection: &str) -> Result<Vec<SnapshotMeta>, SnapshotError> {
    let dir = dir_for(log_dir, projection);
    let entries = match fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(io("read_dir", &dir)(e)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let path = entry.map_err(io("read_dir", &dir))?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("fsnap") {
            continue;
        }
        match read_meta(&path) {
            Ok(m) => out.push(m),
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "skipping unreadable snapshot")
            }
        }
    }
    out.sort_by_key(|m| std::cmp::Reverse(m.checkpoint));
    Ok(out)
}

pub fn path_of(log_dir: &Path, projection: &str, id: &str) -> Result<PathBuf, SnapshotError> {
    if id.is_empty() || id.contains('/') || id.contains("..") {
        return Err(SnapshotError::NotFound {
            projection: projection.to_string(),
            id: id.to_string(),
        });
    }
    let path = dir_for(log_dir, projection).join(format!("{id}.fsnap"));
    if !path.is_file() {
        return Err(SnapshotError::NotFound {
            projection: projection.to_string(),
            id: id.to_string(),
        });
    }
    Ok(path)
}

pub fn delete(log_dir: &Path, projection: &str, id: &str) -> Result<(), SnapshotError> {
    let path = path_of(log_dir, projection, id)?;
    fs::remove_file(&path).map_err(io("remove", &path))
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_damaged_file_is_refused_by_its_checksum() {
        let d = tempfile::tempdir().unwrap();
        let log = fold_core::Log::create(d.path(), "l", fold_core::OpenOptions::default()).unwrap();
        let rm = log.read_models();
        rm.commit(
            "C.P",
            fold_core::GlobalPosition(3),
            vec![("t".into(), b"k".to_vec(), b"{\"a\":1}".to_vec())],
            vec![],
        )
        .unwrap();
        let meta = write(
            log.path(),
            "C.P",
            &["t".to_string()],
            [7u8; 32],
            &rm.snapshot().unwrap(),
        )
        .unwrap();
        assert_eq!(meta.checkpoint, 2);
        assert_eq!(meta.rows, 1);
        let path = path_of(log.path(), "C.P", &meta.id).unwrap();
        let (back, rows) = read(&path).unwrap();
        assert_eq!(back.checkpoint, 2);
        assert_eq!(
            rows,
            vec![("t".to_string(), b"k".to_vec(), b"{\"a\":1}".to_vec())]
        );

        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 8;
        bytes[last] ^= 0x01;
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(read(&path), Err(SnapshotError::Checksum { .. })));
        assert_eq!(
            list(log.path(), "C.P").unwrap().len(),
            1,
            "the header still reads"
        );
        delete(log.path(), "C.P", &meta.id).unwrap();
        assert!(list(log.path(), "C.P").unwrap().is_empty());
        assert!(matches!(
            delete(log.path(), "C.P", &meta.id),
            Err(SnapshotError::NotFound { .. })
        ));
    }
}
