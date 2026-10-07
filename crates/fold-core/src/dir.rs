//! On-disk layout of one log and the pieces that are not segments: the
//! `LOG` identity file, the `LOCK`, the schema file and the segment listing.
//!
//! ```text
//! <data_dir>/<log_name>/
//!   LOG                       64-byte identity
//!   LOCK                      held with File::try_lock while open
//!   schema/current.fold       verbatim text the daemon passes
//!   segments/<base:020>.seg
//!   index.redb
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::error::{Error, Result};
use crate::segment;

pub(crate) const LOG_MAGIC: &[u8; 8] = b"FOLDLOG\0";
pub(crate) const LOG_FORMAT: u32 = 1;
const LOG_FILE_LEN: usize = 64;

/// Paths inside one log directory.
#[derive(Debug, Clone)]
pub(crate) struct Layout {
    pub root: PathBuf,
}

impl Layout {
    pub(crate) fn new(dir: &Path, name: &str) -> Self {
        Layout {
            root: dir.join(name),
        }
    }
    pub(crate) fn log_file(&self) -> PathBuf {
        self.root.join("LOG")
    }
    pub(crate) fn lock_file(&self) -> PathBuf {
        self.root.join("LOCK")
    }
    pub(crate) fn schema_dir(&self) -> PathBuf {
        self.root.join("schema")
    }
    pub(crate) fn schema_file(&self) -> PathBuf {
        self.schema_dir().join("current.fold")
    }
    pub(crate) fn segments_dir(&self) -> PathBuf {
        self.root.join("segments")
    }
    pub(crate) fn index_file(&self) -> PathBuf {
        self.root.join("index.redb")
    }
    pub(crate) fn segment(&self, base: u64) -> PathBuf {
        self.segments_dir().join(segment::file_name(base))
    }
    pub(crate) fn exists(&self) -> bool {
        self.log_file().is_file()
    }
}

/// The 64-byte identity record in `LOG`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Identity {
    pub log_id: Uuid,
    pub created_at: i64,
}

impl Identity {
    fn encode(&self) -> [u8; LOG_FILE_LEN] {
        let mut b = [0u8; LOG_FILE_LEN];
        b[0..8].copy_from_slice(LOG_MAGIC);
        b[8..12].copy_from_slice(&LOG_FORMAT.to_be_bytes());
        b[16..32].copy_from_slice(self.log_id.as_bytes());
        b[32..40].copy_from_slice(&self.created_at.to_be_bytes());
        let crc = crc32fast::hash(&b);
        b[12..16].copy_from_slice(&crc.to_be_bytes());
        b
    }

    fn decode(b: &[u8; LOG_FILE_LEN]) -> std::result::Result<Self, String> {
        if &b[0..8] != LOG_MAGIC {
            return Err("bad magic".into());
        }
        let format = u32::from_be_bytes(b[8..12].try_into().unwrap());
        if format != LOG_FORMAT {
            return Err(format!("unknown log format {format}"));
        }
        let stored = u32::from_be_bytes(b[12..16].try_into().unwrap());
        let mut z = *b;
        z[12..16].fill(0);
        if crc32fast::hash(&z) != stored {
            return Err("identity crc mismatch".into());
        }
        Ok(Identity {
            log_id: Uuid::from_bytes(b[16..32].try_into().unwrap()),
            created_at: i64::from_be_bytes(b[32..40].try_into().unwrap()),
        })
    }

    pub(crate) fn write(&self, layout: &Layout) -> Result<()> {
        let path = layout.log_file();
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| Error::io(&path, "create", e))?;
        f.write_all(&self.encode())
            .map_err(|e| Error::io(&path, "write", e))?;
        f.sync_all().map_err(|e| Error::io(&path, "fsync", e))?;
        Ok(())
    }

    pub(crate) fn read(layout: &Layout) -> Result<Self> {
        let path = layout.log_file();
        let mut f = File::open(&path).map_err(|e| Error::io(&path, "open", e))?;
        let mut b = [0u8; LOG_FILE_LEN];
        f.read_exact(&mut b).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::corrupt(&path, 0, "LOG file shorter than 64 bytes")
            } else {
                Error::io(&path, "read", e)
            }
        })?;
        Identity::decode(&b).map_err(|reason| Error::corrupt(&path, 0, reason))
    }
}

/// Holds `LOCK` for as long as it lives.
#[derive(Debug)]
pub(crate) struct Lock {
    _file: File,
}

impl Lock {
    pub(crate) fn acquire(layout: &Layout) -> Result<Self> {
        let path = layout.lock_file();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| Error::io(&path, "open", e))?;
        match file.try_lock() {
            Ok(()) => Ok(Lock { _file: file }),
            Err(std::fs::TryLockError::WouldBlock) => Err(Error::Locked {
                path: layout.root.clone(),
            }),
            Err(std::fs::TryLockError::Error(e)) => Err(Error::io(&path, "lock", e)),
        }
    }
}

/// Lists `segments/*.seg` sorted **numerically** by base position.
pub(crate) fn list_segments(segments_dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    let entries = fs::read_dir(segments_dir).map_err(|e| Error::io(segments_dir, "read_dir", e))?;
    for entry in entries {
        let entry = entry.map_err(|e| Error::io(segments_dir, "read_dir", e))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(base) = segment::parse_file_name(name) {
            out.push((base, entry.path()));
        }
    }
    out.sort_by_key(|(base, _)| *base);
    Ok(out)
}

pub(crate) fn read_schema(layout: &Layout) -> Result<Option<String>> {
    let path = layout.schema_file();
    match fs::read_to_string(&path) {
        Ok(s) => Ok(Some(s)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::io(&path, "read", e)),
    }
}

/// Writes the schema text atomically (temp file + rename) and fsyncs.
pub(crate) fn write_schema(layout: &Layout, text: &str) -> Result<()> {
    let dir = layout.schema_dir();
    fs::create_dir_all(&dir).map_err(|e| Error::io(&dir, "create_dir", e))?;
    let tmp = dir.join("current.fold.tmp");
    let path = layout.schema_file();
    {
        let mut f = File::create(&tmp).map_err(|e| Error::io(&tmp, "create", e))?;
        f.write_all(text.as_bytes())
            .map_err(|e| Error::io(&tmp, "write", e))?;
        f.sync_all().map_err(|e| Error::io(&tmp, "fsync", e))?;
    }
    fs::rename(&tmp, &path).map_err(|e| Error::io(&path, "rename", e))?;
    segment::sync_dir(&dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segments_sort_numerically_not_lexically() {
        let d = tempfile::tempdir().unwrap();
        // Unpadded names sort lexically as 10 < 9 < 900; numerically 9 < 10 < 900.
        for name in ["10.seg", "9.seg", "900.seg", "notes.txt", "x.seg"] {
            fs::write(d.path().join(name), b"").unwrap();
        }
        let bases: Vec<u64> = list_segments(d.path())
            .unwrap()
            .into_iter()
            .map(|(b, _)| b)
            .collect();
        assert_eq!(bases, vec![9, 10, 900]);
    }

    #[test]
    fn identity_round_trip_and_corruption() {
        let d = tempfile::tempdir().unwrap();
        let layout = Layout::new(d.path(), "l");
        fs::create_dir_all(&layout.root).unwrap();
        let id = Identity {
            log_id: Uuid::from_u128(99),
            created_at: 123,
        };
        id.write(&layout).unwrap();
        assert_eq!(Identity::read(&layout).unwrap(), id);
        let mut bytes = fs::read(layout.log_file()).unwrap();
        bytes[20] ^= 1;
        fs::write(layout.log_file(), &bytes).unwrap();
        assert!(matches!(
            Identity::read(&layout),
            Err(Error::Corrupt { .. })
        ));
    }

    #[test]
    fn lock_is_exclusive_and_released_on_drop() {
        let d = tempfile::tempdir().unwrap();
        let layout = Layout::new(d.path(), "l");
        fs::create_dir_all(&layout.root).unwrap();
        let first = Lock::acquire(&layout).unwrap();
        assert!(matches!(Lock::acquire(&layout), Err(Error::Locked { .. })));
        drop(first);
        Lock::acquire(&layout).unwrap();
    }

    #[test]
    fn schema_read_write() {
        let d = tempfile::tempdir().unwrap();
        let layout = Layout::new(d.path(), "l");
        fs::create_dir_all(&layout.root).unwrap();
        assert_eq!(read_schema(&layout).unwrap(), None);
        write_schema(&layout, "context A {}").unwrap();
        assert_eq!(
            read_schema(&layout).unwrap().as_deref(),
            Some("context A {}")
        );
        write_schema(&layout, "context B {}").unwrap();
        assert_eq!(
            read_schema(&layout).unwrap().as_deref(),
            Some("context B {}")
        );
    }
}
