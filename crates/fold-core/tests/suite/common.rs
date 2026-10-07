#![allow(dead_code)]

use std::path::{Path, PathBuf};

use bytes::Bytes;
use fold_core::{EventType, Log, NewEvent, OpenOptions, RecordedEvent, StreamId};
use tempfile::TempDir;

pub const NAME: &str = "testlog";

pub fn tmp() -> TempDir {
    tempfile::tempdir().expect("tempdir")
}

pub fn sid(s: &str) -> StreamId {
    StreamId::new(s).unwrap()
}

pub fn ty(name: &str) -> EventType {
    EventType::new("Orders", name, 1)
}

pub fn ev(name: &str, payload: &str) -> NewEvent {
    NewEvent::new(ty(name), Bytes::copy_from_slice(payload.as_bytes()))
}

pub fn create(dir: &Path) -> Log {
    Log::create(dir, NAME, OpenOptions::default()).unwrap()
}

pub fn create_with(dir: &Path, opts: OpenOptions) -> Log {
    Log::create(dir, NAME, opts).unwrap()
}

pub fn open(dir: &Path) -> Log {
    Log::open(dir, NAME, OpenOptions::default()).unwrap()
}

pub fn open_with(dir: &Path, opts: OpenOptions) -> Log {
    Log::open(dir, NAME, opts).unwrap()
}

pub fn root(dir: &Path) -> PathBuf {
    dir.join(NAME)
}

pub fn index_path(dir: &Path) -> PathBuf {
    root(dir).join("index.redb")
}

/// Segment files sorted by base.
pub fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(root(dir).join("segments"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "seg"))
        .collect();
    v.sort();
    v
}

pub fn last_segment(dir: &Path) -> PathBuf {
    segments(dir).pop().unwrap()
}

pub fn payloads(events: &[RecordedEvent]) -> Vec<String> {
    events
        .iter()
        .map(|e| String::from_utf8(e.payload.to_vec()).unwrap())
        .collect()
}

pub fn positions(events: &[RecordedEvent]) -> Vec<u64> {
    events.iter().map(|e| e.position.0).collect()
}

/// Truncates `path` by `k` bytes.
pub fn chop(path: &Path, k: u64) {
    let len = std::fs::metadata(path).unwrap().len();
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.set_len(len - k).unwrap();
}

/// XORs one byte of `path`.
pub fn flip(path: &Path, offset: u64, mask: u8) {
    let mut bytes = std::fs::read(path).unwrap();
    bytes[offset as usize] ^= mask;
    std::fs::write(path, bytes).unwrap();
}
