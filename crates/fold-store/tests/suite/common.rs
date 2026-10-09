#![allow(dead_code)]

use std::path::Path;

use fold_core::{EventId, FsyncPolicy, StreamId};
use fold_store::{Checkpoint, DerivedStore};
use tempfile::TempDir;
use uuid::Uuid;

pub fn tmp() -> TempDir {
    tempfile::tempdir().expect("tempdir")
}

pub fn open(dir: &Path) -> DerivedStore {
    DerivedStore::open_or_create(&dir.join("derived.redb"), FsyncPolicy::Never).unwrap()
}

pub fn sid(s: &str) -> StreamId {
    StreamId::new(s).unwrap()
}

pub fn id(n: u128) -> EventId {
    EventId(Uuid::from_u128(n))
}

/// A checkpoint at `next` with no fingerprint.
pub fn cp(next: u64) -> Checkpoint {
    Checkpoint::at(next)
}

/// The `next` of a checkpoint, for assertions on position alone.
pub fn next_of(c: Option<Checkpoint>) -> Option<u64> {
    c.map(|c| c.next.0)
}
