//! Integration suite for `fold-core`, one binary.

mod common;

mod append_read;
mod backup;
mod concurrency;
mod lock;
mod recovery;
mod replicate;
mod roll;
mod subscribe;
mod tracing_smoke;
mod truncate;
