//! The fold WASM host.
//!
//! A guest module is a plain core WebAssembly module (no WASI, no component
//! model) that exports the three ABI functions from `fold-guest`'s `module!`
//! plus one `(i32, i32) -> i64` export per schema entry point. The host hands
//! a JSON document to an export and reads a JSON reply back; the shared
//! document types live in [`fold_guest::abi`] so guest and host cannot drift.
//!
//! This crate knows nothing about the schema. It runs the guest within
//! limits (fuel, wall-clock epochs, memory, output size) and returns typed
//! replies for the derivation layer's three roles (projection steps,
//! evolves, upcasters); checking those replies against the domain is the
//! derivation node's job. Commands, invariants and process managers are
//! Rust code on the `fold-app` SDK, not guests.

#![forbid(unsafe_code)]

mod cache;
mod engine;
mod error;
mod guest;
mod limits;

pub use cache::{LoadedModule, ModuleCache};
pub use engine::Engine;
pub use error::WasmError;
pub use fold_guest::abi::{
    Event, EvolveInput, EvolveOutput, ProjectionInput, ProjectionOutput, UpcastEvent, UpcastInput,
    UpcastOutput,
};
pub use fold_guest::{Mutation, Op, TruncateFrom};
pub use guest::{Guest, RowReader};
pub use limits::Limits;

/// The ABI version this host speaks; a module's `fold_abi_version` must match.
pub const ABI_VERSION: i32 = fold_guest::ABI_VERSION;
