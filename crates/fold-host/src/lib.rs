//! Daemon code shared by the derivation and application services: wire
//! codecs, key encoding, snapshot files, guest linking, upcasting to the
//! latest event version, and the runner status and control types.

pub mod codec;
pub mod event;
pub mod guests;
pub mod keys;
pub mod runner;
pub mod snapshot;
pub mod upcast;

pub use event::{EventError, to_guest_event};
pub use guests::{GuestSource, Guests};
pub use runner::{Control, State, Status, StatusBook};
