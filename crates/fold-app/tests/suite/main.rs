//! The application service's end-to-end suite: each test starts a
//! database node, a derivation node and an application node over the
//! orders example, runs commands through the application node and reads
//! state and rows from the derivation node.

mod boundaries;
mod common;
mod orders;
mod processes;
