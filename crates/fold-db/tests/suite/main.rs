//! The database service's end-to-end suite: one test binary, several
//! modules, each test starting its own database on an ephemeral port in
//! its own temp dir, over the orders example's domain file. There is no
//! application here: tests append events directly.

mod backup;
mod cluster;
mod common;
mod log;
mod replica;
mod schema;
