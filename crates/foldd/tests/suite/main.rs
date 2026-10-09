//! The foldd end-to-end suite over the composite: one test binary, several
//! modules, each test starting its own composite (database, derivation and
//! application nodes in one process) on an ephemeral port in its own temp
//! dir. What one layer does on its own is tested in that layer's crate;
//! here the three work together.

mod common;
mod concurrency;
mod e2e_aggregate;
mod e2e_backup;
mod e2e_boundaries;
mod e2e_derived;
mod e2e_guards;
mod e2e_imports;
mod e2e_orders;
mod e2e_process;
mod e2e_replica;
mod e2e_restore_live;
mod e2e_rywr;
mod e2e_schema_change;
mod e2e_session;
mod e2e_snapshot;
mod e2e_split;
mod e2e_timer;
mod e2e_upcast;
