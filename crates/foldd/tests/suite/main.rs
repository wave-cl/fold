//! The foldd end-to-end suite: one test binary, several modules, each test
//! starting its own daemon on an ephemeral port in its own temp dir.

mod common;
mod concurrency;
mod e2e_aggregate;
mod e2e_backup;
mod e2e_fencing;
mod e2e_lease;
mod e2e_orders;
mod e2e_process;
mod e2e_quorum;
mod e2e_replica;
mod e2e_restore_live;
mod e2e_rywr;
mod e2e_session;
mod e2e_snapshot;
mod e2e_upcast;
