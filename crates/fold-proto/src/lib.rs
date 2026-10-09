//! The fold gRPC protocol: generated tonic clients and servers.
//!
//! fold runs as three services, each with its own package, over the
//! messages they share:
//!
//! - [`common`]: events, expected versions, rows, runner states, snapshot
//!   and rebuild messages, the schema bundle
//! - [`database`]: `Log`, `Cluster`, `Backup` and `Schema`, the database
//!   service's
//! - [`derivation`]: `Query`, `Aggregate`, `DeriveAdmin` and the internal
//!   `Derive`, the derivation service's
//! - [`application`]: `Command` and `AppAdmin`, the application service's
//!
//! [`v1`] is the single-daemon protocol the composite still speaks; it goes
//! once the composite is assembled from the three services.
//!
//! See the `.proto` files under `proto/` for the contracts.

/// Generated code for the `fold.v1` package (the single-daemon protocol).
pub mod v1 {
    #![allow(clippy::all, missing_docs)]
    tonic::include_proto!("fold.v1");
}

/// The messages every layer's services share.
pub mod common {
    /// Generated code for the `fold.common.v1` package.
    pub mod v1 {
        #![allow(clippy::all, missing_docs)]
        tonic::include_proto!("fold.common.v1");
    }
}

/// The database service: the log and the cluster.
pub mod database {
    /// Generated code for the `fold.database.v1` package.
    pub mod v1 {
        #![allow(clippy::all, missing_docs)]
        tonic::include_proto!("fold.database.v1");
    }
}

/// The derivation service: aggregate state and projections.
pub mod derivation {
    /// Generated code for the `fold.derivation.v1` package.
    pub mod v1 {
        #![allow(clippy::all, missing_docs)]
        tonic::include_proto!("fold.derivation.v1");
    }
}

/// The application service: commands and process managers.
pub mod application {
    /// Generated code for the `fold.application.v1` package.
    pub mod v1 {
        #![allow(clippy::all, missing_docs)]
        tonic::include_proto!("fold.application.v1");
    }
}

pub mod token;

/// The content type every payload, row and state carries in this version.
pub const CONTENT_TYPE_JSON: &str = "application/json";

/// Request metadata header carrying the system token that lets the
/// application service append `Fold.*` events (process timers) to the
/// database.
pub const SYSTEM_TOKEN_HEADER: &str = "fold-system-token";

/// Response metadata header on an `ALREADY_EXISTS` from `Log.Append`: the
/// position the duplicate idempotency key first wrote.
pub const FIRST_POSITION_HEADER: &str = "fold-first-position";

/// Response metadata header on a `FAILED_PRECONDITION` from `Log.Append`:
/// the stream's actual version, when the expected one did not match.
pub const CONFLICT_ACTUAL_VERSION_HEADER: &str = "fold-conflict-actual-version";

/// Response metadata header on `Query.Scan`: the session token (see
/// `GetResponse.token`).
pub const SESSION_HEADER: &str = "fold-session";
