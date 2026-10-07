//! The fold gRPC protocol: generated tonic clients and servers for the
//! `fold.v1` package. See `proto/fold/v1/fold.proto` for the contract.

/// Generated code for the `fold.v1` package.
pub mod v1 {
    #![allow(clippy::all, missing_docs)]
    tonic::include_proto!("fold.v1");
}

/// The content type every payload, row and state carries in this version.
pub const CONTENT_TYPE_JSON: &str = "application/json";
