//! Generates the fold services and message types from `proto/`: the layered
//! packages (`fold.common.v1`, `fold.database.v1`, `fold.derivation.v1`,
//! `fold.application.v1`) and, until the composite daemon speaks them, the
//! single-daemon `fold.v1`.
//!
//! The system `protoc` is used (brew `protobuf` on macOS, `protobuf-compiler`
//! on Debian/Ubuntu). It is looked up before the generator runs so a missing
//! compiler fails with one clear line rather than a stack of build errors.

use std::env;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTOS: [&str; 5] = [
        "proto/fold/common/v1/common.proto",
        "proto/fold/database/v1/database.proto",
        "proto/fold/derivation/v1/derivation.proto",
        "proto/fold/application/v1/application.proto",
        "proto/fold/v1/fold.proto",
    ];
    for p in PROTOS {
        println!("cargo:rerun-if-changed={p}");
    }
    println!("cargo:rerun-if-env-changed=PROTOC");

    let protoc = env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string());
    if Command::new(&protoc).arg("--version").output().is_err() {
        return Err(format!(
            "protoc not found (looked for `{protoc}`). Install it with `brew install protobuf` \
             or `apt-get install protobuf-compiler`, or point PROTOC at the binary."
        )
        .into());
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&PROTOS, &["proto"])?;
    Ok(())
}
