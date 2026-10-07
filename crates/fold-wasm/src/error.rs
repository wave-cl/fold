use std::path::PathBuf;

/// Everything that can go wrong between the host and a guest module.
#[derive(Debug, thiserror::Error)]
pub enum WasmError {
    #[error("cannot read module {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("module path {0:?} must be relative to the schema and may not leave its directory")]
    PathEscapes(String),
    #[error("module {path} does not compile: {reason}")]
    Compile { path: PathBuf, reason: String },
    #[error("module imports {module}.{name}, which the fold host does not provide")]
    UnsupportedImport { module: String, name: String },
    #[error("module does not export `{0}` with the expected signature")]
    MissingExport(String),
    #[error("module speaks ABI version {0}, this host speaks {expected}", expected = crate::ABI_VERSION)]
    BadAbiVersion(i32),
    #[error("guest ran out of fuel ({fuel} units)")]
    OutOfFuel { fuel: u64 },
    #[error("guest exceeded its wall-clock budget ({ticks} ticks)")]
    Timeout { ticks: u64 },
    #[error("guest exceeded its memory limit ({bytes} bytes)")]
    MemoryLimit { bytes: usize },
    #[error("guest trapped: {0}")]
    Trap(String),
    #[error("guest allocation failed (fold_alloc returned 0)")]
    AllocFailed,
    #[error("guest returned a reply of {len} bytes, above the {max} byte limit")]
    OutputTooLarge { len: usize, max: usize },
    #[error("input of {len} bytes is above the {max} byte limit")]
    InputTooLarge { len: usize, max: usize },
    #[error("guest returned a pointer/length outside its memory")]
    BadPointer,
    #[error("guest reply is not the expected JSON: {0}")]
    BadOutput(#[source] serde_json::Error),
    #[error("guest reported an error: {0}")]
    GuestError(String),
    #[error("row reader failed: {0}")]
    RowRead(String),
}
