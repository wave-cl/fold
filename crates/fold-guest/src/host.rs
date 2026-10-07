//! The two host imports a guest may call, and the [`Ctx`] a projection step
//! receives. Both are real only on `wasm32`; native builds (clippy, unit
//! tests) get stubs so the crate compiles everywhere.

use serde_json::Value;

/// Severity for [`log`]; the host maps it onto its own tracing levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum LogLevel {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
}

#[cfg(target_arch = "wasm32")]
mod imports {
    #[link(wasm_import_module = "fold")]
    unsafe extern "C" {
        /// Writes up to `out_cap` bytes of the row at `(table, key)` into
        /// `out_ptr` and returns the row's full length, or -1 when absent.
        /// The projection is implicit: the host knows which one is running.
        pub fn get_row(
            table_ptr: i32,
            table_len: i32,
            key_ptr: i32,
            key_len: i32,
            out_ptr: i32,
            out_cap: i32,
        ) -> i64;
        pub fn log(level: i32, ptr: i32, len: i32);
    }
}

/// Sends a line to the host's log.
pub fn log(level: LogLevel, message: &str) {
    #[cfg(target_arch = "wasm32")]
    // SAFETY: the pointer and length describe a live `&str`.
    unsafe {
        imports::log(level as i32, message.as_ptr() as i32, message.len() as i32);
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = (level, message);
    }
}

/// What a projection step can ask the host.
#[derive(Debug, Clone)]
pub struct Ctx {
    projection: String,
}

impl Ctx {
    #[doc(hidden)]
    pub fn new(projection: String) -> Self {
        Ctx { projection }
    }

    /// `Context.Projection` of the running step.
    pub fn projection(&self) -> &str {
        &self.projection
    }

    /// Reads a row of one of this projection's tables as it stood before
    /// the current batch. `key` is a JSON object of the key fields.
    pub fn get(&self, table: &str, key: &Value) -> Result<Option<Value>, String> {
        let key_bytes = serde_json::to_vec(key).map_err(|e| e.to_string())?;
        match raw_get_row(table, &key_bytes) {
            None => Ok(None),
            Some(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| format!("row of {table} is not valid JSON: {e}")),
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn raw_get_row(table: &str, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf: Vec<u8> = vec![0; 4096];
    loop {
        // SAFETY: every pointer/length pair describes a live buffer we own
        // for the duration of the call.
        let n = unsafe {
            imports::get_row(
                table.as_ptr() as i32,
                table.len() as i32,
                key.as_ptr() as i32,
                key.len() as i32,
                buf.as_mut_ptr() as i32,
                buf.len() as i32,
            )
        };
        if n < 0 {
            return None;
        }
        let n = n as usize;
        if n <= buf.len() {
            buf.truncate(n);
            return Some(buf);
        }
        buf.resize(n, 0);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn raw_get_row(_table: &str, _key: &[u8]) -> Option<Vec<u8>> {
    None
}
