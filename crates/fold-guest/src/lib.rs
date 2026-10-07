//! Guest-side SDK for fold WASM modules.
//!
//! A module declares its ABI plumbing once with [`module!`], then exports one
//! function per schema entry point with [`projection!`], [`aggregate!`] and
//! [`command!`]. Each export takes `(ptr, len)` of a JSON document in linear
//! memory and returns a packed `(ptr << 32) | len` of the JSON reply; the
//! host frees the reply through `fold_free`.
//!
//! ```ignore
//! fold_guest::module!();
//!
//! fold_guest::projection!(project_order_totals = |cx: &Ctx, ev: &Event| {
//!     let row = Row::new("order_totals", json!({ "order_id": ev.payload["order_id"] }));
//!     Ok(vec![row.upsert(json!({ "total": ev.payload["total"], "status": "Pending" }))])
//! });
//! ```
//!
//! Every type here mirrors the JSON contract in the fold design: see
//! `abi` for the exact shapes.

#![forbid(unsafe_op_in_unsafe_fn)]

pub mod abi;
mod host;
mod mutation;

pub use abi::{
    CheckInput, CheckOutput, CmdCtx, Command, Emit, Event, InvCtx, IssuedCommand, PendingEvent,
    ProcCtx, ProcessInput, ProcessOutput, Reaction, Rejected, Trigger,
};
pub use host::{Ctx, LogLevel, log};
pub use mutation::{Mutation, Op, Row, TruncateFrom};
pub use serde_json::{Value, json};

/// The ABI version this SDK speaks; `fold_abi_version` returns it.
pub const ABI_VERSION: i32 = 1;

/// Why a command handler did not emit events.
///
/// A [`Rejected`] is a business decision (`{"rejected": {...}}` on the wire)
/// and reaches the client as `FAILED_PRECONDITION` with its code. An `Error`
/// is a defect (`{"error": "..."}`) and reaches the client as `INTERNAL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fail {
    Rejected(Rejected),
    Error(String),
}

impl From<Rejected> for Fail {
    fn from(r: Rejected) -> Self {
        Fail::Rejected(r)
    }
}

impl From<String> for Fail {
    fn from(s: String) -> Self {
        Fail::Error(s)
    }
}

impl From<&str> for Fail {
    fn from(s: &str) -> Self {
        Fail::Error(s.to_string())
    }
}

/// Plumbing shared by the entry-point macros: read the input document, run
/// the body, serialize the reply, and hand it to the host.
#[doc(hidden)]
pub mod __rt {
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    /// Reconstructs the input bytes the host wrote via `fold_alloc`.
    ///
    /// # Safety
    /// `ptr`/`len` must be a buffer previously returned by `fold_alloc(len)`
    /// and not yet freed; ownership passes to this function.
    pub unsafe fn take_input(ptr: i32, len: i32) -> Vec<u8> {
        // SAFETY: the host obtained `ptr` from `fold_alloc(len)`, which leaked a
        // Vec of exactly this capacity, and promises not to touch it again.
        unsafe { Vec::from_raw_parts(ptr as *mut u8, len as usize, len as usize) }
    }

    /// Leaks `bytes` and returns the packed `(ptr << 32) | len` the host reads.
    pub fn give_output(bytes: Vec<u8>) -> i64 {
        let boxed = bytes.into_boxed_slice();
        let len = boxed.len() as i64;
        let ptr = Box::leak(boxed).as_mut_ptr() as i64;
        (ptr << 32) | len
    }

    /// Runs one entry point: decode, call, encode. Decode failures and
    /// serialization failures become `{"error": ...}` replies, never traps.
    pub fn run<I: DeserializeOwned, O: Serialize>(
        input: Vec<u8>,
        f: impl FnOnce(I) -> O,
        on_decode_error: impl FnOnce(String) -> O,
    ) -> i64 {
        let out = match serde_json::from_slice::<I>(&input) {
            Ok(i) => f(i),
            Err(e) => on_decode_error(format!("fold-guest: cannot decode input: {e}")),
        };
        let bytes = serde_json::to_vec(&out).unwrap_or_else(|e| {
            format!(r#"{{"error":"fold-guest: cannot encode reply: {e}"}}"#).into_bytes()
        });
        give_output(bytes)
    }
}

/// Emits the three ABI functions every module must export. Invoke once.
#[macro_export]
macro_rules! module {
    () => {
        #[cfg_attr(target_arch = "wasm32", unsafe(no_mangle))]
        #[allow(dead_code)]
        pub extern "C" fn fold_abi_version() -> i32 {
            $crate::ABI_VERSION
        }

        #[cfg_attr(target_arch = "wasm32", unsafe(no_mangle))]
        #[allow(dead_code)]
        pub extern "C" fn fold_alloc(len: i32) -> i32 {
            let mut v: Vec<u8> = Vec::with_capacity(len as usize);
            let ptr = v.as_mut_ptr();
            ::std::mem::forget(v);
            ptr as i32
        }

        /// # Safety
        /// `ptr`/`len` must come from `fold_alloc` or from a reply this module
        /// returned, and must not be used afterwards.
        #[cfg_attr(target_arch = "wasm32", unsafe(no_mangle))]
        #[allow(dead_code)]
        pub unsafe extern "C" fn fold_free(ptr: i32, len: i32) {
            if ptr != 0 && len > 0 {
                // SAFETY: by the caller's contract this is a leaked Vec/Box of
                // exactly `len` bytes.
                drop(unsafe { Vec::from_raw_parts(ptr as *mut u8, len as usize, len as usize) });
            }
        }
    };
}

/// Exports a projection step under `$name`.
///
/// The body is `Fn(&Ctx, &Event) -> Result<Vec<Mutation>, String>`.
#[macro_export]
macro_rules! projection {
    ($name:ident = $body:expr) => {
        /// # Safety
        /// Called by the fold host with a buffer from `fold_alloc`.
        #[cfg_attr(target_arch = "wasm32", unsafe(no_mangle))]
        #[allow(dead_code)]
        pub unsafe extern "C" fn $name(ptr: i32, len: i32) -> i64 {
            let input = unsafe { $crate::__rt::take_input(ptr, len) };
            $crate::__rt::run(
                input,
                |i: $crate::abi::ProjectionInput| {
                    let f: &dyn Fn(
                        &$crate::Ctx,
                        &$crate::Event,
                    ) -> Result<Vec<$crate::Mutation>, String> = &$body;
                    let cx = $crate::Ctx::new(i.projection);
                    match f(&cx, &i.event) {
                        Ok(mutations) => $crate::abi::ProjectionOutput::Ok { mutations },
                        Err(error) => $crate::abi::ProjectionOutput::Err { error },
                    }
                },
                |error| $crate::abi::ProjectionOutput::Err { error },
            )
        }
    };
}

/// Exports an aggregate evolve function under `$name`.
///
/// The body is `Fn(Option<Value>, &Event) -> Result<Value, String>`; the state
/// is `None` for the first event of a stream.
#[macro_export]
macro_rules! aggregate {
    ($name:ident = $body:expr) => {
        /// # Safety
        /// Called by the fold host with a buffer from `fold_alloc`.
        #[cfg_attr(target_arch = "wasm32", unsafe(no_mangle))]
        #[allow(dead_code)]
        pub unsafe extern "C" fn $name(ptr: i32, len: i32) -> i64 {
            let input = unsafe { $crate::__rt::take_input(ptr, len) };
            $crate::__rt::run(
                input,
                |i: $crate::abi::EvolveInput| {
                    let f: &dyn Fn(
                        Option<$crate::Value>,
                        &$crate::Event,
                    ) -> Result<$crate::Value, String> = &$body;
                    match f(i.state, &i.event) {
                        Ok(state) => $crate::abi::EvolveOutput::Ok { state },
                        Err(error) => $crate::abi::EvolveOutput::Err { error },
                    }
                },
                |error| $crate::abi::EvolveOutput::Err { error },
            )
        }
    };
}

/// Exports an invariant check under `$name`.
///
/// The body is `Fn(&InvCtx, &Ctx, &Value, &[PendingEvent]) -> Result<(), Fail>`:
/// the invariant context, a row reader (usable only for context invariants,
/// over the projection they name), the candidate state, and the events about
/// to be appended. `Err(Rejected)` is a violation and rejects the command;
/// `Err(Error)` is a defect.
#[macro_export]
macro_rules! invariant {
    ($name:ident = $body:expr) => {
        /// # Safety
        /// Called by the fold host with a buffer from `fold_alloc`.
        #[cfg_attr(target_arch = "wasm32", unsafe(no_mangle))]
        #[allow(dead_code)]
        pub unsafe extern "C" fn $name(ptr: i32, len: i32) -> i64 {
            let input = unsafe { $crate::__rt::take_input(ptr, len) };
            $crate::__rt::run(
                input,
                |i: $crate::abi::CheckInput| {
                    let f: &dyn Fn(
                        &$crate::InvCtx,
                        &$crate::Ctx,
                        &$crate::Value,
                        &[$crate::PendingEvent],
                    ) -> Result<(), $crate::Fail> = &$body;
                    let rows = $crate::Ctx::new(i.ctx.projection.clone().unwrap_or_default());
                    match f(&i.ctx, &rows, &i.state, &i.events) {
                        Ok(()) => $crate::abi::CheckOutput::Ok { ok: true },
                        Err($crate::Fail::Rejected(violation)) => {
                            $crate::abi::CheckOutput::Violation { violation }
                        }
                        Err($crate::Fail::Error(error)) => $crate::abi::CheckOutput::Err { error },
                    }
                },
                |error| $crate::abi::CheckOutput::Err { error },
            )
        }
    };
}

/// Exports a process manager's reaction under `$name`.
///
/// The body is `Fn(&ProcCtx, Option<Value>, &Trigger) -> Result<Reaction, String>`:
/// the instance's state (if any), what woke it, and back the state to keep
/// (`None` ends the instance) plus the commands to issue.
#[macro_export]
macro_rules! process {
    ($name:ident = $body:expr) => {
        /// # Safety
        /// Called by the fold host with a buffer from `fold_alloc`.
        #[cfg_attr(target_arch = "wasm32", unsafe(no_mangle))]
        #[allow(dead_code)]
        pub unsafe extern "C" fn $name(ptr: i32, len: i32) -> i64 {
            let input = unsafe { $crate::__rt::take_input(ptr, len) };
            $crate::__rt::run(
                input,
                |i: $crate::abi::ProcessInput| {
                    let f: &dyn Fn(
                        &$crate::ProcCtx,
                        Option<$crate::Value>,
                        &$crate::Trigger,
                    ) -> Result<$crate::Reaction, String> = &$body;
                    match f(&i.ctx, i.state, &i.trigger) {
                        Ok(reaction) => $crate::abi::ProcessOutput::Ok(reaction),
                        Err(error) => $crate::abi::ProcessOutput::Err { error },
                    }
                },
                |error| $crate::abi::ProcessOutput::Err { error },
            )
        }
    };
}

/// Exports a command handler under `$name`.
///
/// The body is `Fn(&CmdCtx, Option<Value>, &Command) -> Result<Vec<Emit>, Fail>`;
/// the context carries the aggregate key, the stream version and the clock.
#[macro_export]
macro_rules! command {
    ($name:ident = $body:expr) => {
        /// # Safety
        /// Called by the fold host with a buffer from `fold_alloc`.
        #[cfg_attr(target_arch = "wasm32", unsafe(no_mangle))]
        #[allow(dead_code)]
        pub unsafe extern "C" fn $name(ptr: i32, len: i32) -> i64 {
            let input = unsafe { $crate::__rt::take_input(ptr, len) };
            $crate::__rt::run(
                input,
                |i: $crate::abi::CommandInput| {
                    let f: &dyn Fn(
                        &$crate::CmdCtx,
                        Option<$crate::Value>,
                        &$crate::Command,
                    ) -> Result<Vec<$crate::Emit>, $crate::Fail> = &$body;
                    let (cx, state, command) = i.into_parts();
                    match f(&cx, state, &command) {
                        Ok(events) => $crate::abi::CommandOutput::Ok { events },
                        Err($crate::Fail::Rejected(rejected)) => {
                            $crate::abi::CommandOutput::Rejected { rejected }
                        }
                        Err($crate::Fail::Error(error)) => {
                            $crate::abi::CommandOutput::Err { error }
                        }
                    }
                },
                |error| $crate::abi::CommandOutput::Err { error },
            )
        }
    };
}
