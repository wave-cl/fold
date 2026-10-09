use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use wasmtime::{
    Caller, Extern, InstancePre, Linker, Memory, Store, StoreLimits, StoreLimitsBuilder, Trap,
    TypedFunc,
};

use fold_guest::Mutation;
use fold_guest::abi::{
    CheckInput, CheckOutput, CommandInput, CommandOutput, Emit, EvolveInput, EvolveOutput,
    ProcessInput, ProcessOutput, ProjectionInput, ProjectionOutput, Reaction, Rejected,
    UpcastInput, UpcastOutput,
};
use serde_json::Value;

/// What an invariant check decided.
#[derive(Debug, Clone, PartialEq)]
pub enum CheckReply {
    Ok,
    /// The invariant does not hold for the candidate state.
    Violation(Rejected),
}

/// What a command handler decided.
#[derive(Debug, Clone, PartialEq)]
pub enum CommandReply {
    /// Append these events (possibly none).
    Events(Vec<Emit>),
    /// A business rule refused the command.
    Rejected(Rejected),
}

use crate::cache::LoadedModule;
use crate::engine::Engine;
use crate::error::WasmError;
use crate::limits::Limits;

/// How a projection step reads its own tables through `fold.get_row`.
///
/// Implemented by the daemon over a read-model snapshot taken before the
/// batch; `table` is the bare table name and `key` the JSON key object.
pub trait RowReader: Send + Sync {
    fn get_row(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>, String>;
}

/// A reader for guests that must not read anything (evolve, command).
struct NoRows;

impl RowReader for NoRows {
    fn get_row(&self, table: &str, _key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Err(format!(
            "this entry point may not read rows (asked for table {table})"
        ))
    }
}

struct HostState {
    limits: StoreLimits,
    rows: Arc<dyn RowReader>,
    module: String,
    /// Set by a host import when it hits a reader failure, so the error
    /// survives the trap boundary with its message intact.
    row_error: Option<String>,
}

/// One compiled module, ready to instantiate per call.
///
/// Instantiation from an [`InstancePre`] costs microseconds, and a fresh
/// store per call means no guest state leaks between events.
pub struct Guest {
    pre: InstancePre<HostState>,
    engine: Engine,
    limits: Limits,
    name: String,
    hash: [u8; 32],
}

impl Guest {
    /// Links `module` against the fold host imports and checks its ABI
    /// exports and version. Any import outside the `fold` namespace, or any
    /// `fold` import the host does not provide, is refused here.
    pub fn new(engine: &Engine, module: &LoadedModule, limits: Limits) -> Result<Self, WasmError> {
        for import in module.module.imports() {
            let provided = import.module() == "fold" && matches!(import.name(), "get_row" | "log");
            if !provided {
                return Err(WasmError::UnsupportedImport {
                    module: import.module().to_string(),
                    name: import.name().to_string(),
                });
            }
        }
        for (name, want_memory) in [
            ("memory", true),
            ("fold_abi_version", false),
            ("fold_alloc", false),
            ("fold_free", false),
        ] {
            let ok = match module.module.get_export(name) {
                Some(wasmtime::ExternType::Memory(_)) => want_memory,
                Some(wasmtime::ExternType::Func(_)) => !want_memory,
                _ => false,
            };
            if !ok {
                return Err(WasmError::MissingExport(name.to_string()));
            }
        }

        let mut linker: Linker<HostState> = Linker::new(engine.raw());
        linker
            .func_wrap("fold", "log", host_log)
            .and_then(|l| l.func_wrap("fold", "get_row", host_get_row))
            .map_err(|e| WasmError::Compile {
                path: module.path.clone(),
                reason: e.to_string(),
            })?;
        let pre = linker
            .instantiate_pre(&module.module)
            .map_err(|e| WasmError::Compile {
                path: module.path.clone(),
                reason: e.to_string(),
            })?;

        let guest = Guest {
            pre,
            engine: engine.clone(),
            limits,
            name: module.path.display().to_string(),
            hash: module.hash,
        };
        // Check the ABI version once, with a real instance.
        let (mut store, instance) = guest.instantiate(Arc::new(NoRows))?;
        let version = instance
            .get_typed_func::<(), i32>(&mut store, "fold_abi_version")
            .map_err(|_| WasmError::MissingExport("fold_abi_version".into()))?
            .call(&mut store, ())
            .map_err(|e| guest.map_trap(e, &store))?;
        if version != crate::ABI_VERSION {
            return Err(WasmError::BadAbiVersion(version));
        }
        Ok(guest)
    }

    /// SHA-256 of the module bytes this guest was built from.
    pub fn hash(&self) -> [u8; 32] {
        self.hash
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// True when the module exports `name` as an `(i32, i32) -> i64` function.
    pub fn has_export(&self, name: &str) -> bool {
        matches!(
            self.pre.module().get_export(name),
            Some(wasmtime::ExternType::Func(f))
                if f.params().map(|t| t.is_i32()).eq([true, true])
                    && f.results().map(|t| t.is_i64()).eq([true])
        )
    }

    /// Runs a projection step export and returns its mutations.
    pub fn apply(
        &self,
        export: &str,
        input: &ProjectionInput,
        rows: Arc<dyn RowReader>,
    ) -> Result<Vec<Mutation>, WasmError> {
        match self.call(export, input, rows)? {
            ProjectionOutput::Ok { mutations } => Ok(mutations),
            ProjectionOutput::Err { error } => Err(WasmError::GuestError(error)),
        }
    }

    /// Runs an evolve export and returns the new state. Row reads are refused.
    pub fn evolve(&self, export: &str, input: &EvolveInput) -> Result<Value, WasmError> {
        match self.call(export, input, Arc::new(NoRows))? {
            EvolveOutput::Ok { state } => Ok(state),
            EvolveOutput::Err { error } => Err(WasmError::GuestError(error)),
        }
    }

    /// Runs an event upcaster export. Row reads are refused.
    pub fn upcast(&self, export: &str, input: &UpcastInput) -> Result<Value, WasmError> {
        match self.call(export, input, Arc::new(NoRows))? {
            UpcastOutput::Ok { payload } => Ok(payload),
            UpcastOutput::Err { error } => Err(WasmError::GuestError(error)),
        }
    }

    /// Runs a command handler export. Row reads are refused. A business
    /// rejection is a successful reply; only defects are errors.
    pub fn handle(&self, export: &str, input: &CommandInput) -> Result<CommandReply, WasmError> {
        match self.call(export, input, Arc::new(NoRows))? {
            CommandOutput::Ok { events } => Ok(CommandReply::Events(events)),
            CommandOutput::Rejected { rejected } => Ok(CommandReply::Rejected(rejected)),
            CommandOutput::Err { error } => Err(WasmError::GuestError(error)),
        }
    }

    /// Runs a process manager's reaction. Row reads are refused.
    pub fn react(&self, export: &str, input: &ProcessInput) -> Result<Reaction, WasmError> {
        match self.call(export, input, Arc::new(NoRows))? {
            ProcessOutput::Ok(reaction) => Ok(reaction),
            ProcessOutput::Err { error } => Err(WasmError::GuestError(error)),
        }
    }

    /// Runs an invariant check. `rows` is the projection reader for a
    /// context invariant, or a reader that refuses for a state invariant.
    pub fn check(
        &self,
        export: &str,
        input: &CheckInput,
        rows: Arc<dyn RowReader>,
    ) -> Result<CheckReply, WasmError> {
        match self.call(export, input, rows)? {
            CheckOutput::Ok { ok: true } => Ok(CheckReply::Ok),
            CheckOutput::Ok { ok: false } => Err(WasmError::GuestError(
                "invariant check replied ok:false without a violation".into(),
            )),
            CheckOutput::Violation { violation } => Ok(CheckReply::Violation(violation)),
            CheckOutput::Err { error } => Err(WasmError::GuestError(error)),
        }
    }

    /// A reader for guests that must not read rows.
    pub fn no_rows() -> Arc<dyn RowReader> {
        Arc::new(NoRows)
    }

    /// Hands `input` as JSON to `export` and decodes the JSON reply.
    pub fn call<I: Serialize, O: DeserializeOwned>(
        &self,
        export: &str,
        input: &I,
        rows: Arc<dyn RowReader>,
    ) -> Result<O, WasmError> {
        let bytes = self.call_raw(
            export,
            &serde_json::to_vec(input).map_err(WasmError::BadOutput)?,
            rows,
        )?;
        serde_json::from_slice(&bytes).map_err(WasmError::BadOutput)
    }

    /// The untyped form of [`Guest::call`].
    pub fn call_raw(
        &self,
        export: &str,
        input: &[u8],
        rows: Arc<dyn RowReader>,
    ) -> Result<Vec<u8>, WasmError> {
        if input.len() > self.limits.max_input_bytes {
            return Err(WasmError::InputTooLarge {
                len: input.len(),
                max: self.limits.max_input_bytes,
            });
        }
        let span = tracing::debug_span!("fold.wasm.call", module = %self.name, export);
        let _g = span.enter();

        let (mut store, instance) = self.instantiate(rows)?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| WasmError::MissingExport("memory".into()))?;
        let alloc: TypedFunc<i32, i32> = instance
            .get_typed_func(&mut store, "fold_alloc")
            .map_err(|_| WasmError::MissingExport("fold_alloc".into()))?;
        let free: TypedFunc<(i32, i32), ()> = instance
            .get_typed_func(&mut store, "fold_free")
            .map_err(|_| WasmError::MissingExport("fold_free".into()))?;
        let entry: TypedFunc<(i32, i32), i64> = instance
            .get_typed_func(&mut store, export)
            .map_err(|_| WasmError::MissingExport(export.to_string()))?;

        let len = i32::try_from(input.len()).map_err(|_| WasmError::InputTooLarge {
            len: input.len(),
            max: i32::MAX as usize,
        })?;
        let ptr = alloc
            .call(&mut store, len)
            .map_err(|e| self.map_trap(e, &store))?;
        if ptr == 0 && len > 0 {
            return Err(WasmError::AllocFailed);
        }
        memory
            .write(&mut store, ptr as usize, input)
            .map_err(|_| WasmError::BadPointer)?;

        let packed = entry
            .call(&mut store, (ptr, len))
            .map_err(|e| self.map_trap(e, &store))?;
        let out_ptr = (packed >> 32) as u32 as usize;
        let out_len = (packed & 0xffff_ffff) as u32 as usize;
        if out_len > self.limits.max_output_bytes {
            return Err(WasmError::OutputTooLarge {
                len: out_len,
                max: self.limits.max_output_bytes,
            });
        }
        let mut out = vec![0u8; out_len];
        memory
            .read(&store, out_ptr, &mut out)
            .map_err(|_| WasmError::BadPointer)?;
        // Best effort: a guest that cannot free is not an error for the caller.
        let _ = free.call(&mut store, (out_ptr as i32, out_len as i32));
        Ok(out)
    }

    fn instantiate(
        &self,
        rows: Arc<dyn RowReader>,
    ) -> Result<(Store<HostState>, wasmtime::Instance), WasmError> {
        let state = HostState {
            limits: StoreLimitsBuilder::new()
                .memory_size(self.limits.memory_bytes)
                .instances(1)
                .build(),
            rows,
            module: self.name.clone(),
            row_error: None,
        };
        let mut store = Store::new(self.engine.raw(), state);
        store.limiter(|s| &mut s.limits);
        store
            .set_fuel(self.limits.fuel)
            .map_err(|e| WasmError::Trap(e.to_string()))?;
        store.set_epoch_deadline(self.limits.epoch_ticks);
        let instance = self.pre.instantiate(&mut store).map_err(|e| {
            let msg = e.to_string();
            if msg.contains("memory")
                && (msg.contains("limit") || msg.contains("exceed") || msg.contains("minimum"))
            {
                WasmError::MemoryLimit {
                    bytes: self.limits.memory_bytes,
                }
            } else {
                WasmError::Compile {
                    path: self.name.clone().into(),
                    reason: msg,
                }
            }
        })?;
        Ok((store, instance))
    }

    fn map_trap(&self, e: wasmtime::Error, store: &Store<HostState>) -> WasmError {
        if let Some(msg) = &store.data().row_error {
            return WasmError::RowRead(msg.clone());
        }
        match e.downcast_ref::<Trap>() {
            Some(Trap::OutOfFuel) => WasmError::OutOfFuel {
                fuel: self.limits.fuel,
            },
            Some(Trap::Interrupt) => WasmError::Timeout {
                ticks: self.limits.epoch_ticks,
            },
            Some(t) => WasmError::Trap(format!("{t:?}")),
            None => {
                let msg = e.to_string();
                if msg.contains("fuel") {
                    WasmError::OutOfFuel {
                        fuel: self.limits.fuel,
                    }
                } else if msg.contains("epoch") || msg.contains("interrupt") {
                    WasmError::Timeout {
                        ticks: self.limits.epoch_ticks,
                    }
                } else {
                    WasmError::Trap(msg)
                }
            }
        }
    }
}

fn guest_memory(caller: &mut Caller<'_, HostState>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(Extern::Memory(m)) => Some(m),
        _ => None,
    }
}

fn read_guest_bytes(
    caller: &mut Caller<'_, HostState>,
    memory: Memory,
    ptr: i32,
    len: i32,
) -> Option<Vec<u8>> {
    let mut buf = vec![0u8; usize::try_from(len).ok()?];
    memory
        .read(&*caller, usize::try_from(ptr).ok()?, &mut buf)
        .ok()?;
    Some(buf)
}

fn host_log(mut caller: Caller<'_, HostState>, level: i32, ptr: i32, len: i32) {
    let Some(memory) = guest_memory(&mut caller) else {
        return;
    };
    let Some(bytes) = read_guest_bytes(&mut caller, memory, ptr, len) else {
        return;
    };
    let msg = String::from_utf8_lossy(&bytes);
    let module = caller.data().module.as_str();
    match level {
        1 => tracing::error!(target: "fold_wasm::guest", module, "{msg}"),
        2 => tracing::warn!(target: "fold_wasm::guest", module, "{msg}"),
        3 => tracing::info!(target: "fold_wasm::guest", module, "{msg}"),
        _ => tracing::debug!(target: "fold_wasm::guest", module, "{msg}"),
    }
}

/// `fold.get_row`: writes up to `out_cap` bytes of the row into `out_ptr` and
/// returns its full length, or -1 when the row is absent. A reader failure
/// traps the guest; the message is recovered by `map_trap`.
fn host_get_row(
    mut caller: Caller<'_, HostState>,
    table_ptr: i32,
    table_len: i32,
    key_ptr: i32,
    key_len: i32,
    out_ptr: i32,
    out_cap: i32,
) -> wasmtime::Result<i64> {
    let memory =
        guest_memory(&mut caller).ok_or_else(|| wasmtime::Error::msg("no memory export"))?;
    let table = read_guest_bytes(&mut caller, memory, table_ptr, table_len)
        .ok_or_else(|| wasmtime::Error::msg("get_row: table out of bounds"))?;
    let key = read_guest_bytes(&mut caller, memory, key_ptr, key_len)
        .ok_or_else(|| wasmtime::Error::msg("get_row: key out of bounds"))?;
    let table = String::from_utf8(table)
        .map_err(|_| wasmtime::Error::msg("get_row: table is not UTF-8"))?;

    let rows = caller.data().rows.clone();
    match rows.get_row(&table, &key) {
        Err(msg) => {
            caller.data_mut().row_error = Some(msg.clone());
            Err(wasmtime::Error::msg(msg))
        }
        Ok(None) => Ok(-1),
        Ok(Some(row)) => {
            let n = row.len().min(usize::try_from(out_cap).unwrap_or(0));
            if n > 0 {
                memory
                    .write(
                        &mut caller,
                        usize::try_from(out_ptr).unwrap_or(usize::MAX),
                        &row[..n],
                    )
                    .map_err(|_| wasmtime::Error::msg("get_row: output buffer out of bounds"))?;
            }
            Ok(row.len() as i64)
        }
    }
}
