use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::WasmError;
use crate::limits::Limits;

/// A compiled-code engine shared by every module in a process.
///
/// Owns the epoch ticker thread that bounds guest wall-clock time; the
/// thread stops when the last clone is dropped.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
}

struct Inner {
    engine: wasmtime::Engine,
    stop: Arc<AtomicBool>,
    ticker: Option<JoinHandle<()>>,
}

impl Engine {
    /// How often the epoch advances; `Limits::epoch_ticks` is counted in these.
    pub const TICK: Duration = Duration::from_millis(10);

    pub fn new() -> Result<Self, WasmError> {
        let mut config = wasmtime::Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        config.cranelift_opt_level(wasmtime::OptLevel::Speed);
        config.max_wasm_stack(1024 * 1024);
        let engine = wasmtime::Engine::new(&config).map_err(|e| WasmError::Compile {
            path: "<engine>".into(),
            reason: e.to_string(),
        })?;

        let stop = Arc::new(AtomicBool::new(false));
        let ticker = {
            let engine = engine.clone();
            let stop = stop.clone();
            std::thread::Builder::new()
                .name("fold-wasm-epoch".into())
                .spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(Self::TICK);
                        engine.increment_epoch();
                    }
                })
                .map_err(|e| WasmError::Io {
                    path: "<epoch ticker>".into(),
                    source: e,
                })?
        };

        Ok(Engine {
            inner: Arc::new(Inner {
                engine,
                stop,
                ticker: Some(ticker),
            }),
        })
    }

    pub(crate) fn raw(&self) -> &wasmtime::Engine {
        &self.inner.engine
    }

    /// Limits the engine applies by default; callers may pass their own per guest.
    pub fn default_limits(&self) -> Limits {
        Limits::default()
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.ticker.take() {
            let _ = t.join();
        }
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Engine")
    }
}
