/// Resource limits applied to every guest call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Fuel per call. One unit is roughly one WebAssembly instruction.
    pub fuel: u64,
    /// Maximum linear memory a guest instance may grow to, in bytes.
    pub memory_bytes: usize,
    /// Wall-clock budget per call, in epoch ticks of [`Engine::TICK`](crate::Engine::TICK).
    pub epoch_ticks: u64,
    /// Largest reply the host will read back.
    pub max_output_bytes: usize,
    /// Largest document the host will hand to a guest.
    pub max_input_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            fuel: 50_000_000,
            memory_bytes: 64 * 1024 * 1024,
            epoch_ticks: 100,
            max_output_bytes: 4 * 1024 * 1024,
            max_input_bytes: 16 * 1024 * 1024,
        }
    }
}
