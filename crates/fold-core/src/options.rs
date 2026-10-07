/// Whether the log fdatasyncs segment writes and commits the index durably.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FsyncPolicy {
    /// `fdatasync` every batch and commit redb with `Durability::Immediate`.
    /// An acknowledged append survives a power loss.
    #[default]
    Always,
    /// No fdatasync; redb commits with `Durability::None`. An acknowledged
    /// append survives a process crash but not a power loss. `Log::flush`
    /// (also run on drop) makes everything durable.
    Never,
}

/// Options for `Log::create`, `Log::open` and `Log::open_or_create`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenOptions {
    /// Roll to a new segment after a batch once the current one is at least
    /// this large. Default 256 MiB. Tests use ~1 KiB.
    pub segment_max_bytes: u64,
    /// Largest encoded record (framing + body) accepted. Default 16 MiB.
    pub max_record_bytes: usize,
    pub fsync: FsyncPolicy,
    /// On open, CRC-check every segment instead of only the last one.
    pub verify_all_segments: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        OpenOptions {
            segment_max_bytes: 256 * 1024 * 1024,
            max_record_bytes: 16 * 1024 * 1024,
            fsync: FsyncPolicy::Always,
            verify_all_segments: false,
        }
    }
}

impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn segment_max_bytes(mut self, bytes: u64) -> Self {
        self.segment_max_bytes = bytes;
        self
    }

    pub fn max_record_bytes(mut self, bytes: usize) -> Self {
        self.max_record_bytes = bytes;
        self
    }

    pub fn fsync(mut self, policy: FsyncPolicy) -> Self {
        self.fsync = policy;
        self
    }

    pub fn verify_all_segments(mut self, verify: bool) -> Self {
        self.verify_all_segments = verify;
        self
    }
}
