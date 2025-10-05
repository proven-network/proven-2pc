//! Storage configuration

use std::path::PathBuf;
use std::time::Duration;

/// Configuration for MVCC storage
#[derive(Clone)]
pub struct StorageConfig {
    /// Directory for storage data
    pub data_dir: PathBuf,

    /// Block cache size for Fjall (in bytes)
    pub block_cache_size: u64,

    /// Compression type for data
    pub compression: fjall::CompressionType,

    /// How long to retain uncommitted data (for crash recovery)
    /// Default: 5 minutes
    pub uncommitted_retention_window: Duration,

    /// How long to retain committed history (for time-travel)
    /// Default: 5 minutes
    pub history_retention_window: Duration,

    /// Duration of each time bucket for uncommitted data
    /// Default: 1 minute
    pub uncommitted_bucket_duration: Duration,

    /// Duration of each time bucket for history
    /// Default: 1 minute
    pub history_bucket_duration: Duration,

    /// How often to run cleanup (drop old buckets)
    /// Default: 30 seconds
    pub cleanup_interval: Duration,

    /// Persist mode for Fjall
    pub persist_mode: fjall::PersistMode,
}

impl Default for StorageConfig {
    fn default() -> Self {
        // Use tempfile to create a proper temporary directory
        // Using .keep() to persist the directory (won't be auto-deleted)
        let temp_dir = tempfile::tempdir()
            .expect("Failed to create temporary directory")
            .keep();

        Self {
            data_dir: temp_dir,
            block_cache_size: 64 * 1024 * 1024, // 64 MB
            compression: fjall::CompressionType::Lz4,
            uncommitted_retention_window: Duration::from_secs(5 * 60), // 5 minutes
            history_retention_window: Duration::from_secs(5 * 60),     // 5 minutes
            uncommitted_bucket_duration: Duration::from_secs(60),      // 1 minute
            history_bucket_duration: Duration::from_secs(60),          // 1 minute
            cleanup_interval: Duration::from_secs(30),                 // 30 seconds
            persist_mode: fjall::PersistMode::Buffer,
        }
    }
}

impl StorageConfig {
    /// Create a new config with the given data directory
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            ..Default::default()
        }
    }

    /// Set block cache size
    pub fn with_block_cache_size(mut self, size: u64) -> Self {
        self.block_cache_size = size;
        self
    }

    /// Set compression type
    pub fn with_compression(mut self, compression: fjall::CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Set retention windows
    pub fn with_retention_windows(mut self, uncommitted: Duration, history: Duration) -> Self {
        self.uncommitted_retention_window = uncommitted;
        self.history_retention_window = history;
        self
    }

    /// Set bucket durations
    pub fn with_bucket_durations(mut self, uncommitted: Duration, history: Duration) -> Self {
        self.uncommitted_bucket_duration = uncommitted;
        self.history_bucket_duration = history;
        self
    }

    /// Set cleanup interval
    pub fn with_cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }

    /// Set persist mode
    pub fn with_persist_mode(mut self, mode: fjall::PersistMode) -> Self {
        self.persist_mode = mode;
        self
    }
}
