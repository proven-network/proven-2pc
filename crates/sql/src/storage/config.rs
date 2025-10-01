//! Storage configuration

use fjall::{CompressionType, PersistMode};
use std::path::PathBuf;
use std::time::Duration;

/// Storage engine configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Base directory for storage files
    pub data_dir: PathBuf,

    /// Fjall block cache size in bytes
    pub block_cache_size: u64,

    /// Compression type for data
    pub compression: CompressionType,

    /// Persistence mode for commits
    pub persist_mode: PersistMode,

    /// Duration of each history time bucket (default: 60 seconds)
    pub history_bucket_duration: Duration,

    /// Duration of each uncommitted data time bucket (default: 30 seconds)
    pub uncommitted_bucket_duration: Duration,

    /// Retention window for history data (default: 5 minutes)
    pub history_retention_window: Duration,

    /// Retention window for uncommitted data (default: 2 minutes)
    /// This should be longer than the longest expected transaction duration
    pub uncommitted_retention_window: Duration,

    /// How often to run cleanup (default: 30 seconds)
    /// Should be much smaller than retention windows to avoid accumulation
    pub cleanup_interval: Duration,
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
            block_cache_size: 1024 * 1024 * 1024, // 1 GB for testing
            compression: CompressionType::Lz4,
            persist_mode: PersistMode::Buffer,
            history_bucket_duration: Duration::from_secs(60),
            uncommitted_bucket_duration: Duration::from_secs(30),
            history_retention_window: Duration::from_secs(300),
            uncommitted_retention_window: Duration::from_secs(120), // 2 minutes
            cleanup_interval: Duration::from_secs(30),
        }
    }
}

impl StorageConfig {
    /// Create config optimized for testing
    pub fn for_testing() -> Self {
        // Use tempfile to create a proper temporary directory
        // Using .keep() to persist the directory (won't be auto-deleted)
        let temp_dir = tempfile::tempdir()
            .expect("Failed to create temporary directory")
            .keep();

        Self {
            data_dir: temp_dir,
            block_cache_size: 64 * 1024 * 1024, // 64 MB
            compression: CompressionType::None, // Faster for tests
            persist_mode: PersistMode::Buffer,  // Don't sync to disk in tests
            history_bucket_duration: Duration::from_secs(60),
            uncommitted_bucket_duration: Duration::from_secs(30),
            history_retention_window: Duration::from_secs(300),
            uncommitted_retention_window: Duration::from_secs(120), // 2 minutes
            cleanup_interval: Duration::from_secs(30),
        }
    }
}
