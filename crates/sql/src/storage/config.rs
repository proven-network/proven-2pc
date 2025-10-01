//! Storage configuration

use fjall::{CompressionType, PersistMode};
use std::path::PathBuf;

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
        }
    }
}
