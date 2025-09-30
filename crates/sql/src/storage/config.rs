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

    /// Fjall write buffer size in bytes
    pub write_buffer_size: usize,

    /// Compression type for data
    pub compression: CompressionType,

    /// Size of in-memory cache for recent commits
    pub memory_cache_size: usize,

    /// Interval for cleaning up old versions
    pub cleanup_interval: Duration,

    /// Persistence mode for commits
    pub persist_mode: PersistMode,

    /// Enable background compaction
    pub enable_compaction: bool,
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
            write_buffer_size: 64 * 1024 * 1024,  // 64 MB
            compression: CompressionType::Lz4,
            memory_cache_size: 128 * 1024 * 1024, // 128 MB
            cleanup_interval: Duration::from_secs(10),
            persist_mode: PersistMode::Buffer,
            enable_compaction: true,
        }
    }
}

impl StorageConfig {
    /// Create config optimized for testing
    pub fn for_testing() -> Self {
        Self {
            data_dir: PathBuf::from("/tmp/test_sql_storage"),
            block_cache_size: 64 * 1024 * 1024,  // 64 MB
            write_buffer_size: 16 * 1024 * 1024, // 16 MB
            compression: CompressionType::None,  // Faster for tests
            memory_cache_size: 32 * 1024 * 1024, // 32 MB
            cleanup_interval: Duration::from_secs(1),
            persist_mode: PersistMode::Buffer, // Don't sync to disk in tests
            enable_compaction: false,          // Disable for tests
        }
    }

    /// Create config optimized for production OLTP workloads
    pub fn for_oltp() -> Self {
        Self {
            data_dir: PathBuf::from("./data/sql"),
            block_cache_size: 20 * 1024 * 1024 * 1024, // 20 GB
            write_buffer_size: 512 * 1024 * 1024,      // 512 MB
            compression: CompressionType::Lz4,
            memory_cache_size: 2 * 1024 * 1024 * 1024, // 2 GB
            cleanup_interval: Duration::from_secs(10),
            persist_mode: PersistMode::SyncData,
            enable_compaction: true,
        }
    }

    /// Create config optimized for analytical workloads
    pub fn for_olap() -> Self {
        Self {
            data_dir: PathBuf::from("./data/sql"),
            block_cache_size: 30 * 1024 * 1024 * 1024, // 30 GB
            write_buffer_size: 1024 * 1024 * 1024,     // 1 GB
            compression: CompressionType::Lz4,         // Better compression
            memory_cache_size: 4 * 1024 * 1024 * 1024, // 4 GB
            cleanup_interval: Duration::from_secs(30),
            persist_mode: PersistMode::SyncAll,
            enable_compaction: true,
        }
    }
}
