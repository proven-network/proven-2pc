//! Storage configuration

use std::path::PathBuf;

/// Configuration for MVCC storage
#[derive(Clone)]
pub struct StorageConfig {
    /// Directory for storage data
    pub data_dir: PathBuf,

    /// Block cache size for Fjall (in bytes)
    pub block_cache_size: u64,

    /// Compression type for data
    pub compression: fjall::CompressionType,

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

    /// Set persist mode
    pub fn with_persist_mode(mut self, mode: fjall::PersistMode) -> Self {
        self.persist_mode = mode;
        self
    }
}
