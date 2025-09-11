//! Filesystem-based snapshot storage implementation

use proven_hlc::HlcTimestamp;
use proven_snapshot::{SnapshotMetadata, SnapshotStore};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

/// Filesystem-based snapshot store
pub struct FileSnapshotStore {
    /// Base directory for storing snapshots
    base_path: PathBuf,
    /// Cache of metadata for quick access
    metadata_cache: RwLock<Vec<SnapshotMetadata>>,
}

impl FileSnapshotStore {
    /// Create a new file-based snapshot store
    pub fn new(base_path: impl AsRef<Path>) -> Result<Self, String> {
        let base_path = base_path.as_ref().to_path_buf();

        // Create the base directory if it doesn't exist
        fs::create_dir_all(&base_path)
            .map_err(|e| format!("Failed to create snapshot directory: {}", e))?;

        Ok(Self {
            base_path,
            metadata_cache: RwLock::new(Vec::new()),
        })
    }

    /// Get the path for a stream's snapshot directory
    fn stream_dir(&self, stream: &str) -> PathBuf {
        self.base_path.join(stream)
    }

    /// Get the path for a specific snapshot
    fn snapshot_path(&self, stream: &str, offset: u64) -> PathBuf {
        self.stream_dir(stream)
            .join(format!("snapshot_{:020}.bin", offset))
    }

    /// Get the metadata path for a specific snapshot
    fn metadata_path(&self, stream: &str, offset: u64) -> PathBuf {
        self.stream_dir(stream)
            .join(format!("snapshot_{:020}.meta", offset))
    }

    /// Load metadata for a stream
    fn load_metadata(&self, stream: &str) -> Vec<SnapshotMetadata> {
        let stream_dir = self.stream_dir(stream);
        if !stream_dir.exists() {
            return Vec::new();
        }

        let mut metadata = Vec::new();

        if let Ok(entries) = fs::read_dir(&stream_dir) {
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("meta")
                    && let Ok(contents) = fs::read_to_string(&path)
                    && let Ok(meta) = serde_json::from_str::<SnapshotMetadata>(&contents)
                {
                    metadata.push(meta);
                }
            }
        }

        // Sort by offset (newest first)
        metadata.sort_by(|a, b| b.log_offset.cmp(&a.log_offset));
        metadata
    }
}

impl SnapshotStore for FileSnapshotStore {
    fn save_snapshot(
        &self,
        stream: &str,
        offset: u64,
        timestamp: HlcTimestamp,
        data: Vec<u8>,
    ) -> Result<(), String> {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Create stream directory if needed
        let stream_dir = self.stream_dir(stream);
        fs::create_dir_all(&stream_dir)
            .map_err(|e| format!("Failed to create stream directory: {}", e))?;

        let size_bytes = data.len() as u64;

        // Calculate SHA256 checksum
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let checksum_vec = hasher.finalize();
        let mut checksum = [0u8; 32];
        checksum.copy_from_slice(&checksum_vec);

        let metadata = SnapshotMetadata {
            stream: stream.to_string(),
            log_offset: offset,
            log_timestamp: timestamp,
            size_bytes,
            checksum,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_millis() as u64,
        };

        // Write metadata
        let metadata_path = self.metadata_path(stream, offset);
        let metadata_json = serde_json::to_string(&metadata)
            .map_err(|e| format!("Failed to serialize metadata: {}", e))?;
        fs::write(&metadata_path, metadata_json)
            .map_err(|e| format!("Failed to write metadata: {}", e))?;

        // Write snapshot data
        let snapshot_path = self.snapshot_path(stream, offset);
        fs::write(&snapshot_path, data).map_err(|e| format!("Failed to write snapshot: {}", e))?;

        // Update cache
        if let Ok(mut cache) = self.metadata_cache.write() {
            cache.clear(); // Clear cache to force reload
        }

        Ok(())
    }

    fn get_latest_snapshot(&self, stream: &str) -> Option<(SnapshotMetadata, Vec<u8>)> {
        let metadata_list = self.load_metadata(stream);

        // Get the latest (first in sorted list)
        let metadata = metadata_list.into_iter().next()?;

        // Read the snapshot data
        let snapshot_path = self.snapshot_path(stream, metadata.log_offset);
        let data = fs::read(&snapshot_path).ok()?;

        // Verify checksum
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let checksum_vec = hasher.finalize();

        if checksum_vec.as_slice() != metadata.checksum {
            tracing::warn!("Snapshot checksum mismatch for stream {}", stream);
            return None;
        }

        Some((metadata, data))
    }

    fn list_snapshots(&self, stream: &str) -> Vec<SnapshotMetadata> {
        self.load_metadata(stream)
    }

    fn cleanup_old_snapshots(&self, stream: &str, keep_count: usize) -> Result<(), String> {
        if keep_count == 0 {
            return Err("keep_count must be at least 1".to_string());
        }

        let metadata_list = self.load_metadata(stream);

        if metadata_list.len() > keep_count {
            // Delete old snapshots
            for metadata in metadata_list.iter().skip(keep_count) {
                let snapshot_path = self.snapshot_path(stream, metadata.log_offset);
                let metadata_path = self.metadata_path(stream, metadata.log_offset);

                // Delete files (ignore errors for missing files)
                let _ = fs::remove_file(&snapshot_path);
                let _ = fs::remove_file(&metadata_path);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_file_snapshot_store() {
        // Create a temporary directory for testing
        let temp_dir = env::temp_dir().join(format!("snapshot_test_{}", uuid::Uuid::new_v4()));
        let store = FileSnapshotStore::new(&temp_dir).unwrap();

        let stream = "test-stream";
        let timestamp = HlcTimestamp::new(1000, 0, proven_hlc::NodeId::new(1));

        // Save a snapshot
        let data = vec![1, 2, 3, 4, 5];
        store
            .save_snapshot(stream, 100, timestamp, data.clone())
            .unwrap();

        // Retrieve it
        let (metadata, retrieved_data) = store.get_latest_snapshot(stream).unwrap();
        assert_eq!(metadata.log_offset, 100);
        assert_eq!(retrieved_data, data);

        // List snapshots
        let snapshots = store.list_snapshots(stream);
        assert_eq!(snapshots.len(), 1);

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }
}
