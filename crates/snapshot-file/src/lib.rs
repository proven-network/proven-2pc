//! Filesystem-based snapshot storage implementation

use proven_snapshot::SnapshotStore;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

/// Filesystem-based snapshot store
pub struct FileSnapshotStore {
    /// Base directory for storing snapshots
    base_path: PathBuf,
    /// In-memory cache of offsets
    offsets: RwLock<HashMap<String, u64>>,
}

impl FileSnapshotStore {
    /// Create a new file-based snapshot store
    pub fn new(base_path: impl AsRef<Path>) -> Result<Self, String> {
        let base_path = base_path.as_ref().to_path_buf();

        // Create the base directory if it doesn't exist
        fs::create_dir_all(&base_path)
            .map_err(|e| format!("Failed to create snapshot directory: {}", e))?;

        // Load existing offsets from disk
        let offsets = Self::load_offsets_from_disk(&base_path)?;

        Ok(Self {
            base_path,
            offsets: RwLock::new(offsets),
        })
    }

    /// Get the path for a stream's offset file
    fn offset_file(&self, stream: &str) -> PathBuf {
        self.base_path.join(format!("{}.offset", stream))
    }

    /// Load all offsets from disk
    fn load_offsets_from_disk(base_path: &Path) -> Result<HashMap<String, u64>, String> {
        let mut offsets = HashMap::new();

        if let Ok(entries) = fs::read_dir(base_path) {
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("offset")
                    && let Some(stream) = path.file_stem().and_then(|s| s.to_str())
                    && let Ok(contents) = fs::read_to_string(&path)
                    && let Ok(offset) = contents.trim().parse::<u64>()
                {
                    offsets.insert(stream.to_string(), offset);
                }
            }
        }

        Ok(offsets)
    }
}

impl SnapshotStore for FileSnapshotStore {
    fn update(&self, stream: &str, offset: u64) {
        // Update in-memory cache
        if let Ok(mut offsets) = self.offsets.write() {
            offsets.insert(stream.to_string(), offset);
        }

        // Write to disk
        let offset_file = self.offset_file(stream);
        let _ = fs::write(&offset_file, offset.to_string());
    }

    fn get(&self, stream: &str) -> Option<u64> {
        self.offsets.read().ok()?.get(stream).copied()
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

        // Initially no offset
        assert_eq!(store.get(stream), None);

        // Update offset
        store.update(stream, 100);
        assert_eq!(store.get(stream), Some(100));

        // Update to higher offset
        store.update(stream, 200);
        assert_eq!(store.get(stream), Some(200));

        // Verify persistence - create a new store instance
        let store2 = FileSnapshotStore::new(&temp_dir).unwrap();
        assert_eq!(store2.get(stream), Some(200));

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }
}
