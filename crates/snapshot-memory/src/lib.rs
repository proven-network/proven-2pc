//! In-memory snapshot storage implementation

use proven_snapshot::SnapshotStore;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// In-memory snapshot store for testing
pub struct MemorySnapshotStore {
    // stream -> offset
    offsets: Arc<RwLock<HashMap<String, u64>>>,
}

impl MemorySnapshotStore {
    pub fn new() -> Self {
        Self {
            offsets: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl SnapshotStore for MemorySnapshotStore {
    fn update(&self, stream: &str, offset: u64) {
        if let Ok(mut offsets) = self.offsets.write() {
            offsets.insert(stream.to_string(), offset);
        }
    }

    fn get(&self, stream: &str) -> Option<u64> {
        self.offsets.read().ok()?.get(stream).copied()
    }
}

impl Default for MemorySnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}
