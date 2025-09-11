//! In-memory snapshot storage implementation

use proven_hlc::HlcTimestamp;
use proven_snapshot::{SnapshotMetadata, SnapshotStore};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

/// Type alias for snapshot data storage
type SnapshotData = (SnapshotMetadata, Vec<u8>);

/// Type alias for stream snapshots indexed by offset
type StreamSnapshots = BTreeMap<u64, SnapshotData>;

/// In-memory snapshot store for testing
pub struct MemorySnapshotStore {
    // stream -> (offset -> (metadata, data))
    snapshots: Arc<RwLock<BTreeMap<String, StreamSnapshots>>>,
}

impl MemorySnapshotStore {
    pub fn new() -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl SnapshotStore for MemorySnapshotStore {
    fn save_snapshot(
        &self,
        stream: &str,
        offset: u64,
        timestamp: HlcTimestamp,
        data: Vec<u8>,
    ) -> Result<(), String> {
        use std::time::{SystemTime, UNIX_EPOCH};

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

        let mut snapshots = self.snapshots.write().map_err(|e| e.to_string())?;
        snapshots
            .entry(stream.to_string())
            .or_insert_with(BTreeMap::new)
            .insert(offset, (metadata, data));

        Ok(())
    }

    fn get_latest_snapshot(&self, stream: &str) -> Option<(SnapshotMetadata, Vec<u8>)> {
        let snapshots = self.snapshots.read().ok()?;
        let stream_snapshots = snapshots.get(stream)?;

        // BTreeMap keeps keys sorted, so last entry has highest offset
        stream_snapshots
            .iter()
            .last()
            .and_then(|(_, (metadata, data))| {
                // Verify checksum
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(data);
                let checksum_vec = hasher.finalize();

                if checksum_vec.as_slice() == metadata.checksum {
                    Some((metadata.clone(), data.clone()))
                } else {
                    tracing::warn!("Snapshot checksum mismatch for stream {}", stream);
                    None
                }
            })
    }

    fn list_snapshots(&self, stream: &str) -> Vec<SnapshotMetadata> {
        let snapshots = self.snapshots.read().unwrap_or_else(|e| e.into_inner());

        snapshots
            .get(stream)
            .map(|stream_snapshots| {
                stream_snapshots
                    .iter()
                    .rev() // Reverse to get newest first
                    .map(|(_, (metadata, _))| metadata.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn cleanup_old_snapshots(&self, stream: &str, keep_count: usize) -> Result<(), String> {
        if keep_count == 0 {
            return Err("keep_count must be at least 1".to_string());
        }

        let mut snapshots = self.snapshots.write().map_err(|e| e.to_string())?;

        if let Some(stream_snapshots) = snapshots.get_mut(stream) {
            let total = stream_snapshots.len();
            if total > keep_count {
                // Collect offsets to remove (oldest ones)
                let to_remove: Vec<u64> = stream_snapshots
                    .keys()
                    .take(total - keep_count)
                    .cloned()
                    .collect();

                for offset in to_remove {
                    stream_snapshots.remove(&offset);
                }
            }
        }

        Ok(())
    }
}

impl Default for MemorySnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}
