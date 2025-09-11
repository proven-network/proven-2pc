//! Snapshot storage trait and types for stream processors

use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};

/// Metadata about a stored snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Stream this snapshot belongs to
    pub stream: String,
    /// Log offset where snapshot was taken
    pub log_offset: u64,
    /// Timestamp of the last log entry in snapshot
    pub log_timestamp: HlcTimestamp,
    /// Size of snapshot data in bytes
    pub size_bytes: u64,
    /// SHA256 checksum of snapshot data
    pub checksum: [u8; 32],
    /// System time when snapshot was created (ms since epoch)
    pub created_at: u64,
}

/// Trait for snapshot storage backends
pub trait SnapshotStore: Send + Sync {
    /// Save a snapshot
    fn save_snapshot(
        &self,
        stream: &str,
        offset: u64,
        timestamp: HlcTimestamp,
        data: Vec<u8>,
    ) -> Result<(), String>;

    /// Get the latest snapshot for a stream
    fn get_latest_snapshot(&self, stream: &str) -> Option<(SnapshotMetadata, Vec<u8>)>;

    /// List all snapshots for a stream (sorted by offset, newest first)
    fn list_snapshots(&self, stream: &str) -> Vec<SnapshotMetadata>;

    /// Delete old snapshots, keeping only the N most recent
    fn cleanup_old_snapshots(&self, stream: &str, keep_count: usize) -> Result<(), String>;
}
