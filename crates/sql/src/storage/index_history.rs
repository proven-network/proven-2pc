//! Fjall-based index history store for time-travel snapshot reads
//!
//! NEW ARCHITECTURE: Uses time-bucketed partitions for O(1) cleanup
//! - Partition by index × time (e.g., _index_history_idx_age_bucket_0000000001)
//! - Key format: {commit_time(20)}{row_id(8)}{seq(4)} (index is in partition name)
//! - Cleanup drops entire partitions instead of scanning keys

use crate::error::Result;
use crate::storage::bucket_manager::BucketManager;
use crate::storage::encoding::{deserialize, serialize};
use crate::storage::uncommitted_index::IndexOp;
use proven_hlc::HlcTimestamp;
use std::collections::HashMap;
use std::time::Duration;

/// Fjall-based store for committed index operations within retention window (index history)
/// Maintains a sliding window of operations for time-travel queries on indexes
pub struct IndexHistoryStore {
    bucket_manager: BucketManager,
    retention_window: Duration,
}

impl IndexHistoryStore {
    /// Create a new index history store
    pub fn new(bucket_manager: BucketManager, retention_window: Duration) -> Self {
        Self {
            bucket_manager,
            retention_window,
        }
    }

    /// Encode key: {commit_time(20)}{row_id(8)}{seq(4)}
    /// Index name is in partition name, not in key
    fn encode_key(commit_time: HlcTimestamp, row_id: u64, seq: u32) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(&commit_time.to_lexicographic_bytes());
        key.extend_from_slice(&row_id.to_be_bytes());
        key.extend_from_slice(&seq.to_be_bytes());
        key
    }

    /// Add committed ops (for writes - requires &mut)
    pub fn add_committed_ops_to_batch(
        &mut self,
        batch: &mut fjall::Batch,
        commit_time: HlcTimestamp,
        ops: Vec<IndexOp>,
    ) -> Result<()> {
        // Group by index
        let mut ops_by_index: HashMap<String, Vec<IndexOp>> = HashMap::new();
        for op in ops {
            ops_by_index
                .entry(op.index_name().to_string())
                .or_default()
                .push(op);
        }

        // Write to index-specific time buckets
        for (index_name, index_ops) in ops_by_index {
            let partition = self
                .bucket_manager
                .get_or_create_partition(&index_name, commit_time)?;
            for (seq, op) in index_ops.into_iter().enumerate() {
                let key = Self::encode_key(commit_time, op.row_id(), seq as u32);
                batch.insert(partition, key, serialize(&op)?);
            }
        }

        Ok(())
    }

    /// Get index ops after timestamp (for reads - uses &self)
    /// CRITICAL: Uses get_existing_partitions (read-only, no partition creation)
    pub fn get_index_ops_after(
        &self,
        snapshot_txn_id: HlcTimestamp,
        index_name: &str,
    ) -> Result<Vec<IndexOp>> {
        let mut ops = Vec::new();

        // Get EXISTING partitions only (no creation)
        let far_future = HlcTimestamp::new(u64::MAX, 0, snapshot_txn_id.node_id);
        let partitions = self.bucket_manager.get_existing_partitions_for_range(
            index_name,
            snapshot_txn_id,
            far_future,
        );

        // Scan existing partitions
        let snapshot_bytes = snapshot_txn_id.to_lexicographic_bytes();
        for partition in partitions {
            for result in partition.range(snapshot_bytes.to_vec()..) {
                let (_key, value) = result?;
                if let Ok(op) = deserialize::<IndexOp>(&value) {
                    ops.push(op);
                }
            }
        }

        Ok(ops)
    }

    /// Cleanup old buckets - O(1) per bucket!
    pub fn cleanup_old_buckets(&mut self, current_time: HlcTimestamp) -> Result<usize> {
        self.bucket_manager
            .cleanup_old_buckets(current_time, self.retention_window)
    }
}
