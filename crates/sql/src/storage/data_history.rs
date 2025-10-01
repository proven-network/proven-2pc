//! Fjall-based data history store for time-travel snapshot reads
//!
//! NEW ARCHITECTURE: Uses time-bucketed partitions for O(1) cleanup
//! - Partition by table × time (e.g., _data_history_users_bucket_0000000001)
//! - Key format: {commit_time(20)}{row_id(8)}{seq(4)} (table is in partition name)
//! - Cleanup drops entire partitions instead of scanning keys

use crate::error::Result;
use crate::storage::bucket_manager::BucketManager;
use crate::storage::encoding::{deserialize, serialize};
use crate::storage::types::{RowId, WriteOp};
use proven_hlc::HlcTimestamp;
use std::collections::HashMap;
use std::time::Duration;

/// Fjall-based store for committed operations within retention window (data history)
/// Maintains a sliding window of operations for time-travel queries
pub struct DataHistoryStore {
    bucket_manager: BucketManager,
    retention_window: Duration,
}

impl DataHistoryStore {
    /// Create a new data history store
    pub fn new(bucket_manager: BucketManager, retention_window: Duration) -> Self {
        Self {
            bucket_manager,
            retention_window,
        }
    }

    /// Check if the store is empty (fast check for optimization)
    pub fn is_empty(&self) -> bool {
        // With bucketed partitions, check if we have any active partitions
        // This is a fast in-memory check
        self.bucket_manager.active_partitions.is_empty()
    }

    /// Encode key: {commit_time(20)}{row_id(8)}{seq(4)}
    /// Table is in partition name, not in key
    fn encode_key(commit_time: HlcTimestamp, row_id: RowId, seq: u32) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(&commit_time.to_lexicographic_bytes());
        key.extend_from_slice(&row_id.to_be_bytes());
        key.extend_from_slice(&seq.to_be_bytes());
        key
    }

    /// Add committed operations (for writes - requires &mut)
    pub fn add_committed_ops_to_batch(
        &mut self,
        batch: &mut fjall::Batch,
        commit_time: HlcTimestamp,
        ops: Vec<WriteOp>,
    ) -> Result<()> {
        // Group by table
        let mut ops_by_table: HashMap<String, Vec<WriteOp>> = HashMap::new();
        for op in ops {
            if let Some(table) = op.table_name() {
                ops_by_table.entry(table.to_string()).or_default().push(op);
            }
        }

        // Write to table-specific time buckets
        for (table, table_ops) in ops_by_table {
            let partition = self.bucket_manager.get_or_create_partition(&table, commit_time)?;
            for (seq, op) in table_ops.into_iter().enumerate() {
                let key = Self::encode_key(commit_time, op.row_id(), seq as u32);
                batch.insert(partition, key, serialize(&op)?);
            }
        }

        Ok(())
    }

    /// Get table ops after timestamp (for reads - uses &self)
    /// CRITICAL: Uses get_existing_partitions (read-only, no partition creation)
    pub fn get_table_ops_after(
        &self,
        snapshot_time: HlcTimestamp,
        table: &str,
    ) -> Result<HashMap<RowId, Vec<WriteOp>>> {
        let mut result: HashMap<RowId, Vec<WriteOp>> = HashMap::new();

        // Get EXISTING partitions only (no creation)
        let far_future = HlcTimestamp::new(u64::MAX, 0, snapshot_time.node_id);
        let partitions = self.bucket_manager.get_existing_partitions_for_range(
            table,
            snapshot_time,
            far_future,
        );

        // Scan existing partitions
        let snapshot_bytes = snapshot_time.to_lexicographic_bytes();
        for partition in partitions {
            for entry in partition.range(snapshot_bytes.to_vec()..) {
                let (_key, value) = entry?;
                if let Ok(op) = deserialize::<WriteOp>(&value) {
                    result.entry(op.row_id()).or_default().push(op);
                }
            }
        }

        Ok(result)
    }

    /// Cleanup old buckets - O(1) per bucket!
    pub fn cleanup_old_buckets(&mut self, current_time: HlcTimestamp) -> Result<usize> {
        self.bucket_manager
            .cleanup_old_buckets(current_time, self.retention_window)
    }
}
