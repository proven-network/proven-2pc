//! Fjall-based recent index versions store for time-travel snapshot reads

use crate::storage::encoding::{deserialize, serialize};
use crate::storage::types::{StorageResult, TransactionId};
use crate::storage::uncommitted_index::IndexOp;
use fjall::{Keyspace, Partition};
use proven_hlc::HlcTimestamp;
use std::time::Duration;

/// Fjall-based store for recently committed index operations
/// Maintains a sliding window of operations for time-travel queries on indexes
pub struct IndexHistoryStore {
    partition: Partition,
    keyspace: Keyspace,
    retention_window: Duration,
}

impl IndexHistoryStore {
    /// Create a new recent index versions store
    pub fn new(partition: Partition, keyspace: Keyspace, retention_window: Duration) -> Self {
        Self {
            partition,
            keyspace,
            retention_window,
        }
    }

    /// Check if the store is empty (fast check for optimization)
    pub fn is_empty(&self) -> bool {
        // This is a very fast check - just see if there's any entry at all
        match self.partition.first_key_value() {
            Ok(None) => true,     // No entries, it's empty
            Ok(Some(_)) => false, // Has entries, not empty
            Err(_) => true,       // Error reading, treat as empty for safety
        }
    }

    /// Encode key: {index_name_len(4)}{index_name}{commit_time(24)}{row_id(8)}{seq(4)}
    ///
    /// KEY DESIGN: index_name comes FIRST for efficient prefix scans!
    /// This allows O(k) lookup of all ops for a specific index, then filter by time.
    fn encode_key(commit_time: TransactionId, op: &IndexOp, seq: u32) -> Vec<u8> {
        let mut key = Vec::new();

        // Put index name FIRST for efficient prefix scans
        let index_name = op.index_name();
        key.extend_from_slice(&(index_name.len() as u32).to_be_bytes());
        key.extend_from_slice(index_name.as_bytes());

        // Then commit_time for chronological ordering within index
        // Use lexicographic encoding to ensure byte-wise ordering matches logical ordering
        let time_bytes = commit_time.to_lexicographic_bytes();
        key.extend_from_slice(&time_bytes);

        // Then row_id for filtering by row
        key.extend_from_slice(&op.row_id().to_be_bytes());

        // Finally sequence number for ordering within transaction
        key.extend_from_slice(&seq.to_be_bytes());

        key
    }

    /// Add committed index operations from a transaction to an existing batch
    pub fn add_committed_ops_to_batch(
        &self,
        batch: &mut fjall::Batch,
        commit_time: TransactionId,
        ops: Vec<IndexOp>,
    ) -> StorageResult<()> {
        // Use sequence numbers to preserve order within transaction
        for (seq, op) in ops.into_iter().enumerate() {
            let key = Self::encode_key(commit_time, &op, seq as u32);
            let value = serialize(&op)?;
            batch.insert(&self.partition, key, value);
        }
        Ok(())
    }

    /// Get all index operations for an index after a transaction ID
    /// Returns all IndexOps for the index that were committed after snapshot_txn_id
    ///
    /// OPTIMIZED: Uses bounded range scan - stops at end of index namespace automatically
    pub fn get_index_ops_after(
        &self,
        snapshot_txn_id: TransactionId,
        index_name: &str,
    ) -> StorageResult<Vec<IndexOp>> {
        let mut ops = Vec::new();

        // Build start key: {index_name_len}{index_name}{snapshot_txn_id}
        let mut start_key = Vec::new();
        start_key.extend_from_slice(&(index_name.len() as u32).to_be_bytes());
        start_key.extend_from_slice(index_name.as_bytes());
        let snapshot_time_bytes = snapshot_txn_id.to_lexicographic_bytes();
        start_key.extend_from_slice(&snapshot_time_bytes);

        // Build end key: {index_name_len}{index_name}{max_time}
        let mut end_key = Vec::new();
        end_key.extend_from_slice(&(index_name.len() as u32).to_be_bytes());
        end_key.extend_from_slice(index_name.as_bytes());
        end_key.extend_from_slice(&[0xFF; 20]); // Max timestamp (20 bytes)

        // Bounded range scan - automatically stops at end of index
        for result in self.partition.range(start_key..end_key) {
            let (_key, value) = result?;

            // Just deserialize - no need to extract commit_time
            if let Ok(op) = deserialize::<IndexOp>(&value) {
                ops.push(op);
            }
        }

        Ok(ops)
    }

    /// Cleanup old operations (older than retention window)
    pub fn cleanup_old_operations(&self, current_time: HlcTimestamp) -> StorageResult<()> {
        // Calculate the cutoff time (retention window ago)
        // Note: HlcTimestamp's physical component is in microseconds
        let retention_micros = self.retention_window.as_micros() as u64;
        let cutoff_physical = current_time.physical.saturating_sub(retention_micros);
        let cutoff = HlcTimestamp::new(cutoff_physical, 0, current_time.node_id);

        let mut batch = self.keyspace.batch();
        let mut removed_count = 0;

        // Key format: {index_name_len(4)}{index_name}{commit_time(20)}{row_id(8)}{seq(8)}
        for result in self.partition.iter() {
            let (key, _value) = result?;

            // Extract index name length to find timestamp offset
            if key.len() < 4 {
                continue;
            }

            let index_name_len = u32::from_be_bytes([key[0], key[1], key[2], key[3]]) as usize;
            let timestamp_offset = 4 + index_name_len;

            // Check if key has enough bytes for timestamp (20 bytes)
            if key.len() < timestamp_offset + 20 {
                continue;
            }

            // Extract and decode timestamp using lexicographic encoding
            let timestamp_bytes = &key[timestamp_offset..timestamp_offset + 20];
            if let Ok(commit_time) = HlcTimestamp::from_lexicographic_bytes(timestamp_bytes)
                && commit_time < cutoff
            {
                batch.remove(&self.partition, key);
                removed_count += 1;
            }
            // Note: We can't break early since keys are ordered by index name first, not time
        }

        if removed_count > 0 {
            batch.commit()?;
        }

        Ok(())
    }
}
