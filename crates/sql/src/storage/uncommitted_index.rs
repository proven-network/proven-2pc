//! Store for uncommitted index operations
//!
//! NEW ARCHITECTURE: Uses time-bucketed partitions for O(1) cleanup
//! - Partition by TIME ONLY (e.g., _uncommitted_index_uncommitted_bucket_0000000001)
//! - NOT partitioned by index (need to scan all indexes for get_transaction_ops)
//! - Key format: {txn_id(20)}{index_name_len(4)}{index_name}{seq(8)}
//! - Cleanup drops entire partitions instead of scanning keys

use crate::error::Result;
use crate::storage::bucket_manager::BucketManager;
use crate::storage::encoding::serialize;
use crate::storage::types::RowId;
use crate::types::value::Value;
use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Index operation type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexOp {
    Insert {
        index_name: String,
        values: Vec<Value>,
        row_id: RowId,
    },
    Delete {
        index_name: String,
        values: Vec<Value>,
        row_id: RowId,
    },
}

impl IndexOp {
    /// Get the index name for this operation
    pub fn index_name(&self) -> &str {
        match self {
            IndexOp::Insert { index_name, .. } | IndexOp::Delete { index_name, .. } => index_name,
        }
    }

    /// Get the values for this operation
    pub fn values(&self) -> &[Value] {
        match self {
            IndexOp::Insert { values, .. } | IndexOp::Delete { values, .. } => values,
        }
    }

    /// Get the row_id for this operation
    pub fn row_id(&self) -> RowId {
        match self {
            IndexOp::Insert { row_id, .. } | IndexOp::Delete { row_id, .. } => *row_id,
        }
    }
}

/// Fjall-based store for uncommitted index operations
pub struct UncommittedIndexStore {
    bucket_manager: BucketManager,
    retention_window: std::time::Duration,
    /// Counter for operation sequence numbers per transaction
    /// Note: This is in-memory only, resets on restart (which is fine for active transactions)
    next_seq: AtomicU64,
}

impl UncommittedIndexStore {
    /// Create a new index version store with BucketManager
    pub fn new(bucket_manager: BucketManager, retention_window: std::time::Duration) -> Self {
        Self {
            bucket_manager,
            retention_window,
            next_seq: AtomicU64::new(0),
        }
    }

    /// Encode key for index operation: {txn_id}{index_name}{seq}
    fn encode_key(txn_id: HlcTimestamp, index_name: &str, seq: u64) -> Vec<u8> {
        let mut key = Vec::new();

        // Serialize txn_id first for efficient prefix scans by transaction
        // Use lexicographic encoding to ensure byte-wise ordering matches logical ordering
        let tx_bytes = txn_id.to_lexicographic_bytes();
        key.extend_from_slice(&tx_bytes);

        // Add index name
        key.extend_from_slice(&(index_name.len() as u32).to_be_bytes());
        key.extend_from_slice(index_name.as_bytes());

        // Add sequence number to maintain order
        key.extend_from_slice(&seq.to_be_bytes());
        key
    }

    /// Encode prefix for transaction operations
    fn encode_tx_prefix(txn_id: HlcTimestamp) -> Vec<u8> {
        txn_id.to_lexicographic_bytes().to_vec()
    }

    /// Encode prefix for transaction + index operations
    fn encode_tx_index_prefix(txn_id: HlcTimestamp, index_name: &str) -> Vec<u8> {
        let mut key = Vec::new();

        // Serialize txn_id first
        let tx_bytes = txn_id.to_lexicographic_bytes();
        key.extend_from_slice(&tx_bytes);

        // Add index name
        key.extend_from_slice(&(index_name.len() as u32).to_be_bytes());
        key.extend_from_slice(index_name.as_bytes());

        key
    }

    /// Get existing partition (read-only) - uses entity name "uncommitted_index"
    fn get_existing_partition_for_time(&self, time: HlcTimestamp) -> Option<&fjall::PartitionHandle> {
        self.bucket_manager
            .get_existing_partition("uncommitted_index", time)
    }

    /// Get or create partition (for writes) - uses entity name "uncommitted_index"
    fn get_or_create_partition_for_time(
        &mut self,
        time: HlcTimestamp,
    ) -> Result<&fjall::PartitionHandle> {
        self.bucket_manager
            .get_or_create_partition("uncommitted_index", time)
    }

    /// Add an index operation for a transaction (requires &mut)
    pub fn add_operation(&mut self, txn_id: HlcTimestamp, op: IndexOp) -> Result<()> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let partition = self.get_or_create_partition_for_time(txn_id)?;
        let key = Self::encode_key(txn_id, op.index_name(), seq);
        partition.insert(key, serialize(&op)?)?;
        Ok(())
    }

    /// Get all index operations for a transaction (uses &self - read-only)
    pub fn get_transaction_ops(&self, txn_id: HlcTimestamp) -> Vec<IndexOp> {
        let Some(partition) = self.get_existing_partition_for_time(txn_id) else {
            return Vec::new(); // Partition doesn't exist = no data
        };

        let prefix = Self::encode_tx_prefix(txn_id);
        partition
            .prefix(prefix)
            .filter_map(|result| {
                result
                    .ok()
                    .and_then(|(_, value)| bincode::deserialize::<IndexOp>(&value).ok())
            })
            .collect()
    }

    /// Get index operations for a specific index in a transaction (uses &self - read-only)
    pub fn get_index_ops(&self, txn_id: HlcTimestamp, index_name: &str) -> Vec<IndexOp> {
        let Some(partition) = self.get_existing_partition_for_time(txn_id) else {
            return Vec::new(); // Partition doesn't exist = no data
        };

        let prefix = Self::encode_tx_index_prefix(txn_id, index_name);
        partition
            .prefix(prefix)
            .filter_map(|result| {
                result
                    .ok()
                    .and_then(|(_, value)| bincode::deserialize::<IndexOp>(&value).ok())
            })
            .collect()
    }

    /// Clear all operations for a transaction (used on commit or abort - requires &mut)
    pub fn clear_transaction(
        &mut self,
        batch: &mut fjall::Batch,
        txn_id: HlcTimestamp,
    ) -> Result<()> {
        let Some(partition) = self.get_existing_partition_for_time(txn_id) else {
            return Ok(()); // Partition doesn't exist = nothing to clear
        };

        let prefix = Self::encode_tx_prefix(txn_id);
        // Collect all keys with this prefix
        let keys_to_remove: Vec<_> = partition
            .prefix(&prefix)
            .filter_map(|result| result.ok().map(|(key, _)| key.to_vec()))
            .collect();

        // Remove all keys using the batch
        for key in keys_to_remove {
            batch.remove(partition, key);
        }

        Ok(())
    }

    /// Cleanup old buckets - O(1) per bucket!
    pub fn cleanup_old_buckets(&mut self, current_time: HlcTimestamp) -> Result<usize> {
        self.bucket_manager
            .cleanup_old_buckets(current_time, self.retention_window)
    }
}
