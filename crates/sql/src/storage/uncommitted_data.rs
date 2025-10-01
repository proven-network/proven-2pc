//! Fjall-based uncommitted data store for uncommitted transaction operations
//!
//! NEW ARCHITECTURE: Uses time-bucketed partitions for O(1) cleanup
//! - Partition by TIME ONLY (e.g., _uncommitted_data_uncommitted_bucket_0000000001)
//! - NOT partitioned by table (need to scan all tables for get_transaction_writes)
//! - Key format: {txn_id(20)}{table_len(4)}{table}{row_id(8)}{seq(8)}
//! - Cleanup drops entire partitions instead of scanning keys

use crate::error::Result;
use crate::storage::bucket_manager::BucketManager;
use crate::storage::encoding::{deserialize, serialize};
use crate::storage::types::{Row, RowId, WriteOp};
use proven_hlc::HlcTimestamp;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Fjall-based store for uncommitted transaction writes
pub struct UncommittedDataStore {
    bucket_manager: BucketManager,
    retention_window: Duration,
    /// Counter for operation sequence numbers per transaction
    /// Note: This is in-memory only, resets on restart (which is fine for uncommitted transactions)
    next_seq: AtomicU64,
}

/// State of a row in uncommitted data
#[derive(Debug, Clone, PartialEq)]
pub enum RowState {
    /// No operations for this row
    NoOps,
    /// Row was explicitly deleted
    Deleted,
    /// Row exists with data
    Exists(Arc<Row>),
}

/// Efficient structure for table iteration
/// Built once at the start of iteration to avoid repeated scans
pub struct TableActiveData {
    /// Uncommitted writes for this table (ordered by RowId)
    pub writes: BTreeMap<RowId, Arc<Row>>,
    /// Deleted rows for this table
    pub deletes: HashSet<RowId>,
}

impl UncommittedDataStore {
    /// Create a new uncommitted data store with BucketManager
    pub fn new(bucket_manager: BucketManager, retention_window: Duration) -> Self {
        Self {
            bucket_manager,
            retention_window,
            next_seq: AtomicU64::new(0),
        }
    }

    /// Encode key for uncommitted data: {txn_id}{table}{row_id}{seq}
    /// seq: sequence number to maintain operation order
    fn encode_key(txn_id: HlcTimestamp, op: &WriteOp, seq: u64) -> Vec<u8> {
        let mut key = Vec::new();
        // Serialize txn_id first for efficient prefix scans by transaction
        // Use lexicographic encoding to ensure byte-wise ordering matches logical ordering
        let tx_bytes = txn_id.to_lexicographic_bytes();
        key.extend_from_slice(&tx_bytes);

        // Add table name
        if let Some(table) = op.table_name() {
            key.extend_from_slice(&(table.len() as u32).to_be_bytes());
            key.extend_from_slice(table.as_bytes());
        }

        // Add row_id
        key.extend_from_slice(&op.row_id().to_be_bytes());
        // Add sequence number to maintain order
        key.extend_from_slice(&seq.to_be_bytes());
        key
    }

    /// Encode prefix for transaction operations
    fn encode_tx_prefix(txn_id: HlcTimestamp) -> Vec<u8> {
        txn_id.to_lexicographic_bytes().to_vec()
    }

    /// Get existing partition (read-only) - uses entity name "uncommitted"
    fn get_existing_partition_for_time(&self, time: HlcTimestamp) -> Option<&fjall::PartitionHandle> {
        self.bucket_manager.get_existing_partition("uncommitted", time)
    }

    /// Get or create partition (for writes) - uses entity name "uncommitted"
    fn get_or_create_partition_for_time(
        &mut self,
        time: HlcTimestamp,
    ) -> Result<&fjall::PartitionHandle> {
        self.bucket_manager
            .get_or_create_partition("uncommitted", time)
    }

    /// Add a write operation for a transaction (requires &mut)
    pub fn add_write(&mut self, txn_id: HlcTimestamp, op: WriteOp) -> Result<()> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let partition = self.get_or_create_partition_for_time(txn_id)?;
        let key = Self::encode_key(txn_id, &op, seq);
        partition.insert(key, serialize(&op)?)?;
        Ok(())
    }

    /// Get a specific row for a transaction (uses &self - read-only)
    pub fn get_row(&self, txn_id: HlcTimestamp, table: &str, row_id: RowId) -> RowState {
        // Check if partition exists - if not, no data
        let Some(partition) = self.get_existing_partition_for_time(txn_id) else {
            return RowState::NoOps; // Partition doesn't exist = no data
        };

        // Scan for the specific data operation
        let prefix = Self::encode_tx_prefix(txn_id);
        let mut last_op: Option<WriteOp> = None;

        for result in partition.prefix(prefix) {
            if let Ok((_, value)) = result
                && let Ok(op) = deserialize::<WriteOp>(&value)
            {
                // Check if this is the data operation we're looking for
                if op.table_name() == Some(table) && op.row_id() == row_id {
                    // Since keys are ordered by sequence, this will be the latest
                    last_op = Some(op);
                }
            }
        }

        // Return based on the last operation
        match last_op {
            Some(WriteOp::Insert { row, .. }) => RowState::Exists(row),
            Some(WriteOp::Update { new_row, .. }) => RowState::Exists(new_row),
            Some(WriteOp::Delete { .. }) => RowState::Deleted,
            None => RowState::NoOps,
        }
    }

    /// Get all write operations for a transaction (uses &self - read-only)
    pub fn get_transaction_writes(&self, txn_id: HlcTimestamp) -> Vec<WriteOp> {
        let Some(partition) = self.get_existing_partition_for_time(txn_id) else {
            return Vec::new(); // Partition doesn't exist = no data
        };

        let prefix = Self::encode_tx_prefix(txn_id);
        partition
            .prefix(prefix)
            .filter_map(|result| {
                result
                    .ok()
                    .and_then(|(_, value)| deserialize::<WriteOp>(&value).ok())
            })
            .collect()
    }

    /// Get table-specific active data for efficient iteration (uses &self - read-only)
    /// This scans uncommitted_data once and returns pre-built structures
    pub fn get_table_active_data(&self, txn_id: HlcTimestamp, table_name: &str) -> TableActiveData {
        let mut writes = BTreeMap::new();
        let mut deletes = HashSet::new();

        let Some(partition) = self.get_existing_partition_for_time(txn_id) else {
            return TableActiveData { writes, deletes }; // Partition doesn't exist = no data
        };

        // Single scan through transaction's operations
        let prefix = Self::encode_tx_prefix(txn_id);
        for result in partition.prefix(prefix) {
            if let Ok((_, value)) = result
                && let Ok(op) = deserialize::<WriteOp>(&value)
                && op.table_name() == Some(table_name)
            {
                match op {
                    WriteOp::Insert { row_id, row, .. } => {
                        writes.insert(row_id, row);
                    }
                    WriteOp::Update {
                        row_id, new_row, ..
                    } => {
                        writes.insert(row_id, new_row);
                    }
                    WriteOp::Delete { row_id, .. } => {
                        deletes.insert(row_id);
                        writes.remove(&row_id);
                    }
                }
            }
        }

        TableActiveData { writes, deletes }
    }

    /// Remove all operations for a transaction (on commit/abort - requires &mut for batch)
    pub fn remove_transaction(
        &mut self,
        batch: &mut fjall::Batch,
        txn_id: HlcTimestamp,
    ) -> Result<()> {
        let Some(partition) = self.get_existing_partition_for_time(txn_id) else {
            return Ok(()); // Partition doesn't exist = nothing to remove
        };

        let prefix = Self::encode_tx_prefix(txn_id);
        for result in partition.prefix(prefix) {
            let (key, _) = result?;
            batch.remove(partition, key);
        }

        Ok(())
    }

    /// Cleanup old buckets - O(1) per bucket!
    /// Note: Uncommitted data is typically very short-lived (seconds)
    pub fn cleanup_old_buckets(&mut self, current_time: HlcTimestamp) -> Result<usize> {
        self.bucket_manager
            .cleanup_old_buckets(current_time, self.retention_window)
    }
}
