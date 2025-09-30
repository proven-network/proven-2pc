//! Fjall-based active version store for uncommitted transaction operations

use crate::error::Result;
use crate::storage::encoding::{deserialize, serialize};
use crate::storage::types::{Row, RowId, WriteOp};
use fjall::{Keyspace, Partition};
use proven_hlc::HlcTimestamp;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Fjall-based store for active (uncommitted) transaction writes
pub struct UncommittedDataStore {
    partition: Partition,
    keyspace: Keyspace,
    /// Counter for operation sequence numbers per transaction
    /// Note: This is in-memory only, resets on restart (which is fine for active transactions)
    next_seq: std::sync::atomic::AtomicU64,
}

/// Efficient structure for table iteration
/// Built once at the start of iteration to avoid repeated scans
pub struct TableActiveData {
    /// Active writes for this table (ordered by RowId)
    pub writes: BTreeMap<RowId, Arc<Row>>,
    /// Deleted rows for this table
    pub deletes: HashSet<RowId>,
}

impl UncommittedDataStore {
    /// Create a new active version store with the given partition
    pub fn new(partition: Partition, keyspace: Keyspace) -> Self {
        Self {
            partition,
            keyspace,
            next_seq: AtomicU64::new(0),
        }
    }

    /// Encode key for active version: {txn_id}{table}{row_id}{seq}
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

    /// Add a write operation for a transaction
    pub fn add_write(&self, txn_id: HlcTimestamp, op: WriteOp) -> Result<()> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let key = Self::encode_key(txn_id, &op, seq);
        let value = serialize(&op)?;

        self.partition.insert(key, value)?;
        Ok(())
    }

    /// Get a specific row for a transaction
    /// Returns:
    /// - Some(Some(row)) if row exists (inserted/updated)
    /// - Some(None) if row was deleted
    /// - None if no operations for this row
    pub fn get_row(
        &self,
        txn_id: HlcTimestamp,
        table: &str,
        row_id: RowId,
    ) -> Option<Option<Arc<Row>>> {
        // We need to scan for the specific data operation
        // Collect all operations for this row and return the last one
        let prefix = Self::encode_tx_prefix(txn_id);
        let mut last_op: Option<WriteOp> = None;

        for result in self.partition.prefix(prefix) {
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
            Some(WriteOp::Insert { row, .. }) => Some(Some(row)),
            Some(WriteOp::Update { new_row, .. }) => Some(Some(new_row)),
            Some(WriteOp::Delete { .. }) => Some(None), // Deleted rows
            None => None,                               // No operations for this row
        }
    }

    /// Get all write operations for a transaction
    pub fn get_transaction_writes(&self, txn_id: HlcTimestamp) -> Vec<WriteOp> {
        let prefix = Self::encode_tx_prefix(txn_id);

        self.partition
            .prefix(prefix)
            .filter_map(|result| {
                result
                    .ok()
                    .and_then(|(_, value)| deserialize::<WriteOp>(&value).ok())
            })
            .collect()
    }

    /// Get table-specific active data for efficient iteration
    /// This scans active_versions once and returns pre-built structures
    pub fn get_table_active_data(&self, txn_id: HlcTimestamp, table_name: &str) -> TableActiveData {
        let mut writes = BTreeMap::new();
        let mut deletes = HashSet::new();

        // Single scan through transaction's operations
        let prefix = Self::encode_tx_prefix(txn_id);
        for result in self.partition.prefix(prefix) {
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

    /// Remove all operations for a transaction (on commit or abort)
    pub fn remove_transaction(&self, batch: &mut fjall::Batch, txn_id: HlcTimestamp) -> Result<()> {
        let prefix = Self::encode_tx_prefix(txn_id);

        for result in self.partition.prefix(prefix) {
            let (key, _) = result?;
            batch.remove(&self.partition, key);
        }

        Ok(())
    }

    /// Clean up operations before a given timestamp
    pub fn cleanup_before(&self, timestamp: HlcTimestamp) -> Result<()> {
        let mut batch = self.keyspace.batch();
        let mut removed_count = 0;

        // Iterate through all entries
        for result in self.partition.iter() {
            let (key, _) = result?;

            // Try to decode the txn_id from the key
            if let Ok(txn_id) = bincode::deserialize::<HlcTimestamp>(&key[..key.len().min(24)])
                && txn_id < timestamp
            {
                batch.remove(&self.partition, key);
                removed_count += 1;
            }
        }

        if removed_count > 0 {
            batch.commit()?;
        }

        Ok(())
    }

    /// Remove all operations for a specific table (used when dropping a table)
    pub fn remove_table(&self, table_name: &str) -> Result<()> {
        let mut batch = self.keyspace.batch();

        // Scan all entries and remove those matching the table
        for result in self.partition.iter() {
            let (key, value) = result?;

            // Deserialize the operation to check the table
            if let Ok(op) = deserialize::<WriteOp>(&value) {
                // Check if this operation is for the table being dropped
                if op.table_name() == Some(table_name) {
                    batch.remove(&self.partition, key);
                }
                // Also remove index operations for indexes on this table
                // (We'd need to track which indexes belong to which tables)
            }
        }

        batch.commit()?;
        Ok(())
    }

    // Index operations removed - now tracked separately in IndexVersionStore
}
