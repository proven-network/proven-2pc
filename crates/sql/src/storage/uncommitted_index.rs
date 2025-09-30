//! Store for uncommitted index operations
//!
//! This module provides a fjall-based store for tracking uncommitted index operations,
//! similar to UncommittedDataStore but specifically for index operations.

use crate::error::Result;
use crate::storage::encoding::serialize;
use crate::storage::types::RowId;
use crate::types::value::Value;
use fjall::{Keyspace, Partition};
use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
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
    partition: Partition,
    keyspace: Keyspace,
    /// Counter for operation sequence numbers per transaction
    /// Note: This is in-memory only, resets on restart (which is fine for active transactions)
    next_seq: AtomicU64,
}

impl UncommittedIndexStore {
    /// Create a new index version store with the given partition
    pub fn new(partition: Partition, keyspace: Keyspace) -> Self {
        Self {
            partition,
            keyspace,
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

    /// Add an index operation for a transaction
    pub fn add_operation(&self, txn_id: HlcTimestamp, op: IndexOp) -> Result<()> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let key = Self::encode_key(txn_id, op.index_name(), seq);
        let value = serialize(&op)?;

        self.partition.insert(key, value)?;
        Ok(())
    }

    /// Get all index operations for a transaction
    pub fn get_transaction_ops(&self, txn_id: HlcTimestamp) -> Vec<IndexOp> {
        let prefix = Self::encode_tx_prefix(txn_id);

        self.partition
            .prefix(prefix)
            .filter_map(|result| {
                result
                    .ok()
                    .and_then(|(_, value)| bincode::deserialize::<IndexOp>(&value).ok())
            })
            .collect()
    }

    /// Get index operations for a specific index in a transaction
    pub fn get_index_ops(&self, txn_id: HlcTimestamp, index_name: &str) -> Vec<IndexOp> {
        let prefix = Self::encode_tx_index_prefix(txn_id, index_name);

        self.partition
            .prefix(prefix)
            .filter_map(|result| {
                result
                    .ok()
                    .and_then(|(_, value)| bincode::deserialize::<IndexOp>(&value).ok())
            })
            .collect()
    }

    /// Get index lookups for a transaction (for unique constraint checking)
    /// Returns a map of (index_name, values) -> row_ids
    pub fn get_index_lookups(
        &self,
        txn_id: HlcTimestamp,
        index_name: &str,
    ) -> HashMap<Vec<Value>, HashSet<RowId>> {
        let mut lookups: HashMap<Vec<Value>, HashSet<RowId>> = HashMap::new();

        for op in self.get_index_ops(txn_id, index_name) {
            match op {
                IndexOp::Insert { values, row_id, .. } => {
                    lookups.entry(values).or_default().insert(row_id);
                }
                IndexOp::Delete { values, row_id, .. } => {
                    if let Some(set) = lookups.get_mut(&values) {
                        set.remove(&row_id);
                        if set.is_empty() {
                            lookups.remove(&values);
                        }
                    }
                }
            }
        }

        lookups
    }

    /// Clear all operations for a transaction (used on commit or abort)
    pub fn clear_transaction(&self, txn_id: HlcTimestamp) -> Result<()> {
        let prefix = Self::encode_tx_prefix(txn_id);

        // Collect all keys with this prefix
        let keys_to_remove: Vec<_> = self
            .partition
            .prefix(&prefix)
            .filter_map(|result| result.ok().map(|(key, _)| key.to_vec()))
            .collect();

        // Remove all keys
        for key in keys_to_remove {
            self.partition.remove(key)?;
        }

        Ok(())
    }

    /// Check if there are any operations for a transaction
    pub fn has_transaction_ops(&self, txn_id: HlcTimestamp) -> bool {
        let prefix = Self::encode_tx_prefix(txn_id);
        self.partition.prefix(prefix).next().is_some()
    }

    /// Persist the store (flush to disk)
    pub fn persist(&self) -> Result<()> {
        self.keyspace.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }
}
