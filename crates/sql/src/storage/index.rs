//! Index management and operations

use crate::storage::encoding::{decode_row_id_from_index_key, encode_index_key};
use crate::storage::index_history::IndexHistoryStore;
use crate::storage::types::{
    FjallIterator, Row, RowId, StorageError, StorageResult, TransactionId,
};
use crate::storage::uncommitted_index::{IndexOp, UncommittedIndexStore};
use crate::types::value::Value;
use fjall::{Batch, Partition, PartitionCreateOptions};
use parking_lot::RwLockReadGuard;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Index type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    Unique,
}

/// Index metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
    pub unique: bool,
}

/// Index manager
pub struct IndexManager {
    indexes: BTreeMap<String, IndexMetadata>,
    partitions: BTreeMap<String, Partition>,
    /// Store for uncommitted index operations
    index_versions: Arc<UncommittedIndexStore>,
    /// Store for recently committed index operations (for time-travel)
    index_history: Arc<IndexHistoryStore>,
}

impl IndexManager {
    pub fn new(
        index_versions: Arc<UncommittedIndexStore>,
        index_history: Arc<IndexHistoryStore>,
    ) -> Self {
        Self {
            indexes: BTreeMap::new(),
            partitions: BTreeMap::new(),
            index_versions,
            index_history,
        }
    }

    /// Create a new index
    pub fn create_index(
        &mut self,
        keyspace: &fjall::Keyspace,
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
    ) -> StorageResult<()> {
        // Check if index already exists
        if self.indexes.contains_key(&name) {
            return Err(StorageError::Other(format!(
                "Index {} already exists",
                name
            )));
        }

        // Create index partition
        let partition = keyspace.open_partition(
            &format!("{}_idx_{}", table, name),
            PartitionCreateOptions::default()
                .block_size(32 * 1024)
                .compression(fjall::CompressionType::Lz4),
        )?;

        // Create metadata
        let metadata = IndexMetadata {
            name: name.clone(),
            table,
            columns,
            index_type: if unique {
                IndexType::Unique
            } else {
                IndexType::BTree
            },
            unique,
        };

        self.indexes.insert(name.clone(), metadata);
        self.partitions.insert(name, partition);

        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&mut self, name: &str) -> StorageResult<()> {
        self.indexes
            .remove(name)
            .ok_or_else(|| StorageError::IndexNotFound(name.to_string()))?;
        self.partitions.remove(name);
        Ok(())
    }

    /// Get all index metadata
    pub fn get_all_metadata(&self) -> BTreeMap<String, IndexMetadata> {
        self.indexes.clone()
    }

    /// Get metadata for a specific index
    pub fn get_index_metadata(&self, index_name: &str) -> Option<&IndexMetadata> {
        self.indexes.get(index_name)
    }

    /// Add an entry to an index (tracks as uncommitted)
    pub fn add_entry(
        &mut self,
        index_name: &str,
        values: Vec<Value>,
        row_id: RowId,
        tx_id: TransactionId,
    ) -> StorageResult<()> {
        // Track the operation in UncommittedIndexStore
        let op = IndexOp::Insert {
            index_name: index_name.to_string(),
            values,
            row_id,
        };
        self.index_versions.add_operation(tx_id, op)?;
        Ok(())
    }

    /// Remove an entry from an index (tracks as uncommitted)
    pub fn remove_entry(
        &mut self,
        index_name: &str,
        values: &[Value],
        row_id: RowId,
        tx_id: TransactionId,
    ) -> StorageResult<()> {
        // Track the delete operation in UncommittedIndexStore
        let op = IndexOp::Delete {
            index_name: index_name.to_string(),
            values: values.to_vec(),
            row_id,
        };
        self.index_versions.add_operation(tx_id, op)?;
        Ok(())
    }

    /// Remove an index entry from uncommitted operations by value
    pub fn remove_by_value(
        &mut self,
        index_name: &str,
        values: &[Value],
        row_id: RowId,
        tx_id: TransactionId,
    ) -> StorageResult<()> {
        // Track the delete operation in UncommittedIndexStore
        let op = IndexOp::Delete {
            index_name: index_name.to_string(),
            values: values.to_vec(),
            row_id,
        };
        self.index_versions.add_operation(tx_id, op)?;
        Ok(())
    }

    /// Lookup entries by exact value
    /// For snapshot reads, pass the snapshot transaction ID as `tx_id`.
    pub fn lookup(
        &self,
        index_name: &str,
        values: Vec<Value>,
        tx_id: TransactionId,
    ) -> StorageResult<Vec<RowId>> {
        let partition = self
            .partitions
            .get(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        let mut results = HashSet::new();
        let key_prefix = encode_index_key(&values, 0);

        // First, find all committed entries with matching prefix
        for entry in partition.prefix(&key_prefix[..key_prefix.len() - 8]) {
            let (key, _value) = entry?;
            if let Some(row_id) = decode_row_id_from_index_key(&key) {
                results.insert(row_id);
            }
        }

        // Apply MVCC: hide commits that happened after this transaction
        // Get all index operations committed after tx_id and reverse their effects
        //
        // OPTIMIZATION: Only query history if it might contain relevant operations
        if !self.index_history.is_empty() {
            let recent_ops = self.index_history.get_index_ops_after(tx_id, index_name)?;

            for op in recent_ops {
                if op.values() == values {
                    match op {
                        IndexOp::Insert { row_id, .. } => {
                            // Row was inserted after us, hide it
                            results.remove(&row_id);
                        }
                        IndexOp::Delete { row_id, .. } => {
                            // Row was deleted after us, restore it
                            results.insert(row_id);
                        }
                    }
                }
            }
        }

        // Then, apply uncommitted operations for the current transaction
        let ops = self.index_versions.get_index_ops(tx_id, index_name);
        for op in ops {
            if op.values() == values {
                match op {
                    IndexOp::Insert { row_id, .. } => {
                        results.insert(row_id);
                    }
                    IndexOp::Delete { row_id, .. } => {
                        results.remove(&row_id);
                    }
                }
            }
        }

        Ok(results.into_iter().collect())
    }

    /// Range scan on index
    pub fn range_scan(
        &self,
        index_name: &str,
        start_values: Option<Vec<Value>>,
        end_values: Option<Vec<Value>>,
        tx_id: TransactionId,
    ) -> StorageResult<Vec<RowId>> {
        let partition = self
            .partitions
            .get(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        let mut results = HashSet::new();

        // Build range bounds
        let start_key = start_values.as_ref().map(|v| encode_index_key(v, 0));
        let end_key = end_values.as_ref().map(|v| encode_index_key(v, u64::MAX));

        // Create appropriate range and collect results
        let entries: Vec<_> = match (start_key, end_key) {
            (None, None) => partition.iter().collect::<Result<Vec<_>, _>>()?,
            (Some(s), None) => partition.range(s..).collect::<Result<Vec<_>, _>>()?,
            (None, Some(e)) => partition.range(..=e).collect::<Result<Vec<_>, _>>()?,
            (Some(s), Some(e)) => partition.range(s..=e).collect::<Result<Vec<_>, _>>()?,
        };

        // First, collect committed entries
        for (key, _value) in entries {
            if let Some(row_id) = decode_row_id_from_index_key(&key) {
                results.insert(row_id);
            }
        }

        // Apply MVCC: hide commits that happened after this transaction
        let recent_ops = self.index_history.get_index_ops_after(tx_id, index_name)?;

        for op in recent_ops {
            let values = op.values();
            let in_range = match (&start_values, &end_values) {
                (None, None) => true,
                (Some(start), None) => values >= start,
                (None, Some(end)) => values <= end,
                (Some(start), Some(end)) => values >= start && values <= end,
            };

            if in_range {
                match op {
                    IndexOp::Insert { row_id, .. } => {
                        // Row was inserted after us, hide it
                        results.remove(&row_id);
                    }
                    IndexOp::Delete { row_id, .. } => {
                        // Row was deleted after us, restore it
                        results.insert(row_id);
                    }
                }
            }
        }

        // Then, apply uncommitted operations for the current transaction
        for op in self.index_versions.get_index_ops(tx_id, index_name) {
            match op {
                IndexOp::Insert {
                    values: idx_values,
                    row_id,
                    ..
                } => {
                    // Check if values fall within range
                    let in_range = match (&start_values, &end_values) {
                        (None, None) => true,
                        (Some(start), None) => &idx_values >= start,
                        (None, Some(end)) => &idx_values <= end,
                        (Some(start), Some(end)) => &idx_values >= start && &idx_values <= end,
                    };
                    if in_range {
                        results.insert(row_id);
                    }
                }
                IndexOp::Delete {
                    values: idx_values,
                    row_id,
                    ..
                } => {
                    // Check if values fall within range
                    let in_range = match (&start_values, &end_values) {
                        (None, None) => true,
                        (Some(start), None) => &idx_values >= start,
                        (None, Some(end)) => &idx_values <= end,
                        (Some(start), Some(end)) => &idx_values >= start && &idx_values <= end,
                    };
                    if in_range {
                        results.remove(&row_id);
                    }
                }
            }
        }

        Ok(results.into_iter().collect())
    }

    /// Check if a value exists in unique index
    pub fn check_unique(
        &self,
        index_name: &str,
        values: &[Value],
        tx_id: TransactionId,
    ) -> StorageResult<bool> {
        let metadata = self
            .indexes
            .get(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        if !metadata.unique {
            return Ok(false);
        }

        // NULL values don't violate unique constraints in SQL
        // Multiple NULLs are allowed in a unique column
        if values.iter().any(|v| matches!(v, Value::Null)) {
            return Ok(false);
        }

        // Check both committed and uncommitted entries including our own transaction
        let existing = self.lookup(index_name, values.to_vec(), tx_id)?;

        Ok(!existing.is_empty())
    }

    /// Build index from existing rows
    pub fn build_index(
        &mut self,
        index_name: &str,
        rows: &[(RowId, Arc<Row>)],
        column_indices: &[usize],
        tx_id: TransactionId,
    ) -> StorageResult<()> {
        for (row_id, row) in rows {
            // Extract values for index columns
            let values: Vec<Value> = column_indices
                .iter()
                .filter_map(|&idx| row.values.get(idx).cloned())
                .collect();

            if values.len() == column_indices.len() {
                self.add_entry(index_name, values, *row_id, tx_id)?;
            }
        }

        Ok(())
    }

    /// Get a streaming iterator for exact index lookup
    pub fn lookup_iter<'a>(
        &'a self,
        index_name: &str,
        values: Vec<Value>,
    ) -> StorageResult<impl Iterator<Item = StorageResult<RowId>> + 'a> {
        let partition = self
            .partitions
            .get(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?
            .clone();

        let key_prefix = encode_index_key(&values, 0);
        let prefix_len = key_prefix.len() - 8; // Exclude RowId part
        let prefix_bytes = key_prefix[..prefix_len].to_vec();

        Ok(partition.prefix(prefix_bytes).map(move |entry| {
            let (key, _value) = entry?;
            decode_row_id_from_index_key(&key)
                .ok_or_else(|| StorageError::Other("Invalid index key".to_string()))
        }))
    }

    /// Get a streaming iterator for range scan
    pub fn range_scan_iter<'a>(
        &'a self,
        index_name: &str,
        start_values: Option<Vec<Value>>,
        end_values: Option<Vec<Value>>,
    ) -> StorageResult<Box<dyn Iterator<Item = StorageResult<RowId>> + 'a>> {
        let partition = self
            .partitions
            .get(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?
            .clone();

        // Build range bounds
        let start_key = start_values.map(|v| encode_index_key(&v, 0));
        let end_key = end_values.map(|v| encode_index_key(&v, u64::MAX));

        // Create appropriate range iterator without collecting
        let iter: Box<dyn Iterator<Item = StorageResult<RowId>>> = match (start_key, end_key) {
            (None, None) => Box::new(partition.iter().map(|entry| {
                let (key, _value) = entry?;
                decode_row_id_from_index_key(&key)
                    .ok_or_else(|| StorageError::Other("Invalid index key".to_string()))
            })),
            (Some(s), None) => Box::new(partition.range(s..).map(|entry| {
                let (key, _value) = entry?;
                decode_row_id_from_index_key(&key)
                    .ok_or_else(|| StorageError::Other("Invalid index key".to_string()))
            })),
            (None, Some(e)) => Box::new(partition.range(..=e).map(|entry| {
                let (key, _value) = entry?;
                decode_row_id_from_index_key(&key)
                    .ok_or_else(|| StorageError::Other("Invalid index key".to_string()))
            })),
            (Some(s), Some(e)) => Box::new(partition.range(s..=e).map(|entry| {
                let (key, _value) = entry?;
                decode_row_id_from_index_key(&key)
                    .ok_or_else(|| StorageError::Other("Invalid index key".to_string()))
            })),
        };

        Ok(iter)
    }

    /// Batch update indexes - much more efficient than individual updates
    pub fn batch_update<F>(
        &self,
        batch: &mut Batch,
        updates: Vec<IndexUpdate>,
        get_partition: F,
    ) -> StorageResult<()>
    where
        F: Fn(&str) -> Option<&Partition>,
    {
        for update in updates {
            match update {
                IndexUpdate::Add {
                    index_name,
                    values,
                    row_id,
                    tx_id: _,
                } => {
                    if let Some(partition) = get_partition(&index_name) {
                        let key = encode_index_key(&values, row_id);
                        // Store empty value - key contains all needed info
                        batch.insert(partition, key, []);
                    }
                }
                IndexUpdate::Remove {
                    index_name,
                    values,
                    row_id,
                } => {
                    if let Some(partition) = get_partition(&index_name) {
                        let key = encode_index_key(&values, row_id);
                        batch.remove(partition, key);
                    }
                }
            }
        }
        Ok(())
    }

    /// Commit index operations for a transaction
    pub fn commit_transaction(
        &mut self,
        batch: &mut Batch,
        tx_id: TransactionId,
    ) -> StorageResult<()> {
        for op in self.index_versions.get_transaction_ops(tx_id) {
            match op {
                IndexOp::Insert {
                    index_name,
                    values,
                    row_id,
                } => {
                    if let Some(partition) = self.partitions.get(&index_name) {
                        let key = encode_index_key(&values, row_id);
                        // Store empty value - we only need the key (which encodes values + row_id)
                        // Temporal info is tracked in index_history for MVCC
                        batch.insert(partition, key, []);
                    }
                }
                IndexOp::Delete {
                    index_name,
                    values,
                    row_id,
                } => {
                    if let Some(partition) = self.partitions.get(&index_name) {
                        let key = encode_index_key(&values, row_id);
                        batch.remove(partition, key);
                    }
                }
            }
        }
        // Clear the transaction's operations from UncommittedIndexStore
        self.index_versions.clear_transaction(tx_id)?;
        Ok(())
    }

    /// Abort index operations for a transaction
    pub fn abort_transaction(&mut self, tx_id: TransactionId) -> StorageResult<()> {
        // Clear the transaction's operations from UncommittedIndexStore
        self.index_versions.clear_transaction(tx_id)?;
        Ok(())
    }
}

/// Index update operation for batching
pub enum IndexUpdate {
    Add {
        index_name: String,
        values: Vec<Value>,
        row_id: RowId,
        tx_id: TransactionId,
    },
    Remove {
        index_name: String,
        values: Vec<Value>,
        row_id: RowId,
    },
}

/// Streaming iterator for index lookups that properly holds locks
pub struct IndexLookupIterator<'a> {
    // Hold the index manager lock for the lifetime of the iterator
    _index_guard: RwLockReadGuard<'a, IndexManager>,

    // The underlying fjall iterator
    iter: FjallIterator<'a>,
}

impl<'a> IndexLookupIterator<'a> {
    pub fn new(
        index_guard: RwLockReadGuard<'a, IndexManager>,
        index_name: &str,
        values: Vec<Value>,
    ) -> StorageResult<Self> {
        let partition = index_guard
            .partitions
            .get(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        let key_prefix = encode_index_key(&values, 0);
        let prefix_len = key_prefix.len() - 8; // Exclude RowId part
        let prefix_bytes = key_prefix[..prefix_len].to_vec();

        let iter = Box::new(partition.prefix(prefix_bytes).map(|result| {
            result.map(|(k, v)| {
                let k_bytes: Box<[u8]> = k.to_vec().into_boxed_slice();
                let v_bytes: Box<[u8]> = v.to_vec().into_boxed_slice();
                (k_bytes, v_bytes)
            })
        }));

        Ok(Self {
            _index_guard: index_guard,
            iter,
        })
    }
}

impl<'a> Iterator for IndexLookupIterator<'a> {
    type Item = StorageResult<RowId>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|result| {
            let (key, _value) = result?;
            decode_row_id_from_index_key(&key)
                .ok_or_else(|| StorageError::Other("Invalid index key".to_string()))
        })
    }
}

/// Streaming iterator for index range scans that properly holds locks
pub struct IndexRangeScanIterator<'a> {
    // Hold the index manager lock for the lifetime of the iterator
    _index_guard: RwLockReadGuard<'a, IndexManager>,

    // The underlying fjall iterator
    iter: FjallIterator<'a>,
}

impl<'a> IndexRangeScanIterator<'a> {
    pub fn new(
        index_guard: RwLockReadGuard<'a, IndexManager>,
        index_name: &str,
        start_values: Option<Vec<Value>>,
        end_values: Option<Vec<Value>>,
    ) -> StorageResult<Self> {
        let partition = index_guard
            .partitions
            .get(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        // Build range bounds
        let start_key = start_values.map(|v| encode_index_key(&v, 0));
        let end_key = end_values.map(|v| encode_index_key(&v, u64::MAX));

        // Create appropriate range iterator
        let iter: FjallIterator<'a> = match (start_key, end_key) {
            (None, None) => Box::new(partition.iter().map(|result| {
                result.map(|(k, v)| {
                    let k_bytes: Box<[u8]> = k.to_vec().into_boxed_slice();
                    let v_bytes: Box<[u8]> = v.to_vec().into_boxed_slice();
                    (k_bytes, v_bytes)
                })
            })),
            (Some(s), None) => Box::new(partition.range(s..).map(|result| {
                result.map(|(k, v)| {
                    let k_bytes: Box<[u8]> = k.to_vec().into_boxed_slice();
                    let v_bytes: Box<[u8]> = v.to_vec().into_boxed_slice();
                    (k_bytes, v_bytes)
                })
            })),
            (None, Some(e)) => Box::new(partition.range(..=e).map(|result| {
                result.map(|(k, v)| {
                    let k_bytes: Box<[u8]> = k.to_vec().into_boxed_slice();
                    let v_bytes: Box<[u8]> = v.to_vec().into_boxed_slice();
                    (k_bytes, v_bytes)
                })
            })),
            (Some(s), Some(e)) => Box::new(partition.range(s..=e).map(|result| {
                result.map(|(k, v)| {
                    let k_bytes: Box<[u8]> = k.to_vec().into_boxed_slice();
                    let v_bytes: Box<[u8]> = v.to_vec().into_boxed_slice();
                    (k_bytes, v_bytes)
                })
            })),
        };

        Ok(Self {
            _index_guard: index_guard,
            iter,
        })
    }
}

impl<'a> Iterator for IndexRangeScanIterator<'a> {
    type Item = StorageResult<RowId>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|result| {
            let (key, _value) = result?;
            decode_row_id_from_index_key(&key)
                .ok_or_else(|| StorageError::Other("Invalid index key".to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjall::Keyspace;
    use proven_hlc::{HlcTimestamp, NodeId};
    use std::path::Path;

    fn create_test_keyspace() -> fjall::Keyspace {
        let test_id = uuid::Uuid::new_v4().to_string();
        let path = Path::new("/tmp").join(format!("test_index_manager_{}", test_id));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        fjall::Config::new(&path).open().unwrap()
    }

    fn create_tx_id(ts: u64) -> TransactionId {
        HlcTimestamp::new(ts, 0, NodeId::new(1))
    }

    fn create_index_versions(keyspace: &Keyspace) -> Arc<UncommittedIndexStore> {
        let partition = keyspace
            .open_partition("_index_versions_test", PartitionCreateOptions::default())
            .unwrap();
        Arc::new(UncommittedIndexStore::new(partition, keyspace.clone()))
    }

    fn create_recent_index_versions(keyspace: &Keyspace) -> Arc<IndexHistoryStore> {
        let partition = keyspace
            .open_partition(
                "_recent_index_versions_test",
                PartitionCreateOptions::default(),
            )
            .unwrap();
        Arc::new(IndexHistoryStore::new(
            partition,
            keyspace.clone(),
            std::time::Duration::from_secs(300),
        ))
    }

    #[test]
    fn test_create_and_drop_index() {
        let keyspace = create_test_keyspace();
        let index_versions = create_index_versions(&keyspace);
        let recent_index_versions = create_recent_index_versions(&keyspace);
        let mut manager = IndexManager::new(index_versions, recent_index_versions);

        // Create index
        manager
            .create_index(
                &keyspace,
                "idx_name".to_string(),
                "users".to_string(),
                vec!["name".to_string()],
                false,
            )
            .unwrap();

        assert!(manager.indexes.contains_key("idx_name"));

        // Try to create duplicate
        let result = manager.create_index(
            &keyspace,
            "idx_name".to_string(),
            "users".to_string(),
            vec!["name".to_string()],
            false,
        );
        assert!(result.is_err());

        // Drop index
        manager.drop_index("idx_name").unwrap();
        assert!(!manager.indexes.contains_key("idx_name"));
    }

    #[test]
    fn test_index_operations() {
        let keyspace = create_test_keyspace();
        let index_versions = create_index_versions(&keyspace);
        let recent_index_versions = create_recent_index_versions(&keyspace);
        let mut manager = IndexManager::new(index_versions, recent_index_versions);
        let tx_id = create_tx_id(100);

        // Create index
        manager
            .create_index(
                &keyspace,
                "idx_age".to_string(),
                "users".to_string(),
                vec!["age".to_string()],
                false,
            )
            .unwrap();

        // Add entries
        manager
            .add_entry("idx_age", vec![Value::I64(25)], 1, tx_id)
            .unwrap();
        manager
            .add_entry("idx_age", vec![Value::I64(30)], 2, tx_id)
            .unwrap();
        manager
            .add_entry("idx_age", vec![Value::I64(25)], 3, tx_id)
            .unwrap();

        // Lookup
        let results = manager
            .lookup("idx_age", vec![Value::I64(25)], tx_id)
            .unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&3));

        // Range scan
        let results = manager
            .range_scan(
                "idx_age",
                Some(vec![Value::I64(20)]),
                Some(vec![Value::I64(30)]),
                tx_id,
            )
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_unique_index() {
        let keyspace = create_test_keyspace();
        let index_versions = create_index_versions(&keyspace);
        let recent_index_versions = create_recent_index_versions(&keyspace);
        let mut manager = IndexManager::new(index_versions, recent_index_versions);
        let tx_id = create_tx_id(100);

        // Create unique index
        manager
            .create_index(
                &keyspace,
                "idx_email".to_string(),
                "users".to_string(),
                vec!["email".to_string()],
                true,
            )
            .unwrap();

        // Add entry
        let email = Value::Str("alice@example.com".to_string());
        manager
            .add_entry("idx_email", vec![email.clone()], 1, tx_id)
            .unwrap();

        // Check uniqueness
        assert!(manager.check_unique("idx_email", &[email], tx_id).unwrap());

        // Different email should be fine
        let email2 = Value::Str("bob@example.com".to_string());
        assert!(!manager.check_unique("idx_email", &[email2], tx_id).unwrap());
    }
}
