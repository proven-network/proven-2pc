//! Index management and operations

use crate::error::{Error, Result};
use crate::storage::encoding::encode_index_key;
use crate::storage::index_history::IndexHistoryStore;
use crate::storage::types::{Row, RowId};
use crate::storage::uncommitted_index::{IndexOp, UncommittedIndexStore};
use crate::types::value::Value;
use fjall::{Batch, Partition, PartitionCreateOptions};
use proven_hlc::HlcTimestamp;
use std::collections::BTreeMap;
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
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indexes: BTreeMap::new(),
            partitions: BTreeMap::new(),
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
    ) -> Result<()> {
        // Check if index already exists
        if self.indexes.contains_key(&name) {
            return Err(Error::Other(format!("Index {} already exists", name)));
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
    pub fn drop_index(&mut self, name: &str) -> Result<()> {
        self.indexes
            .remove(name)
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;
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
        uncommitted_index: &mut UncommittedIndexStore,
        index_name: &str,
        values: Vec<Value>,
        row_id: RowId,
        txn_id: HlcTimestamp,
    ) -> Result<()> {
        // Track the operation in UncommittedIndexStore
        let op = IndexOp::Insert {
            index_name: index_name.to_string(),
            values,
            row_id,
        };
        uncommitted_index.add_operation(txn_id, op)?;
        Ok(())
    }

    /// Remove an entry from an index (tracks as uncommitted)
    pub fn remove_entry(
        &mut self,
        uncommitted_index: &mut UncommittedIndexStore,
        index_name: &str,
        values: &[Value],
        row_id: RowId,
        txn_id: HlcTimestamp,
    ) -> Result<()> {
        // Track the delete operation in UncommittedIndexStore
        let op = IndexOp::Delete {
            index_name: index_name.to_string(),
            values: values.to_vec(),
            row_id,
        };
        uncommitted_index.add_operation(txn_id, op)?;
        Ok(())
    }

    /// Check if a value exists in unique index
    pub fn check_unique(
        &self,
        uncommitted_index: &UncommittedIndexStore,
        index_history: &IndexHistoryStore,
        index_name: &str,
        values: &[Value],
        txn_id: HlcTimestamp,
    ) -> Result<bool> {
        let metadata = self
            .indexes
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        if !metadata.unique {
            return Ok(false);
        }

        // NULL values don't violate unique constraints in SQL
        // Multiple NULLs are allowed in a unique column
        if values.iter().any(|v| matches!(v, Value::Null)) {
            return Ok(false);
        }

        // Check both committed and uncommitted entries including our own transaction
        // Use streaming API to check if any rows exist
        let mut existing_iter = self.lookup_iter_mvcc(
            uncommitted_index,
            index_history,
            index_name,
            values.to_vec(),
            txn_id,
        )?;

        // Check if there's at least one entry
        Ok(existing_iter.next().is_some())
    }

    /// Build index from existing rows
    pub fn build_index(
        &mut self,
        uncommitted_index: &mut UncommittedIndexStore,
        index_name: &str,
        rows: &[(RowId, Arc<Row>)],
        column_indices: &[usize],
        txn_id: HlcTimestamp,
    ) -> Result<()> {
        for (row_id, row) in rows {
            // Extract values for index columns
            let values: Vec<Value> = column_indices
                .iter()
                .filter_map(|&idx| row.values.get(idx).cloned())
                .collect();

            if values.len() == column_indices.len() {
                self.add_entry(uncommitted_index, index_name, values, *row_id, txn_id)?;
            }
        }

        Ok(())
    }

    /// Get MVCC-aware streaming iterator for exact index lookup
    pub fn lookup_iter_mvcc<'a>(
        &'a self,
        uncommitted_index: &UncommittedIndexStore,
        index_history: &crate::storage::index_history::IndexHistoryStore,
        index_name: &str,
        values: Vec<Value>,
        txn_id: HlcTimestamp,
    ) -> Result<impl Iterator<Item = Result<RowId>> + 'a> {
        // 1. Get fjall iterator
        let partition = self
            .partitions
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        let key_prefix = encode_index_key(&values, 0);
        let prefix_len = key_prefix.len() - 8;
        let prefix_bytes = key_prefix[..prefix_len].to_vec();

        let fjall_iter: crate::storage::types::FjallIterator<'a> =
            Box::new(partition.prefix(prefix_bytes).map(|result| {
                result.map(|(k, v)| {
                    let k_bytes: Box<[u8]> = k.to_vec().into_boxed_slice();
                    let v_bytes: Box<[u8]> = v.to_vec().into_boxed_slice();
                    (k_bytes, v_bytes)
                })
            }));

        // 2. Capture uncommitted operations (from our transaction)
        let uncommitted_ops: Vec<IndexOp> = uncommitted_index
            .get_index_ops(txn_id, index_name)
            .into_iter()
            .filter(|op| op.values() == values.as_slice())
            .collect();

        // 3. Capture history operations (committed AFTER our snapshot)
        let history_ops_list = index_history.get_index_ops_after(txn_id, index_name)?;
        let mut history_ops: std::collections::HashMap<RowId, Vec<IndexOp>> =
            std::collections::HashMap::new();
        for op in history_ops_list {
            if op.values() == values.as_slice() {
                history_ops.entry(op.row_id()).or_default().push(op);
            }
        }

        // 4. Create streaming iterator
        Ok(crate::storage::index_iterator::IndexLookupIterator::new(
            fjall_iter,
            uncommitted_ops,
            history_ops,
            values,
        ))
    }

    /// Get MVCC-aware streaming iterator for range scan
    pub fn range_scan_iter_mvcc<'a>(
        &'a self,
        uncommitted_index: &UncommittedIndexStore,
        index_history: &crate::storage::index_history::IndexHistoryStore,
        index_name: &str,
        start_values: Option<Vec<Value>>,
        end_values: Option<Vec<Value>>,
        txn_id: HlcTimestamp,
    ) -> Result<impl Iterator<Item = Result<RowId>> + 'a> {
        // 1. Get fjall range iterator
        let partition = self
            .partitions
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        // Build range bounds
        let start_key = start_values.as_ref().map(|v| encode_index_key(v, 0));
        let end_key = end_values.as_ref().map(|v| encode_index_key(v, u64::MAX));

        // Create appropriate range iterator
        let fjall_iter: crate::storage::types::FjallIterator<'a> = match (start_key, end_key) {
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

        // 2. Capture uncommitted operations
        let uncommitted_ops: Vec<IndexOp> = uncommitted_index
            .get_index_ops(txn_id, index_name)
            .into_iter()
            .collect();

        // 3. Capture history operations
        let history_ops_list = index_history.get_index_ops_after(txn_id, index_name)?;
        let mut history_ops: std::collections::HashMap<RowId, Vec<IndexOp>> =
            std::collections::HashMap::new();
        for op in history_ops_list {
            history_ops.entry(op.row_id()).or_default().push(op);
        }

        // 4. Create streaming iterator
        Ok(crate::storage::index_iterator::IndexRangeScanIterator::new(
            fjall_iter,
            uncommitted_ops,
            history_ops,
            start_values,
            end_values,
        ))
    }

    /// Commit index operations for a transaction
    pub fn commit_transaction(
        &mut self,
        uncommitted_index: &mut UncommittedIndexStore,
        batch: &mut Batch,
        txn_id: HlcTimestamp,
    ) -> Result<()> {
        for op in uncommitted_index.get_transaction_ops(txn_id) {
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
        uncommitted_index.clear_transaction(batch, txn_id)?;
        Ok(())
    }

    /// Abort index operations for a transaction
    pub fn abort_transaction(
        &mut self,
        uncommitted_index: &mut UncommittedIndexStore,
        batch: &mut fjall::Batch,
        txn_id: HlcTimestamp,
    ) -> Result<()> {
        // Clear the transaction's operations from UncommittedIndexStore
        uncommitted_index.clear_transaction(batch, txn_id)?;
        Ok(())
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

    fn create_txn_id(ts: u64) -> HlcTimestamp {
        HlcTimestamp::new(ts, 0, NodeId::new(1))
    }

    fn create_index_versions(keyspace: &Keyspace) -> UncommittedIndexStore {
        use crate::storage::bucket_manager::BucketManager;
        let bucket_mgr = BucketManager::new(
            keyspace.clone(),
            "_index_versions_test".to_string(),
            std::time::Duration::from_secs(30),
            PartitionCreateOptions::default(),
        );
        UncommittedIndexStore::new(bucket_mgr, std::time::Duration::from_secs(120))
    }

    fn create_recent_index_versions(keyspace: &Keyspace) -> IndexHistoryStore {
        use crate::storage::bucket_manager::BucketManager;
        let bucket_mgr = BucketManager::new(
            keyspace.clone(),
            "_recent_index_versions_test".to_string(),
            std::time::Duration::from_secs(60),
            PartitionCreateOptions::default(),
        );
        IndexHistoryStore::new(bucket_mgr, std::time::Duration::from_secs(300))
    }

    #[test]
    fn test_create_and_drop_index() {
        let keyspace = create_test_keyspace();
        let _index_versions = create_index_versions(&keyspace);
        let _recent_index_versions = create_recent_index_versions(&keyspace);
        let mut manager = IndexManager::new();

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
        let mut index_versions = create_index_versions(&keyspace);
        let recent_index_versions = create_recent_index_versions(&keyspace);
        let mut manager = IndexManager::new();
        let txn_id = create_txn_id(100);

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
            .add_entry(
                &mut index_versions,
                "idx_age",
                vec![Value::I64(25)],
                1,
                txn_id,
            )
            .unwrap();
        manager
            .add_entry(
                &mut index_versions,
                "idx_age",
                vec![Value::I64(30)],
                2,
                txn_id,
            )
            .unwrap();
        manager
            .add_entry(
                &mut index_versions,
                "idx_age",
                vec![Value::I64(25)],
                3,
                txn_id,
            )
            .unwrap();

        // Lookup (using streaming API)
        let results: Vec<_> = manager
            .lookup_iter_mvcc(
                &index_versions,
                &recent_index_versions,
                "idx_age",
                vec![Value::I64(25)],
                txn_id,
            )
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&3));

        // Range scan (using streaming API)
        let results: Vec<_> = manager
            .range_scan_iter_mvcc(
                &index_versions,
                &recent_index_versions,
                "idx_age",
                Some(vec![Value::I64(20)]),
                Some(vec![Value::I64(30)]),
                txn_id,
            )
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_unique_index() {
        let keyspace = create_test_keyspace();
        let mut index_versions = create_index_versions(&keyspace);
        let recent_index_versions = create_recent_index_versions(&keyspace);
        let mut manager = IndexManager::new();
        let txn_id = create_txn_id(100);

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
            .add_entry(
                &mut index_versions,
                "idx_email",
                vec![email.clone()],
                1,
                txn_id,
            )
            .unwrap();

        // Check uniqueness
        assert!(
            manager
                .check_unique(
                    &index_versions,
                    &recent_index_versions,
                    "idx_email",
                    &[email],
                    txn_id
                )
                .unwrap()
        );

        // Different email should be fine
        let email2 = Value::Str("bob@example.com".to_string());
        assert!(
            !manager
                .check_unique(
                    &index_versions,
                    &recent_index_versions,
                    "idx_email",
                    &[email2],
                    txn_id
                )
                .unwrap()
        );
    }
}
