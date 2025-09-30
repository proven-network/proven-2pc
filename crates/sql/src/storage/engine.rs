//! Main storage engine with fjall backend

use crate::storage::config::StorageConfig;
use crate::storage::data_history::DataHistoryStore;
use crate::storage::encoding::{deserialize, serialize};
use crate::storage::index::{IndexManager, IndexMetadata};
use crate::storage::index_history::IndexHistoryStore;
use crate::storage::types::{Row, RowId, StorageError, StorageResult, TransactionId, WriteOp};
use crate::storage::uncommitted_data::UncommittedDataStore;
use crate::storage::uncommitted_index::UncommittedIndexStore;
use crate::types::schema::Table as TableSchema;
use crate::types::value::Value;
use fjall::{Batch, Keyspace, Partition, PartitionCreateOptions};
use parking_lot::RwLock;
use proven_hlc::HlcTimestamp;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Metadata for a table
pub struct TableMetadata {
    pub schema: TableSchema,
    pub data_partition: Partition,
    pub next_row_id: AtomicU64,
}

/// Main storage engine
pub struct Storage {
    // Fjall keyspace
    keyspace: Keyspace,

    // Metadata partition
    metadata_partition: Partition,

    // Table metadata
    tables: Arc<RwLock<HashMap<String, Arc<TableMetadata>>>>,

    // Index manager
    index_manager: Arc<RwLock<IndexManager>>,

    // Index versions (uncommitted index operations)
    index_versions: Arc<UncommittedIndexStore>,

    // Uncommitted data operations - using fjall
    uncommitted_data: Arc<UncommittedDataStore>,

    // Data history (committed) for time-travel - using fjall
    data_history: Arc<DataHistoryStore>,

    // Index history (committed) for time-travel on indexes
    index_history: Arc<IndexHistoryStore>,

    // Configuration
    config: StorageConfig,
}

impl Storage {
    /// Create a new storage engine
    pub fn new(config: StorageConfig) -> StorageResult<Self> {
        Self::open_at_path(&config.data_dir.clone(), config)
    }

    /// Open storage at a specific path
    pub fn open_at_path(path: &Path, config: StorageConfig) -> StorageResult<Self> {
        // Ensure directory exists
        std::fs::create_dir_all(path)?;

        // Open fjall keyspace
        let keyspace = fjall::Config::new(path)
            .cache_size(config.block_cache_size)
            .open()?;

        // Open metadata partition
        let metadata_partition = keyspace.open_partition(
            "_metadata",
            PartitionCreateOptions::default()
                .block_size(16 * 1024) // Small blocks for metadata
                .compression(fjall::CompressionType::None),
        )?;

        // Open active versions partition (for uncommitted operations)
        let active_versions_partition = keyspace.open_partition(
            "_active_versions",
            PartitionCreateOptions::default()
                .block_size(32 * 1024)
                .compression(fjall::CompressionType::None),
        )?;
        let uncommitted_data = Arc::new(UncommittedDataStore::new(
            active_versions_partition,
            keyspace.clone(),
        ));

        // Open data history partition (for committed operations within retention window)
        let recent_versions_partition = keyspace.open_partition(
            "_recent_versions",
            PartitionCreateOptions::default()
                .block_size(64 * 1024)
                .compression(fjall::CompressionType::Lz4),
        )?;
        let data_history = Arc::new(DataHistoryStore::new(
            recent_versions_partition,
            keyspace.clone(),
            std::time::Duration::from_secs(300), // 5 minutes retention
        ));

        // Open index versions partition (for uncommitted index operations)
        let index_versions_partition = keyspace.open_partition(
            "_index_versions",
            PartitionCreateOptions::default()
                .block_size(32 * 1024)
                .compression(fjall::CompressionType::None),
        )?;
        let index_versions = Arc::new(UncommittedIndexStore::new(
            index_versions_partition,
            keyspace.clone(),
        ));

        // Open index history partition (for committed index operations within retention window)
        let recent_index_versions_partition = keyspace.open_partition(
            "_recent_index_versions",
            PartitionCreateOptions::default()
                .block_size(64 * 1024)
                .compression(fjall::CompressionType::Lz4),
        )?;
        let index_history = Arc::new(IndexHistoryStore::new(
            recent_index_versions_partition,
            keyspace.clone(),
            std::time::Duration::from_secs(300), // 5 minutes retention
        ));

        // Load existing tables
        let tables = Arc::new(RwLock::new(HashMap::new()));
        let tables_clone = tables.clone();

        // Scan metadata for existing tables
        for entry in metadata_partition.prefix("table:") {
            let (key, value) = entry?;
            let table_name = std::str::from_utf8(&key[6..])
                .map_err(|e| StorageError::Other(format!("Invalid table name: {}", e)))?
                .to_string();

            let schema: TableSchema = deserialize(&value)?;

            // Open table partitions
            let data_partition = keyspace.open_partition(
                &format!("{}_data", table_name),
                PartitionCreateOptions::default()
                    .block_size(64 * 1024)
                    .compression(config.compression),
            )?;

            // Find max row ID
            let mut max_row_id = 0u64;
            for entry in data_partition.iter() {
                let (key, _) = entry?;
                if key.len() == 8 {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&key);
                    let row_id = u64::from_be_bytes(bytes);
                    max_row_id = max_row_id.max(row_id);
                }
            }

            let metadata = Arc::new(TableMetadata {
                schema,
                data_partition,
                next_row_id: AtomicU64::new(max_row_id + 1),
            });

            tables_clone.write().insert(table_name, metadata);
        }

        Ok(Self {
            keyspace,
            metadata_partition,
            tables,
            index_manager: Arc::new(RwLock::new(IndexManager::new(
                index_versions.clone(),
                index_history.clone(),
            ))),
            index_versions,
            uncommitted_data,
            data_history,
            index_history,
            config,
        })
    }

    /// Create a new table
    pub fn create_table(&self, name: String, schema: TableSchema) -> StorageResult<()> {
        // Check if table already exists
        if self.tables.read().contains_key(&name) {
            return Err(StorageError::DuplicateTable(name.clone()));
        }

        // Create partitions
        let data_partition = self.keyspace.open_partition(
            &format!("{}_data", name),
            PartitionCreateOptions::default()
                .block_size(64 * 1024)
                .compression(self.config.compression),
        )?;

        // Store schema in metadata
        self.metadata_partition
            .insert(format!("table:{}", name), serialize(&schema)?)?;

        // Create table metadata
        let metadata = Arc::new(TableMetadata {
            schema: schema.clone(),
            data_partition,
            next_row_id: AtomicU64::new(1),
        });

        // Add to tables map
        self.tables.write().insert(name.clone(), metadata);

        // Persist changes
        self.keyspace.persist(self.config.persist_mode)?;

        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&self, name: &str) -> StorageResult<()> {
        // Remove from tables map and get partition handles
        let metadata = self
            .tables
            .write()
            .remove(name)
            .ok_or_else(|| StorageError::TableNotFound(name.to_string()))?;

        // Remove schema from metadata
        self.metadata_partition.remove(format!("table:{}", name))?;

        // Clear any active versions for this table
        self.uncommitted_data.remove_table(name)?;

        // Get partition handle before dropping
        let data_partition = metadata.data_partition.clone();

        // Clear all data from partition before dropping
        // This ensures data doesn't persist when partition is recreated
        // Iterate and remove all entries
        let mut batch = self.keyspace.batch();
        for entry in data_partition.iter() {
            let (key, _) = entry?;
            batch.remove(&data_partition, key);
        }
        batch.commit()?;

        // Release the metadata Arc
        drop(metadata);

        // Drop partition using handle
        self.keyspace.delete_partition(data_partition)?;

        // Persist changes
        self.keyspace.persist(self.config.persist_mode)?;

        Ok(())
    }

    /// Create an index on a table
    pub fn create_index(
        &self,
        tx_id: HlcTimestamp,
        index_name: String,
        table_name: String,
        columns: Vec<String>,
        unique: bool,
    ) -> StorageResult<()> {
        // Verify table exists
        let tables = self.tables.read();
        let table_meta = tables
            .get(&table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Verify columns exist in table
        for column in &columns {
            if !table_meta.schema.has_column(column) {
                return Err(StorageError::Other(format!(
                    "Column {} not found in table {}",
                    column, table_name
                )));
            }
        }

        // Get column indices for the index
        let column_indices: Vec<usize> = columns
            .iter()
            .filter_map(|col| table_meta.schema.column_index(col))
            .collect();

        if column_indices.len() != columns.len() {
            return Err(StorageError::Other(
                "Failed to resolve all column indices".to_string(),
            ));
        }

        // Create the index
        self.index_manager.write().create_index(
            &self.keyspace,
            index_name.clone(),
            table_name.clone(),
            columns.clone(),
            unique,
        )?;

        // Build index from existing data
        // Collect all existing rows
        let mut rows_to_index = Vec::new();
        for row_result in self.iter(tx_id, &table_name)? {
            let row = row_result?;
            rows_to_index.push((row.id, row));
        }

        // Build the index
        self.index_manager.write().build_index(
            &index_name,
            &rows_to_index,
            &column_indices,
            tx_id,
        )?;

        // Persist metadata about the index
        let index_meta_key = format!("index:{}", index_name);
        let index_metadata = IndexMetadata {
            name: index_name.clone(),
            table: table_name,
            columns,
            index_type: if unique {
                crate::storage::index::IndexType::Unique
            } else {
                crate::storage::index::IndexType::BTree
            },
            unique,
        };

        self.metadata_partition
            .insert(index_meta_key, serialize(&index_metadata)?)?;

        self.keyspace.persist(self.config.persist_mode)?;
        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&self, index_name: &str) -> StorageResult<()> {
        // Remove the index
        self.index_manager.write().drop_index(index_name)?;

        // Remove metadata
        self.metadata_partition
            .remove(format!("index:{}", index_name))?;

        self.keyspace.persist(self.config.persist_mode)?;
        Ok(())
    }

    /// Lookup rows by index (exact match) - optimized version
    pub fn index_lookup(
        &self,
        index_name: &str,
        values: Vec<Value>,
        tx_id: TransactionId,
    ) -> StorageResult<Vec<Arc<Row>>> {
        // Cache metadata lookup outside the loop
        let index_meta_key = format!("index:{}", index_name);
        let meta_bytes = self
            .metadata_partition
            .get(&index_meta_key)?
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;
        let index_meta: IndexMetadata = deserialize(&meta_bytes)?;
        let table_name = index_meta.table.clone();

        // Get row IDs from index
        let row_ids = self
            .index_manager
            .read()
            .lookup(index_name, values, tx_id)?;

        // Read actual rows (checking MVCC visibility)
        let mut results = Vec::new();
        for row_id in row_ids {
            if let Some(row) = self.read(tx_id, &table_name, row_id)? {
                results.push(row);
            }
        }

        Ok(results)
    }

    /// Get a streaming iterator for index lookup
    pub fn index_lookup_iter<'a>(
        &'a self,
        index_name: &str,
        values: Vec<Value>,
    ) -> StorageResult<crate::storage::index::IndexLookupIterator<'a>> {
        let index_guard = self.index_manager.read();
        crate::storage::index::IndexLookupIterator::new(index_guard, index_name, values)
    }

    /// Get a streaming iterator for index range scan
    pub fn index_range_scan_iter<'a>(
        &'a self,
        index_name: &str,
        start_values: Option<Vec<Value>>,
        end_values: Option<Vec<Value>>,
    ) -> StorageResult<crate::storage::index::IndexRangeScanIterator<'a>> {
        let index_guard = self.index_manager.read();
        crate::storage::index::IndexRangeScanIterator::new(
            index_guard,
            index_name,
            start_values,
            end_values,
        )
    }

    /// Range scan on index - optimized version
    pub fn index_range_scan(
        &self,
        index_name: &str,
        start_values: Option<Vec<Value>>,
        end_values: Option<Vec<Value>>,
        tx_id: TransactionId,
    ) -> StorageResult<Vec<Arc<Row>>> {
        // Cache metadata lookup outside the loop
        let index_meta_key = format!("index:{}", index_name);
        let meta_bytes = self
            .metadata_partition
            .get(&index_meta_key)?
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;
        let index_meta: IndexMetadata = deserialize(&meta_bytes)?;
        let table_name = index_meta.table.clone();

        // Get row IDs from index
        let row_ids =
            self.index_manager
                .read()
                .range_scan(index_name, start_values, end_values, tx_id)?;

        // Read actual rows (checking MVCC visibility)
        let mut results = Vec::new();
        for row_id in row_ids {
            if let Some(row) = self.read(tx_id, &table_name, row_id)? {
                results.push(row);
            }
        }

        Ok(results)
    }

    /// Check if values would violate a unique constraint
    pub fn check_unique_constraint(
        &self,
        index_name: &str,
        values: &[Value],
        tx_id: TransactionId,
    ) -> StorageResult<bool> {
        self.index_manager
            .read()
            .check_unique(index_name, values, tx_id)
    }

    /// Insert a row
    pub fn insert(
        &self,
        tx_id: TransactionId,
        table: &str,
        values: Vec<crate::types::value::Value>,
    ) -> StorageResult<RowId> {
        // Get table metadata
        let tables = self.tables.read();
        let table_meta = tables
            .get(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;

        // Validate values against schema
        table_meta.schema.validate_row(&values)?;

        // Generate row ID
        let row_id = table_meta.next_row_id.fetch_add(1, Ordering::SeqCst);

        // Create row
        let row = Arc::new(Row::new(row_id, values));

        // Add to active versions
        let write_op = WriteOp::Insert {
            table: table.to_string(),
            row_id,
            row: row.clone(),
        };

        self.uncommitted_data.add_write(tx_id, write_op.clone())?;

        // Update indexes for this table
        self.update_indexes_on_insert(table, &row, table_meta, tx_id)?;

        Ok(row_id)
    }

    /// Insert multiple rows atomically - all validation checks are done before any inserts
    pub fn insert_batch(
        &self,
        tx_id: TransactionId,
        table: &str,
        values_batch: Vec<Vec<crate::types::value::Value>>,
    ) -> StorageResult<Vec<RowId>> {
        // Get table metadata
        let tables = self.tables.read();
        let table_meta = tables
            .get(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;

        // Phase 1: Validate all rows and prepare data structures
        let mut prepared_rows = Vec::new();
        for values in values_batch {
            // Validate values against schema
            table_meta.schema.validate_row(&values)?;

            // Generate row ID
            let row_id = table_meta.next_row_id.fetch_add(1, Ordering::SeqCst);

            // Create row
            let row = Arc::new(Row::new(row_id, values));
            prepared_rows.push((row_id, row));
        }

        // Phase 2: Check for duplicates within the batch itself
        let indexes_to_update = self.get_indexes_for_table(table)?;
        let mut batch_unique_values: HashMap<String, HashSet<Vec<Value>>> = HashMap::new();

        for (_row_id, row) in &prepared_rows {
            for (index_name, index_meta) in &indexes_to_update {
                if !index_meta.unique {
                    continue;
                }

                // Get column indices for the index
                let column_indices: Vec<usize> = index_meta
                    .columns
                    .iter()
                    .filter_map(|col| table_meta.schema.column_index(col))
                    .collect();

                // Extract values for index columns
                let index_values: Vec<Value> = column_indices
                    .iter()
                    .filter_map(|&idx| row.values.get(idx).cloned())
                    .collect();

                if index_values.len() == column_indices.len() {
                    // Check if we've seen these values before in this batch
                    let values_set = batch_unique_values.entry(index_name.clone()).or_default();
                    if !values_set.insert(index_values.clone()) {
                        return Err(StorageError::UniqueConstraintViolation(format!(
                            "Duplicate value within batch for unique index {}",
                            index_name
                        )));
                    }
                }
            }
        }

        // Phase 3: Check unique constraints against existing data
        for (_row_id, row) in &prepared_rows {
            for (index_name, index_meta) in &indexes_to_update {
                if !index_meta.unique {
                    continue;
                }

                // Get column indices for the index
                let column_indices: Vec<usize> = index_meta
                    .columns
                    .iter()
                    .filter_map(|col| table_meta.schema.column_index(col))
                    .collect();

                // Extract values for index columns
                let index_values: Vec<Value> = column_indices
                    .iter()
                    .filter_map(|&idx| row.values.get(idx).cloned())
                    .collect();

                if index_values.len() == column_indices.len() {
                    // Check against existing committed and uncommitted data
                    if self
                        .index_manager
                        .read()
                        .check_unique(index_name, &index_values, tx_id)?
                    {
                        return Err(StorageError::UniqueConstraintViolation(format!(
                            "Duplicate value for unique index {}",
                            index_name
                        )));
                    }
                }
            }
        }

        // Phase 4: All validations passed - now insert all rows and update indexes
        let mut inserted_row_ids = Vec::new();
        for (row_id, row) in prepared_rows {
            // Add to active versions
            let write_op = WriteOp::Insert {
                table: table.to_string(),
                row_id,
                row: row.clone(),
            };

            self.uncommitted_data.add_write(tx_id, write_op.clone())?;

            // Update indexes for this table
            self.update_indexes_on_insert_unchecked(table, &row, table_meta, tx_id)?;

            inserted_row_ids.push(row_id);
        }

        Ok(inserted_row_ids)
    }

    /// Update a row
    pub fn update(
        &self,
        tx_id: TransactionId,
        table: &str,
        row_id: RowId,
        values: Vec<crate::types::value::Value>,
    ) -> StorageResult<()> {
        // Get table metadata
        let tables = self.tables.read();
        let table_meta = tables
            .get(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;

        // Validate values against schema
        table_meta.schema.validate_row(&values)?;

        // Read current row
        let old_row = self
            .read(tx_id, table, row_id)?
            .ok_or(StorageError::RowNotFound(row_id))?;

        // Create new row
        let new_row = Arc::new(Row::new(row_id, values));

        // Add to active versions
        let write_op = WriteOp::Update {
            table: table.to_string(),
            row_id,
            old_row: old_row.clone(),
            new_row: new_row.clone(),
        };

        self.uncommitted_data.add_write(tx_id, write_op.clone())?;

        // Update indexes for this table (remove old, add new)
        self.update_indexes_on_update(table, &old_row, &new_row, table_meta, tx_id)?;

        Ok(())
    }

    /// Delete a row
    pub fn delete(&self, tx_id: TransactionId, table: &str, row_id: RowId) -> StorageResult<()> {
        // Read current row
        let row = self
            .read(tx_id, table, row_id)?
            .ok_or(StorageError::RowNotFound(row_id))?;

        // Mark as deleted
        let deleted_row = Arc::new(Row {
            id: row_id,
            values: row.values.clone(),
            deleted: true,
        });

        // Add to active versions
        let write_op = WriteOp::Delete {
            table: table.to_string(),
            row_id,
            row: deleted_row.clone(),
        };

        self.uncommitted_data.add_write(tx_id, write_op.clone())?;

        // Update indexes for this table (remove entries)
        let tables = self.tables.read();
        if let Some(table_meta) = tables.get(table) {
            self.update_indexes_on_delete(table, &row, table_meta, tx_id)?;
        }

        Ok(())
    }

    /// Read a row
    pub fn read(
        &self,
        tx_id: TransactionId,
        table: &str,
        row_id: RowId,
    ) -> StorageResult<Option<Arc<Row>>> {
        // L1: Check active transaction writes
        match self.uncommitted_data.get_row(tx_id, table, row_id) {
            Some(Some(row)) => {
                // Row exists in active transaction
                if !row.deleted {
                    return Ok(Some(row));
                } else {
                    return Ok(None); // Marked as deleted
                }
            }
            Some(None) => {
                // Row was deleted in active transaction
                return Ok(None);
            }
            None => {
                // No operations for this row in active transaction, check disk
            }
        }

        // L2: Read from disk with time-travel support
        let tables = self.tables.read();
        let table_meta = tables
            .get(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;

        // Try to read the row from disk
        let row_key = crate::storage::encoding::encode_row_key(row_id);
        if let Some(row_bytes) = table_meta.data_partition.get(row_key)? {
            let row: Row = deserialize(&row_bytes)?;

            // Check if this row has been modified recently (< 5 minutes)
            // If not, it's visible (committed > 5 minutes ago)
            if !self.data_history.is_empty() {
                // Get all recent ops for this table and filter by row_id
                let table_ops = self.data_history.get_table_ops_after(tx_id, table)?;

                if let Some(ops_for_row) = table_ops.get(&row_id) {
                    let mut historical_row = row.clone();

                    // Apply operations in reverse to reconstruct historical state
                    for op in ops_for_row.iter().rev() {
                        match op {
                            WriteOp::Update {
                                old_row, new_row, ..
                            } => {
                                // If current row matches new_row, revert to old_row
                                if historical_row.values == new_row.values {
                                    historical_row = (**old_row).clone();
                                }
                            }
                            WriteOp::Insert { .. } => {
                                // Row was inserted after our snapshot, so it shouldn't exist
                                return Ok(None);
                            }
                            WriteOp::Delete {
                                row: deleted_row, ..
                            } => {
                                // Row was deleted after our snapshot, restore it
                                historical_row = (**deleted_row).clone();
                            }
                        }
                    }
                    return Ok(Some(Arc::new(historical_row)));
                }
            }

            // Row exists and hasn't been modified recently - it's visible
            return Ok(Some(Arc::new(row)));
        }

        // Row doesn't exist in data partition
        Ok(None)
    }

    /// Scan all rows in a table (backwards compatible Vec-based version)
    pub fn scan(&self, tx_id: TransactionId, table: &str) -> StorageResult<Vec<Arc<Row>>> {
        // Use the new streaming iterator and collect results
        let iterator = self.iter(tx_id, table)?;
        let mut results = Vec::new();

        for row_result in iterator {
            results.push(row_result?);
        }

        Ok(results)
    }

    /// Get an iterator over all visible rows in a table
    /// The iterator holds necessary locks for its lifetime
    pub fn iter<'a>(
        &'a self,
        tx_id: TransactionId,
        table: &str,
    ) -> StorageResult<crate::storage::iterator::TableIterator<'a>> {
        let tables_guard = self.tables.read();
        let active_versions_clone = self.uncommitted_data.clone();
        let recent_versions_clone = self.data_history.clone();

        crate::storage::iterator::TableIterator::new(
            tx_id,
            tables_guard,
            active_versions_clone,
            recent_versions_clone,
            table,
        )
    }

    /// Get an iterator with row IDs (for UPDATE/DELETE operations)
    pub fn iter_with_ids<'a>(
        &'a self,
        tx_id: TransactionId,
        table: &str,
    ) -> StorageResult<crate::storage::iterator::TableIteratorWithIds<'a>> {
        let tables_guard = self.tables.read();
        let active_versions_clone = self.uncommitted_data.clone();
        let recent_versions_clone = self.data_history.clone();

        crate::storage::iterator::TableIteratorWithIds::new(
            tx_id,
            tables_guard,
            active_versions_clone,
            recent_versions_clone,
            table,
        )
    }

    /// Get a reverse iterator (scanning backwards)
    pub fn iter_reverse<'a>(
        &'a self,
        tx_id: TransactionId,
        table: &str,
    ) -> StorageResult<crate::storage::iterator::TableIteratorReverse<'a>> {
        let tables_guard = self.tables.read();
        let active_versions_clone = self.uncommitted_data.clone();
        let recent_versions_clone = self.data_history.clone();

        crate::storage::iterator::TableIteratorReverse::new(
            tx_id,
            tables_guard,
            active_versions_clone,
            recent_versions_clone,
            table,
        )
    }

    /// Get all table schemas
    pub fn get_schemas(&self) -> HashMap<String, TableSchema> {
        let tables = self.tables.read();
        tables
            .iter()
            .map(|(name, meta)| (name.clone(), meta.schema.clone()))
            .collect()
    }

    /// Get all index metadata
    pub fn get_index_metadata(&self) -> HashMap<String, crate::storage::index::IndexMetadata> {
        self.index_manager
            .read()
            .get_all_metadata()
            .into_iter()
            .collect()
    }

    /// Check if a table exists
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.tables.read().contains_key(table_name)
    }

    /// Get index columns for a specific index
    pub fn get_index_columns(&self, table_name: &str, index_name: &str) -> Option<Vec<String>> {
        self.index_manager
            .read()
            .get_index_metadata(index_name)
            .filter(|meta| meta.table == table_name)
            .map(|meta| meta.columns.clone())
    }

    /// Check if an index exists on a table
    pub fn has_index(&self, table_name: &str, index_name: &str) -> bool {
        // Check both the direct index name and the primary key index name
        let pk_index_name = format!("{}_{}_pkey", table_name, index_name);

        self.index_manager
            .read()
            .get_index_metadata(index_name)
            .map(|meta| meta.table == table_name)
            .unwrap_or(false)
            || self
                .index_manager
                .read()
                .get_index_metadata(&pk_index_name)
                .map(|meta| meta.table == table_name)
                .unwrap_or(false)
    }

    /// Perform index lookup for exact match
    pub fn index_lookup_rows(
        &self,
        index_name: &str,
        values: Vec<Value>,
        tx_id: TransactionId,
    ) -> StorageResult<Vec<Arc<Row>>> {
        // Try the direct index name first
        match self.index_lookup(index_name, values.clone(), tx_id) {
            Ok(rows) if !rows.is_empty() => Ok(rows),
            _ => {
                // If not found or empty, try the primary key index name
                // Get table name from index metadata
                if let Some(meta) = self.index_manager.read().get_index_metadata(index_name) {
                    let pk_index_name = format!("{}_{}_pkey", meta.table, index_name);
                    self.index_lookup(&pk_index_name, values, tx_id)
                } else {
                    // Try to infer table name from the pattern
                    // This is a fallback for when column name is used as index_name
                    // Look for any index that has this column
                    let all_indexes = self.index_manager.read().get_all_metadata();
                    for (idx_name, idx_meta) in all_indexes {
                        if idx_meta.columns.contains(&index_name.to_string()) {
                            return self.index_lookup(&idx_name, values, tx_id);
                        }
                    }
                    Ok(vec![])
                }
            }
        }
    }

    /// Perform index range lookup
    #[allow(clippy::too_many_arguments)]
    pub fn index_range_lookup_rows(
        &self,
        index_name: &str,
        start_values: Option<Vec<Value>>,
        _start_inclusive: bool,
        end_values: Option<Vec<Value>>,
        _end_inclusive: bool,
        reverse: bool,
        tx_id: TransactionId,
    ) -> StorageResult<Vec<Arc<Row>>> {
        // Get index metadata
        let index_meta = self
            .index_manager
            .read()
            .get_index_metadata(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?
            .clone();

        // Use the range scan method
        // Note: The current index manager doesn't support inclusive/exclusive bounds or reverse
        // We'll need to filter results accordingly
        let mut row_ids = self.index_manager.read().range_scan(
            index_name,
            start_values.clone(),
            end_values.clone(),
            tx_id,
        )?;

        // Apply reverse if needed
        if reverse {
            row_ids.reverse();
        }

        // Read actual rows (checking MVCC visibility)
        let mut results = Vec::new();
        for row_id in row_ids {
            if let Some(row) = self.read(tx_id, &index_meta.table, row_id)? {
                results.push(row);
            }
        }

        Ok(results)
    }

    /// Execute DDL operations
    pub fn execute_ddl(
        &self,
        plan: &crate::planning::plan::Plan,
        tx_id: TransactionId,
    ) -> StorageResult<String> {
        use crate::planning::plan::Plan;

        match plan {
            Plan::CreateTable {
                name,
                schema,
                foreign_keys: _,
                if_not_exists,
            } => {
                // TODO: Store and validate foreign key constraints
                // For now, just create the table without FK validation
                match self.create_table(name.clone(), schema.clone()) {
                    Ok(_) => {
                        // Create indexes for PRIMARY KEY columns
                        if let Some(pk_idx) = schema.primary_key
                            && pk_idx < schema.columns.len()
                        {
                            let pk_column = &schema.columns[pk_idx];
                            let index_name = format!("{}_{}_pkey", name, pk_column.name);
                            // Create a unique index for the primary key
                            self.create_index(
                                tx_id,
                                index_name,
                                name.clone(),
                                vec![pk_column.name.clone()],
                                true, // unique
                            )?;
                        }

                        // Create indexes for columns with UNIQUE constraints
                        for (col_idx, column) in schema.columns.iter().enumerate() {
                            // Skip if this is the primary key (already handled above)
                            if Some(col_idx) == schema.primary_key {
                                continue;
                            }

                            // Create unique index if column has UNIQUE constraint
                            if column.unique {
                                let index_name = format!("{}_{}_unique", name, column.name);
                                self.create_index(
                                    tx_id,
                                    index_name,
                                    name.clone(),
                                    vec![column.name.clone()],
                                    true, // unique
                                )?;
                            } else if column.index {
                                // Create regular (non-unique) index if column has INDEX constraint
                                let index_name = format!("{}_{}_idx", name, column.name);
                                self.create_index(
                                    tx_id,
                                    index_name,
                                    name.clone(),
                                    vec![column.name.clone()],
                                    false, // not unique
                                )?;
                            }
                        }
                        Ok(format!("Table '{}' created", name))
                    }
                    Err(StorageError::DuplicateTable(_)) if *if_not_exists => Ok(format!(
                        "Table '{}' already exists (IF NOT EXISTS specified)",
                        name
                    )),
                    Err(e) => Err(e),
                }
            }

            Plan::DropTable {
                names,
                if_exists,
                cascade: _,
            } => {
                let mut dropped_count = 0;
                let mut errors = Vec::new();

                for name in names {
                    match self.drop_table(name) {
                        Ok(_) => dropped_count += 1,
                        Err(StorageError::TableNotFound(_)) if *if_exists => {
                            // Ignore if IF EXISTS is specified
                        }
                        Err(e) => errors.push((name.clone(), e)),
                    }
                }

                if !errors.is_empty() {
                    // Return first error
                    let (_name, err) = errors.into_iter().next().unwrap();
                    return Err(err);
                }

                if dropped_count == 0 && *if_exists {
                    Ok("No tables dropped (IF EXISTS specified)".to_string())
                } else if dropped_count == 1 {
                    Ok("Table dropped".to_string())
                } else {
                    Ok(format!("{} tables dropped", dropped_count))
                }
            }

            Plan::CreateIndex {
                name,
                table,
                columns,
                unique,
                included_columns: _,
            } => {
                // Check if table exists
                if !self.tables.read().contains_key(table) {
                    return Err(StorageError::TableNotFound(table.clone()));
                }

                // Extract column names from IndexColumn structs
                // For now, we'll just use the expression as a string
                // TODO: Properly evaluate IndexColumn expressions
                let column_names: Vec<String> = columns
                    .iter()
                    .map(|_col| {
                        // Simplified: just use a placeholder for now
                        // In a full implementation, we'd parse the expression
                        "column".to_string()
                    })
                    .collect();

                // Try to create the index
                match self.index_manager.write().create_index(
                    &self.keyspace,
                    name.clone(),
                    table.clone(),
                    column_names,
                    *unique,
                ) {
                    Ok(_) => Ok(format!("Index '{}' created", name)),
                    Err(StorageError::Other(msg)) if msg.contains("already exists") => {
                        Ok(format!("Index '{}' already exists", name))
                    }
                    Err(e) => Err(e),
                }
            }

            Plan::DropIndex { name, if_exists } => {
                match self.index_manager.write().drop_index(name) {
                    Ok(_) => Ok(format!("Index '{}' dropped", name)),
                    Err(StorageError::IndexNotFound(_)) if *if_exists => Ok(format!(
                        "Index '{}' does not exist (IF EXISTS specified)",
                        name
                    )),
                    Err(e) => Err(e),
                }
            }

            _ => Err(StorageError::InvalidOperation(
                "Unsupported DDL operation".to_string(),
            )),
        }
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, tx_id: TransactionId) -> StorageResult<()> {
        // Get writes for this transaction
        let writes = self.uncommitted_data.get_transaction_writes(tx_id);

        // Create batch for atomic commit (even if no writes, we might have index ops)
        let mut batch = self.keyspace.batch();

        // Separate data and index operations
        let mut data_ops = Vec::new();

        if !writes.is_empty() {
            for write in &writes {
                match write {
                    WriteOp::Insert { table, row_id, row } => {
                        self.persist_insert(&mut batch, table, *row_id, row.clone(), tx_id)?;
                        data_ops.push(write.clone());
                    }
                    WriteOp::Update {
                        table,
                        row_id,
                        new_row,
                        ..
                    } => {
                        self.persist_update(&mut batch, table, *row_id, new_row.clone(), tx_id)?;
                        data_ops.push(write.clone());
                    }
                    WriteOp::Delete { table, row_id, .. } => {
                        self.persist_delete(&mut batch, table, *row_id, tx_id)?;
                        data_ops.push(write.clone());
                    }
                }
            }
        }

        // Get index operations before committing (so we can move to index_history)
        let index_ops = self.index_versions.get_transaction_ops(tx_id);

        // Commit index operations to the batch
        self.index_manager
            .write()
            .commit_transaction(&mut batch, tx_id)?;

        // Add data operations to data_history (in the same batch)
        // OPTIMIZED: Uses commit_time-first key design for efficient range queries
        if !writes.is_empty() {
            self.data_history
                .add_committed_ops_to_batch(&mut batch, tx_id, writes)?;
        }

        // Add index operations to index_history (in the same batch)
        // OPTIMIZED: Uses commit_time-first key design for efficient range queries
        if !index_ops.is_empty() {
            self.index_history
                .add_committed_ops_to_batch(&mut batch, tx_id, index_ops)?;
        }

        // Remove from uncommitted stores (this is fast, no disk I/O)
        self.uncommitted_data
            .remove_transaction(&mut batch, tx_id)?;

        // Commit single atomic batch (includes data, indexes, and history)
        batch.commit()?;

        // Persist to disk once
        self.keyspace.persist(self.config.persist_mode)?;

        Ok(())
    }

    /// Abort a transaction
    pub fn abort_transaction(&self, tx_id: TransactionId) -> StorageResult<()> {
        // Create batch for atomic commit (even if no writes, we might have index ops)
        let mut batch = self.keyspace.batch();

        // Remove from active versions
        self.uncommitted_data
            .remove_transaction(&mut batch, tx_id)?;

        // Abort index operations
        self.index_manager.write().abort_transaction(tx_id)?;

        Ok(())
    }

    // Helper methods for persisting operations

    fn persist_insert(
        &self,
        batch: &mut Batch,
        table: &str,
        row_id: RowId,
        row: Arc<Row>,
        _tx_id: TransactionId,
    ) -> StorageResult<()> {
        let tables = self.tables.read();
        let table_meta = tables
            .get(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;

        // Write row data
        let row_key = crate::storage::encoding::encode_row_key(row_id);
        batch.insert(&table_meta.data_partition, row_key, serialize(&*row)?);

        Ok(())
    }

    fn persist_update(
        &self,
        batch: &mut Batch,
        table: &str,
        row_id: RowId,
        row: Arc<Row>,
        _tx_id: TransactionId,
    ) -> StorageResult<()> {
        let tables = self.tables.read();
        let table_meta = tables
            .get(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;

        // Update row data
        let row_key = crate::storage::encoding::encode_row_key(row_id);
        batch.insert(&table_meta.data_partition, row_key, serialize(&*row)?);

        Ok(())
    }

    fn persist_delete(
        &self,
        batch: &mut Batch,
        table: &str,
        row_id: RowId,
        _tx_id: TransactionId,
    ) -> StorageResult<()> {
        let tables = self.tables.read();
        let table_meta = tables
            .get(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;

        // Delete the actual row data
        let row_key = crate::storage::encoding::encode_row_key(row_id);
        batch.remove(&table_meta.data_partition, row_key);

        Ok(())
    }

    /// Get table schema
    pub fn get_table_schema(&self, table: &str) -> StorageResult<TableSchema> {
        let tables = self.tables.read();
        let table_meta = tables
            .get(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;
        Ok(table_meta.schema.clone())
    }

    /// List all tables
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    // Helper methods for index maintenance

    fn update_indexes_on_insert(
        &self,
        table_name: &str,
        row: &Arc<Row>,
        table_meta: &TableMetadata,
        tx_id: TransactionId,
    ) -> StorageResult<()> {
        // Get all indexes for this table
        let indexes_to_update = self.get_indexes_for_table(table_name)?;

        for (index_name, index_meta) in indexes_to_update {
            // Get column indices for the index
            let column_indices: Vec<usize> = index_meta
                .columns
                .iter()
                .filter_map(|col| table_meta.schema.column_index(col))
                .collect();

            // Extract values for index columns
            let index_values: Vec<Value> = column_indices
                .iter()
                .filter_map(|&idx| row.values.get(idx).cloned())
                .collect();

            if index_values.len() == column_indices.len() {
                // Check unique constraint if needed BEFORE adding
                if index_meta.unique
                    && self
                        .index_manager
                        .read()
                        .check_unique(&index_name, &index_values, tx_id)?
                {
                    return Err(StorageError::UniqueConstraintViolation(format!(
                        "Duplicate value for unique index {}",
                        index_name
                    )));
                }

                // Add to index
                self.index_manager
                    .write()
                    .add_entry(&index_name, index_values, row.id, tx_id)?;
            }
        }

        Ok(())
    }

    /// Update indexes on insert without unique constraint checking (used in batch operations)
    fn update_indexes_on_insert_unchecked(
        &self,
        table_name: &str,
        row: &Arc<Row>,
        table_meta: &TableMetadata,
        tx_id: TransactionId,
    ) -> StorageResult<()> {
        // Get all indexes for this table
        let indexes_to_update = self.get_indexes_for_table(table_name)?;

        for (index_name, index_meta) in indexes_to_update {
            // Get column indices for the index
            let column_indices: Vec<usize> = index_meta
                .columns
                .iter()
                .filter_map(|col| table_meta.schema.column_index(col))
                .collect();

            // Extract values for index columns
            let index_values: Vec<Value> = column_indices
                .iter()
                .filter_map(|&idx| row.values.get(idx).cloned())
                .collect();

            if index_values.len() == column_indices.len() {
                // Skip unique constraint checking - add to index directly
                self.index_manager
                    .write()
                    .add_entry(&index_name, index_values, row.id, tx_id)?;
            }
        }

        Ok(())
    }

    fn update_indexes_on_update(
        &self,
        table_name: &str,
        old_row: &Arc<Row>,
        new_row: &Arc<Row>,
        table_meta: &TableMetadata,
        tx_id: TransactionId,
    ) -> StorageResult<()> {
        // Get all indexes for this table
        let indexes_to_update = self.get_indexes_for_table(table_name)?;

        // Collect all index updates that need to be made
        let mut updates = Vec::new();

        for (index_name, index_meta) in &indexes_to_update {
            // Get column indices for the index
            let column_indices: Vec<usize> = index_meta
                .columns
                .iter()
                .filter_map(|col| table_meta.schema.column_index(col))
                .collect();

            // Extract old values for index columns
            let old_index_values: Vec<Value> = column_indices
                .iter()
                .filter_map(|&idx| old_row.values.get(idx).cloned())
                .collect();

            // Extract new values for index columns
            let new_index_values: Vec<Value> = column_indices
                .iter()
                .filter_map(|&idx| new_row.values.get(idx).cloned())
                .collect();

            // Only update if values actually changed
            if old_index_values != new_index_values
                && old_index_values.len() == column_indices.len()
                && new_index_values.len() == column_indices.len()
            {
                updates.push((
                    index_name.clone(),
                    index_meta.clone(),
                    old_index_values,
                    new_index_values,
                ));
            }
        }

        // Phase 1: Check all unique constraints BEFORE making any changes
        for (index_name, index_meta, _old_values, new_values) in &updates {
            if index_meta.unique
                && self
                    .index_manager
                    .read()
                    .check_unique(index_name, new_values, tx_id)?
            {
                return Err(StorageError::UniqueConstraintViolation(format!(
                    "Duplicate value for unique index {}",
                    index_name
                )));
            }
        }

        // Phase 2: All constraints passed - now apply the changes
        for (index_name, _index_meta, old_values, new_values) in updates {
            // Remove old entry
            self.index_manager
                .write()
                .remove_entry(&index_name, &old_values, old_row.id, tx_id)?;

            // Add new entry
            self.index_manager
                .write()
                .add_entry(&index_name, new_values, new_row.id, tx_id)?;
        }

        Ok(())
    }

    fn update_indexes_on_delete(
        &self,
        table_name: &str,
        row: &Arc<Row>,
        table_meta: &TableMetadata,
        _tx_id: TransactionId,
    ) -> StorageResult<()> {
        // Get all indexes for this table
        let indexes_to_update = self.get_indexes_for_table(table_name)?;

        for (index_name, index_meta) in indexes_to_update {
            // Get column indices for the index
            let column_indices: Vec<usize> = index_meta
                .columns
                .iter()
                .filter_map(|col| table_meta.schema.column_index(col))
                .collect();

            // Extract values for index columns
            let index_values: Vec<Value> = column_indices
                .iter()
                .filter_map(|&idx| row.values.get(idx).cloned())
                .collect();

            if index_values.len() == column_indices.len() {
                // Remove from index
                self.index_manager.write().remove_entry(
                    &index_name,
                    &index_values,
                    row.id,
                    _tx_id,
                )?;
            }
        }

        Ok(())
    }

    fn get_indexes_for_table(
        &self,
        table_name: &str,
    ) -> StorageResult<Vec<(String, IndexMetadata)>> {
        let mut indexes = Vec::new();

        // Scan metadata partition for indexes
        let prefix = "index:".as_bytes();
        for entry in self.metadata_partition.prefix(prefix) {
            let (key, value) = entry?;
            let index_meta: IndexMetadata = deserialize(&value)?;
            if index_meta.table == table_name {
                let index_name = String::from_utf8_lossy(&key[prefix.len()..]).to_string();
                indexes.push((index_name, index_meta));
            }
        }

        Ok(indexes)
    }
}
