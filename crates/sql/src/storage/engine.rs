//! Main storage engine with fjall backend

use crate::error::{Error, Result};
use crate::storage::bucket_manager::BucketManager;
use crate::storage::config::StorageConfig;
use crate::storage::data_history::DataHistoryStore;
use crate::storage::encoding;
use crate::storage::index::{IndexManager, IndexMetadata};
use crate::storage::index_history::IndexHistoryStore;
use crate::storage::types::{Row, RowId, WriteOp};
use crate::storage::uncommitted_data::UncommittedDataStore;
use crate::storage::uncommitted_index::UncommittedIndexStore;
use crate::types::schema::Table as TableSchema;
use crate::types::value::Value;
use fjall::{Batch, Keyspace, Partition, PartitionCreateOptions};
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
    tables: HashMap<String, Arc<TableMetadata>>,

    // Index manager
    index_manager: IndexManager,

    // Index versions (uncommitted index operations)
    uncommitted_index: UncommittedIndexStore,

    // Uncommitted data operations - using fjall
    uncommitted_data: UncommittedDataStore,

    // Data history (committed) for time-travel - using fjall
    data_history: DataHistoryStore,

    // Index history (committed) for time-travel on indexes
    index_history: IndexHistoryStore,

    // Cache of table -> indexes mapping for performance
    // Invalidated on DDL operations (create_index, drop_index, drop_table)
    table_indexes_cache: HashMap<String, Vec<(String, IndexMetadata)>>,

    // Last cleanup timestamp - used to throttle cleanup operations
    last_cleanup: Option<HlcTimestamp>,

    // Last processed log index - persisted with each write batch
    log_index: AtomicU64,

    // Configuration
    config: StorageConfig,
}

impl Storage {
    /// Create a new storage engine
    pub fn new(config: StorageConfig) -> Result<Self> {
        Self::open_at_path(&config.data_dir.clone(), config)
    }

    /// Open storage at a specific path
    pub fn open_at_path(path: &Path, config: StorageConfig) -> Result<Self> {
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

        // Create BucketManager for uncommitted data (time-only partitioning)
        let uncommitted_data_mgr = BucketManager::new(
            keyspace.clone(),
            "_uncommitted_data".to_string(),
            config.uncommitted_bucket_duration,
            PartitionCreateOptions::default()
                .block_size(32 * 1024)
                .compression(fjall::CompressionType::None),
        );
        let uncommitted_data =
            UncommittedDataStore::new(uncommitted_data_mgr, config.uncommitted_retention_window);

        // Create BucketManager for data history (table × time partitioning)
        let data_history_mgr = BucketManager::new(
            keyspace.clone(),
            "_data_history".to_string(),
            config.history_bucket_duration,
            PartitionCreateOptions::default()
                .block_size(4 * 1024)
                .compression(fjall::CompressionType::None),
        );
        let data_history = DataHistoryStore::new(data_history_mgr, config.history_retention_window);

        // Create BucketManager for uncommitted index (time-only partitioning)
        let uncommitted_index_mgr = BucketManager::new(
            keyspace.clone(),
            "_uncommitted_index".to_string(),
            config.uncommitted_bucket_duration,
            PartitionCreateOptions::default()
                .block_size(4 * 1024)
                .compression(fjall::CompressionType::None),
        );
        let uncommitted_index =
            UncommittedIndexStore::new(uncommitted_index_mgr, config.uncommitted_retention_window);

        // Create BucketManager for index history (index × time partitioning)
        let index_history_mgr = BucketManager::new(
            keyspace.clone(),
            "_index_history".to_string(),
            config.history_bucket_duration,
            PartitionCreateOptions::default()
                .block_size(4 * 1024)
                .compression(fjall::CompressionType::None),
        );
        let index_history =
            IndexHistoryStore::new(index_history_mgr, config.history_retention_window);

        // Load log_index from metadata (default to 0 if not found)
        let log_index = if let Some(bytes) = metadata_partition.get("log_index")? {
            if bytes.len() == 8 {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes);
                u64::from_be_bytes(buf)
            } else {
                0
            }
        } else {
            0
        };

        // Load existing tables
        let mut tables = HashMap::new();

        // Scan metadata for existing tables
        for entry in metadata_partition.prefix("table:") {
            let (key, value) = entry?;
            let table_name = std::str::from_utf8(&key[6..])
                .map_err(|e| Error::Other(format!("Invalid table name: {}", e)))?
                .to_string();

            let schema: TableSchema =
                bincode::deserialize(&value).map_err(|e| Error::Serialization(e.to_string()))?;

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

            tables.insert(table_name, metadata);
        }

        Ok(Self {
            keyspace,
            metadata_partition,
            tables,
            index_manager: IndexManager::new(),
            uncommitted_index,
            uncommitted_data,
            data_history,
            index_history,
            table_indexes_cache: HashMap::new(),
            last_cleanup: None,
            log_index: AtomicU64::new(log_index),
            config,
        })
    }

    /// Create a new table
    pub fn create_table(&mut self, name: String, schema: TableSchema) -> Result<()> {
        // Check if table already exists
        if self.tables.contains_key(&name) {
            return Err(Error::DuplicateTable(name.clone()));
        }

        // Create partitions
        let data_partition = self.keyspace.open_partition(
            &format!("{}_data", name),
            PartitionCreateOptions::default()
                .block_size(64 * 1024)
                .compression(self.config.compression),
        )?;

        // Store schema in metadata
        self.metadata_partition.insert(
            format!("table:{}", name),
            bincode::serialize(&schema).map_err(|e| Error::Serialization(e.to_string()))?,
        )?;

        // Create table metadata
        let metadata = Arc::new(TableMetadata {
            schema: schema.clone(),
            data_partition,
            next_row_id: AtomicU64::new(1),
        });

        // Add to tables map
        self.tables.insert(name.clone(), metadata);

        // Persist changes
        self.keyspace.persist(self.config.persist_mode)?;

        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        // Remove from tables map and get partition handles
        let metadata = self
            .tables
            .remove(name)
            .ok_or_else(|| Error::TableNotFound(name.to_string()))?;

        // Remove schema from metadata
        self.metadata_partition.remove(format!("table:{}", name))?;

        // NOTE: Uncommitted data for this table will be cleaned up when time buckets expire
        // No need to explicitly remove it

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

        // Invalidate cache for this table
        self.invalidate_table_indexes_cache(name);

        // Persist changes
        self.keyspace.persist(self.config.persist_mode)?;

        Ok(())
    }

    /// Create an index on a table
    pub fn create_index(
        &mut self,
        txn_id: HlcTimestamp,
        index_name: String,
        table_name: String,
        columns: Vec<String>,
        unique: bool,
    ) -> Result<()> {
        // Verify table exists
        let tables = &self.tables;
        let table_meta = tables
            .get(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        // Verify columns exist in table
        for column in &columns {
            if !table_meta.schema.has_column(column) {
                return Err(Error::Other(format!(
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
            return Err(Error::Other(
                "Failed to resolve all column indices".to_string(),
            ));
        }

        // Create the index
        self.index_manager.create_index(
            &self.keyspace,
            index_name.clone(),
            table_name.clone(),
            columns.clone(),
            unique,
        )?;

        // Build index from existing data
        // Collect all existing rows
        let mut rows_to_index = Vec::new();
        for row_result in self.iter(txn_id, &table_name)? {
            let row = row_result?;
            rows_to_index.push((row.id, row));
        }

        // Build the index
        self.index_manager.build_index(
            &mut self.uncommitted_index,
            &index_name,
            &rows_to_index,
            &column_indices,
            txn_id,
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

        self.metadata_partition.insert(
            index_meta_key,
            bincode::serialize(&index_metadata).map_err(|e| Error::Serialization(e.to_string()))?,
        )?;

        // Invalidate cache for this table
        self.invalidate_table_indexes_cache(&index_metadata.table);

        self.keyspace.persist(self.config.persist_mode)?;
        Ok(())
    }

    /// Drop an index
    /// TODO: This needs integrating with the DDL methods (probably needs thinging about MVCC more)
    #[allow(dead_code)]
    pub fn drop_index(&mut self, index_name: &str) -> Result<()> {
        // Get table name before removing metadata
        let index_meta_key = format!("index:{}", index_name);
        let table_name = if let Some(meta_bytes) = self.metadata_partition.get(&index_meta_key)? {
            let index_meta: IndexMetadata = bincode::deserialize(&meta_bytes)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            Some(index_meta.table)
        } else {
            None
        };

        // Remove the index
        self.index_manager.drop_index(index_name)?;

        // Remove metadata
        self.metadata_partition.remove(index_meta_key)?;

        // Invalidate cache if we found the table name
        if let Some(table) = table_name {
            self.invalidate_table_indexes_cache(&table);
        }

        self.keyspace.persist(self.config.persist_mode)?;
        Ok(())
    }

    /// Insert multiple rows atomically - all validation checks are done before any inserts
    pub fn insert_batch(
        &mut self,
        txn_id: HlcTimestamp,
        table: &str,
        values_batch: Vec<Vec<crate::types::value::Value>>,
        log_index: u64,
    ) -> Result<Vec<RowId>> {
        // Get table metadata
        let table_meta = self
            .tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?
            .clone();

        // Phase 1: Validate all rows and prepare data structures
        let mut prepared_rows = Vec::new();
        for values in values_batch {
            // Validate values against schema
            table_meta.schema.validate_row(&values)?;

            // Generate row ID
            let row_id = table_meta.next_row_id.fetch_add(1, Ordering::SeqCst);

            // Create row with schema_id
            let row = Arc::new(
                Row::new(row_id, values).with_schema_version(table_meta.schema.schema_version),
            );
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
                        return Err(Error::UniqueConstraintViolation(format!(
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
                    if self.index_manager.check_unique(
                        &self.uncommitted_index,
                        &self.index_history,
                        index_name,
                        &index_values,
                        txn_id,
                    )? {
                        return Err(Error::UniqueConstraintViolation(format!(
                            "Duplicate value for unique index {}",
                            index_name
                        )));
                    }
                }
            }
        }

        // Phase 4: All validations passed - now insert all rows and update indexes atomically
        let mut batch = self.keyspace.batch();
        let mut inserted_row_ids = Vec::new();

        for (row_id, row) in prepared_rows {
            // Add to uncommitted data batch
            let write_op = WriteOp::Insert {
                table: table.to_string(),
                row_id,
                row: row.clone(),
            };

            self.uncommitted_data
                .add_write_to_batch(&mut batch, txn_id, write_op.clone())?;

            // Update indexes for this table
            self.update_indexes_on_insert_unchecked(table, &row, &table_meta, txn_id)?;

            inserted_row_ids.push(row_id);
        }

        // Add log_index to the same batch for atomicity
        batch.insert(
            &self.metadata_partition,
            "log_index",
            log_index.to_be_bytes(),
        );

        // Update in-memory log_index
        self.log_index.store(log_index, Ordering::SeqCst);

        // Commit the entire batch atomically
        batch.commit()?;

        Ok(inserted_row_ids)
    }

    /// Update a row
    pub fn update(
        &mut self,
        txn_id: HlcTimestamp,
        table: &str,
        row_id: RowId,
        values: Vec<crate::types::value::Value>,
        log_index: u64,
    ) -> Result<()> {
        // Get table metadata
        let table_meta = self
            .tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?
            .clone();

        // Validate values against schema
        table_meta.schema.validate_row(&values)?;

        // Read current row
        let old_row = self
            .read(txn_id, table, row_id)?
            .ok_or(Error::RowNotFound(row_id))?;

        // Create new row with schema_version
        let new_row = Arc::new(
            Row::new(row_id, values).with_schema_version(table_meta.schema.schema_version),
        );

        // Create write operation
        let write_op = WriteOp::Update {
            table: table.to_string(),
            row_id,
            old_row: old_row.clone(),
            new_row: new_row.clone(),
        };

        // Update indexes for this table (remove old, add new)
        // This checks unique constraints and will error if violated
        // IMPORTANT: Do this BEFORE adding to uncommitted data so that
        // if constraint check fails, the write is not committed
        self.update_indexes_on_update(table, &old_row, &new_row, &table_meta, txn_id)?;

        // Create batch for atomic write
        let mut batch = self.keyspace.batch();

        // Add to uncommitted data batch AFTER all validations pass
        self.uncommitted_data
            .add_write_to_batch(&mut batch, txn_id, write_op.clone())?;

        // Add log_index to the same batch for atomicity
        batch.insert(
            &self.metadata_partition,
            "log_index",
            log_index.to_be_bytes(),
        );

        // Update in-memory log_index
        self.log_index.store(log_index, Ordering::SeqCst);

        // Commit the batch atomically
        batch.commit()?;

        Ok(())
    }

    /// Delete a row
    pub fn delete(
        &mut self,
        txn_id: HlcTimestamp,
        table: &str,
        row_id: RowId,
        log_index: u64,
    ) -> Result<()> {
        // Read current row
        let row = self
            .read(txn_id, table, row_id)?
            .ok_or(Error::RowNotFound(row_id))?;

        // Mark as deleted
        let deleted_row = Arc::new(Row {
            id: row_id,
            values: row.values.clone(),
            deleted: true,
            schema_version: row.schema_version,
        });

        // Update indexes for this table (remove entries) BEFORE creating the batch
        if let Some(table_meta) = self.tables.get(table) {
            let table_meta = table_meta.clone();
            self.update_indexes_on_delete(table, &row, &table_meta, txn_id)?;
        }

        // Create batch for atomic write
        let mut batch = self.keyspace.batch();

        // Add to uncommitted data batch
        let write_op = WriteOp::Delete {
            table: table.to_string(),
            row_id,
            row: deleted_row.clone(),
        };

        self.uncommitted_data
            .add_write_to_batch(&mut batch, txn_id, write_op.clone())?;

        // Add log_index to the same batch for atomicity
        batch.insert(
            &self.metadata_partition,
            "log_index",
            log_index.to_be_bytes(),
        );

        // Update in-memory log_index
        self.log_index.store(log_index, Ordering::SeqCst);

        // Commit the batch atomically
        batch.commit()?;

        Ok(())
    }

    /// Read a row
    pub fn read(
        &self,
        txn_id: HlcTimestamp,
        table: &str,
        row_id: RowId,
    ) -> Result<Option<Arc<Row>>> {
        // L1: Check uncommitted transaction writes
        use crate::storage::uncommitted_data::RowState;
        match self.uncommitted_data.get_row(txn_id, table, row_id) {
            RowState::Exists(row) => {
                // Row exists in uncommitted transaction
                if !row.deleted {
                    return Ok(Some(row));
                } else {
                    return Ok(None); // Marked as deleted
                }
            }
            RowState::Deleted => {
                // Row was deleted in uncommitted transaction
                return Ok(None);
            }
            RowState::NoOps => {
                // No operations for this row in uncommitted data, check disk
            }
        }

        // L2: Read from disk with time-travel support
        let tables = &self.tables;
        let table_meta = tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        // Try to read the row from disk
        let row_key = row_id.to_be_bytes();
        if let Some(row_bytes) = table_meta.data_partition.get(row_key)? {
            // Decode using compact encoding
            let values = encoding::decode_row(&row_bytes, &table_meta.schema)?;
            let row = Row {
                id: row_id,
                values,
                deleted: false,
                schema_version: table_meta.schema.schema_version,
            };

            // Check if this row has been modified in history window (< 5 minutes)
            // If not, it's visible (committed > 5 minutes ago)
            if !self.data_history.is_empty() {
                // Get all history ops for this table and filter by row_id
                let table_ops = self.data_history.get_table_ops_after(txn_id, table)?;

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

            // Row exists and hasn't been modified in history window - it's visible
            return Ok(Some(Arc::new(row)));
        }

        // Row doesn't exist in data partition
        Ok(None)
    }

    /// Get an iterator over all visible rows in a table
    /// The iterator holds necessary locks for its lifetime
    pub fn iter<'a>(
        &'a self,
        txn_id: HlcTimestamp,
        table: &str,
    ) -> Result<crate::storage::iterator::TableIterator<'a>> {
        // Pre-load all data (both use &self - read-only)
        let active_data = self.uncommitted_data.get_table_active_data(txn_id, table);
        let history_ops = self.data_history.get_table_ops_after(txn_id, table)?;

        crate::storage::iterator::TableIterator::new(
            &self.tables,
            table,
            active_data,
            history_ops,
            false,
        )
    }

    /// Get an iterator with row IDs (for UPDATE/DELETE operations)
    pub fn iter_with_ids<'a>(
        &'a self,
        txn_id: HlcTimestamp,
        table: &str,
    ) -> Result<crate::storage::iterator::TableIteratorWithIds<'a>> {
        // Pre-load all data (both use &self - read-only)
        let active_data = self.uncommitted_data.get_table_active_data(txn_id, table);
        let history_ops = self.data_history.get_table_ops_after(txn_id, table)?;

        crate::storage::iterator::TableIteratorWithIds::new(
            &self.tables,
            table,
            active_data,
            history_ops,
            false,
        )
    }

    /// Get a reverse iterator (scanning backwards)
    #[allow(dead_code)]
    pub fn iter_reverse<'a>(
        &'a self,
        txn_id: HlcTimestamp,
        table: &str,
    ) -> Result<crate::storage::iterator::TableIterator<'a>> {
        // Pre-load all data (both use &self - read-only)
        let active_data = self.uncommitted_data.get_table_active_data(txn_id, table);
        let history_ops = self.data_history.get_table_ops_after(txn_id, table)?;

        crate::storage::iterator::TableIterator::new(
            &self.tables,
            table,
            active_data,
            history_ops,
            true,
        )
    }

    /// Get all table schemas
    pub fn get_schemas(&self) -> HashMap<String, TableSchema> {
        let tables = &self.tables;
        tables
            .iter()
            .map(|(name, meta)| (name.clone(), meta.schema.clone()))
            .collect()
    }

    /// Get all index metadata
    pub fn get_index_metadata(&self) -> HashMap<String, crate::storage::index::IndexMetadata> {
        self.index_manager.get_all_metadata().into_iter().collect()
    }

    /// Get index columns for a specific index
    pub fn get_index_columns(&self, table_name: &str, index_name: &str) -> Option<Vec<String>> {
        self.index_manager
            .get_index_metadata(index_name)
            .filter(|meta| meta.table == table_name)
            .map(|meta| meta.columns.clone())
    }

    /// Check if an index exists on a table
    pub fn has_index(&self, table_name: &str, index_name: &str) -> bool {
        // Check both the direct index name and the primary key index name
        let pk_index_name = format!("{}_{}_pkey", table_name, index_name);

        self.index_manager
            .get_index_metadata(index_name)
            .map(|meta| meta.table == table_name)
            .unwrap_or(false)
            || self
                .index_manager
                .get_index_metadata(&pk_index_name)
                .map(|meta| meta.table == table_name)
                .unwrap_or(false)
    }

    /// Streaming index lookup - returns iterator of rows (MVCC-aware)
    pub fn index_lookup_rows_streaming<'a>(
        &'a self,
        index_name: &str,
        values: Vec<Value>,
        txn_id: HlcTimestamp,
    ) -> Result<impl Iterator<Item = Result<Arc<Row>>> + 'a> {
        // Get index metadata
        let index_meta_key = format!("index:{}", index_name);
        let meta_bytes = self
            .metadata_partition
            .get(&index_meta_key)?
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;
        let index_meta: IndexMetadata =
            bincode::deserialize(&meta_bytes).map_err(|e| Error::Serialization(e.to_string()))?;
        let table_name = index_meta.table.clone();

        // Get streaming row IDs (MVCC-aware)
        let row_ids = self.index_manager.lookup_iter_mvcc(
            &self.uncommitted_index,
            &self.index_history,
            index_name,
            values,
            txn_id,
        )?;

        // Map row IDs to rows (streaming)
        Ok(
            row_ids.filter_map(move |row_id_result| match row_id_result {
                Ok(row_id) => match self.read(txn_id, &table_name, row_id) {
                    Ok(Some(row)) => Some(Ok(row)),
                    Ok(None) => None,
                    Err(e) => Some(Err(e)),
                },
                Err(e) => Some(Err(e)),
            }),
        )
    }

    /// Streaming index range scan - returns iterator of rows (MVCC-aware)
    pub fn index_range_lookup_rows_streaming<'a>(
        &'a self,
        index_name: &str,
        start_values: Option<Vec<Value>>,
        end_values: Option<Vec<Value>>,
        _reverse: bool,
        txn_id: HlcTimestamp,
    ) -> Result<impl Iterator<Item = Result<Arc<Row>>> + 'a> {
        // Get index metadata
        let index_meta_key = format!("index:{}", index_name);
        let meta_bytes = self
            .metadata_partition
            .get(&index_meta_key)?
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;
        let index_meta: IndexMetadata =
            bincode::deserialize(&meta_bytes).map_err(|e| Error::Serialization(e.to_string()))?;
        let table_name = index_meta.table.clone();

        // Get streaming row IDs (MVCC-aware)
        let row_ids = self.index_manager.range_scan_iter_mvcc(
            &self.uncommitted_index,
            &self.index_history,
            index_name,
            start_values,
            end_values,
            txn_id,
        )?;

        // Map row IDs to rows (streaming)
        Ok(
            row_ids.filter_map(move |row_id_result| match row_id_result {
                Ok(row_id) => match self.read(txn_id, &table_name, row_id) {
                    Ok(Some(row)) => Some(Ok(row)),
                    Ok(None) => None,
                    Err(e) => Some(Err(e)),
                },
                Err(e) => Some(Err(e)),
            }),
        )
    }

    /// Execute DDL operations
    pub fn execute_ddl(
        &mut self,
        plan: &crate::planning::plan::Plan,
        txn_id: HlcTimestamp,
    ) -> Result<String> {
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
                                txn_id,
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
                                    txn_id,
                                    index_name,
                                    name.clone(),
                                    vec![column.name.clone()],
                                    true, // unique
                                )?;
                            } else if column.index {
                                // Create regular (non-unique) index if column has INDEX constraint
                                let index_name = format!("{}_{}_idx", name, column.name);
                                self.create_index(
                                    txn_id,
                                    index_name,
                                    name.clone(),
                                    vec![column.name.clone()],
                                    false, // not unique
                                )?;
                            }
                        }
                        Ok(format!("Table '{}' created", name))
                    }
                    Err(Error::DuplicateTable(_)) if *if_not_exists => Ok(format!(
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
                        Err(Error::TableNotFound(_)) if *if_exists => {
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
                if !&self.tables.contains_key(table) {
                    return Err(Error::TableNotFound(table.clone()));
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
                match self.index_manager.create_index(
                    &self.keyspace,
                    name.clone(),
                    table.clone(),
                    column_names,
                    *unique,
                ) {
                    Ok(_) => Ok(format!("Index '{}' created", name)),
                    Err(Error::Other(msg)) if msg.contains("already exists") => {
                        Ok(format!("Index '{}' already exists", name))
                    }
                    Err(e) => Err(e),
                }
            }

            Plan::DropIndex { name, if_exists } => match self.index_manager.drop_index(name) {
                Ok(_) => Ok(format!("Index '{}' dropped", name)),
                Err(Error::IndexNotFound(_)) if *if_exists => Ok(format!(
                    "Index '{}' does not exist (IF EXISTS specified)",
                    name
                )),
                Err(e) => Err(e),
            },

            _ => Err(Error::InvalidOperation(
                "Unsupported DDL operation".to_string(),
            )),
        }
    }

    /// Commit a transaction
    pub fn commit_transaction(&mut self, txn_id: HlcTimestamp, log_index: u64) -> Result<()> {
        // Update in-memory log_index
        self.log_index.store(log_index, Ordering::SeqCst);
        // Get writes for this transaction
        let writes = self.uncommitted_data.get_transaction_writes(txn_id);

        // Create batch for atomic commit (even if no writes, we might have index ops)
        let mut batch = self.keyspace.batch();

        // Separate data and index operations
        let mut data_ops = Vec::new();

        if !writes.is_empty() {
            for write in &writes {
                match write {
                    WriteOp::Insert { table, row_id, row } => {
                        self.persist_insert(&mut batch, table, *row_id, row.clone())?;
                        data_ops.push(write.clone());
                    }
                    WriteOp::Update {
                        table,
                        row_id,
                        new_row,
                        ..
                    } => {
                        self.persist_update(&mut batch, table, *row_id, new_row.clone())?;
                        data_ops.push(write.clone());
                    }
                    WriteOp::Delete { table, row_id, .. } => {
                        self.persist_delete(&mut batch, table, *row_id)?;
                        data_ops.push(write.clone());
                    }
                }
            }
        }

        // Get index operations before committing (so we can move to index_history)
        let index_ops = self.uncommitted_index.get_transaction_ops(txn_id);

        // Commit index operations to the batch
        self.index_manager
            .commit_transaction(&mut self.uncommitted_index, &mut batch, txn_id)?;

        // Add data operations to data_history (in the same batch)
        // OPTIMIZED: Uses commit_time-first key design for efficient range queries
        if !writes.is_empty() {
            self.data_history
                .add_committed_ops_to_batch(&mut batch, txn_id, writes)?;
        }

        // Add index operations to index_history (in the same batch)
        // OPTIMIZED: Uses commit_time-first key design for efficient range queries
        if !index_ops.is_empty() {
            self.index_history
                .add_committed_ops_to_batch(&mut batch, txn_id, index_ops)?;
        }

        // Persist log_index atomically with the commit
        batch.insert(
            &self.metadata_partition,
            "log_index",
            log_index.to_be_bytes(),
        );

        // Remove from uncommitted stores (this is fast, no disk I/O)
        self.uncommitted_data
            .remove_transaction(&mut batch, txn_id)?;

        // Commit single atomic batch (includes data, indexes, history)
        batch.commit()?;

        // Cleanup old buckets AFTER commit (since it drops partitions)
        let should_cleanup = match self.last_cleanup {
            None => true,
            Some(last) => {
                let cleanup_interval_micros = self.config.cleanup_interval.as_micros() as u64;
                txn_id.physical.saturating_sub(last.physical) >= cleanup_interval_micros
            }
        };

        if should_cleanup {
            self.data_history.cleanup_old_buckets(txn_id)?;
            self.index_history.cleanup_old_buckets(txn_id)?;
            // Also cleanup uncommitted stores (to catch orphaned transactions)
            self.uncommitted_data.cleanup_old_buckets(txn_id)?;
            self.uncommitted_index.cleanup_old_buckets(txn_id)?;
            self.last_cleanup = Some(txn_id);
        }

        // Persist to disk once
        if should_cleanup {
            self.keyspace.persist(fjall::PersistMode::SyncAll)?;
        } else {
            self.keyspace.persist(self.config.persist_mode)?;
        }

        Ok(())
    }

    /// Abort a transaction
    pub fn abort_transaction(&mut self, txn_id: HlcTimestamp, log_index: u64) -> Result<()> {
        // Update in-memory log_index
        self.log_index.store(log_index, Ordering::SeqCst);

        // Create batch for atomic commit (even if no writes, we might have index ops)
        let mut batch = self.keyspace.batch();

        // Remove from uncommitted data
        self.uncommitted_data
            .remove_transaction(&mut batch, txn_id)?;

        // Abort index operations (uses batch now)
        self.index_manager
            .abort_transaction(&mut self.uncommitted_index, &mut batch, txn_id)?;

        // Persist log_index atomically with the abort
        batch.insert(
            &self.metadata_partition,
            "log_index",
            log_index.to_be_bytes(),
        );

        // Commit the batch (includes uncommitted data removal and index abort)
        batch.commit()?;

        // Cleanup old buckets AFTER commit (since it drops partitions)
        let should_cleanup = match self.last_cleanup {
            None => true,
            Some(last) => {
                let cleanup_interval_micros = self.config.cleanup_interval.as_micros() as u64;
                txn_id.physical.saturating_sub(last.physical) >= cleanup_interval_micros
            }
        };

        if should_cleanup {
            self.data_history.cleanup_old_buckets(txn_id)?;
            self.index_history.cleanup_old_buckets(txn_id)?;
            // Also cleanup uncommitted stores (to catch orphaned transactions)
            self.uncommitted_data.cleanup_old_buckets(txn_id)?;
            self.uncommitted_index.cleanup_old_buckets(txn_id)?;
            self.last_cleanup = Some(txn_id);
        }

        // Persist to disk once
        if should_cleanup {
            self.keyspace.persist(fjall::PersistMode::SyncAll)?;
        } else {
            self.keyspace.persist(self.config.persist_mode)?;
        }

        Ok(())
    }

    // Helper methods for persisting operations

    fn persist_insert(
        &self,
        batch: &mut Batch,
        table: &str,
        row_id: RowId,
        row: Arc<Row>,
    ) -> Result<()> {
        let tables = &self.tables;
        let table_meta = tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        // Write row data using compact encoding
        let row_key = row_id.to_be_bytes();
        let encoded = encoding::encode_row(&row.values, &table_meta.schema)?;
        batch.insert(&table_meta.data_partition, row_key, encoded);

        Ok(())
    }

    fn persist_update(
        &self,
        batch: &mut Batch,
        table: &str,
        row_id: RowId,
        row: Arc<Row>,
    ) -> Result<()> {
        let tables = &self.tables;
        let table_meta = tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        // Update row data using compact encoding
        let row_key = row_id.to_be_bytes();
        let encoded = encoding::encode_row(&row.values, &table_meta.schema)?;
        batch.insert(&table_meta.data_partition, row_key, encoded);

        Ok(())
    }

    fn persist_delete(&mut self, batch: &mut Batch, table: &str, row_id: RowId) -> Result<()> {
        let tables = &self.tables;
        let table_meta = tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        // Delete the actual row data
        let row_key = row_id.to_be_bytes();
        batch.remove(&table_meta.data_partition, row_key);

        Ok(())
    }

    /// Update indexes on insert without unique constraint checking (used in batch operations)
    fn update_indexes_on_insert_unchecked(
        &mut self,
        table_name: &str,
        row: &Arc<Row>,
        table_meta: &TableMetadata,
        txn_id: HlcTimestamp,
    ) -> Result<()> {
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
                self.index_manager.add_entry(
                    &mut self.uncommitted_index,
                    &index_name,
                    index_values,
                    row.id,
                    txn_id,
                )?;
            }
        }

        Ok(())
    }

    fn update_indexes_on_update(
        &mut self,
        table_name: &str,
        old_row: &Arc<Row>,
        new_row: &Arc<Row>,
        table_meta: &TableMetadata,
        txn_id: HlcTimestamp,
    ) -> Result<()> {
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
                && self.index_manager.check_unique(
                    &self.uncommitted_index,
                    &self.index_history,
                    index_name,
                    new_values,
                    txn_id,
                )?
            {
                return Err(Error::UniqueConstraintViolation(format!(
                    "Duplicate value for unique index {}",
                    index_name
                )));
            }
        }

        // Phase 2: All constraints passed - now apply the changes
        for (index_name, _index_meta, old_values, new_values) in updates {
            // Remove old entry
            self.index_manager.remove_entry(
                &mut self.uncommitted_index,
                &index_name,
                &old_values,
                old_row.id,
                txn_id,
            )?;

            // Add new entry
            self.index_manager.add_entry(
                &mut self.uncommitted_index,
                &index_name,
                new_values,
                new_row.id,
                txn_id,
            )?;
        }

        Ok(())
    }

    fn update_indexes_on_delete(
        &mut self,
        table_name: &str,
        row: &Arc<Row>,
        table_meta: &TableMetadata,
        txn_id: HlcTimestamp,
    ) -> Result<()> {
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
                self.index_manager.remove_entry(
                    &mut self.uncommitted_index,
                    &index_name,
                    &index_values,
                    row.id,
                    txn_id,
                )?;
            }
        }

        Ok(())
    }

    fn get_indexes_for_table(&mut self, table_name: &str) -> Result<Vec<(String, IndexMetadata)>> {
        // Check cache first
        {
            let cache = &self.table_indexes_cache;
            if let Some(indexes) = cache.get(table_name) {
                return Ok(indexes.clone());
            }
        }

        // Cache miss - scan metadata partition for indexes
        let mut indexes = Vec::new();
        let prefix = "index:".as_bytes();
        for entry in self.metadata_partition.prefix(prefix) {
            let (key, value) = entry?;
            let index_meta: IndexMetadata =
                bincode::deserialize(&value).map_err(|e| Error::Serialization(e.to_string()))?;
            if index_meta.table == table_name {
                let index_name = String::from_utf8_lossy(&key[prefix.len()..]).to_string();
                indexes.push((index_name, index_meta));
            }
        }

        // Store in cache
        self.table_indexes_cache
            .insert(table_name.to_string(), indexes.clone());

        Ok(indexes)
    }

    fn invalidate_table_indexes_cache(&mut self, table_name: &str) {
        self.table_indexes_cache.remove(table_name);
    }

    /// Get the last processed log index
    pub fn get_log_index(&self) -> u64 {
        self.log_index.load(Ordering::SeqCst)
    }
}
