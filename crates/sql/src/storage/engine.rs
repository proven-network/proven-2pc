//! New SQL storage engine using proven-mvcc
//!
//! This is a reimplementation of the SQL storage layer using the proven-mvcc
//! abstraction. Key design:
//! - One MvccStorage<TableEntity> per table (separate partitions)
//! - One MvccStorage<IndexEntity> per index (separate partitions)
//! - Shared keyspace for atomic cross-entity batching
//! - Schema-aware row encoding preserved (36.1 bytes/row)

use crate::error::{Error, Result};
use crate::semantic::predicate::{Predicate, QueryPredicates};
use crate::storage::codec::{decode_row, encode_row};
use crate::storage::ddl_store::DdlStore;
use crate::storage::entity::{IndexDelta, IndexEntity, IndexKey, TableDelta, TableEntity};
use crate::storage::predicate_store::PredicateStore;
use crate::types::Value;
use crate::types::index::IndexMetadata;
use crate::types::schema::Table as TableSchema;

use fjall::{Keyspace, PartitionCreateOptions};
use proven_hlc::HlcTimestamp;
use proven_mvcc::{MvccStorage, StorageConfig as MvccConfig};

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Configuration for SQL storage
#[derive(Clone)]
pub struct SqlStorageConfig {
    pub data_dir: std::path::PathBuf,
    pub block_cache_size: usize,
    pub compression: fjall::CompressionType,
    pub persist_mode: fjall::PersistMode,
}

impl SqlStorageConfig {
    pub fn with_data_dir(data_dir: std::path::PathBuf) -> Self {
        Self {
            data_dir,
            ..Default::default()
        }
    }
}

impl Default for SqlStorageConfig {
    fn default() -> Self {
        // Use tempfile to create a proper temporary directory
        // Using .keep() to persist the directory (won't be auto-deleted)
        let temp_dir = tempfile::tempdir()
            .expect("Failed to create temporary directory")
            .keep();

        Self {
            data_dir: temp_dir,
            block_cache_size: 512 * 1024 * 1024, // 512MB
            compression: fjall::CompressionType::Lz4,
            persist_mode: fjall::PersistMode::Buffer,
        }
    }
}

/// Main SQL storage engine using MVCC
pub struct SqlStorage {
    /// Shared keyspace for atomic batching across all tables and indexes
    keyspace: Keyspace,

    /// Cached metadata partition handle (opened once, used frequently)
    metadata: fjall::PartitionHandle,

    /// One MvccStorage per table (table_name -> storage)
    /// Each table gets separate partitions via with_shared_keyspace()
    table_storages: HashMap<String, MvccStorage<TableEntity>>,

    /// One MvccStorage per index (index_name -> storage)
    /// Each index gets separate partitions via with_shared_keyspace()
    index_storages: HashMap<String, MvccStorage<IndexEntity>>,

    /// Schema registry (table_name -> schema)
    /// Needed for schema-aware row encoding/decoding
    schemas: HashMap<String, Arc<TableSchema>>,

    /// Table generation counters (table_name -> generation)
    /// Incremented each time a table is created (including after DROP)
    /// Used to create unique partition names to avoid data reuse
    table_generations: HashMap<String, u64>,

    /// Index metadata (index_name -> metadata)
    index_metadata: HashMap<String, IndexMetadata>,

    /// Predicate store for crash recovery
    predicate_store: PredicateStore,

    /// DDL store for crash recovery
    ddl_store: DdlStore,

    /// Row ID counters per table (for generating unique row IDs)
    next_row_ids: HashMap<String, AtomicU64>,

    /// Configuration
    config: SqlStorageConfig,
}

impl SqlStorage {
    /// Create new SQL storage
    pub fn new(config: SqlStorageConfig) -> Result<Self> {
        Self::open_at_path(&config.data_dir.clone(), config)
    }

    /// Open storage at specific path
    pub fn open_at_path(path: &Path, config: SqlStorageConfig) -> Result<Self> {
        // Ensure directory exists
        std::fs::create_dir_all(path)?;

        // Open shared keyspace
        let keyspace = fjall::Config::new(path)
            .cache_size(config.block_cache_size as u64)
            .open()?;

        // Open metadata partition once (used frequently)
        let metadata = keyspace.open_partition(
            "_metadata",
            PartitionCreateOptions::default()
                .block_size(16 * 1024)
                .compression(fjall::CompressionType::None),
        )?;

        // Create predicate store with dedicated partition
        let predicate_store = PredicateStore::new(&keyspace)?;

        // Create DDL store with dedicated partition
        let ddl_store = DdlStore::new(&keyspace)?;

        let mut storage = Self {
            keyspace,
            metadata,
            table_storages: HashMap::new(),
            index_storages: HashMap::new(),
            schemas: HashMap::new(),
            table_generations: HashMap::new(),
            index_metadata: HashMap::new(),
            predicate_store,
            ddl_store,
            next_row_ids: HashMap::new(),
            config,
        };

        // Load existing tables and schemas from metadata
        storage.load_metadata()?;

        Ok(storage)
    }

    /// Load table schemas and indexes from metadata partition
    fn load_metadata(&mut self) -> Result<()> {
        // Scan for table schemas
        for result in self.metadata.prefix(b"schema_") {
            let (key, value) = result?;
            let key_str = String::from_utf8_lossy(&key);

            if let Some(table_name) = key_str.strip_prefix("schema_") {
                // Decode schema
                let schema: TableSchema = ciborium::from_reader(&value[..])
                    .map_err(|e| Error::Serialization(e.to_string()))?;

                // Load table generation
                let gen_key = format!("table_generation_{}", table_name);
                let generation = self
                    .metadata
                    .get(&gen_key)?
                    .map(|bytes| {
                        let mut buf = [0u8; 8];
                        buf.copy_from_slice(&bytes);
                        u64::from_be_bytes(buf)
                    })
                    .unwrap_or(1); // Default to 1 if not found (for old tables)

                // Create MvccStorage for this table
                let mvcc_config = MvccConfig {
                    data_dir: self.config.data_dir.clone(),
                    block_cache_size: self.config.block_cache_size as u64,
                    compression: self.config.compression,
                    persist_mode: self.config.persist_mode,
                    history_bucket_duration: std::time::Duration::from_secs(60),
                    uncommitted_bucket_duration: std::time::Duration::from_secs(30),
                    history_retention_window: std::time::Duration::from_secs(300),
                    uncommitted_retention_window: std::time::Duration::from_secs(120),
                    cleanup_interval: std::time::Duration::from_secs(30),
                };

                // Use generation in partition name
                let partition_name = format!("{}_g{}", table_name, generation);
                let table_storage = MvccStorage::<TableEntity>::with_shared_keyspace(
                    self.keyspace.clone(),
                    partition_name,
                    mvcc_config,
                )?;

                // Load or initialize next_row_id counter
                let row_id_key = format!("next_row_id_{}", table_name);
                let next_row_id = self
                    .metadata
                    .get(&row_id_key)?
                    .map(|bytes| {
                        let mut buf = [0u8; 8];
                        buf.copy_from_slice(&bytes);
                        u64::from_be_bytes(buf)
                    })
                    .unwrap_or(1); // Default to 1 if not found

                self.table_storages
                    .insert(table_name.to_string(), table_storage);
                self.schemas
                    .insert(table_name.to_string(), Arc::new(schema));
                self.table_generations
                    .insert(table_name.to_string(), generation);
                self.next_row_ids
                    .insert(table_name.to_string(), AtomicU64::new(next_row_id));
            }
        }

        // Scan for index metadata
        for result in self.metadata.prefix(b"index_") {
            let (key, value) = result?;
            let key_str = String::from_utf8_lossy(&key);

            if let Some(index_name) = key_str.strip_prefix("index_") {
                // Decode index metadata
                let index_meta: IndexMetadata = ciborium::from_reader(&value[..])
                    .map_err(|e| Error::Serialization(e.to_string()))?;

                // Create MvccStorage for this index
                let mvcc_config = MvccConfig {
                    data_dir: self.config.data_dir.clone(),
                    block_cache_size: self.config.block_cache_size as u64,
                    compression: self.config.compression,
                    persist_mode: self.config.persist_mode,
                    history_bucket_duration: std::time::Duration::from_secs(60),
                    uncommitted_bucket_duration: std::time::Duration::from_secs(30),
                    history_retention_window: std::time::Duration::from_secs(300),
                    uncommitted_retention_window: std::time::Duration::from_secs(120),
                    cleanup_interval: std::time::Duration::from_secs(30),
                };

                let index_storage = MvccStorage::<IndexEntity>::with_shared_keyspace(
                    self.keyspace.clone(),
                    index_name.to_string(),
                    mvcc_config,
                )?;

                self.index_storages
                    .insert(index_name.to_string(), index_storage);
                self.index_metadata
                    .insert(index_name.to_string(), index_meta);
            }
        }

        Ok(())
    }

    /// Create a new batch for atomic operations
    pub fn batch(&self) -> fjall::Batch {
        self.keyspace.batch()
    }

    /// Get schemas (for query planning)
    pub fn get_schemas(&self) -> HashMap<String, Arc<TableSchema>> {
        self.schemas.clone()
    }

    /// Get index metadata (for query planning)
    pub fn get_index_metadata(&self) -> HashMap<String, IndexMetadata> {
        self.index_metadata.clone()
    }

    /// Reload schemas and indexes from metadata (used after rollback)
    pub fn reload_metadata(&mut self) -> Result<()> {
        // Clear existing state
        self.table_storages.clear();
        self.index_storages.clear();
        self.schemas.clear();
        self.table_generations.clear();
        self.index_metadata.clear();
        self.next_row_ids.clear();

        // Reload from persistent metadata
        self.load_metadata()
    }

    /// Rollback CREATE TABLE: remove schema from metadata (batched)
    pub fn rollback_create_table(
        &mut self,
        batch: &mut fjall::Batch,
        table_name: &str,
    ) -> Result<()> {
        let schema_key = format!("schema_{}", table_name);
        batch.remove(self.metadata.clone(), schema_key);

        let row_id_key = format!("next_row_id_{}", table_name);
        batch.remove(self.metadata.clone(), row_id_key);

        Ok(())
    }

    /// Rollback DROP TABLE: restore old schema to metadata (batched)
    pub fn rollback_drop_table(
        &mut self,
        batch: &mut fjall::Batch,
        table_name: &str,
        old_schema: &crate::types::schema::Table,
    ) -> Result<()> {
        let schema_key = format!("schema_{}", table_name);
        let mut schema_bytes = Vec::new();
        ciborium::into_writer(old_schema, &mut schema_bytes)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        batch.insert(&self.metadata, schema_key, schema_bytes);

        Ok(())
    }

    /// Rollback ALTER TABLE: restore old schema to metadata (batched)
    pub fn rollback_alter_table(
        &mut self,
        batch: &mut fjall::Batch,
        table_name: &str,
        old_schema: &crate::types::schema::Table,
    ) -> Result<()> {
        // Same as rollback_drop_table - just restore the old schema
        self.rollback_drop_table(batch, table_name, old_schema)
    }

    /// Rollback RENAME TABLE: restore old name's schema AND remove new name's schema (batched)
    pub fn rollback_rename_table(
        &mut self,
        batch: &mut fjall::Batch,
        old_name: &str,
        new_name: &str,
        old_schema: &crate::types::schema::Table,
    ) -> Result<()> {
        // Restore old name's schema
        let old_schema_key = format!("schema_{}", old_name);
        let mut schema_bytes = Vec::new();
        ciborium::into_writer(old_schema, &mut schema_bytes)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        batch.insert(&self.metadata, old_schema_key, schema_bytes);

        // Remove new name's schema
        let new_schema_key = format!("schema_{}", new_name);
        batch.remove(self.metadata.clone(), new_schema_key);

        Ok(())
    }

    /// Rollback CREATE INDEX: remove index from metadata (batched)
    pub fn rollback_create_index(
        &mut self,
        batch: &mut fjall::Batch,
        index_name: &str,
    ) -> Result<()> {
        // Remove from in-memory state
        self.index_storages.remove(index_name);
        self.index_metadata.remove(index_name);

        // Remove from persistent metadata
        let index_key = format!("index_{}", index_name);
        batch.remove(self.metadata.clone(), index_key);
        Ok(())
    }

    /// Rollback DROP INDEX: restore index metadata (batched)
    pub fn rollback_drop_index(
        &mut self,
        batch: &mut fjall::Batch,
        index_name: &str,
        metadata: &IndexMetadata,
    ) -> Result<()> {
        // Restore in-memory metadata
        self.index_metadata
            .insert(index_name.to_string(), metadata.clone());

        // Recreate MvccStorage for this index
        let mvcc_config = MvccConfig {
            data_dir: self.config.data_dir.clone(),
            block_cache_size: self.config.block_cache_size as u64,
            compression: self.config.compression,
            persist_mode: self.config.persist_mode,
            history_bucket_duration: std::time::Duration::from_secs(60),
            uncommitted_bucket_duration: std::time::Duration::from_secs(30),
            history_retention_window: std::time::Duration::from_secs(300),
            uncommitted_retention_window: std::time::Duration::from_secs(120),
            cleanup_interval: std::time::Duration::from_secs(30),
        };

        let index_storage = MvccStorage::<IndexEntity>::with_shared_keyspace(
            self.keyspace.clone(),
            index_name.to_string(),
            mvcc_config,
        )?;

        self.index_storages
            .insert(index_name.to_string(), index_storage);

        // Restore persistent metadata
        let index_key = format!("index_{}", index_name);
        let mut index_bytes = Vec::new();
        ciborium::into_writer(metadata, &mut index_bytes)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        batch.insert(&self.metadata, index_key, index_bytes);
        Ok(())
    }

    /// Get and increment table generation counter
    /// This ensures each CREATE gets a unique generation, even after DROP
    fn get_and_increment_table_generation(
        &mut self,
        batch: &mut fjall::Batch,
        table_name: &str,
    ) -> Result<u64> {
        let gen_key = format!("table_generation_{}", table_name);

        // Read current generation (0 if doesn't exist)
        let current_gen = if let Some(gen_bytes) = self.metadata.get(&gen_key)? {
            u64::from_be_bytes(
                gen_bytes[..8]
                    .try_into()
                    .map_err(|_| Error::Other("Invalid generation bytes".to_string()))?,
            )
        } else {
            0
        };

        // Increment generation
        let new_gen = current_gen + 1;

        // Store new generation in batch
        batch.insert(&self.metadata, gen_key, new_gen.to_be_bytes());

        // Update in-memory cache
        self.table_generations
            .insert(table_name.to_string(), new_gen);

        Ok(new_gen)
    }

    /// Create a new table
    pub fn create_table(
        &mut self,
        batch: &mut fjall::Batch,
        table_name: String,
        schema: TableSchema,
    ) -> Result<()> {
        // Check if table already exists
        if self.schemas.contains_key(&table_name) {
            return Err(Error::DuplicateTable(table_name));
        }

        // Get and increment generation for this table name
        let generation = self.get_and_increment_table_generation(batch, &table_name)?;

        // Create MvccStorage for this table with generation-based partition name
        let mvcc_config = MvccConfig {
            data_dir: self.config.data_dir.clone(),
            block_cache_size: self.config.block_cache_size as u64,
            compression: self.config.compression,
            persist_mode: self.config.persist_mode,
            history_bucket_duration: std::time::Duration::from_secs(60),
            uncommitted_bucket_duration: std::time::Duration::from_secs(30),
            history_retention_window: std::time::Duration::from_secs(300),
            uncommitted_retention_window: std::time::Duration::from_secs(120),
            cleanup_interval: std::time::Duration::from_secs(30),
        };

        // Use generation in partition name to avoid reusing old data
        let partition_name = format!("{}_g{}", table_name, generation);
        let table_storage = MvccStorage::<TableEntity>::with_shared_keyspace(
            self.keyspace.clone(),
            partition_name,
            mvcc_config,
        )?;

        // Persist schema to batch (atomic with transaction)
        self.persist_schema_to_batch(batch, &table_name, &schema)?;

        // Initialize next_row_id counter in batch

        let row_id_key = format!("next_row_id_{}", table_name);
        batch.insert(&self.metadata, row_id_key, 1u64.to_be_bytes());

        // Store in memory
        self.table_storages
            .insert(table_name.clone(), table_storage);
        let schema_arc = Arc::new(schema.clone());
        self.schemas.insert(table_name.clone(), schema_arc);
        self.next_row_ids
            .insert(table_name.clone(), AtomicU64::new(1));

        // Create unique indexes for columns marked with unique constraint
        for column in &schema.columns {
            if column.unique {
                let index_name = format!("{}_{}_unique", table_name, column.name);
                // Create the index using the existing create_index method
                self.create_index(
                    index_name,
                    table_name.clone(),
                    vec![column.name.clone()],
                    true, // unique
                )?;
            }
        }

        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&mut self, batch: &mut fjall::Batch, table_name: &str) -> Result<()> {
        // Remove from in-memory state
        self.table_storages.remove(table_name);
        self.schemas.remove(table_name);
        self.next_row_ids.remove(table_name);

        // NOTE: We don't physically delete the partition here because:
        // 1. delete_partition() is not transactional (commits immediately)
        // 2. If transaction aborts, we'd have deleted data but schema remains
        // The partition becomes orphaned but will be cleaned up by removing
        // schema metadata (so it won't be loaded on restart)

        // Remove schema from metadata using batch (atomic with transaction)

        let schema_key = format!("schema_{}", table_name);
        batch.remove(self.metadata.clone(), schema_key);

        let row_id_key = format!("next_row_id_{}", table_name);
        batch.remove(self.metadata.clone(), row_id_key);

        Ok(())
    }

    /// Add a column to an existing table
    pub fn alter_table_add_column(
        &mut self,
        batch: &mut fjall::Batch,
        table_name: &str,
        column: crate::types::schema::Column,
        default_value: Value,
        txn_id: HlcTimestamp,
        log_index: u64,
    ) -> Result<()> {
        // Get current schema
        let mut schema = self
            .schemas
            .get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .as_ref()
            .clone();

        // Increment schema version
        schema.schema_version += 1;

        // Add column to schema
        schema.columns.push(column);

        // Scan all existing rows and append default value
        let table_storage = self
            .table_storages
            .get_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        // Collect all rows (row_id, values) to update
        let mut rows_to_update = Vec::new();
        {
            let old_schema = self
                .schemas
                .get(table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

            let iter = table_storage.iter(txn_id)?;
            for result in iter {
                let (row_id, encoded_row) = result?;
                let mut values = decode_row(&encoded_row, old_schema)?;
                values.push(default_value.clone());
                rows_to_update.push((row_id, values));
            }
        }

        // Update schema first (needed for encode_row to work correctly)
        let schema_arc = Arc::new(schema.clone());
        self.schemas.insert(table_name.to_string(), schema_arc);

        // Write all updated rows with new schema
        for (row_id, values) in rows_to_update {
            self.write_row(batch, table_name, row_id, &values, txn_id, log_index)?;
        }

        // Persist updated schema to batch (atomic with transaction)
        self.persist_schema_to_batch(batch, table_name, &schema)?;

        Ok(())
    }

    /// Drop a column from an existing table
    pub fn alter_table_drop_column(
        &mut self,
        batch: &mut fjall::Batch,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
        txn_id: HlcTimestamp,
        log_index: u64,
    ) -> Result<()> {
        // Get current schema
        let mut schema = self
            .schemas
            .get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .as_ref()
            .clone();

        // Find column index
        let column_index = match schema.columns.iter().position(|c| c.name == column_name) {
            Some(idx) => idx,
            None => {
                if if_exists {
                    // Column doesn't exist but IF EXISTS was specified, so just succeed
                    return Ok(());
                } else {
                    return Err(Error::DroppingColumnNotFound(column_name.to_string()));
                }
            }
        };

        // Check if column is referenced by foreign key in other tables
        self.check_column_not_referenced_by_fk(table_name, column_name)?;

        // Check if column has foreign key constraint
        self.check_column_not_referencing_fk(&schema, column_name)?;

        // Drop any indexes on this column
        let indexes_to_drop: Vec<String> = self
            .index_metadata
            .iter()
            .filter(|(_, idx)| {
                idx.table == table_name && idx.columns.contains(&column_name.to_string())
            })
            .map(|(name, _)| name.clone())
            .collect();

        for index_name in indexes_to_drop {
            self.drop_index(&index_name)?;
        }

        // Increment schema version
        schema.schema_version += 1;

        // Remove column from schema
        schema.columns.remove(column_index);

        // Scan all existing rows and remove column value
        let table_storage = self
            .table_storages
            .get_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        // Collect all rows to update
        let mut rows_to_update = Vec::new();
        {
            let old_schema = self
                .schemas
                .get(table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

            let iter = table_storage.iter(txn_id)?;
            for result in iter {
                let (row_id, encoded_row) = result?;
                let mut values = decode_row(&encoded_row, old_schema)?;
                values.remove(column_index);
                rows_to_update.push((row_id, values));
            }
        }

        // Update schema first
        let schema_arc = Arc::new(schema.clone());
        self.schemas.insert(table_name.to_string(), schema_arc);

        // Write all updated rows with new schema
        for (row_id, values) in rows_to_update {
            self.write_row(batch, table_name, row_id, &values, txn_id, log_index)?;
        }

        // Persist updated schema to batch (atomic with transaction)
        self.persist_schema_to_batch(batch, table_name, &schema)?;

        Ok(())
    }

    /// Rename a column in an existing table
    pub fn alter_table_rename_column(
        &mut self,
        batch: &mut fjall::Batch,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()> {
        // Get current schema
        let mut schema = self
            .schemas
            .get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .as_ref()
            .clone();

        // Find and rename column
        let column = schema
            .columns
            .iter_mut()
            .find(|c| c.name == old_column_name)
            .ok_or_else(|| Error::RenamingColumnNotFound(old_column_name.to_string()))?;

        column.name = new_column_name.to_string();

        // Check if column is referenced by foreign key in other tables
        self.check_column_not_referenced_by_fk(table_name, old_column_name)?;

        // Check if column has foreign key constraint
        self.check_column_not_referencing_fk(&schema, old_column_name)?;

        // DON'T increment schema version for rename - row structure doesn't change
        // Rows are still compatible with the schema, only the column name changes
        // schema.schema_version += 1;

        // Update indexes that reference this column
        for (_, index_meta) in self.index_metadata.iter_mut() {
            if index_meta.table == table_name {
                for col in index_meta.columns.iter_mut() {
                    if col == old_column_name {
                        *col = new_column_name.to_string();
                    }
                }
            }
        }

        // Update schema in memory and persist to batch (atomic with transaction)
        let schema_arc = Arc::new(schema.clone());
        self.schemas.insert(table_name.to_string(), schema_arc);
        self.persist_schema_to_batch(batch, table_name, &schema)?;

        Ok(())
    }

    /// Rename a table (metadata-only operation)
    pub fn alter_table_rename_table(
        &mut self,
        batch: &mut fjall::Batch,
        old_name: &str,
        new_name: &str,
        _txn_id: HlcTimestamp,
        _log_index: u64,
    ) -> Result<()> {
        // Get current schema
        let mut schema = self
            .schemas
            .get(old_name)
            .ok_or_else(|| Error::TableNotFound(old_name.to_string()))?
            .as_ref()
            .clone();

        // Update table name in schema
        schema.name = new_name.to_string();
        // DON'T increment schema version - row structure doesn't change, just metadata
        // schema.schema_version += 1;

        // Move storage reference (the underlying partition stays the same)
        let storage = self
            .table_storages
            .remove(old_name)
            .ok_or_else(|| Error::TableNotFound(old_name.to_string()))?;

        self.table_storages.insert(new_name.to_string(), storage);

        // Update schema references
        let schema_arc = Arc::new(schema.clone());
        self.schemas.remove(old_name);
        self.schemas
            .insert(new_name.to_string(), schema_arc.clone());

        // Move row ID counter
        if let Some(counter) = self.next_row_ids.remove(old_name) {
            self.next_row_ids.insert(new_name.to_string(), counter);
        }

        // Update index metadata
        for (_, index_meta) in self.index_metadata.iter_mut() {
            if index_meta.table == old_name {
                index_meta.table = new_name.to_string();
            }
        }

        // Persist new schema and remove old using batch (atomic with transaction)
        self.persist_schema_to_batch(batch, new_name, &schema)?;

        let old_schema_key = format!("schema_{}", old_name);
        batch.remove(self.metadata.clone(), old_schema_key);

        Ok(())
    }

    /// Helper: Persist schema to metadata partition using a batch (transactional)
    ///
    /// This adds the schema update to the provided batch, ensuring it commits
    /// atomically with other operations (data modifications, etc.).
    fn persist_schema_to_batch(
        &self,
        batch: &mut fjall::Batch,
        table_name: &str,
        schema: &TableSchema,
    ) -> Result<()> {
        let schema_key = format!("schema_{}", table_name);
        let mut schema_bytes = Vec::new();
        ciborium::into_writer(schema, &mut schema_bytes)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Add to batch instead of immediate insert - commits atomically
        batch.insert(&self.metadata, schema_key, schema_bytes);

        Ok(())
    }

    /// Check if a column is referenced by foreign key in other tables
    fn check_column_not_referenced_by_fk(&self, table_name: &str, column_name: &str) -> Result<()> {
        // Check all other tables for FK constraints referencing this column
        for (other_table_name, other_schema) in &self.schemas {
            if other_table_name == table_name {
                continue; // Skip self
            }

            for fk in &other_schema.foreign_keys {
                if fk.referenced_table == table_name
                    && fk.referenced_columns.contains(&column_name.to_string())
                {
                    return Err(Error::CannotAlterReferencedColumn(column_name.to_string()));
                }
            }
        }

        Ok(())
    }

    /// Check if a column has a foreign key constraint pointing to another table
    fn check_column_not_referencing_fk(
        &self,
        schema: &TableSchema,
        column_name: &str,
    ) -> Result<()> {
        // Check if this column has an outgoing FK constraint
        for fk in &schema.foreign_keys {
            if fk.columns.contains(&column_name.to_string()) {
                return Err(Error::CannotAlterReferencingColumn(column_name.to_string()));
            }
        }

        Ok(())
    }

    /// Read a row from a table
    pub fn read_row(
        &self,
        table_name: &str,
        row_id: u64,
        txn_id: HlcTimestamp,
    ) -> Result<Option<Vec<Value>>> {
        // Get table storage
        let table_storage = self
            .table_storages
            .get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        // Get schema
        let schema = self
            .schemas
            .get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        // Read opaque bytes from MVCC
        let encoded_bytes = table_storage.read(&row_id, txn_id)?;

        // If no data, return None
        let Some(bytes) = encoded_bytes else {
            return Ok(None);
        };

        // Decode with schema (preserves optimization!)
        let values = decode_row(&bytes, schema)?;

        Ok(Some(values))
    }

    /// Write a row to a table
    pub fn write_row(
        &mut self,
        batch: &mut fjall::Batch,
        table_name: &str,
        row_id: u64,
        values: &[Value],
        txn_id: HlcTimestamp,
        log_index: u64,
    ) -> Result<()> {
        // Get table storage
        let table_storage = self
            .table_storages
            .get_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        // Get schema
        let schema = self
            .schemas
            .get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        // Encode row WITH SCHEMA (preserves 36.1 bytes/row optimization!)
        let encoded_row = encode_row(values, schema)?;

        // Check if row exists (for update vs insert)
        let encoded_old = table_storage.read(&row_id, txn_id)?;

        // Create appropriate delta
        let delta = if let Some(old) = encoded_old {
            TableDelta::Update {
                row_id,
                encoded_old: old,
                encoded_new: encoded_row,
            }
        } else {
            TableDelta::Insert {
                row_id,
                encoded_row,
            }
        };

        // Write to MVCC storage via batch
        table_storage.write_to_batch(batch, delta, txn_id, log_index)?;

        Ok(())
    }

    /// Scan all rows in a table using MVCC iterator
    ///
    /// Returns an iterator over (row_id, values) pairs visible at the snapshot time.
    /// This properly handles uncommitted changes and history reconstruction.
    pub fn scan_table<'a>(
        &'a self,
        table_name: &str,
        txn_id: HlcTimestamp,
    ) -> Result<TableScanIterator<'a>> {
        // Get table storage
        let table_storage = self
            .table_storages
            .get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        // Get schema for decoding
        let schema = self
            .schemas
            .get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .clone();

        // Use MVCC iterator - this handles uncommitted + history automatically!
        let iter = table_storage.iter(txn_id)?;

        Ok(TableScanIterator {
            mvcc_iter: iter,
            schema,
        })
    }

    /// Delete a row from a table
    pub fn delete_row(
        &mut self,
        batch: &mut fjall::Batch,
        table_name: &str,
        row_id: u64,
        txn_id: HlcTimestamp,
        log_index: u64,
    ) -> Result<bool> {
        // Get table storage
        let table_storage = self
            .table_storages
            .get_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        // Read current value
        let encoded_old = table_storage.read(&row_id, txn_id)?;

        // If doesn't exist, nothing to delete
        let Some(old) = encoded_old else {
            return Ok(false);
        };

        // Create delete delta
        let delta = TableDelta::Delete {
            row_id,
            encoded_old: old,
        };

        // Write to MVCC storage via batch
        table_storage.write_to_batch(batch, delta, txn_id, log_index)?;

        Ok(true)
    }

    /// Commit a transaction (atomic across all tables and indexes)
    pub fn commit_transaction(&mut self, txn_id: HlcTimestamp, log_index: u64) -> Result<()> {
        // Create shared batch for atomic commit
        let mut batch = self.keyspace.batch();

        // Commit all table storages
        for table_storage in self.table_storages.values_mut() {
            table_storage.commit_transaction_to_batch(&mut batch, txn_id, log_index)?;
        }

        // Commit all index storages
        for index_storage in self.index_storages.values_mut() {
            index_storage.commit_transaction_to_batch(&mut batch, txn_id, log_index)?;
        }

        // Remove predicates (on commit)
        self.predicate_store.remove_all(&mut batch, txn_id)?;

        // Remove pending DDLs (on commit)
        self.cleanup_pending_ddls(&mut batch, txn_id)?;

        // Atomic commit across ALL entities!
        batch.commit()?;

        // Cleanup old buckets if needed (throttled internally by each storage)
        for table_storage in self.table_storages.values_mut() {
            table_storage.maybe_cleanup(txn_id).ok();
        }
        for index_storage in self.index_storages.values_mut() {
            index_storage.maybe_cleanup(txn_id).ok();
        }

        Ok(())
    }

    /// Abort a transaction (atomic across all tables and indexes)
    /// Now accepts an external batch for full atomicity with DDL rollbacks
    pub fn abort_transaction_to_batch(
        &mut self,
        batch: &mut fjall::Batch,
        txn_id: HlcTimestamp,
        log_index: u64,
    ) -> Result<()> {
        // Abort all table storages
        for table_storage in self.table_storages.values_mut() {
            table_storage.abort_transaction_to_batch(batch, txn_id, log_index)?;
        }

        // Abort all index storages
        for index_storage in self.index_storages.values_mut() {
            index_storage.abort_transaction_to_batch(batch, txn_id, log_index)?;
        }

        // Remove predicates (on abort)
        self.predicate_store.remove_all(batch, txn_id)?;

        // Remove pending DDLs (on abort)
        self.cleanup_pending_ddls(batch, txn_id)?;

        // Cleanup old buckets if needed (throttled internally by each storage)
        for table_storage in self.table_storages.values_mut() {
            table_storage.maybe_cleanup(txn_id).ok();
        }
        for index_storage in self.index_storages.values_mut() {
            index_storage.maybe_cleanup(txn_id).ok();
        }

        Ok(())
    }

    /// Get log index (for crash recovery)
    pub fn get_log_index(&self) -> u64 {
        // Get log index from any table storage (they should all be in sync)
        // If no tables, return 0
        self.table_storages
            .values()
            .next()
            .map(|s| s.get_log_index())
            .unwrap_or(0)
    }

    // ========================================================================
    // DDL Persistence (for crash recovery)
    // ========================================================================

    /// Persist pending DDL immediately (for crash recovery)
    pub fn persist_pending_ddl(
        &self,
        txn_id: HlcTimestamp,
        ddl: &crate::types::context::PendingDdl,
    ) -> Result<()> {
        let mut batch = self.batch();
        self.ddl_store.add_to_batch(&mut batch, txn_id, ddl)?;
        batch
            .commit()
            .map_err(|e| Error::Other(format!("Failed to persist DDL: {}", e)))?;
        Ok(())
    }

    /// Get all active DDL operations (for recovery)
    pub fn get_active_ddls(
        &self,
    ) -> Result<HashMap<HlcTimestamp, Vec<crate::types::context::PendingDdl>>> {
        self.ddl_store.get_all_active_ddls()
    }

    /// Remove all pending DDLs for a transaction (on commit/abort)
    pub fn cleanup_pending_ddls(
        &self,
        batch: &mut fjall::Batch,
        txn_id: HlcTimestamp,
    ) -> Result<()> {
        self.ddl_store.remove_all(batch, txn_id)
    }

    // ========================================================================
    // Predicate Persistence (for crash recovery)
    // ========================================================================

    /// Add predicates to batch (atomic with data operations)
    pub fn add_predicates_to_batch(
        &self,
        batch: &mut fjall::Batch,
        txn_id: HlcTimestamp,
        predicates: &QueryPredicates,
    ) -> Result<()> {
        // Add read predicates
        for predicate in &predicates.reads {
            self.predicate_store
                .add_to_batch(batch, txn_id, predicate, false)?;
        }

        // Add write predicates
        for predicate in &predicates.writes {
            self.predicate_store
                .add_to_batch(batch, txn_id, predicate, true)?;
        }

        // Add insert predicates as write predicates
        for predicate in &predicates.inserts {
            self.predicate_store
                .add_to_batch(batch, txn_id, predicate, true)?;
        }

        Ok(())
    }

    /// Prepare transaction (release read predicates)
    pub fn prepare_transaction(&self, txn_id: HlcTimestamp) -> Result<()> {
        let mut batch = self.keyspace.batch();
        self.predicate_store.remove_reads(&mut batch, txn_id)?;
        batch.commit()?;
        Ok(())
    }

    /// Get active transactions from storage (for recovery)
    pub fn get_active_transactions(&self) -> Result<HashMap<HlcTimestamp, Vec<Predicate>>> {
        self.predicate_store.get_all_active_transactions()
    }

    // ========================================================================
    // Helper Methods for Executors
    // ========================================================================

    /// Generate next row ID for a table (thread-safe)
    pub fn generate_row_id(&self, table: &str) -> Result<u64> {
        self.next_row_ids
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))
            .map(|counter| counter.fetch_add(1, Ordering::SeqCst))
    }

    /// Get all indexes for a table
    pub fn get_table_indexes(&self, table: &str) -> Vec<&IndexMetadata> {
        self.index_metadata
            .values()
            .filter(|meta| meta.table == table)
            .collect()
    }

    /// Get all unique indexes for a table
    pub fn get_unique_indexes(&self, table: &str) -> Vec<&IndexMetadata> {
        self.get_table_indexes(table)
            .into_iter()
            .filter(|meta| meta.unique)
            .collect()
    }

    // ========================================================================
    // Index Operations
    // ========================================================================

    /// Create a new index
    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        columns: Vec<String>,
        unique: bool,
    ) -> Result<()> {
        // Check if index already exists
        if self.index_metadata.contains_key(&index_name) {
            return Err(Error::Other(format!("Index {} already exists", index_name)));
        }

        // Verify table exists
        if !self.schemas.contains_key(&table_name) {
            return Err(Error::TableNotFound(table_name));
        }

        // Create MvccStorage for this index
        let mvcc_config = MvccConfig {
            data_dir: self.config.data_dir.clone(),
            block_cache_size: self.config.block_cache_size as u64,
            compression: self.config.compression,
            persist_mode: self.config.persist_mode,
            history_bucket_duration: std::time::Duration::from_secs(60),
            uncommitted_bucket_duration: std::time::Duration::from_secs(30),
            history_retention_window: std::time::Duration::from_secs(300),
            uncommitted_retention_window: std::time::Duration::from_secs(120),
            cleanup_interval: std::time::Duration::from_secs(30),
        };

        let index_storage = MvccStorage::<IndexEntity>::with_shared_keyspace(
            self.keyspace.clone(),
            index_name.clone(),
            mvcc_config,
        )?;

        // Create metadata
        let metadata = IndexMetadata {
            name: index_name.clone(),
            table: table_name,
            columns,
            index_type: if unique {
                crate::types::index::IndexType::Unique
            } else {
                crate::types::index::IndexType::BTree
            },
            unique,
        };

        // Store in memory immediately (for transaction-local visibility)
        self.index_storages
            .insert(index_name.clone(), index_storage);
        self.index_metadata
            .insert(index_name.clone(), metadata.clone());

        // Persist metadata (will be rolled back if transaction aborts)
        let index_key = format!("index_{}", index_name);
        let mut index_bytes = Vec::new();
        ciborium::into_writer(&metadata, &mut index_bytes)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        self.metadata.insert(index_key, index_bytes)?;

        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> Result<()> {
        // Remove from in-memory state immediately (for transaction-local visibility)
        self.index_storages.remove(index_name);
        self.index_metadata.remove(index_name);

        // Remove metadata from storage (will be rolled back if transaction aborts)
        let index_key = format!("index_{}", index_name);
        self.metadata.remove(index_key)?;

        // Note: MvccStorage partitions will be cleaned up when dropped

        Ok(())
    }

    /// Insert an entry into an index
    pub fn insert_index_entry(
        &mut self,
        batch: &mut fjall::Batch,
        index_name: &str,
        index_values: Vec<Value>,
        row_id: u64,
        txn_id: HlcTimestamp,
        log_index: u64,
    ) -> Result<()> {
        // Get index storage
        let index_storage = self
            .index_storages
            .get_mut(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        // Create index key
        let key = IndexKey {
            values: index_values,
            row_id,
        };

        // Create insert delta
        let delta = IndexDelta::Insert { key };

        // Write to MVCC storage via batch
        index_storage.write_to_batch(batch, delta, txn_id, log_index)?;

        Ok(())
    }

    /// Delete an entry from an index
    pub fn delete_index_entry(
        &mut self,
        batch: &mut fjall::Batch,
        index_name: &str,
        index_values: Vec<Value>,
        row_id: u64,
        txn_id: HlcTimestamp,
        log_index: u64,
    ) -> Result<()> {
        // Get index storage
        let index_storage = self
            .index_storages
            .get_mut(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        // Create index key
        let key = IndexKey {
            values: index_values,
            row_id,
        };

        // Create delete delta
        let delta = IndexDelta::Delete { key };

        // Write to MVCC storage via batch
        index_storage.write_to_batch(batch, delta, txn_id, log_index)?;

        Ok(())
    }

    /// Update index entries for a row (delete old, insert new)
    #[allow(clippy::too_many_arguments)]
    pub fn update_index_entries(
        &mut self,
        batch: &mut fjall::Batch,
        index_name: &str,
        old_values: Vec<Value>,
        new_values: Vec<Value>,
        row_id: u64,
        txn_id: HlcTimestamp,
        log_index: u64,
    ) -> Result<()> {
        // Delete old entry
        self.delete_index_entry(batch, index_name, old_values, row_id, txn_id, log_index)?;

        // Insert new entry
        self.insert_index_entry(batch, index_name, new_values, row_id, txn_id, log_index)?;

        Ok(())
    }

    /// Lookup rows by exact index values (point query)
    ///
    /// Returns an iterator over rows that match the indexed values.
    /// Example: SELECT * FROM users WHERE name = 'Alice'
    pub fn index_lookup<'a>(
        &'a self,
        index_name: &str,
        values: Vec<Value>,
        txn_id: HlcTimestamp,
    ) -> Result<impl Iterator<Item = Result<(u64, Vec<Value>)>> + 'a> {
        // Get index metadata
        let index_meta = self
            .index_metadata
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;
        let table_name = index_meta.table.clone();

        // Get index storage
        let index_storage = self
            .index_storages
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        // For point lookup, use range scan from row_id=0 to row_id=MAX
        // This ensures we only get exact matches for the values
        use std::ops::Bound;
        let start_key = crate::storage::codec::encode_index_key(&values, 0);
        let end_key = crate::storage::codec::encode_index_key(&values, u64::MAX);

        // Use range scan to find all matching entries
        let index_iter = index_storage.range(
            (Bound::Included(start_key), Bound::Included(end_key)),
            txn_id,
        )?;

        // Extract row IDs and map to rows
        let row_iter = IndexLookupIterator {
            index_iter,
            storage: self,
            table_name,
            indexed_columns: index_meta.columns.clone(),
            txn_id,
        };

        Ok(row_iter)
    }

    /// Range scan over index (range query)
    ///
    /// Returns an iterator over rows within the value range.
    /// Example: SELECT * FROM users WHERE age >= 18 AND age <= 65
    pub fn index_range_scan<'a>(
        &'a self,
        index_name: &str,
        start_values: Option<Vec<Value>>,
        start_inclusive: bool,
        end_values: Option<Vec<Value>>,
        end_inclusive: bool,
        txn_id: HlcTimestamp,
    ) -> Result<impl Iterator<Item = Result<(u64, Vec<Value>)>> + 'a> {
        // Get index metadata
        let index_meta = self
            .index_metadata
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;
        let table_name = index_meta.table.clone();

        // Get index storage
        let index_storage = self
            .index_storages
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        // Build range bounds
        use std::ops::Bound;
        let start_bound = match start_values {
            Some(values) => {
                if start_inclusive {
                    // Inclusive: start from (value, row_id=0)
                    let key = crate::storage::codec::encode_index_key(&values, 0);
                    let prefix_len = key.len() - 8; // Remove row_id
                    Bound::Included(key[..prefix_len].to_vec())
                } else {
                    // Exclusive: start after (value, row_id=MAX)
                    // This skips all entries with this value
                    let key = crate::storage::codec::encode_index_key(&values, u64::MAX);
                    Bound::Excluded(key)
                }
            }
            None => Bound::Unbounded,
        };

        let end_bound = match end_values {
            Some(values) => {
                if end_inclusive {
                    // Inclusive: end at (value, row_id=MAX)
                    let key = crate::storage::codec::encode_index_key(&values, u64::MAX);
                    Bound::Included(key)
                } else {
                    // Exclusive: end before (value, row_id=0)
                    let key = crate::storage::codec::encode_index_key(&values, 0);
                    let prefix_len = key.len() - 8; // Remove row_id
                    Bound::Excluded(key[..prefix_len].to_vec())
                }
            }
            None => Bound::Unbounded,
        };

        // Use range scan
        let index_iter = index_storage.range((start_bound, end_bound), txn_id)?;

        // Extract row IDs and map to rows
        let row_iter = IndexLookupIterator {
            index_iter,
            storage: self,
            table_name,
            indexed_columns: index_meta.columns.clone(),
            txn_id,
        };

        Ok(row_iter)
    }

    /// Check if a unique index would be violated by a value
    ///
    /// Returns true if the value already exists in the index (for a different row)
    pub fn check_unique_violation(
        &self,
        index_name: &str,
        values: Vec<Value>,
        exclude_row_id: Option<u64>,
        txn_id: HlcTimestamp,
    ) -> Result<bool> {
        // Get index metadata
        let index_meta = self
            .index_metadata
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        if !index_meta.unique {
            // Not a unique index, no violation possible
            return Ok(false);
        }

        // Get index storage
        let index_storage = self
            .index_storages
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        // Use range scan for exact value match
        use std::ops::Bound;
        let start_key = crate::storage::codec::encode_index_key(&values, 0);
        let end_key = crate::storage::codec::encode_index_key(&values, u64::MAX);

        // Scan for any matching entries
        let index_iter = index_storage.range(
            (Bound::Included(start_key), Bound::Included(end_key)),
            txn_id,
        )?;

        // Check if any entry exists (excluding the specified row if provided)
        for result in index_iter {
            let (index_key, _) = result?;

            // If we should exclude a specific row, check the row_id
            if let Some(exclude_id) = exclude_row_id
                && index_key.row_id == exclude_id
            {
                continue; // Skip this row
            }

            // Found a matching entry - violation!
            return Ok(true);
        }

        Ok(false)
    }
}

/// Iterator over table rows with schema-aware decoding
pub struct TableScanIterator<'a> {
    mvcc_iter: proven_mvcc::MvccIterator<'a, TableEntity>,
    schema: Arc<crate::types::schema::Table>,
}

impl<'a> Iterator for TableScanIterator<'a> {
    type Item = Result<(u64, Vec<Value>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.mvcc_iter.next() {
            Some(Ok((row_id, encoded_bytes))) => {
                // Decode row with schema
                match decode_row(&encoded_bytes, &self.schema) {
                    Ok(values) => Some(Ok((row_id, values))),
                    Err(e) => Some(Err(e)),
                }
            }
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }
}

/// Iterator for index lookups - maps index keys to table rows
struct IndexLookupIterator<'a> {
    index_iter: proven_mvcc::MvccIterator<'a, IndexEntity>,
    storage: &'a SqlStorage,
    table_name: String,
    txn_id: HlcTimestamp,
    indexed_columns: Vec<String>, // Column names in the index
}

impl<'a> Iterator for IndexLookupIterator<'a> {
    type Item = Result<(u64, Vec<Value>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next index entry
        loop {
            match self.index_iter.next() {
                Some(Ok((index_key, _))) => {
                    // Extract row_id from index key
                    let row_id = index_key.row_id;

                    // Look up the row
                    match self.storage.read_row(&self.table_name, row_id, self.txn_id) {
                        Ok(Some(values)) => {
                            // Filter out rows with NULL values in indexed columns
                            // Per SQL standard: NULL never matches equality or range predicates
                            let has_null = self.indexed_columns.iter().any(|col_name| {
                                if let Some(schema) = self.storage.schemas.get(&self.table_name)
                                    && let Some(col_idx) =
                                        schema.columns.iter().position(|c| &c.name == col_name)
                                    && let Some(val) = values.get(col_idx)
                                {
                                    return val == &Value::Null;
                                }
                                false
                            });

                            if has_null {
                                continue; // Skip rows with NULL in indexed columns
                            }

                            return Some(Ok((row_id, values)));
                        }
                        Ok(None) => continue, // Row was deleted, skip to next
                        Err(e) => return Some(Err(e)),
                    }
                }
                Some(Err(e)) => return Some(Err(e.into())),
                None => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::data_type::DataType;
    use crate::types::schema::{Column, Table};
    use proven_hlc::NodeId;

    fn test_config() -> SqlStorageConfig {
        SqlStorageConfig {
            data_dir: tempfile::tempdir().unwrap().path().to_path_buf(),
            ..Default::default()
        }
    }

    fn create_test_schema() -> TableSchema {
        Table {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::I64,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    index: false,
                    unique: false,
                    references: None,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Str,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    index: false,
                    unique: false,
                    references: None,
                },
            ],
            primary_key: Some(0), // id column
            foreign_keys: vec![],
            schema_version: 1,
        }
    }

    fn create_test_schema_with_city() -> TableSchema {
        Table {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::I64,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    index: false,
                    unique: false,
                    references: None,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Str,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    index: false,
                    unique: false,
                    references: None,
                },
                Column {
                    name: "city".to_string(),
                    data_type: DataType::Str,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    index: false,
                    unique: false,
                    references: None,
                },
            ],
            primary_key: Some(0),
            foreign_keys: vec![],
            schema_version: 1,
        }
    }

    fn create_test_schema_with_age() -> TableSchema {
        Table {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::I64,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    index: false,
                    unique: false,
                    references: None,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Str,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    index: false,
                    unique: false,
                    references: None,
                },
                Column {
                    name: "age".to_string(),
                    data_type: DataType::I64,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    index: false,
                    unique: false,
                    references: None,
                },
            ],
            primary_key: Some(0),
            foreign_keys: vec![],
            schema_version: 1,
        }
    }

    #[test]
    fn test_create_table() {
        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();

        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        assert!(storage.schemas.contains_key("users"));
        assert!(storage.table_storages.contains_key("users"));
    }

    #[test]
    fn test_write_and_read_row() {
        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();
        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        let txn_id = HlcTimestamp::new(1000, 0, NodeId::new(1));
        let values = vec![Value::integer(123), Value::string("Alice")];

        // Write row
        let mut batch = storage.batch();
        storage
            .write_row(&mut batch, "users", 1, &values, txn_id, 1)
            .unwrap();
        batch.commit().unwrap();

        // Read row
        let read_values = storage.read_row("users", 1, txn_id).unwrap();
        assert_eq!(read_values, Some(values));
    }

    #[test]
    fn test_delete_row() {
        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();
        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        let txn_id = HlcTimestamp::new(1000, 0, NodeId::new(1));
        let values = vec![Value::integer(123), Value::string("Alice")];

        // Write row
        let mut batch = storage.batch();
        storage
            .write_row(&mut batch, "users", 1, &values, txn_id, 1)
            .unwrap();
        batch.commit().unwrap();

        // Delete row
        let mut batch = storage.batch();
        let deleted = storage
            .delete_row(&mut batch, "users", 1, txn_id, 2)
            .unwrap();
        batch.commit().unwrap();

        assert!(deleted);

        // Verify deleted
        let read_values = storage.read_row("users", 1, txn_id).unwrap();
        assert_eq!(read_values, None);
    }

    #[test]
    fn test_create_index() {
        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();
        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        // Create index
        storage
            .create_index(
                "idx_name".to_string(),
                "users".to_string(),
                vec!["name".to_string()],
                false,
            )
            .unwrap();

        assert!(storage.index_metadata.contains_key("idx_name"));
        assert!(storage.index_storages.contains_key("idx_name"));
    }

    #[test]
    fn test_index_entries() {
        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();
        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        // Create index
        storage
            .create_index(
                "idx_name".to_string(),
                "users".to_string(),
                vec!["name".to_string()],
                false,
            )
            .unwrap();

        let txn_id = HlcTimestamp::new(1000, 0, NodeId::new(1));

        // Insert index entry
        let mut batch = storage.batch();
        storage
            .insert_index_entry(
                &mut batch,
                "idx_name",
                vec![Value::string("Alice")],
                1,
                txn_id,
                1,
            )
            .unwrap();
        batch.commit().unwrap();

        // Delete index entry
        let mut batch = storage.batch();
        storage
            .delete_index_entry(
                &mut batch,
                "idx_name",
                vec![Value::string("Alice")],
                1,
                txn_id,
                2,
            )
            .unwrap();
        batch.commit().unwrap();
    }

    #[test]
    fn test_commit_abort_atomic() {
        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();
        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        let txn_id = HlcTimestamp::new(1000, 0, NodeId::new(1));
        let values = vec![Value::integer(123), Value::string("Alice")];

        // Write row
        let mut batch = storage.batch();
        storage
            .write_row(&mut batch, "users", 1, &values, txn_id, 1)
            .unwrap();
        batch.commit().unwrap();

        // Commit transaction
        storage.commit_transaction(txn_id, 2).unwrap();

        // Verify readable by LATER transaction (committed data)
        let later_txn = HlcTimestamp::new(2000, 0, NodeId::new(1));
        let read_values = storage.read_row("users", 1, later_txn).unwrap();
        assert_eq!(read_values, Some(values));
    }

    #[test]
    fn test_table_scan() {
        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();
        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        let txn_id = HlcTimestamp::new(1000, 0, NodeId::new(1));

        // Insert multiple rows
        let mut batch = storage.batch();
        storage
            .write_row(
                &mut batch,
                "users",
                1,
                &[Value::integer(1), Value::string("Alice")],
                txn_id,
                1,
            )
            .unwrap();
        storage
            .write_row(
                &mut batch,
                "users",
                2,
                &[Value::integer(2), Value::string("Bob")],
                txn_id,
                2,
            )
            .unwrap();
        storage
            .write_row(
                &mut batch,
                "users",
                5,
                &[Value::integer(5), Value::string("Charlie")],
                txn_id,
                3,
            )
            .unwrap();
        batch.commit().unwrap();

        // Scan table
        let rows: Vec<_> = storage
            .scan_table("users", txn_id)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        // Should find 3 rows
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[0].1, vec![Value::integer(1), Value::string("Alice")]);
        assert_eq!(rows[1].0, 2);
        assert_eq!(rows[1].1, vec![Value::integer(2), Value::string("Bob")]);
        assert_eq!(rows[2].0, 5);
        assert_eq!(rows[2].1, vec![Value::integer(5), Value::string("Charlie")]);
    }

    #[test]
    fn test_index_lookup() {
        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema_with_city();
        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        let txn_id = HlcTimestamp::new(1000, 0, NodeId::new(1));

        // Create non-unique index on city
        storage
            .create_index(
                "idx_city".to_string(),
                "users".to_string(),
                vec!["city".to_string()],
                false, // non-unique
            )
            .unwrap();

        // Insert multiple rows with same city
        let mut batch = storage.batch();
        storage
            .write_row(
                &mut batch,
                "users",
                1,
                &[
                    Value::integer(1),
                    Value::string("Alice"),
                    Value::string("Seattle"),
                ],
                txn_id,
                1,
            )
            .unwrap();
        storage
            .write_row(
                &mut batch,
                "users",
                2,
                &[
                    Value::integer(2),
                    Value::string("Bob"),
                    Value::string("Seattle"),
                ],
                txn_id,
                2,
            )
            .unwrap();
        storage
            .write_row(
                &mut batch,
                "users",
                3,
                &[
                    Value::integer(3),
                    Value::string("Charlie"),
                    Value::string("Portland"),
                ],
                txn_id,
                3,
            )
            .unwrap();
        storage
            .write_row(
                &mut batch,
                "users",
                4,
                &[
                    Value::integer(4),
                    Value::string("Diana"),
                    Value::string("Seattle"),
                ],
                txn_id,
                4,
            )
            .unwrap();
        batch.commit().unwrap();

        // Add index entries
        let mut batch = storage.batch();
        storage
            .insert_index_entry(
                &mut batch,
                "idx_city",
                vec![Value::string("Seattle")],
                1,
                txn_id,
                1,
            )
            .unwrap();
        storage
            .insert_index_entry(
                &mut batch,
                "idx_city",
                vec![Value::string("Seattle")],
                2,
                txn_id,
                2,
            )
            .unwrap();
        storage
            .insert_index_entry(
                &mut batch,
                "idx_city",
                vec![Value::string("Portland")],
                3,
                txn_id,
                3,
            )
            .unwrap();
        storage
            .insert_index_entry(
                &mut batch,
                "idx_city",
                vec![Value::string("Seattle")],
                4,
                txn_id,
                4,
            )
            .unwrap();
        batch.commit().unwrap();

        // Lookup by city = Seattle (should find 3 rows)
        let results: Vec<_> = storage
            .index_lookup("idx_city", vec![Value::string("Seattle")], txn_id)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 1); // row_id
        assert_eq!(results[0].1[1], Value::string("Alice")); // name
        assert_eq!(results[1].0, 2);
        assert_eq!(results[1].1[1], Value::string("Bob"));
        assert_eq!(results[2].0, 4);
        assert_eq!(results[2].1[1], Value::string("Diana"));

        // Lookup by city = Portland (should find 1 row)
        let results: Vec<_> = storage
            .index_lookup("idx_city", vec![Value::string("Portland")], txn_id)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 3);
        assert_eq!(results[0].1[1], Value::string("Charlie"));

        // Lookup by city = New York (should find 0 rows)
        let results: Vec<_> = storage
            .index_lookup("idx_city", vec![Value::string("New York")], txn_id)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_index_range_scan() {
        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema_with_age();
        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        let txn_id = HlcTimestamp::new(1000, 0, NodeId::new(1));

        // Create index on age
        storage
            .create_index(
                "idx_age".to_string(),
                "users".to_string(),
                vec!["age".to_string()],
                false,
            )
            .unwrap();

        // Insert rows with different ages
        let mut batch = storage.batch();
        storage
            .write_row(
                &mut batch,
                "users",
                1,
                &[
                    Value::integer(1),
                    Value::string("Alice"),
                    Value::integer(25),
                ],
                txn_id,
                1,
            )
            .unwrap();
        storage
            .write_row(
                &mut batch,
                "users",
                2,
                &[Value::integer(2), Value::string("Bob"), Value::integer(30)],
                txn_id,
                2,
            )
            .unwrap();
        storage
            .write_row(
                &mut batch,
                "users",
                3,
                &[
                    Value::integer(3),
                    Value::string("Charlie"),
                    Value::integer(35),
                ],
                txn_id,
                3,
            )
            .unwrap();
        storage
            .write_row(
                &mut batch,
                "users",
                4,
                &[
                    Value::integer(4),
                    Value::string("Diana"),
                    Value::integer(40),
                ],
                txn_id,
                4,
            )
            .unwrap();
        storage
            .write_row(
                &mut batch,
                "users",
                5,
                &[Value::integer(5), Value::string("Eve"), Value::integer(45)],
                txn_id,
                5,
            )
            .unwrap();
        batch.commit().unwrap();

        // Add index entries
        let mut batch = storage.batch();
        storage
            .insert_index_entry(
                &mut batch,
                "idx_age",
                vec![Value::integer(25)],
                1,
                txn_id,
                1,
            )
            .unwrap();
        storage
            .insert_index_entry(
                &mut batch,
                "idx_age",
                vec![Value::integer(30)],
                2,
                txn_id,
                2,
            )
            .unwrap();
        storage
            .insert_index_entry(
                &mut batch,
                "idx_age",
                vec![Value::integer(35)],
                3,
                txn_id,
                3,
            )
            .unwrap();
        storage
            .insert_index_entry(
                &mut batch,
                "idx_age",
                vec![Value::integer(40)],
                4,
                txn_id,
                4,
            )
            .unwrap();
        storage
            .insert_index_entry(
                &mut batch,
                "idx_age",
                vec![Value::integer(45)],
                5,
                txn_id,
                5,
            )
            .unwrap();
        batch.commit().unwrap();

        // Range scan: age >= 30 AND age <= 40
        let results: Vec<_> = storage
            .index_range_scan(
                "idx_age",
                Some(vec![Value::integer(30)]),
                true,
                Some(vec![Value::integer(40)]),
                true,
                txn_id,
            )
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1[1], Value::string("Bob")); // age 30
        assert_eq!(results[1].1[1], Value::string("Charlie")); // age 35
        assert_eq!(results[2].1[1], Value::string("Diana")); // age 40

        // Range scan: age >= 35 (no upper bound)
        let results: Vec<_> = storage
            .index_range_scan(
                "idx_age",
                Some(vec![Value::integer(35)]),
                true,
                None,
                true,
                txn_id,
            )
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1[1], Value::string("Charlie")); // age 35
        assert_eq!(results[1].1[1], Value::string("Diana")); // age 40
        assert_eq!(results[2].1[1], Value::string("Eve")); // age 45

        // Range scan: age <= 30 (no lower bound)
        let results: Vec<_> = storage
            .index_range_scan(
                "idx_age",
                None,
                true,
                Some(vec![Value::integer(30)]),
                true,
                txn_id,
            )
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1[1], Value::string("Alice")); // age 25
        assert_eq!(results[1].1[1], Value::string("Bob")); // age 30
    }

    #[test]
    fn test_check_unique_violation() {
        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();
        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        let txn_id = HlcTimestamp::new(1000, 0, NodeId::new(1));

        // Create unique index on name
        storage
            .create_index(
                "idx_name_unique".to_string(),
                "users".to_string(),
                vec!["name".to_string()],
                true, // unique
            )
            .unwrap();

        // Insert first row
        let mut batch = storage.batch();
        storage
            .write_row(
                &mut batch,
                "users",
                1,
                &[Value::integer(1), Value::string("Alice")],
                txn_id,
                1,
            )
            .unwrap();
        batch.commit().unwrap();

        // Add index entry
        let mut batch = storage.batch();
        storage
            .insert_index_entry(
                &mut batch,
                "idx_name_unique",
                vec![Value::string("Alice")],
                1,
                txn_id,
                1,
            )
            .unwrap();
        batch.commit().unwrap();

        // Check if "Alice" would violate (should return true - already exists)
        let violation = storage
            .check_unique_violation(
                "idx_name_unique",
                vec![Value::string("Alice")],
                None,
                txn_id,
            )
            .unwrap();
        assert!(violation, "Should detect violation - Alice already exists");

        // Check if "Bob" would violate (should return false - doesn't exist)
        let violation = storage
            .check_unique_violation("idx_name_unique", vec![Value::string("Bob")], None, txn_id)
            .unwrap();
        assert!(
            !violation,
            "Should not detect violation - Bob doesn't exist"
        );

        // Check if "Alice" would violate excluding row_id 1 (should return false - same row)
        let violation = storage
            .check_unique_violation(
                "idx_name_unique",
                vec![Value::string("Alice")],
                Some(1),
                txn_id,
            )
            .unwrap();
        assert!(
            !violation,
            "Should not detect violation - excluding the same row"
        );

        // Insert another row with different name
        let mut batch = storage.batch();
        storage
            .write_row(
                &mut batch,
                "users",
                2,
                &[Value::integer(2), Value::string("Bob")],
                txn_id,
                2,
            )
            .unwrap();
        batch.commit().unwrap();

        let mut batch = storage.batch();
        storage
            .insert_index_entry(
                &mut batch,
                "idx_name_unique",
                vec![Value::string("Bob")],
                2,
                txn_id,
                2,
            )
            .unwrap();
        batch.commit().unwrap();

        // Now check if "Bob" would violate (should return true)
        let violation = storage
            .check_unique_violation("idx_name_unique", vec![Value::string("Bob")], None, txn_id)
            .unwrap();
        assert!(violation, "Should detect violation - Bob already exists");

        // Check if "Bob" would violate excluding row_id 2 (should return false)
        let violation = storage
            .check_unique_violation(
                "idx_name_unique",
                vec![Value::string("Bob")],
                Some(2),
                txn_id,
            )
            .unwrap();
        assert!(
            !violation,
            "Should not detect violation - excluding the same row"
        );

        // Test non-unique index (should never report violations)
        storage
            .create_index(
                "idx_name_regular".to_string(),
                "users".to_string(),
                vec!["name".to_string()],
                false, // NOT unique
            )
            .unwrap();

        let violation = storage
            .check_unique_violation(
                "idx_name_regular",
                vec![Value::string("Alice")],
                None,
                txn_id,
            )
            .unwrap();
        assert!(
            !violation,
            "Non-unique index should never report violations"
        );
    }

    #[test]
    fn test_predicate_persistence() {
        use crate::semantic::predicate::{Predicate, QueryPredicates};

        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();
        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        let txn_id = HlcTimestamp::new(1000, 0, NodeId::new(1));

        // Create some predicates
        let read_pred = Predicate::full_table("users".to_string());
        let write_pred = Predicate::full_table("users".to_string());

        let predicates = QueryPredicates {
            reads: vec![read_pred.clone()],
            writes: vec![write_pred.clone()],
            inserts: vec![],
        };

        // Persist predicates
        let mut batch = storage.batch();
        storage
            .add_predicates_to_batch(&mut batch, txn_id, &predicates)
            .unwrap();
        batch.commit().unwrap();

        // Verify predicates are persisted
        let active_txns = storage.get_active_transactions().unwrap();
        assert_eq!(active_txns.len(), 1, "Should have one active transaction");
        assert!(
            active_txns.contains_key(&txn_id),
            "Should contain our transaction"
        );

        let stored_predicates = &active_txns[&txn_id];
        assert_eq!(stored_predicates.len(), 2, "Should have 2 predicates");

        // Cleanup
        storage.commit_transaction(txn_id, 1).unwrap();

        // Verify predicates are removed
        let active_txns = storage.get_active_transactions().unwrap();
        assert_eq!(active_txns.len(), 0, "Should have no active transactions");
    }

    #[test]
    fn test_predicate_atomic_persistence() {
        use crate::semantic::predicate::{Predicate, QueryPredicates};

        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();
        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        let txn_id = HlcTimestamp::new(1000, 0, NodeId::new(1));

        // Create predicate
        let read_pred = Predicate::full_table("users".to_string());

        let predicates = QueryPredicates {
            reads: vec![read_pred.clone()],
            writes: vec![],
            inserts: vec![],
        };

        // Insert data and predicates atomically
        let mut batch = storage.batch();
        storage
            .write_row(
                &mut batch,
                "users",
                1,
                &[Value::integer(1), Value::string("Alice")],
                txn_id,
                1,
            )
            .unwrap();
        storage
            .add_predicates_to_batch(&mut batch, txn_id, &predicates)
            .unwrap();
        batch.commit().unwrap();

        // Verify both data and predicates persisted
        let row = storage.read_row("users", 1, txn_id).unwrap();
        assert!(row.is_some(), "Row should be persisted");

        let active_txns = storage.get_active_transactions().unwrap();
        assert_eq!(active_txns.len(), 1, "Should have one active transaction");
        assert_eq!(active_txns[&txn_id].len(), 1, "Should have 1 predicate");

        // Commit and verify cleanup
        storage.commit_transaction(txn_id, 2).unwrap();

        let active_txns = storage.get_active_transactions().unwrap();
        assert_eq!(active_txns.len(), 0, "Should have no active transactions");
    }

    #[test]
    fn test_predicate_prepare_transaction() {
        use crate::semantic::predicate::{Predicate, QueryPredicates};

        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();
        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        let txn_id = HlcTimestamp::new(1000, 0, NodeId::new(1));

        // Create both read and write predicates
        let read_pred = Predicate::full_table("users".to_string());
        let write_pred = Predicate::full_table("users".to_string());

        let predicates = QueryPredicates {
            reads: vec![read_pred.clone()],
            writes: vec![write_pred.clone()],
            inserts: vec![],
        };

        // Persist predicates
        let mut batch = storage.batch();
        storage
            .add_predicates_to_batch(&mut batch, txn_id, &predicates)
            .unwrap();
        batch.commit().unwrap();

        // Verify both predicates exist
        let active_txns = storage.get_active_transactions().unwrap();
        assert_eq!(
            active_txns[&txn_id].len(),
            2,
            "Should have 2 predicates before prepare"
        );

        // Prepare transaction (removes read predicates only)
        storage.prepare_transaction(txn_id).unwrap();

        // Verify only write predicate remains
        let active_txns = storage.get_active_transactions().unwrap();
        assert_eq!(
            active_txns[&txn_id].len(),
            1,
            "Should have 1 predicate after prepare (write only)"
        );

        // Cleanup
        storage.commit_transaction(txn_id, 1).unwrap();

        let active_txns = storage.get_active_transactions().unwrap();
        assert_eq!(active_txns.len(), 0, "Should have no active transactions");
    }

    #[test]
    fn test_predicate_abort_cleanup() {
        use crate::semantic::predicate::{Predicate, QueryPredicates};

        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();
        let mut batch = storage.batch();
        storage
            .create_table(&mut batch, "users".to_string(), schema)
            .unwrap();
        batch.commit().unwrap();

        let txn_id = HlcTimestamp::new(1000, 0, NodeId::new(1));

        // Create predicates
        let read_pred = Predicate::full_table("users".to_string());

        let predicates = QueryPredicates {
            reads: vec![read_pred.clone()],
            writes: vec![],
            inserts: vec![],
        };

        // Persist predicates
        let mut batch = storage.batch();
        storage
            .add_predicates_to_batch(&mut batch, txn_id, &predicates)
            .unwrap();
        batch.commit().unwrap();

        // Verify predicates exist
        let active_txns = storage.get_active_transactions().unwrap();
        assert_eq!(active_txns.len(), 1, "Should have one active transaction");

        // Abort transaction
        let mut batch = storage.batch();
        storage
            .abort_transaction_to_batch(&mut batch, txn_id, 1)
            .unwrap();
        batch.commit().unwrap();

        // Verify predicates are cleaned up
        let active_txns = storage.get_active_transactions().unwrap();
        assert_eq!(
            active_txns.len(),
            0,
            "Should have no active transactions after abort"
        );
    }
}
