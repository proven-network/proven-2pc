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
use crate::storage::encoding::{decode_row, encode_row};
use crate::storage::entity::{IndexDelta, IndexEntity, IndexKey, TableDelta, TableEntity};
use crate::storage::predicate_store::PredicateStore;
use crate::types::index::IndexMetadata;
use crate::types::schema::Table as TableSchema;
use crate::types::value::Value;

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

    /// One MvccStorage per table (table_name -> storage)
    /// Each table gets separate partitions via with_shared_keyspace()
    table_storages: HashMap<String, MvccStorage<TableEntity>>,

    /// One MvccStorage per index (index_name -> storage)
    /// Each index gets separate partitions via with_shared_keyspace()
    index_storages: HashMap<String, MvccStorage<IndexEntity>>,

    /// Schema registry (table_name -> schema)
    /// Needed for schema-aware row encoding/decoding
    schemas: HashMap<String, Arc<TableSchema>>,

    /// Index metadata (index_name -> metadata)
    index_metadata: HashMap<String, IndexMetadata>,

    /// Predicate store for crash recovery
    predicate_store: PredicateStore,

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

        // Create predicate store with dedicated partition
        let predicate_store = PredicateStore::new(&keyspace)?;

        let mut storage = Self {
            keyspace,
            table_storages: HashMap::new(),
            index_storages: HashMap::new(),
            schemas: HashMap::new(),
            index_metadata: HashMap::new(),
            predicate_store,
            next_row_ids: HashMap::new(),
            config,
        };

        // Load existing tables and schemas from metadata
        storage.load_metadata()?;

        Ok(storage)
    }

    /// Load table schemas and indexes from metadata partition
    fn load_metadata(&mut self) -> Result<()> {
        // Open metadata partition
        let metadata = self.keyspace.open_partition(
            "_metadata",
            PartitionCreateOptions::default()
                .block_size(16 * 1024)
                .compression(fjall::CompressionType::None),
        )?;

        // Scan for table schemas
        for result in metadata.prefix(b"schema_") {
            let (key, value) = result?;
            let key_str = String::from_utf8_lossy(&key);

            if let Some(table_name) = key_str.strip_prefix("schema_") {
                // Decode schema
                let schema: TableSchema = bincode::deserialize(&value)
                    .map_err(|e| Error::Serialization(e.to_string()))?;

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

                let table_storage = MvccStorage::<TableEntity>::with_shared_keyspace(
                    self.keyspace.clone(),
                    table_name.to_string(),
                    mvcc_config,
                )?;

                // Load or initialize next_row_id counter
                let row_id_key = format!("next_row_id_{}", table_name);
                let next_row_id = metadata
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
                self.next_row_ids
                    .insert(table_name.to_string(), AtomicU64::new(next_row_id));
            }
        }

        // Scan for index metadata
        for result in metadata.prefix(b"index_") {
            let (key, value) = result?;
            let key_str = String::from_utf8_lossy(&key);

            if let Some(index_name) = key_str.strip_prefix("index_") {
                // Decode index metadata
                let index_meta: IndexMetadata = bincode::deserialize(&value)
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

    /// Create a new table
    pub fn create_table(&mut self, table_name: String, schema: TableSchema) -> Result<()> {
        // Check if table already exists
        if self.schemas.contains_key(&table_name) {
            return Err(Error::DuplicateTable(table_name));
        }

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

        let table_storage = MvccStorage::<TableEntity>::with_shared_keyspace(
            self.keyspace.clone(),
            table_name.clone(),
            mvcc_config,
        )?;

        // Persist schema to metadata
        let metadata = self.keyspace.open_partition(
            "_metadata",
            PartitionCreateOptions::default()
                .block_size(16 * 1024)
                .compression(fjall::CompressionType::None),
        )?;

        let schema_key = format!("schema_{}", table_name);
        let schema_bytes =
            bincode::serialize(&schema).map_err(|e| Error::Serialization(e.to_string()))?;

        metadata.insert(schema_key, schema_bytes)?;

        // Initialize next_row_id counter
        let row_id_key = format!("next_row_id_{}", table_name);
        metadata.insert(row_id_key, 1u64.to_be_bytes())?;

        // Store in memory
        self.table_storages
            .insert(table_name.clone(), table_storage);
        self.schemas.insert(table_name.clone(), Arc::new(schema));
        self.next_row_ids.insert(table_name, AtomicU64::new(1));

        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&mut self, table_name: &str) -> Result<()> {
        // Remove from in-memory state
        self.table_storages.remove(table_name);
        self.schemas.remove(table_name);

        // Remove schema from metadata
        let metadata = self.keyspace.open_partition(
            "_metadata",
            PartitionCreateOptions::default()
                .block_size(16 * 1024)
                .compression(fjall::CompressionType::None),
        )?;

        let schema_key = format!("schema_{}", table_name);
        metadata.remove(schema_key)?;

        // Note: MvccStorage partitions will be cleaned up when dropped
        // or can be manually cleaned via fjall keyspace operations

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

    /// Scan rows in a table within a row_id range using MVCC iterator
    ///
    /// Returns an iterator over (row_id, values) pairs visible at the snapshot time
    /// for row_ids within the specified range.
    /// Supports all Rust range types: .., a.., ..b, a..b, a..=b
    pub fn scan_table_range<'a, R>(
        &'a self,
        table_name: &str,
        range: R,
        txn_id: HlcTimestamp,
    ) -> Result<TableScanIterator<'a>>
    where
        R: std::ops::RangeBounds<u64> + 'a,
    {
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

        // Convert u64 range to Vec<u8> range for MVCC storage
        use std::ops::Bound;
        let start_bound = match range.start_bound() {
            Bound::Included(&start) => Bound::Included(start.to_be_bytes().to_vec()),
            Bound::Excluded(&start) => Bound::Excluded(start.to_be_bytes().to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bound = match range.end_bound() {
            Bound::Included(&end) => Bound::Included(end.to_be_bytes().to_vec()),
            Bound::Excluded(&end) => Bound::Excluded(end.to_be_bytes().to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        // Use MVCC range iterator
        let iter = table_storage.range((start_bound, end_bound), txn_id)?;

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
    pub fn abort_transaction(&mut self, txn_id: HlcTimestamp, log_index: u64) -> Result<()> {
        // Create shared batch for atomic abort
        let mut batch = self.keyspace.batch();

        // Abort all table storages
        for table_storage in self.table_storages.values_mut() {
            table_storage.abort_transaction_to_batch(&mut batch, txn_id, log_index)?;
        }

        // Abort all index storages
        for index_storage in self.index_storages.values_mut() {
            index_storage.abort_transaction_to_batch(&mut batch, txn_id, log_index)?;
        }

        // Remove predicates (on abort)
        self.predicate_store.remove_all(&mut batch, txn_id)?;

        // Atomic abort across ALL entities!
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

    /// Persist predicates (for read-only transactions)
    pub fn persist_predicates(
        &self,
        txn_id: HlcTimestamp,
        predicates: &QueryPredicates,
    ) -> Result<()> {
        let mut batch = self.keyspace.batch();
        self.add_predicates_to_batch(&mut batch, txn_id, predicates)?;
        batch.commit()?;
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

        // Persist metadata
        let metadata_partition = self.keyspace.open_partition(
            "_metadata",
            PartitionCreateOptions::default()
                .block_size(16 * 1024)
                .compression(fjall::CompressionType::None),
        )?;

        let index_key = format!("index_{}", index_name);
        let index_bytes =
            bincode::serialize(&metadata).map_err(|e| Error::Serialization(e.to_string()))?;

        metadata_partition.insert(index_key, index_bytes)?;

        // Store in memory
        self.index_storages
            .insert(index_name.clone(), index_storage);
        self.index_metadata.insert(index_name, metadata);

        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> Result<()> {
        // Remove from in-memory state
        self.index_storages.remove(index_name);
        self.index_metadata.remove(index_name);

        // Remove metadata
        let metadata_partition = self.keyspace.open_partition(
            "_metadata",
            PartitionCreateOptions::default()
                .block_size(16 * 1024)
                .compression(fjall::CompressionType::None),
        )?;

        let index_key = format!("index_{}", index_name);
        metadata_partition.remove(index_key)?;

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
        let start_key = crate::storage::encoding::encode_index_key(&values, 0);
        let end_key = crate::storage::encoding::encode_index_key(&values, u64::MAX);

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
        end_values: Option<Vec<Value>>,
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
                let key = crate::storage::encoding::encode_index_key(&values, 0);
                let prefix_len = key.len() - 8; // Remove row_id
                Bound::Included(key[..prefix_len].to_vec())
            }
            None => Bound::Unbounded,
        };

        let end_bound = match end_values {
            Some(values) => {
                let key = crate::storage::encoding::encode_index_key(&values, u64::MAX);
                Bound::Included(key)
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
        let start_key = crate::storage::encoding::encode_index_key(&values, 0);
        let end_key = crate::storage::encoding::encode_index_key(&values, u64::MAX);

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
                        Ok(Some(values)) => return Some(Ok((row_id, values))),
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

        storage.create_table("users".to_string(), schema).unwrap();

        assert!(storage.schemas.contains_key("users"));
        assert!(storage.table_storages.contains_key("users"));
    }

    #[test]
    fn test_write_and_read_row() {
        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();
        storage.create_table("users".to_string(), schema).unwrap();

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
        storage.create_table("users".to_string(), schema).unwrap();

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
        storage.create_table("users".to_string(), schema).unwrap();

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
        storage.create_table("users".to_string(), schema).unwrap();

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
        storage.create_table("users".to_string(), schema).unwrap();

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
        storage.create_table("users".to_string(), schema).unwrap();

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
    fn test_table_scan_range() {
        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema();
        storage.create_table("users".to_string(), schema).unwrap();

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
                3,
                &[Value::integer(3), Value::string("Bob")],
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
        storage
            .write_row(
                &mut batch,
                "users",
                7,
                &[Value::integer(7), Value::string("Diana")],
                txn_id,
                4,
            )
            .unwrap();
        storage
            .write_row(
                &mut batch,
                "users",
                10,
                &[Value::integer(10), Value::string("Eve")],
                txn_id,
                5,
            )
            .unwrap();
        batch.commit().unwrap();

        // Scan range 3..=7 (inclusive)
        let rows: Vec<_> = storage
            .scan_table_range("users", 3..=7, txn_id)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        // Should find 3 rows: 3, 5, 7
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, 3);
        assert_eq!(rows[0].1, vec![Value::integer(3), Value::string("Bob")]);
        assert_eq!(rows[1].0, 5);
        assert_eq!(rows[1].1, vec![Value::integer(5), Value::string("Charlie")]);
        assert_eq!(rows[2].0, 7);
        assert_eq!(rows[2].1, vec![Value::integer(7), Value::string("Diana")]);

        // Scan range 5.. (from 5 onwards)
        let rows: Vec<_> = storage
            .scan_table_range("users", 5.., txn_id)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        // Should find 3 rows: 5, 7, 10
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, 5);
        assert_eq!(rows[1].0, 7);
        assert_eq!(rows[2].0, 10);

        // Scan range ..5 (up to but not including 5)
        let rows: Vec<_> = storage
            .scan_table_range("users", ..5, txn_id)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        // Should find 2 rows: 1, 3
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[1].0, 3);
    }

    #[test]
    fn test_index_lookup() {
        let mut storage = SqlStorage::new(test_config()).unwrap();
        let schema = create_test_schema_with_city();
        storage.create_table("users".to_string(), schema).unwrap();

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
        storage.create_table("users".to_string(), schema).unwrap();

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
                Some(vec![Value::integer(40)]),
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
            .index_range_scan("idx_age", Some(vec![Value::integer(35)]), None, txn_id)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1[1], Value::string("Charlie")); // age 35
        assert_eq!(results[1].1[1], Value::string("Diana")); // age 40
        assert_eq!(results[2].1[1], Value::string("Eve")); // age 45

        // Range scan: age <= 30 (no lower bound)
        let results: Vec<_> = storage
            .index_range_scan("idx_age", None, Some(vec![Value::integer(30)]), txn_id)
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
        storage.create_table("users".to_string(), schema).unwrap();

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
        storage.create_table("users".to_string(), schema).unwrap();

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
        storage.persist_predicates(txn_id, &predicates).unwrap();

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
        storage.create_table("users".to_string(), schema).unwrap();

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
        storage.create_table("users".to_string(), schema).unwrap();

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
        storage.persist_predicates(txn_id, &predicates).unwrap();

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
        storage.create_table("users".to_string(), schema).unwrap();

        let txn_id = HlcTimestamp::new(1000, 0, NodeId::new(1));

        // Create predicates
        let read_pred = Predicate::full_table("users".to_string());

        let predicates = QueryPredicates {
            reads: vec![read_pred.clone()],
            writes: vec![],
            inserts: vec![],
        };

        // Persist predicates
        storage.persist_predicates(txn_id, &predicates).unwrap();

        // Verify predicates exist
        let active_txns = storage.get_active_transactions().unwrap();
        assert_eq!(active_txns.len(), 1, "Should have one active transaction");

        // Abort transaction
        storage.abort_transaction(txn_id, 1).unwrap();

        // Verify predicates are cleaned up
        let active_txns = storage.get_active_transactions().unwrap();
        assert_eq!(
            active_txns.len(),
            0,
            "Should have no active transactions after abort"
        );
    }
}
