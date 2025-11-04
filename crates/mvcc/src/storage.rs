//! Main MVCC storage implementation
//!
//! Provides snapshot isolation with:
//! - Read-your-own-writes (via UncommittedStore)
//! - Time-travel queries (via HistoryStore)
//! - Crash recovery (persistent log index)
//! - Efficient cleanup (time-bucketed partitions)

use crate::FjallIter;
use crate::config::StorageConfig;
use crate::encoding::{Decode, Encode};
use crate::entity::{MvccDelta, MvccEntity};
use crate::error::Result;
use crate::history::HistoryStore;
use crate::uncommitted::UncommittedStore;
use fjall::{Batch, Keyspace, Partition, PartitionCreateOptions};
use proven_common::TransactionId;
use std::marker::PhantomData;
use std::path::Path;

/// Type-erased iterator to avoid complex trait bounds
pub type MvccIterator<E> = Box<dyn Iterator<Item = Result<<E as MvccEntity>::Value>>>;

/// Generic MVCC storage with snapshot isolation and crash recovery
pub struct MvccStorage<E: MvccEntity> {
    // Fjall backend
    keyspace: Keyspace,
    metadata_partition: Partition,

    // Main data partition (committed data)
    data_partition: Partition,

    // MVCC components
    uncommitted: UncommittedStore<E>,
    history: HistoryStore<E>,

    _phantom: PhantomData<E>,
}

impl<E: MvccEntity> MvccStorage<E> {
    /// Create new MVCC storage with its own keyspace
    pub fn new(config: StorageConfig) -> Result<Self> {
        Self::open_at_path(&config.data_dir.clone(), config)
    }

    /// Create new MVCC storage with a shared keyspace (for SQL multi-table storage)
    ///
    /// This allows multiple MvccStorage instances to share a single keyspace,
    /// enabling atomic batching across multiple entities (e.g., tables + indexes).
    ///
    /// The `instance_name` is used as a prefix for all partitions to avoid collisions.
    /// Example: instance_name="users" creates partitions like "table_data_users_uncommitted"
    pub fn with_shared_keyspace(
        keyspace: Keyspace,
        instance_name: String,
        config: StorageConfig,
    ) -> Result<Self> {
        let entity_prefix = format!("{}_{}", E::entity_name(), instance_name);

        // Open metadata partition
        let metadata_partition = keyspace.open_partition(
            &format!("{}_metadata", entity_prefix),
            PartitionCreateOptions::default()
                .block_size(16 * 1024)
                .compression(fjall::CompressionType::None),
        )?;

        // Open main data partition (for committed data)
        let data_partition = keyspace.open_partition(
            &format!("{}_data", entity_prefix),
            PartitionCreateOptions::default()
                .block_size(64 * 1024)
                .compression(config.compression),
        )?;

        // Create single partition for uncommitted data
        let uncommitted_partition = keyspace.open_partition(
            &format!("{}_uncommitted", entity_prefix),
            PartitionCreateOptions::default()
                .block_size(32 * 1024)
                .compression(config.compression),
        )?;
        let uncommitted = UncommittedStore::new(uncommitted_partition);

        // Create single partition for history
        let history_partition = keyspace.open_partition(
            &format!("{}_history", entity_prefix),
            PartitionCreateOptions::default()
                .block_size(32 * 1024)
                .compression(config.compression),
        )?;
        let history = HistoryStore::new(history_partition);

        let mut storage = Self {
            keyspace,
            metadata_partition,
            data_partition,
            uncommitted,
            history,
            _phantom: PhantomData,
        };

        // Cleanup old history on startup (anything older than 5 minutes)
        storage.cleanup_old_history();

        Ok(storage)
    }

    /// Open storage at a specific path
    pub fn open_at_path(path: &Path, config: StorageConfig) -> Result<Self> {
        // Ensure directory exists
        std::fs::create_dir_all(path)?;

        // Open Fjall keyspace
        let keyspace = fjall::Config::new(path)
            .cache_size(config.block_cache_size)
            .open()?;

        // Open metadata partition
        let metadata_partition = keyspace.open_partition(
            "_metadata",
            PartitionCreateOptions::default()
                .block_size(16 * 1024)
                .compression(fjall::CompressionType::None),
        )?;

        // Open main data partition (for committed data)
        let data_partition = keyspace.open_partition(
            &format!("{}_data", E::entity_name()),
            PartitionCreateOptions::default()
                .block_size(64 * 1024)
                .compression(config.compression),
        )?;

        // Create single partition for uncommitted data
        let uncommitted_partition = keyspace.open_partition(
            &format!("{}_{}", E::entity_name(), "uncommitted"),
            PartitionCreateOptions::default()
                .block_size(32 * 1024)
                .compression(config.compression),
        )?;
        let uncommitted = UncommittedStore::new(uncommitted_partition);

        // Create single partition for history
        let history_partition = keyspace.open_partition(
            &format!("{}_{}", E::entity_name(), "history"),
            PartitionCreateOptions::default()
                .block_size(32 * 1024)
                .compression(config.compression),
        )?;
        let history = HistoryStore::new(history_partition);

        let mut storage = Self {
            keyspace,
            metadata_partition,
            data_partition,
            uncommitted,
            history,
            _phantom: PhantomData,
        };

        // Cleanup old history on startup (anything older than 5 minutes)
        storage.cleanup_old_history();

        Ok(storage)
    }

    /// Read a value with MVCC semantics
    ///
    /// 1. Check uncommitted writes first (read-your-own-writes)
    /// 2. Fall back to committed data with time-travel
    pub fn read(&self, key: &E::Key, txn_id: TransactionId) -> Result<Option<E::Value>> {
        // L1: Check uncommitted writes (read-your-own-writes)
        if let Some(value) = self.uncommitted.get(txn_id, key)? {
            return Ok(Some(value));
        }

        // L2: Read from committed data partition
        let key_bytes = key.encode()?;
        let committed_value = if let Some(value_bytes) = self.data_partition.get(&key_bytes)? {
            Some(E::Value::decode(&value_bytes)?)
        } else {
            None
        };

        // L3: Apply time-travel if needed (check history for deltas after txn_id)
        if !self.history.is_empty() {
            let deltas = self.history.get_key_deltas_after(key, txn_id)?;

            if !deltas.is_empty() {
                // Reconstruct value at snapshot time by unapplying deltas
                let mut value = committed_value;
                for delta in deltas.iter().rev() {
                    value = delta.unapply(value);
                }
                return Ok(value);
            }
        }

        Ok(committed_value)
    }

    /// Create a new batch for atomic operations
    pub fn batch(&self) -> Batch {
        self.keyspace.batch()
    }

    /// Add write delta to batch (uncommitted until commit)
    pub fn write_to_batch(
        &mut self,
        batch: &mut Batch,
        delta: E::Delta,
        txn_id: TransactionId,
    ) -> Result<()> {
        // Add to uncommitted store
        self.uncommitted.add_delta_to_batch(batch, txn_id, delta)?;

        Ok(())
    }

    /// Commit transaction to existing batch - allows atomic batching with other operations
    pub fn commit_transaction_to_batch(
        &mut self,
        batch: &mut Batch,
        txn_id: TransactionId,
    ) -> Result<()> {
        // Get uncommitted deltas
        let deltas = self.uncommitted.get_transaction_deltas(txn_id)?;

        if !deltas.is_empty() {
            // Apply deltas to get final state for each key
            let mut key_final_state: std::collections::HashMap<Vec<u8>, Option<E::Value>> =
                std::collections::HashMap::new();

            for delta in &deltas {
                let key_bytes = delta.key().encode()?;
                let current = key_final_state.get(&key_bytes).cloned().flatten();
                let new_value = delta.apply(current);
                key_final_state.insert(key_bytes, new_value);
            }

            // Write final state to committed data partition
            for (key_bytes, value_opt) in key_final_state {
                if let Some(value) = value_opt {
                    let value_bytes = value.encode()?;
                    batch.insert(&self.data_partition, key_bytes, value_bytes);
                } else {
                    // Delete the key
                    batch.remove(self.data_partition.clone(), key_bytes);
                }
            }

            // Add deltas to history with commit transaction ID
            self.history
                .add_committed_deltas_to_batch(batch, txn_id, deltas)?;
        }

        // Remove from uncommitted
        self.uncommitted.remove_transaction(batch, txn_id)?;

        Ok(())
    }

    /// Abort transaction to existing batch - allows atomic batching with other operations
    pub fn abort_transaction_to_batch(
        &mut self,
        batch: &mut Batch,
        txn_id: TransactionId,
    ) -> Result<()> {
        // Remove from uncommitted
        self.uncommitted.remove_transaction(batch, txn_id)?;

        Ok(())
    }

    // ========================================================================
    // Iterator Methods
    // ========================================================================

    /// Iterate over all entities visible at snapshot time
    ///
    /// This captures all MVCC state at construction and provides consistent iteration
    pub fn iter(&'_ self, txn_id: TransactionId) -> Result<crate::iterator::MvccIterator<'_, E>> {
        self.prefix(b"", txn_id)
    }

    /// Iterate over entities with keys matching a prefix
    ///
    /// Essential for index scans (e.g., all entries starting with "Alice")
    pub fn prefix(
        &'_ self,
        prefix: &[u8],
        txn_id: TransactionId,
    ) -> Result<crate::iterator::MvccIterator<'_, E>> {
        // Pre-load uncommitted deltas for this transaction
        let uncommitted_deltas = self.uncommitted.get_transaction_deltas(txn_id)?;

        // Consolidate deltas by key - if multiple deltas for same key, merge them
        // The deltas are in sequence order, so we merge earlier ones with later ones
        // Use BTreeMap to maintain sorted order for deterministic iteration
        let mut uncommitted_map: std::collections::BTreeMap<Vec<u8>, E::Delta> =
            std::collections::BTreeMap::new();
        for delta in uncommitted_deltas {
            let key_bytes = delta.key().encode()?;
            if let Some(existing) = uncommitted_map.get(&key_bytes) {
                // Merge earlier delta with later delta
                let merged = existing.clone().merge(delta);
                uncommitted_map.insert(key_bytes, merged);
            } else {
                uncommitted_map.insert(key_bytes, delta);
            }
        }

        // Pre-load history deltas (committed after our snapshot)
        // Returns BTreeMap for deterministic ordering
        let history_map = self.history.get_deltas_after(txn_id)?;

        // Create iterator over committed data partition
        let committed_iter: FjallIter = if prefix.is_empty() {
            Box::new(self.data_partition.iter().map(|result| {
                result.map(|(k, v)| {
                    let k_bytes: Box<[u8]> = k.to_vec().into_boxed_slice();
                    let v_bytes: Box<[u8]> = v.to_vec().into_boxed_slice();
                    (k_bytes, v_bytes)
                })
            }))
        } else {
            Box::new(self.data_partition.prefix(prefix).map(|result| {
                result.map(|(k, v)| {
                    let k_bytes: Box<[u8]> = k.to_vec().into_boxed_slice();
                    let v_bytes: Box<[u8]> = v.to_vec().into_boxed_slice();
                    (k_bytes, v_bytes)
                })
            }))
        };

        Ok(crate::iterator::MvccIterator::new(
            committed_iter,
            uncommitted_map,
            history_map,
        ))
    }

    /// Iterate over entities with keys in a given range
    ///
    /// Essential for range scans (e.g., all entries from "Alice" to "Bob")
    /// Supports all Rust range types: .., a.., ..b, a..b, a..=b
    pub fn range<'a, R>(
        &'a self,
        range: R,
        txn_id: TransactionId,
    ) -> Result<crate::iterator::MvccIterator<'a, E>>
    where
        R: std::ops::RangeBounds<Vec<u8>> + 'a + Clone,
    {
        // Pre-load uncommitted deltas for this transaction
        let uncommitted_deltas = self.uncommitted.get_transaction_deltas(txn_id)?;

        // Consolidate deltas by key - if multiple deltas for same key, merge them
        // The deltas are in sequence order, so we merge earlier ones with later ones
        // Use BTreeMap to maintain sorted order for deterministic iteration
        let mut uncommitted_map: std::collections::BTreeMap<Vec<u8>, E::Delta> =
            std::collections::BTreeMap::new();
        for delta in uncommitted_deltas {
            let key_bytes = delta.key().encode()?;
            if let Some(existing) = uncommitted_map.get(&key_bytes) {
                // Merge earlier delta with later delta
                let merged = existing.clone().merge(delta);
                uncommitted_map.insert(key_bytes, merged);
            } else {
                uncommitted_map.insert(key_bytes, delta);
            }
        }

        // Filter uncommitted_map to only include keys in the range
        let uncommitted_in_range: std::collections::BTreeMap<Vec<u8>, E::Delta> = uncommitted_map
            .range(range.clone())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Pre-load history deltas (committed after our snapshot)
        // Returns BTreeMap for deterministic ordering
        let history_map = self.history.get_deltas_after(txn_id)?;

        // Create iterator over committed data partition using range
        let committed_iter: FjallIter = Box::new(self.data_partition.range(range).map(|result| {
            result.map(|(k, v)| {
                let k_bytes: Box<[u8]> = k.to_vec().into_boxed_slice();
                let v_bytes: Box<[u8]> = v.to_vec().into_boxed_slice();
                (k_bytes, v_bytes)
            })
        }));

        Ok(crate::iterator::MvccIterator::new(
            committed_iter,
            uncommitted_in_range,
            history_map,
        ))
    }

    /// Get direct access to data partition (for advanced use cases)
    pub fn data_partition(&self) -> &fjall::PartitionHandle {
        &self.data_partition
    }

    /// Cleanup old deltas based on active transactions
    ///
    /// Removes uncommitted and history deltas that are older than the oldest
    /// active transaction, ensuring correct snapshot isolation semantics.
    ///
    /// This should be called periodically with the oldest active transaction ID.
    /// For best results, call after significant transaction completion activity.
    pub fn cleanup_before(&mut self, oldest_txn_id: TransactionId) -> Result<(usize, usize)> {
        let mut batch = self.keyspace.batch();

        // Cleanup uncommitted data older than oldest active transaction
        let uncommitted_removed = self.uncommitted.cleanup_before(&mut batch, oldest_txn_id)?;

        // Cleanup history deltas older than oldest active transaction
        let history_removed = self.history.cleanup_before(&mut batch, oldest_txn_id)?;

        // Commit cleanup atomically
        batch.commit()?;

        // Persist after cleanup
        self.keyspace.persist(fjall::PersistMode::SyncAll)?;

        Ok((uncommitted_removed, history_removed))
    }

    /// Cleanup old history on startup (removes data older than 5 minutes)
    ///
    /// This is called automatically when opening storage to remove stale MVCC
    /// history that can no longer be accessed. Uses a 5-minute retention window
    /// which is appropriate for most workloads.
    fn cleanup_old_history(&mut self) {
        // Create a TransactionId representing "5 minutes ago"
        let cutoff_time_ms = (proven_common::Timestamp::now()
            .as_micros()
            .saturating_sub(5 * 60 * 1_000_000))
            / 1000;

        // Create a minimal UUIDv7-like bytes with the cutoff timestamp
        // UUIDv7 stores the timestamp in the first 48 bits (6 bytes)
        let mut cutoff_bytes = [0u8; 16];
        cutoff_bytes[0..6].copy_from_slice(&cutoff_time_ms.to_be_bytes()[2..8]);
        let cutoff_txn = TransactionId::from_bytes(cutoff_bytes);

        // Cleanup history older than cutoff (ignore errors - this is best-effort)
        let _ = self.cleanup_before(cutoff_txn);
    }

    /// Get the keyspace (for custom operations)
    pub fn keyspace(&self) -> &Keyspace {
        &self.keyspace
    }

    /// Get the metadata partition (for custom metadata)
    pub fn metadata_partition(&self) -> &Partition {
        &self.metadata_partition
    }
}

impl<E: MvccEntity> Drop for MvccStorage<E> {
    fn drop(&mut self) {
        // Ensure data is persisted on drop
        let _ = self.keyspace.persist(fjall::PersistMode::SyncAll);
    }
}
