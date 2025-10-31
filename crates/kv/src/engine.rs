//! KV Transaction Engine implementation for the generic stream processor
//!
//! This module implements the TransactionEngine trait, providing KV-specific
//! operation execution while delegating message handling to the generic processor.

use proven_common::TransactionId;
use proven_mvcc::{MvccStorage, StorageConfig};
use proven_stream::engine::{BatchOperations, BlockingInfo};
use proven_stream::{OperationResult, RetryOn, TransactionEngine};

use crate::storage::entity::{KvDelta, KvEntity, KvKey};
use crate::storage::lock::{LockAttemptResult, LockManager, LockMode};
use crate::storage::lock_persistence::{TransactionLocks, encode_transaction_locks};
use crate::types::{KvOperation, KvResponse};

/// Wrapper around MVCC Batch that implements BatchOperations
///
/// This is a thin wrapper that adds transaction metadata capabilities
/// to the MVCC batch type. The underlying batch already
/// accumulates operations, so we just need to provide metadata methods.
pub struct KvBatch {
    /// The underlying MVCC batch (accumulates all operations)
    inner: proven_mvcc::Batch,

    /// Metadata partition handle (Arc internally, cheap to clone)
    metadata_partition: fjall::PartitionHandle,
}

impl KvBatch {
    /// Create a new batch (crate-local only)
    pub(crate) fn new(storage: &MvccStorage<KvEntity>) -> Self {
        Self {
            inner: storage.batch(),
            metadata_partition: storage.metadata_partition().clone(),
        }
    }

    /// Get mutable access to the inner Fjall batch (crate-local only)
    ///
    /// This allows engine implementations to add engine-specific
    /// operations to the batch.
    pub(crate) fn inner(&mut self) -> &mut proven_mvcc::Batch {
        &mut self.inner
    }

    /// Consume and commit the batch with log_index (crate-local only)
    ///
    /// This is called by TransactionEngine::commit_batch().
    /// Stream processor cannot call this directly.
    pub(crate) fn commit(mut self, log_index: u64) -> Result<(), String> {
        // Add log_index to batch
        self.inner.insert(
            &self.metadata_partition,
            b"_log_index",
            log_index.to_le_bytes(),
        );

        // Commit atomically
        self.inner.commit().map_err(|e| e.to_string())
    }
}

impl BatchOperations for KvBatch {
    fn insert_metadata(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.inner.insert(&self.metadata_partition, key, value);
    }

    fn remove_metadata(&mut self, key: Vec<u8>) {
        self.inner.remove(self.metadata_partition.clone(), key);
    }
}

/// KV-specific transaction engine with persistent storage
pub struct KvTransactionEngine {
    /// MVCC storage for versioned data (now persistent via proven-mvcc)
    storage: MvccStorage<KvEntity>,

    /// Lock manager for pessimistic concurrency control
    lock_manager: LockManager,
}

impl KvTransactionEngine {
    /// Create a new KV transaction engine with default storage
    pub fn new() -> Self {
        // StorageConfig::default() creates a unique temp directory per instance
        let config = StorageConfig::default();
        Self::with_config(config)
    }

    /// Create a new KV transaction engine with custom config
    pub fn with_config(config: StorageConfig) -> Self {
        let storage = MvccStorage::<KvEntity>::new(config).expect("Failed to create MVCC storage");

        let mut engine = Self {
            storage,
            lock_manager: LockManager::new(),
        };

        // Recover locks from persisted state (crash recovery)
        // This is critical because the stream processor won't replay
        // already-processed operations (it uses get_log_index() to resume)
        engine.recover_locks_from_storage();

        engine
    }

    /// Recover locks from persisted state (crash recovery)
    fn recover_locks_from_storage(&mut self) {
        let metadata = self.storage.metadata_partition();

        // Scan for all persisted locks
        for (_key_bytes, value_bytes) in metadata.prefix("_locks_").flatten() {
            // Decode the transaction locks
            if let Ok(tx_locks) =
                crate::storage::lock_persistence::decode_transaction_locks(&value_bytes)
            {
                // Restore each lock to the in-memory lock manager
                for lock in tx_locks.locks {
                    self.lock_manager
                        .grant(tx_locks.txn_id, lock.key, lock.mode);
                }
            }
        }
    }

    /// Add locks to batch for atomic persistence
    fn add_locks_to_batch(
        &mut self,
        batch: &mut proven_mvcc::Batch,
        txn_id: TransactionId,
    ) -> Result<(), String> {
        let locks_held = self.lock_manager.locks_held_by(txn_id);

        if !locks_held.is_empty() {
            let mut tx_locks = TransactionLocks::new(txn_id);
            for (key, mode) in locks_held {
                tx_locks.add_lock(key, mode);
            }

            // Lock key: prefix + transaction ID bytes (16 bytes for UUIDv7)
            let mut lock_key = b"_locks_".to_vec();
            lock_key.extend_from_slice(&txn_id.to_bytes());
            let lock_bytes = encode_transaction_locks(&tx_locks)?;

            // Add to batch (will be committed atomically with data)
            let metadata = self.storage.metadata_partition();
            batch.insert(metadata, lock_key, lock_bytes);
        }

        Ok(())
    }

    /// Check for lock conflicts (used by both regular and snapshot reads)
    fn check_read_conflicts(
        &self,
        key: &str,
        read_timestamp: TransactionId,
    ) -> Option<Vec<BlockingInfo>> {
        // Check lock manager for earlier transactions holding exclusive locks on this key
        let lock_holders = self.lock_manager.get_holders(key);
        let mut blockers = Vec::new();

        for (holder_txn, lock_mode) in lock_holders {
            // Only block on EARLIER transactions with EXCLUSIVE locks
            if holder_txn < read_timestamp && lock_mode == LockMode::Exclusive {
                blockers.push(BlockingInfo {
                    txn: holder_txn,
                    retry_on: RetryOn::CommitOrAbort,
                });
            }
        }

        if blockers.is_empty() {
            None
        } else {
            Some(blockers)
        }
    }

    /// Execute a get operation without locking (snapshot reads)
    fn execute_get_without_locking(
        &self,
        key: &str,
        read_timestamp: TransactionId,
    ) -> OperationResult<KvResponse> {
        // Check for conflicts with earlier exclusive locks
        if let Some(blockers) = self.check_read_conflicts(key, read_timestamp) {
            return OperationResult::WouldBlock { blockers };
        }

        // No blocking - safe to read
        let kv_key = KvKey::from(key);
        let value = self
            .storage
            .read(&kv_key, read_timestamp)
            .expect("Read failed");

        OperationResult::Complete(KvResponse::GetResult {
            key: key.to_string(),
            value,
        })
    }
}

impl TransactionEngine for KvTransactionEngine {
    type Operation = KvOperation;
    type Response = KvResponse;
    type Batch = KvBatch;

    // ═══════════════════════════════════════════════════════════
    // BATCH LIFECYCLE
    // ═══════════════════════════════════════════════════════════

    fn start_batch(&mut self) -> Self::Batch {
        KvBatch::new(&self.storage)
    }

    fn commit_batch(&mut self, batch: Self::Batch, log_index: u64) {
        batch.commit(log_index).expect("Batch commit failed");
    }

    // ═══════════════════════════════════════════════════════════
    // READ OPERATIONS
    // ═══════════════════════════════════════════════════════════

    fn read_at_timestamp(
        &mut self,
        operation: Self::Operation,
        read_timestamp: TransactionId,
    ) -> OperationResult<Self::Response> {
        match operation {
            KvOperation::Get { ref key } => self.execute_get_without_locking(key, read_timestamp),
            _ => panic!("Must be read-only operation"),
        }
    }

    // ═══════════════════════════════════════════════════════════
    // WRITE OPERATIONS
    // ═══════════════════════════════════════════════════════════

    fn apply_operation(
        &mut self,
        batch: &mut Self::Batch,
        operation: Self::Operation,
        txn_id: TransactionId,
    ) -> OperationResult<Self::Response> {
        match operation {
            KvOperation::Get { ref key } => {
                // GET with locking (for ReadWrite transactions)
                match self.lock_manager.check(txn_id, key, LockMode::Shared) {
                    LockAttemptResult::WouldGrant => {
                        // Grant the lock
                        self.lock_manager
                            .grant(txn_id, key.clone(), LockMode::Shared);

                        // Execute read
                        let kv_key = KvKey::from(key.as_str());
                        let value = self.storage.read(&kv_key, txn_id).expect("Read failed");

                        // Add lock persistence to batch
                        self.add_locks_to_batch(batch.inner(), txn_id)
                            .expect("Failed to persist locks");

                        OperationResult::Complete(KvResponse::GetResult {
                            key: key.clone(),
                            value,
                        })
                    }
                    LockAttemptResult::Conflict { holders } => {
                        let blockers = holders
                            .into_iter()
                            .map(|(holder_txn, _mode)| BlockingInfo {
                                txn: holder_txn,
                                retry_on: RetryOn::CommitOrAbort,
                            })
                            .collect();

                        OperationResult::WouldBlock { blockers }
                    }
                }
            }
            KvOperation::Put { ref key, ref value } => {
                // Try to acquire exclusive lock
                match self.lock_manager.check(txn_id, key, LockMode::Exclusive) {
                    LockAttemptResult::WouldGrant => {
                        // Grant the lock
                        self.lock_manager
                            .grant(txn_id, key.clone(), LockMode::Exclusive);
                        // Read old value for delta
                        let kv_key = KvKey::from(key.as_str());
                        let old_value = self.storage.read(&kv_key, txn_id).expect("Read failed");

                        // Create delta
                        let delta = KvDelta::Put {
                            key: key.clone(),
                            new_value: value.clone(),
                            old_value: old_value.clone(),
                        };

                        // Add delta to batch
                        self.storage
                            .write_to_batch(batch.inner(), delta, txn_id)
                            .expect("Failed to write to batch");

                        // Add lock persistence to batch
                        self.add_locks_to_batch(batch.inner(), txn_id)
                            .expect("Failed to persist locks");

                        OperationResult::Complete(KvResponse::PutResult {
                            key: key.clone(),
                            previous: old_value,
                        })
                    }
                    LockAttemptResult::Conflict { holders } => {
                        let blockers = holders
                            .into_iter()
                            .map(|(holder_txn, mode)| BlockingInfo {
                                txn: holder_txn,
                                retry_on: if mode == LockMode::Shared {
                                    RetryOn::Prepare
                                } else {
                                    RetryOn::CommitOrAbort
                                },
                            })
                            .collect();

                        OperationResult::WouldBlock { blockers }
                    }
                }
            }
            KvOperation::Delete { ref key } => {
                // Try to acquire exclusive lock
                match self.lock_manager.check(txn_id, key, LockMode::Exclusive) {
                    LockAttemptResult::WouldGrant => {
                        // Grant the lock
                        self.lock_manager
                            .grant(txn_id, key.clone(), LockMode::Exclusive);
                        let kv_key = KvKey::from(key.as_str());
                        let old_value = self.storage.read(&kv_key, txn_id).expect("Read failed");

                        if old_value.is_none() {
                            // Key doesn't exist - still need to persist lock
                            self.add_locks_to_batch(batch.inner(), txn_id)
                                .expect("Failed to persist locks");

                            return OperationResult::Complete(KvResponse::DeleteResult {
                                key: key.clone(),
                                deleted: false,
                            });
                        }

                        // Create delta
                        let delta = KvDelta::Delete {
                            key: key.clone(),
                            old_value: old_value.unwrap(),
                        };

                        // Add delta to batch
                        self.storage
                            .write_to_batch(batch.inner(), delta, txn_id)
                            .expect("Failed to write to batch");

                        // Add lock persistence to batch
                        self.add_locks_to_batch(batch.inner(), txn_id)
                            .expect("Failed to persist locks");

                        OperationResult::Complete(KvResponse::DeleteResult {
                            key: key.clone(),
                            deleted: true,
                        })
                    }
                    LockAttemptResult::Conflict { holders } => {
                        let blockers = holders
                            .into_iter()
                            .map(|(holder_txn, mode)| BlockingInfo {
                                txn: holder_txn,
                                retry_on: if mode == LockMode::Shared {
                                    RetryOn::Prepare
                                } else {
                                    RetryOn::CommitOrAbort
                                },
                            })
                            .collect();

                        OperationResult::WouldBlock { blockers }
                    }
                }
            }
        }
    }

    fn begin(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {
        // KV engine doesn't need to do anything on begin
        // No-op
    }

    fn prepare(&mut self, batch: &mut Self::Batch, txn_id: TransactionId) {
        // Release read locks (in-memory only)
        let locks = self.lock_manager.locks_held_by(txn_id);
        for (key, mode) in locks {
            if mode == LockMode::Shared {
                self.lock_manager.release(txn_id, &key);
            }
        }

        // Persist remaining write locks to batch
        self.add_locks_to_batch(batch.inner(), txn_id)
            .expect("Failed to persist locks");
    }

    fn commit(&mut self, batch: &mut Self::Batch, txn_id: TransactionId) {
        // Commit transaction data (moves from uncommitted to committed)
        self.storage
            .commit_transaction_to_batch(batch.inner(), txn_id)
            .expect("Failed to commit transaction");

        // Clear persisted locks
        let mut lock_key = b"_locks_".to_vec();
        lock_key.extend_from_slice(&txn_id.to_bytes());
        batch.remove_metadata(lock_key);

        // Cleanup old buckets if needed (throttled internally)
        self.storage
            .maybe_cleanup(txn_id.to_timestamp_for_bucketing())
            .ok();

        // Release in-memory locks
        self.lock_manager.release_all(txn_id);
    }

    fn abort(&mut self, batch: &mut Self::Batch, txn_id: TransactionId) {
        // Abort transaction data (removes from uncommitted)
        self.storage
            .abort_transaction_to_batch(batch.inner(), txn_id)
            .expect("Failed to abort transaction");

        // Clear persisted locks
        let mut lock_key = b"_locks_".to_vec();
        lock_key.extend_from_slice(&txn_id.to_bytes());
        batch.remove_metadata(lock_key);

        // Cleanup old buckets if needed (throttled internally)
        self.storage
            .maybe_cleanup(txn_id.to_timestamp_for_bucketing())
            .ok();

        // Release in-memory locks
        self.lock_manager.release_all(txn_id);
    }

    // ═══════════════════════════════════════════════════════════
    // RECOVERY
    // ═══════════════════════════════════════════════════════════

    fn get_log_index(&self) -> Option<u64> {
        // Read log_index from metadata partition where batch.commit() writes it
        let metadata = self.storage.metadata_partition();
        let log_index = metadata
            .get(b"_log_index")
            .ok()
            .flatten()
            .map(|bytes| {
                let array: [u8; 8] = bytes.as_ref().try_into().unwrap_or([0; 8]);
                u64::from_le_bytes(array)
            })
            .unwrap_or(0);

        if log_index > 0 { Some(log_index) } else { None }
    }

    fn scan_transaction_metadata(&self) -> Vec<(TransactionId, Vec<u8>)> {
        let metadata = self.storage.metadata_partition();
        let mut results = Vec::new();

        // Scan for all keys matching _txn_meta_* pattern
        for (key_bytes, value_bytes) in metadata.prefix("_txn_meta_").flatten() {
            // Extract transaction ID from key: _txn_meta_{16-byte-txn-id}
            if key_bytes.len() == 10 + 16 {
                // "_txn_meta_" = 10 bytes
                let txn_id_bytes: [u8; 16] = key_bytes[10..26]
                    .try_into()
                    .expect("Invalid txn_id in metadata key");
                let txn_id = TransactionId::from_bytes(txn_id_bytes);
                results.push((txn_id, value_bytes.to_vec()));
            }
        }

        results
    }

    fn engine_name(&self) -> &str {
        "kv"
    }
}

impl Default for KvTransactionEngine {
    fn default() -> Self {
        Self::new()
    }
}
