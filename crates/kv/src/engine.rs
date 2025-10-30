//! KV Transaction Engine implementation for the generic stream processor
//!
//! This module implements the TransactionEngine trait, providing KV-specific
//! operation execution while delegating message handling to the generic processor.

use proven_common::TransactionId;
use proven_mvcc::{MvccStorage, StorageConfig};
use proven_stream::engine::BlockingInfo;
use proven_stream::{OperationResult, RetryOn, TransactionEngine};

use crate::storage::entity::{KvDelta, KvEntity, KvKey};
use crate::storage::lock::{LockAttemptResult, LockManager, LockMode};
use crate::storage::lock_persistence::{TransactionLocks, encode_transaction_locks};
use crate::types::{KvOperation, KvResponse, Value};

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

    /// Execute a get operation (with locking)
    fn execute_get(
        &mut self,
        key: &str,
        txn_id: TransactionId,
        log_index: u64,
    ) -> OperationResult<KvResponse> {
        // Check if we can acquire the lock
        match self.lock_manager.check(txn_id, key, LockMode::Shared) {
            LockAttemptResult::WouldGrant => {
                // Grant the lock
                self.lock_manager
                    .grant(txn_id, key.to_string(), LockMode::Shared);

                // Perform the read (same conflict check as snapshot reads)
                let kv_key = KvKey::from(key);
                let value = self.storage.read(&kv_key, txn_id).expect("Read failed");

                // Persist locks atomically with log_index update
                let mut batch = self.storage.batch();
                if let Err(e) = self.add_locks_to_batch(&mut batch, txn_id) {
                    eprintln!("Failed to add locks to batch: {}", e);
                }

                // Update log_index in metadata atomically with locks
                let metadata = self.storage.metadata_partition();
                batch.insert(metadata, "_log_index", log_index.to_le_bytes());

                batch.commit().expect("Batch commit failed");

                OperationResult::Complete(KvResponse::GetResult {
                    key: key.to_string(),
                    value,
                })
            }
            LockAttemptResult::Conflict { holders } => {
                // For reads blocked by writes, we always need to wait for commit/abort
                let blockers = holders
                    .into_iter()
                    .map(|(h, _mode)| BlockingInfo {
                        txn: h,
                        retry_on: RetryOn::CommitOrAbort,
                    })
                    .collect();

                OperationResult::WouldBlock { blockers }
            }
        }
    }

    /// Execute a put operation
    fn execute_put(
        &mut self,
        key: &str,
        value: Value,
        txn_id: TransactionId,
        log_index: u64,
    ) -> OperationResult<KvResponse> {
        // Check if we can acquire the lock
        match self.lock_manager.check(txn_id, key, LockMode::Exclusive) {
            LockAttemptResult::WouldGrant => {
                // Grant the lock
                self.lock_manager
                    .grant(txn_id, key.to_string(), LockMode::Exclusive);

                // Get the previous value for the response
                let kv_key = KvKey::from(key);
                let previous = self.storage.read(&kv_key, txn_id).expect("Read failed");

                // Create delta and write to storage
                let delta = KvDelta::Put {
                    key: key.to_string(),
                    new_value: value.clone(),
                    old_value: previous.clone(),
                };

                let mut batch = self.storage.batch();
                self.storage
                    .write_to_batch(&mut batch, delta, txn_id, log_index)
                    .expect("Write failed");

                // Add locks to the same batch (atomic with data + log_index)
                if let Err(e) = self.add_locks_to_batch(&mut batch, txn_id) {
                    eprintln!("Failed to add locks to batch: {}", e);
                }

                // Commit entire batch atomically (data + log_index + locks)
                batch.commit().expect("Batch commit failed");

                OperationResult::Complete(KvResponse::PutResult {
                    key: key.to_string(),
                    previous,
                })
            }
            LockAttemptResult::Conflict { holders } => {
                // Map each blocker to appropriate retry condition
                let blockers = holders
                    .into_iter()
                    .map(|(h, mode)| BlockingInfo {
                        txn: h,
                        retry_on: if mode == LockMode::Shared {
                            // Blocked by reader - can retry after prepare
                            RetryOn::Prepare
                        } else {
                            // Blocked by writer - must wait for commit/abort
                            RetryOn::CommitOrAbort
                        },
                    })
                    .collect();

                OperationResult::WouldBlock { blockers }
            }
        }
    }

    /// Execute a delete operation
    fn execute_delete(
        &mut self,
        key: &str,
        txn_id: TransactionId,
        log_index: u64,
    ) -> OperationResult<KvResponse> {
        // Check if we can acquire the lock
        match self.lock_manager.check(txn_id, key, LockMode::Exclusive) {
            LockAttemptResult::WouldGrant => {
                // Grant the lock
                self.lock_manager
                    .grant(txn_id, key.to_string(), LockMode::Exclusive);

                // Check if key exists
                let kv_key = KvKey::from(key);
                let old_value = self.storage.read(&kv_key, txn_id).expect("Read failed");

                let deleted = if let Some(old_val) = old_value.clone() {
                    // Create delete delta
                    let delta = KvDelta::Delete {
                        key: key.to_string(),
                        old_value: old_val,
                    };

                    let mut batch = self.storage.batch();
                    self.storage
                        .write_to_batch(&mut batch, delta, txn_id, log_index)
                        .expect("Write failed");

                    // Add locks to the same batch (atomic with data + log_index)
                    if let Err(e) = self.add_locks_to_batch(&mut batch, txn_id) {
                        eprintln!("Failed to add locks to batch: {}", e);
                    }

                    // Commit entire batch atomically (data + log_index + locks)
                    batch.commit().expect("Batch commit failed");

                    true
                } else {
                    // Even if no delete, still need to persist locks atomically
                    let mut batch = self.storage.batch();
                    if let Err(e) = self.add_locks_to_batch(&mut batch, txn_id) {
                        eprintln!("Failed to add locks to batch: {}", e);
                    }
                    batch.commit().expect("Batch commit failed");

                    false
                };

                OperationResult::Complete(KvResponse::DeleteResult {
                    key: key.to_string(),
                    deleted,
                })
            }
            LockAttemptResult::Conflict { holders } => {
                // Map each blocker to appropriate retry condition
                let blockers = holders
                    .into_iter()
                    .map(|(h, mode)| BlockingInfo {
                        txn: h,
                        retry_on: if mode == LockMode::Shared {
                            // Blocked by reader - can retry after prepare
                            RetryOn::Prepare
                        } else {
                            // Blocked by writer - must wait for commit/abort
                            RetryOn::CommitOrAbort
                        },
                    })
                    .collect();

                OperationResult::WouldBlock { blockers }
            }
        }
    }
}

impl TransactionEngine for KvTransactionEngine {
    type Operation = KvOperation;
    type Response = KvResponse;

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

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: TransactionId,
        log_index: u64,
    ) -> OperationResult<Self::Response> {
        match operation {
            KvOperation::Get { ref key } => self.execute_get(key, txn_id, log_index),
            KvOperation::Put { ref key, ref value } => {
                self.execute_put(key, value.clone(), txn_id, log_index)
            }
            KvOperation::Delete { ref key } => self.execute_delete(key, txn_id, log_index),
        }
    }

    fn begin(&mut self, _txn_id: TransactionId, _log_index: u64) {
        // Nothing to do - processor tracks active transactions
    }

    fn prepare(&mut self, txn_id: TransactionId, log_index: u64) {
        // Get locks held by this transaction from lock manager
        let locks = self.lock_manager.locks_held_by(txn_id);

        // Release read locks (keep write locks)
        for (key, mode) in locks {
            if mode == LockMode::Shared {
                self.lock_manager.release(txn_id, &key);
            }
        }

        // Update persisted locks atomically with log_index (only write locks remain)
        let mut batch = self.storage.batch();
        if let Err(e) = self.add_locks_to_batch(&mut batch, txn_id) {
            eprintln!("Failed to add locks to batch: {}", e);
        }

        // Update log_index in metadata atomically with locks
        let metadata = self.storage.metadata_partition();
        batch.insert(metadata, "_log_index", log_index.to_le_bytes());

        batch.commit().expect("Batch commit failed");
    }

    fn commit(&mut self, txn_id: TransactionId, log_index: u64) {
        // Commit to storage and clear persisted locks atomically
        let mut batch = self.storage.batch();

        // Commit the transaction via storage
        self.storage
            .commit_transaction_to_batch(&mut batch, txn_id, log_index)
            .expect("Commit failed");

        // Clear persisted locks in the same batch
        let mut lock_key = b"_locks_".to_vec();
        lock_key.extend_from_slice(&txn_id.to_bytes());
        let metadata = self.storage.metadata_partition();
        batch.remove(metadata.clone(), lock_key);

        // Commit atomically: transaction commit + locks cleanup + log_index
        batch.commit().expect("Batch commit failed");

        // Cleanup old buckets if needed (throttled internally)
        // Use timestamp component for cleanup
        self.storage
            .maybe_cleanup(txn_id.to_timestamp_for_bucketing())
            .ok();

        // Release all locks held by this transaction
        self.lock_manager.release_all(txn_id);
    }

    fn abort(&mut self, txn_id: TransactionId, log_index: u64) {
        // Abort in storage and clear persisted locks atomically
        let mut batch = self.storage.batch();

        // Abort the transaction via storage
        self.storage
            .abort_transaction_to_batch(&mut batch, txn_id, log_index)
            .expect("Abort failed");

        // Clear persisted locks in the same batch
        let mut lock_key = b"_locks_".to_vec();
        lock_key.extend_from_slice(&txn_id.to_bytes());
        let metadata = self.storage.metadata_partition();
        batch.remove(metadata.clone(), lock_key);

        // Commit atomically: transaction abort + locks cleanup + log_index
        batch.commit().expect("Batch commit failed");

        // Cleanup old buckets if needed (throttled internally)
        // Use timestamp component for cleanup
        self.storage
            .maybe_cleanup(txn_id.to_timestamp_for_bucketing())
            .ok();

        // Release all locks
        self.lock_manager.release_all(txn_id);
    }

    fn engine_name(&self) -> &'static str {
        "kv"
    }

    fn get_log_index(&self) -> Option<u64> {
        let log_index = self.storage.get_log_index();
        if log_index > 0 { Some(log_index) } else { None }
    }
}

impl Default for KvTransactionEngine {
    fn default() -> Self {
        Self::new()
    }
}
