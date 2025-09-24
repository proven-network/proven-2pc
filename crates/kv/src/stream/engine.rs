//! KV Transaction Engine implementation for the generic stream processor
//!
//! This module implements the TransactionEngine trait, providing KV-specific
//! operation execution while delegating message handling to the generic processor.

use proven_hlc::HlcTimestamp;
use proven_stream::engine::BlockingInfo;
use proven_stream::{OperationResult, RetryOn, TransactionEngine};

use crate::storage::lock::{LockAttemptResult, LockManager, LockMode};
use crate::storage::mvcc::MvccStorage;
use crate::types::Value;

use super::operation::KvOperation;
use super::response::KvResponse;
use super::transaction::TransactionContext;

use std::collections::HashMap;

/// KV-specific transaction engine
pub struct KvTransactionEngine {
    /// MVCC storage for versioned data
    storage: MvccStorage,

    /// Lock manager for pessimistic concurrency control
    lock_manager: LockManager,

    /// Active transaction contexts
    active_transactions: HashMap<HlcTimestamp, TransactionContext>,
}

impl KvTransactionEngine {
    /// Create a new KV transaction engine
    pub fn new() -> Self {
        Self {
            storage: MvccStorage::new(),
            lock_manager: LockManager::new(),
            active_transactions: HashMap::new(),
        }
    }

    /// Execute a get operation without locking
    fn execute_get_without_locking(
        &self,
        key: &str,
        read_timestamp: HlcTimestamp,
    ) -> OperationResult<KvResponse> {
        // Check for pending writes from EARLIER transactions using the new MVCC method
        // These are writes that should be visible to us once they commit
        let pending_writers = self.storage.get_pending_writers(key);
        let earlier_writers: Vec<HlcTimestamp> = pending_writers
            .into_iter()
            .filter(|&tx_id| tx_id < read_timestamp)
            .collect();

        if !earlier_writers.is_empty() {
            // There's a pending write from an earlier transaction
            // We must wait to see if it commits (so we can read its value)
            let blockers = earlier_writers
                .into_iter()
                .map(|txn| BlockingInfo {
                    txn,
                    retry_on: RetryOn::CommitOrAbort,
                })
                .collect();

            return OperationResult::WouldBlock { blockers };
        }

        // No blocking writes from earlier transactions
        // Use the snapshot read method for proper visibility checks
        let value = self.storage.get_at_timestamp(key, read_timestamp);
        OperationResult::Complete(KvResponse::GetResult {
            key: key.to_string(),
            value: value.map(|arc| (*arc).clone()),
        })
    }

    /// Execute a get operation
    fn execute_get(&mut self, key: &str, txn_id: HlcTimestamp) -> OperationResult<KvResponse> {
        // Check if we can acquire the lock
        match self.lock_manager.check(txn_id, key, LockMode::Shared) {
            LockAttemptResult::WouldGrant => {
                // Grant the lock
                self.lock_manager
                    .grant(txn_id, key.to_string(), LockMode::Shared);

                // Perform the read
                let value = self.storage.get(key, txn_id);

                // Track lock in transaction context
                if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
                    tx_ctx.locks_held.push((key.to_string(), LockMode::Shared));
                }

                OperationResult::Complete(KvResponse::GetResult {
                    key: key.to_string(),
                    value: value.map(|arc| (*arc).clone()),
                })
            }
            LockAttemptResult::Conflict { holders } => {
                // Debug: log the blocking situation
                let blocker_list: Vec<String> =
                    holders.iter().map(|(h, _)| h.to_string()).collect();
                println!(
                    "[kv_stream] DEFERRED: WouldBlock for txn {}: {:?} (get)",
                    txn_id, blocker_list
                );

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
        txn_id: HlcTimestamp,
    ) -> OperationResult<KvResponse> {
        // Check if we can acquire the lock
        match self.lock_manager.check(txn_id, key, LockMode::Exclusive) {
            LockAttemptResult::WouldGrant => {
                // Grant the lock
                self.lock_manager
                    .grant(txn_id, key.to_string(), LockMode::Exclusive);

                // Get the previous value for the response
                let previous = self.storage.get(key, txn_id).map(|arc| (*arc).clone());

                // Write to storage
                self.storage
                    .put(key.to_string(), value.clone(), txn_id, txn_id);

                // Track lock in transaction context
                if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
                    tx_ctx
                        .locks_held
                        .push((key.to_string(), LockMode::Exclusive));
                }

                OperationResult::Complete(KvResponse::PutResult {
                    key: key.to_string(),
                    previous,
                })
            }
            LockAttemptResult::Conflict { holders } => {
                // Debug: log the blocking situation
                let blocker_list: Vec<String> =
                    holders.iter().map(|(h, _)| h.to_string()).collect();
                println!(
                    "[kv_stream] DEFERRED: WouldBlock for txn {}: {:?} (put)",
                    txn_id, blocker_list
                );

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
    fn execute_delete(&mut self, key: &str, txn_id: HlcTimestamp) -> OperationResult<KvResponse> {
        // Check if we can acquire the lock
        match self.lock_manager.check(txn_id, key, LockMode::Exclusive) {
            LockAttemptResult::WouldGrant => {
                // Grant the lock
                self.lock_manager
                    .grant(txn_id, key.to_string(), LockMode::Exclusive);

                // Check if key exists
                let existed = self.storage.get(key, txn_id).is_some();

                // Delete from storage
                if existed {
                    self.storage.delete(key, txn_id);
                }

                // Track lock in transaction context
                if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
                    tx_ctx
                        .locks_held
                        .push((key.to_string(), LockMode::Exclusive));
                }

                OperationResult::Complete(KvResponse::DeleteResult {
                    key: key.to_string(),
                    deleted: existed,
                })
            }
            LockAttemptResult::Conflict { holders } => {
                // Debug: log the blocking situation
                let blocker_list: Vec<String> =
                    holders.iter().map(|(h, _)| h.to_string()).collect();
                println!(
                    "[kv_stream] DEFERRED: WouldBlock for txn {}: {:?} (delete)",
                    txn_id, blocker_list
                );

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
        read_timestamp: HlcTimestamp,
    ) -> OperationResult<Self::Response> {
        match operation {
            KvOperation::Get { ref key } => self.execute_get_without_locking(key, read_timestamp),
            _ => panic!("Must be read-only operation"),
        }
    }

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: HlcTimestamp,
    ) -> OperationResult<Self::Response> {
        match operation {
            KvOperation::Get { ref key } => self.execute_get(key, txn_id),
            KvOperation::Put { ref key, ref value } => self.execute_put(key, value.clone(), txn_id),
            KvOperation::Delete { ref key } => self.execute_delete(key, txn_id),
        }
    }

    fn prepare(&mut self, txn_id: HlcTimestamp) {
        // Get transaction context
        let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) else {
            // If transaction doesn't exist, that's fine - it may have been aborted already
            return;
        };

        // Try to prepare the transaction
        if !tx_ctx.prepare() {
            // Transaction cannot be prepared (may be wounded or aborted) - that's fine
            return;
        }

        // Release read locks (keep write locks)
        let locks_to_release: Vec<String> = tx_ctx
            .locks_held
            .iter()
            .filter_map(|(key, mode)| {
                if *mode == LockMode::Shared {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        // Release the read locks from lock manager
        for key in locks_to_release {
            self.lock_manager.release(txn_id, &key);

            // Remove from transaction's held locks
            tx_ctx
                .locks_held
                .retain(|(k, m)| !(k == &key && *m == LockMode::Shared));
        }
    }

    fn commit(&mut self, txn_id: HlcTimestamp) {
        // Remove transaction context
        let Some(tx_ctx) = self.active_transactions.remove(&txn_id) else {
            // If transaction doesn't exist, that's fine - may have been committed already
            return;
        };

        // For auto-commit, we don't require prepare
        // (The transaction is active and can be directly committed)
        // For 2PC, it should be prepared
        if tx_ctx.is_active() || tx_ctx.is_prepared() {
            // Commit to storage
            self.storage.commit_transaction(txn_id);

            // Release all locks held by this transaction
            self.lock_manager.release_all(txn_id);
        }
        // If not committable, just clean up - validation happened in apply_operation
    }

    fn abort(&mut self, txn_id: HlcTimestamp) {
        // Debug: check what locks are being held before release
        let locks_before = self.lock_manager.locks_held_by(txn_id);
        if !locks_before.is_empty() {
            println!(
                "[kv] ABORT: Releasing {} locks for txn {}",
                locks_before.len(),
                txn_id
            );
        }

        // Remove transaction context
        self.active_transactions.remove(&txn_id);

        // Abort in storage
        self.storage.abort_transaction(txn_id);

        // Release all locks
        self.lock_manager.release_all(txn_id);

        // Debug: verify locks are released
        let locks_after = self.lock_manager.locks_held_by(txn_id);
        if !locks_after.is_empty() {
            println!(
                "[kv] ERROR: {} locks still held after abort for txn {}",
                locks_after.len(),
                txn_id
            );
        }
    }

    fn begin(&mut self, txn_id: HlcTimestamp) {
        // Create new transaction context
        let tx_ctx = TransactionContext::new(txn_id);

        // Register with storage
        self.storage.register_transaction(txn_id, txn_id);

        // Store context
        self.active_transactions.insert(txn_id, tx_ctx);
    }

    fn is_transaction_active(&self, txn_id: &HlcTimestamp) -> bool {
        self.active_transactions.contains_key(txn_id)
    }

    fn engine_name(&self) -> &'static str {
        "kv"
    }

    fn snapshot(&self) -> Result<Vec<u8>, String> {
        // Only snapshot when no active transactions
        if !self.active_transactions.is_empty() {
            return Err("Cannot snapshot with active transactions".to_string());
        }

        // Define the snapshot structure
        #[derive(serde::Serialize, serde::Deserialize)]
        struct KvSnapshot {
            // Compacted data - only latest committed version per key
            data: HashMap<String, Value>,
        }

        // Get compacted data from MVCC storage
        let compacted = self.storage.get_compacted_data();

        let snapshot = KvSnapshot { data: compacted };

        // Serialize with CBOR
        let mut buf = Vec::new();
        ciborium::into_writer(&snapshot, &mut buf)
            .map_err(|e| format!("Failed to serialize snapshot: {}", e))?;

        // Compress with zstd (level 3 is a good balance)
        let compressed = zstd::encode_all(&buf[..], 3)
            .map_err(|e| format!("Failed to compress snapshot: {}", e))?;

        Ok(compressed)
    }

    fn restore_from_snapshot(&mut self, data: &[u8]) -> Result<(), String> {
        // Decompress the data
        let decompressed =
            zstd::decode_all(data).map_err(|e| format!("Failed to decompress snapshot: {}", e))?;

        #[derive(serde::Serialize, serde::Deserialize)]
        struct KvSnapshot {
            data: HashMap<String, Value>,
        }

        // Deserialize snapshot
        let snapshot: KvSnapshot = ciborium::from_reader(&decompressed[..])
            .map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;

        // Clear existing state
        self.storage = MvccStorage::new();
        self.lock_manager = LockManager::new();
        self.active_transactions.clear();

        // Restore compacted data
        self.storage.restore_from_compacted(snapshot.data);

        Ok(())
    }
}

impl Default for KvTransactionEngine {
    fn default() -> Self {
        Self::new()
    }
}
