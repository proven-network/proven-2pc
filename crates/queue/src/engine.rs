//! Queue engine implementation for stream processing
//!
//! Integrates with the proven-stream processor to handle distributed
//! queue operations with MVCC and eager locking.
//!
//! Now uses proven-mvcc for persistent storage with crash recovery.

use crate::storage::entity::{QueueDelta, QueueEntity};
use crate::storage::lock_persistence::{
    QueueTransactionLock, decode_transaction_lock, encode_transaction_lock,
};
use crate::storage::{LockAttemptResult, LockManager, LockMode};
use crate::types::{QueueOperation, QueueResponse, QueueValue};
use proven_common::TransactionId;
use proven_mvcc::{MvccStorage, StorageConfig};
use proven_stream::engine::{BlockingInfo, OperationResult, RetryOn, TransactionEngine};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

/// Queue engine that implements the TransactionEngine trait with persistent storage
pub struct QueueTransactionEngine {
    /// MVCC storage for queue data (now persistent via proven-mvcc)
    storage: MvccStorage<QueueEntity>,

    /// Lock manager for pessimistic concurrency control
    lock_manager: LockManager,

    /// Next entry ID counter (for generating unique monotonic IDs)
    next_entry_id: AtomicU64,

    /// Track dequeued entries per transaction (for read-your-own-writes within transaction)
    /// Maps txn_id -> set of dequeued entry_ids
    dequeued_entries: HashMap<TransactionId, HashSet<u64>>,
}

impl QueueTransactionEngine {
    /// Create a new queue engine with default storage
    pub fn new() -> Self {
        // StorageConfig::default() creates a unique temp directory per instance
        let config = StorageConfig::default();
        Self::with_config(config)
    }

    /// Create a new queue engine with custom config
    pub fn with_config(config: StorageConfig) -> Self {
        let storage =
            MvccStorage::<QueueEntity>::new(config).expect("Failed to create MVCC storage");

        let mut engine = Self {
            storage,
            lock_manager: LockManager::new(),
            next_entry_id: AtomicU64::new(1),
            dequeued_entries: HashMap::new(),
        };

        // Recover locks from persisted state (crash recovery)
        // This is critical because the stream processor won't replay
        // already-processed operations (it uses get_log_index() to resume)
        engine.recover_locks_from_storage();

        // Recover next_entry_id from metadata
        engine.recover_next_entry_id();

        engine
    }

    /// Recover locks from persisted state (crash recovery)
    fn recover_locks_from_storage(&mut self) {
        let metadata = self.storage.metadata_partition();

        // Scan for all persisted locks
        for (_key_bytes, value_bytes) in metadata.prefix("_locks_").flatten() {
            // Decode the transaction lock
            if let Ok(tx_lock) = decode_transaction_lock(&value_bytes) {
                // Restore lock to the in-memory lock manager
                self.lock_manager.grant(tx_lock.txn_id, tx_lock.mode);
            }
        }
    }

    /// Recover next_entry_id from metadata (crash recovery)
    fn recover_next_entry_id(&mut self) {
        let metadata = self.storage.metadata_partition();

        if let Some(bytes) = metadata.get("_next_entry_id").ok().flatten()
            && bytes.len() == 8
        {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes);
            let recovered_id = u64::from_be_bytes(buf);
            self.next_entry_id.store(recovered_id, Ordering::SeqCst);
        }
    }

    /// Get next entry ID and increment counter
    fn get_next_entry_id(&self) -> u64 {
        self.next_entry_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Add locks to batch for atomic persistence
    fn add_locks_to_batch(
        &mut self,
        batch: &mut proven_mvcc::Batch,
        txn_id: TransactionId,
    ) -> Result<(), String> {
        let locks_held = self.lock_manager.locks_held_by(txn_id);

        if !locks_held.is_empty() {
            // Queue has only one lock per transaction (unlike KV's multiple keys)
            // So we just take the first (and only) lock mode
            if let Some(mode) = locks_held.first() {
                let tx_lock = QueueTransactionLock::new(txn_id, *mode);

                // Lock key: prefix + lexicographic timestamp bytes
                let mut lock_key = b"_locks_".to_vec();
                lock_key.extend_from_slice(&txn_id.to_bytes());
                let lock_bytes = encode_transaction_lock(&tx_lock)?;

                // Add to batch (will be committed atomically with data)
                let metadata = self.storage.metadata_partition();
                batch.insert(metadata, lock_key, lock_bytes);
            }
        }

        Ok(())
    }

    /// Persist next_entry_id to metadata
    fn persist_next_entry_id(&self, batch: &mut proven_mvcc::Batch) {
        let current_id = self.next_entry_id.load(Ordering::SeqCst);
        let metadata = self.storage.metadata_partition();
        batch.insert(metadata, "_next_entry_id", current_id.to_be_bytes());
    }

    /// Get the head of the queue (oldest non-dequeued entry) for a transaction
    /// This scans from entry_id=1 forward until finding a valid entry
    fn get_head(&self, txn_id: TransactionId) -> Option<(u64, QueueValue)> {
        // Start from entry_id 1 and scan forward
        let max_entry_id = self.next_entry_id.load(Ordering::SeqCst);
        let dequeued = self.dequeued_entries.get(&txn_id);

        for entry_id in 1..max_entry_id {
            // Skip if this entry was dequeued in the current transaction
            if let Some(dequeued_set) = dequeued
                && dequeued_set.contains(&entry_id)
            {
                continue;
            }

            if let Ok(Some(value)) = self.storage.read(&entry_id, txn_id) {
                return Some((entry_id, value));
            }
        }

        None
    }

    /// Get the size of the queue for a transaction
    fn get_size(&self, txn_id: TransactionId) -> usize {
        let max_entry_id = self.next_entry_id.load(Ordering::SeqCst);
        let dequeued = self.dequeued_entries.get(&txn_id);
        let mut count = 0;

        for entry_id in 1..max_entry_id {
            // Skip if this entry was dequeued in the current transaction
            if let Some(dequeued_set) = dequeued
                && dequeued_set.contains(&entry_id)
            {
                continue;
            }

            if self
                .storage
                .read(&entry_id, txn_id)
                .ok()
                .flatten()
                .is_some()
            {
                count += 1;
            }
        }

        count
    }

    /// Check for lock conflicts for snapshot reads
    /// Returns blockers if there are earlier transactions with exclusive locks
    fn check_snapshot_read_conflicts(
        &self,
        read_timestamp: TransactionId,
    ) -> Option<Vec<BlockingInfo>> {
        // For queues, we need to check if ANY earlier transaction has an exclusive lock
        // (since exclusive locks affect the entire queue structure)
        let mut blockers = Vec::new();

        // Get all lock holders
        for (holder_txn, lock_mode) in self.lock_manager.get_all_holders() {
            // Only block on EARLIER transactions with EXCLUSIVE or APPEND locks
            // (both can modify queue state)
            if holder_txn < read_timestamp
                && (lock_mode == LockMode::Exclusive || lock_mode == LockMode::Append)
            {
                blockers.push(BlockingInfo {
                    txn: holder_txn,
                    retry_on: RetryOn::CommitOrAbort,
                });
            }
        }

        if blockers.is_empty() {
            None
        } else {
            // Remove duplicates and sort by timestamp
            blockers.sort_by_key(|b| b.txn);
            blockers.dedup_by_key(|b| b.txn);
            Some(blockers)
        }
    }

    /// Execute a peek operation without locking (snapshot reads)
    fn execute_peek_without_locking(
        &self,
        read_timestamp: TransactionId,
    ) -> OperationResult<QueueResponse> {
        // Check for conflicts with earlier transactions holding exclusive/append locks
        if let Some(blockers) = self.check_snapshot_read_conflicts(read_timestamp) {
            return OperationResult::WouldBlock { blockers };
        }

        // For snapshot reads, we need to reconstruct the queue state at the read timestamp
        // and find the head entry
        let max_entry_id = self.next_entry_id.load(Ordering::SeqCst);

        for entry_id in 1..max_entry_id {
            if let Ok(Some(value)) = self.storage.read(&entry_id, read_timestamp) {
                return OperationResult::Complete(QueueResponse::Peeked(Some(value)));
            }
        }

        OperationResult::Complete(QueueResponse::Peeked(None))
    }

    /// Execute a size operation without locking (snapshot reads)
    fn execute_size_without_locking(
        &self,
        read_timestamp: TransactionId,
    ) -> OperationResult<QueueResponse> {
        // Check for conflicts with earlier transactions holding exclusive/append locks
        if let Some(blockers) = self.check_snapshot_read_conflicts(read_timestamp) {
            return OperationResult::WouldBlock { blockers };
        }

        let max_entry_id = self.next_entry_id.load(Ordering::SeqCst);
        let mut count = 0;

        for entry_id in 1..max_entry_id {
            if self
                .storage
                .read(&entry_id, read_timestamp)
                .ok()
                .flatten()
                .is_some()
            {
                count += 1;
            }
        }

        OperationResult::Complete(QueueResponse::Size(count))
    }

    /// Execute an is_empty operation without locking (snapshot reads)
    fn execute_is_empty_without_locking(
        &self,
        read_timestamp: TransactionId,
    ) -> OperationResult<QueueResponse> {
        // Check for conflicts with earlier transactions holding exclusive/append locks
        if let Some(blockers) = self.check_snapshot_read_conflicts(read_timestamp) {
            return OperationResult::WouldBlock { blockers };
        }

        let max_entry_id = self.next_entry_id.load(Ordering::SeqCst);

        for entry_id in 1..max_entry_id {
            if self
                .storage
                .read(&entry_id, read_timestamp)
                .ok()
                .flatten()
                .is_some()
            {
                return OperationResult::Complete(QueueResponse::IsEmpty(false));
            }
        }

        OperationResult::Complete(QueueResponse::IsEmpty(true))
    }

    /// Execute enqueue operation
    fn execute_enqueue(
        &mut self,
        value: QueueValue,
        txn_id: TransactionId,
        log_index: u64,
    ) -> OperationResult<QueueResponse> {
        // Generate next entry ID
        let entry_id = self.get_next_entry_id();

        // Create delta
        let delta = QueueDelta::Enqueue {
            entry_id,
            value,
            enqueued_at: txn_id,
        };

        // Write to storage with batch
        let mut batch = self.storage.batch();
        self.storage
            .write_to_batch(&mut batch, delta, txn_id, log_index)
            .expect("Write failed");

        // Add locks to the same batch (atomic with data + log_index)
        if let Err(e) = self.add_locks_to_batch(&mut batch, txn_id) {
            eprintln!("Failed to add locks to batch: {}", e);
        }

        // Persist next_entry_id
        self.persist_next_entry_id(&mut batch);

        // Commit entire batch atomically (data + log_index + locks + next_entry_id)
        batch.commit().expect("Batch commit failed");

        OperationResult::Complete(QueueResponse::Enqueued)
    }

    /// Execute dequeue operation
    fn execute_dequeue(
        &mut self,
        txn_id: TransactionId,
        log_index: u64,
    ) -> OperationResult<QueueResponse> {
        // Find the head of the queue
        if let Some((entry_id, value)) = self.get_head(txn_id) {
            // Track this dequeue in-memory for read-your-own-writes
            self.dequeued_entries
                .entry(txn_id)
                .or_default()
                .insert(entry_id);

            // Create dequeue delta
            let delta = QueueDelta::Dequeue {
                entry_id,
                old_value: value.clone(),
            };

            // Write to storage with batch
            let mut batch = self.storage.batch();
            self.storage
                .write_to_batch(&mut batch, delta, txn_id, log_index)
                .expect("Write failed");

            // Add locks to the same batch
            if let Err(e) = self.add_locks_to_batch(&mut batch, txn_id) {
                eprintln!("Failed to add locks to batch: {}", e);
            }

            // Commit batch
            batch.commit().expect("Batch commit failed");

            OperationResult::Complete(QueueResponse::Dequeued(Some(value)))
        } else {
            // Queue is empty - still need to persist locks
            let mut batch = self.storage.batch();
            if let Err(e) = self.add_locks_to_batch(&mut batch, txn_id) {
                eprintln!("Failed to add locks to batch: {}", e);
            }

            // Update log_index even if no dequeue
            let metadata = self.storage.metadata_partition();
            batch.insert(metadata, "_log_index", log_index.to_le_bytes());

            batch.commit().expect("Batch commit failed");

            OperationResult::Complete(QueueResponse::Dequeued(None))
        }
    }

    /// Execute clear operation
    fn execute_clear(
        &mut self,
        txn_id: TransactionId,
        log_index: u64,
    ) -> OperationResult<QueueResponse> {
        // Collect all entries to clear and create individual Dequeue deltas for each
        let max_entry_id = self.next_entry_id.load(Ordering::SeqCst);
        let mut batch = self.storage.batch();

        for entry_id in 1..max_entry_id {
            if let Ok(Some(value)) = self.storage.read(&entry_id, txn_id) {
                // Create a dequeue delta for each entry
                let delta = QueueDelta::Dequeue {
                    entry_id,
                    old_value: value,
                };

                self.storage
                    .write_to_batch(&mut batch, delta, txn_id, log_index)
                    .expect("Write failed");
            }
        }

        // Add locks to the same batch
        if let Err(e) = self.add_locks_to_batch(&mut batch, txn_id) {
            eprintln!("Failed to add locks to batch: {}", e);
        }

        // Commit batch
        batch.commit().expect("Batch commit failed");

        OperationResult::Complete(QueueResponse::Cleared)
    }
}

impl Default for QueueTransactionEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionEngine for QueueTransactionEngine {
    type Operation = QueueOperation;
    type Response = QueueResponse;

    fn read_at_timestamp(
        &mut self,
        operation: Self::Operation,
        read_timestamp: TransactionId,
    ) -> OperationResult<Self::Response> {
        match operation {
            QueueOperation::Peek => self.execute_peek_without_locking(read_timestamp),
            QueueOperation::Size => self.execute_size_without_locking(read_timestamp),
            QueueOperation::IsEmpty => self.execute_is_empty_without_locking(read_timestamp),
            _ => panic!("Must be read-only operation"),
        }
    }

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: TransactionId,
        log_index: u64,
    ) -> OperationResult<Self::Response> {
        // Determine required lock mode
        let lock_mode = match &operation {
            QueueOperation::Enqueue { .. } => LockMode::Append,
            QueueOperation::Dequeue | QueueOperation::Clear => LockMode::Exclusive,
            QueueOperation::Peek | QueueOperation::Size | QueueOperation::IsEmpty => {
                LockMode::Shared
            }
        };

        // Try to acquire lock if not already held
        if !self.lock_manager.has_locks(txn_id) {
            match self.lock_manager.check(txn_id, lock_mode) {
                LockAttemptResult::WouldGrant => {
                    self.lock_manager.grant(txn_id, lock_mode);
                }
                LockAttemptResult::Conflict { holder, mode } => {
                    // Determine retry timing based on conflict type
                    let retry_on = match (lock_mode, mode) {
                        // Exclusive blocked by read - can retry after prepare
                        (LockMode::Exclusive, LockMode::Shared) => RetryOn::Prepare,
                        // Append blocked by read - can continue (they're compatible!)
                        // This shouldn't happen due to compatibility check
                        (LockMode::Append, LockMode::Shared) => RetryOn::Prepare,
                        // All other conflicts need commit/abort
                        _ => RetryOn::CommitOrAbort,
                    };

                    return OperationResult::WouldBlock {
                        blockers: vec![BlockingInfo {
                            txn: holder,
                            retry_on,
                        }],
                    };
                }
            }
        }

        // Execute the operation
        match operation {
            QueueOperation::Enqueue { value } => self.execute_enqueue(value, txn_id, log_index),
            QueueOperation::Dequeue => self.execute_dequeue(txn_id, log_index),
            QueueOperation::Peek => {
                if let Some((_, value)) = self.get_head(txn_id) {
                    OperationResult::Complete(QueueResponse::Peeked(Some(value)))
                } else {
                    OperationResult::Complete(QueueResponse::Peeked(None))
                }
            }
            QueueOperation::Size => {
                let size = self.get_size(txn_id);
                OperationResult::Complete(QueueResponse::Size(size))
            }
            QueueOperation::IsEmpty => {
                let is_empty = self.get_head(txn_id).is_none();
                OperationResult::Complete(QueueResponse::IsEmpty(is_empty))
            }
            QueueOperation::Clear => self.execute_clear(txn_id, log_index),
        }
    }

    fn begin(&mut self, _txn_id: TransactionId, _log_index: u64) {
        // Nothing to do - MVCC storage tracks transactions internally
    }

    fn prepare(&mut self, txn_id: TransactionId, log_index: u64) {
        // Get locks held by this transaction from lock manager
        let locks = self.lock_manager.locks_held_by(txn_id);

        // Release read locks (keep write locks)
        if locks.contains(&LockMode::Shared) {
            self.lock_manager.release(txn_id);
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
        self.storage
            .maybe_cleanup(txn_id.to_timestamp_for_bucketing())
            .ok();

        // Release all locks held by this transaction
        self.lock_manager.release_all(txn_id);

        // Clear in-memory dequeued entries tracking
        self.dequeued_entries.remove(&txn_id);
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
        self.storage
            .maybe_cleanup(txn_id.to_timestamp_for_bucketing())
            .ok();

        // Release all locks
        self.lock_manager.release_all(txn_id);

        // Clear in-memory dequeued entries tracking
        self.dequeued_entries.remove(&txn_id);
    }

    fn engine_name(&self) -> &'static str {
        "queue"
    }

    fn get_log_index(&self) -> Option<u64> {
        let log_index = self.storage.get_log_index();
        if log_index > 0 { Some(log_index) } else { None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_timestamp() -> TransactionId {
        TransactionId::new()
    }

    #[test]
    fn test_engine_basic_operations() {
        let mut engine = QueueTransactionEngine::new();
        let tx1 = create_timestamp();

        engine.begin(tx1, 1);

        // Test enqueue
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::I64(42),
        };

        let result = engine.apply_operation(enqueue_op, tx1, 2);
        assert!(matches!(
            result,
            OperationResult::Complete(QueueResponse::Enqueued)
        ));

        // Test peek
        let peek_op = QueueOperation::Peek;

        let result = engine.apply_operation(peek_op, tx1, 3);
        assert!(matches!(
            result,
            OperationResult::Complete(QueueResponse::Peeked(Some(QueueValue::I64(42))))
        ));

        // Test commit
        engine.commit(tx1, 4);
    }

    #[test]
    fn test_engine_blocking() {
        let mut engine = QueueTransactionEngine::new();
        let tx1 = create_timestamp();
        let tx2 = create_timestamp();

        engine.begin(tx1, 1);
        engine.begin(tx2, 2);

        // tx1 gets append lock
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::Str("tx1".to_string()),
        };

        let result = engine.apply_operation(enqueue_op, tx1, 3);
        assert!(matches!(result, OperationResult::Complete(_)));

        // tx2 should be blocked trying to get exclusive lock
        let dequeue_op = QueueOperation::Dequeue;

        let result = engine.apply_operation(dequeue_op, tx2, 4);
        assert!(matches!(result, OperationResult::WouldBlock { .. }));
    }

    #[test]
    fn test_engine_abort() {
        let mut engine = QueueTransactionEngine::new();
        let tx1 = create_timestamp();

        engine.begin(tx1, 1);

        // Execute operation
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::Bool(true),
        };

        engine.apply_operation(enqueue_op, tx1, 2);

        // Abort
        engine.abort(tx1, 3);

        // Queue should be empty after abort
        let tx2 = create_timestamp();
        engine.begin(tx2, 4);

        let result = engine.apply_operation(QueueOperation::IsEmpty, tx2, 5);
        assert!(matches!(
            result,
            OperationResult::Complete(QueueResponse::IsEmpty(true))
        ));
    }

    #[test]
    fn test_engine_dequeue() {
        let mut engine = QueueTransactionEngine::new();
        let tx1 = create_timestamp();

        engine.begin(tx1, 1);

        // Enqueue two values
        engine.apply_operation(
            QueueOperation::Enqueue {
                value: QueueValue::Str("first".to_string()),
            },
            tx1,
            2,
        );
        engine.apply_operation(
            QueueOperation::Enqueue {
                value: QueueValue::Str("second".to_string()),
            },
            tx1,
            3,
        );

        // Dequeue should return FIFO order
        let result = engine.apply_operation(QueueOperation::Dequeue, tx1, 4);
        assert!(matches!(
            result,
            OperationResult::Complete(QueueResponse::Dequeued(Some(QueueValue::Str(s)))) if s == "first"
        ));

        let result = engine.apply_operation(QueueOperation::Dequeue, tx1, 5);
        assert!(matches!(
            result,
            OperationResult::Complete(QueueResponse::Dequeued(Some(QueueValue::Str(s)))) if s == "second"
        ));

        // Should be empty now
        let result = engine.apply_operation(QueueOperation::Dequeue, tx1, 6);
        assert!(matches!(
            result,
            OperationResult::Complete(QueueResponse::Dequeued(None))
        ));

        engine.commit(tx1, 7);
    }

    #[test]
    fn test_snapshot_after_dequeue() {
        let mut engine = QueueTransactionEngine::new();

        // Enqueue 3 values in separate transactions
        for i in 0..3 {
            let tx = create_timestamp();
            engine.begin(tx, i * 2);
            engine.apply_operation(
                QueueOperation::Enqueue {
                    value: QueueValue::I64(i as i64),
                },
                tx,
                i * 2 + 1,
            );
            engine.commit(tx, i * 2 + 2);
        }

        // Dequeue 2 values at time 400
        let tx = create_timestamp();
        engine.begin(tx, 10);

        let r1 = engine.apply_operation(QueueOperation::Dequeue, tx, 11);
        println!("First dequeue: {:?}", r1);

        let r2 = engine.apply_operation(QueueOperation::Dequeue, tx, 12);
        println!("Second dequeue: {:?}", r2);

        engine.commit(tx, 13);

        // Snapshot peek at 450 should see value 2
        let result = engine.read_at_timestamp(QueueOperation::Peek, create_timestamp());
        println!("Snapshot peek result: {:?}", result);

        assert!(
            matches!(
                result,
                OperationResult::Complete(QueueResponse::Peeked(Some(QueueValue::I64(2))))
            ),
            "Expected to see value 2, got: {:?}",
            result
        );
    }
}
