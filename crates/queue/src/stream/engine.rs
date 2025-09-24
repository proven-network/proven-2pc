//! Queue engine implementation for stream processing
//!
//! Integrates with the proven-stream processor to handle distributed
//! queue operations with MVCC and eager locking.

use crate::storage::{LockAttemptResult, LockManager, LockMode, MvccStorage, QueueEntry};
use crate::stream::{QueueOperation, QueueResponse};
use proven_hlc::HlcTimestamp;
use proven_stream::engine::{BlockingInfo, OperationResult, RetryOn, TransactionEngine};
use std::collections::VecDeque;

/// Queue engine that implements the TransactionEngine trait
pub struct QueueTransactionEngine {
    /// MVCC storage for queue data
    storage: MvccStorage,

    /// Lock manager for concurrency control
    lock_manager: LockManager,
}

impl QueueTransactionEngine {
    /// Create a new queue engine
    pub fn new() -> Self {
        Self {
            storage: MvccStorage::new(),
            lock_manager: LockManager::new(),
        }
    }

    /// Execute a peek operation without locking
    fn execute_peek_without_locking(
        &self,
        read_timestamp: HlcTimestamp,
    ) -> OperationResult<QueueResponse> {
        // Check for pending operations from earlier transactions
        let pending_writers = self.storage.has_pending_operations(read_timestamp);

        if !pending_writers.is_empty() {
            // There are pending operations from earlier transactions
            // We must wait to see the final queue state
            let blockers = pending_writers
                .into_iter()
                .map(|txn| BlockingInfo {
                    txn,
                    retry_on: RetryOn::CommitOrAbort,
                })
                .collect();

            return OperationResult::WouldBlock { blockers };
        }

        // No blocking operations from earlier transactions
        let value = self.storage.peek_at_timestamp(read_timestamp);
        OperationResult::Complete(QueueResponse::Peeked(value.map(|arc| (*arc).clone())))
    }

    /// Execute a size operation without locking
    fn execute_size_without_locking(
        &self,
        read_timestamp: HlcTimestamp,
    ) -> OperationResult<QueueResponse> {
        // Check for pending operations from earlier transactions
        let pending_writers = self.storage.has_pending_operations(read_timestamp);

        if !pending_writers.is_empty() {
            // There are pending operations from earlier transactions
            // We must wait to see the final queue state
            let blockers = pending_writers
                .into_iter()
                .map(|txn| BlockingInfo {
                    txn,
                    retry_on: RetryOn::CommitOrAbort,
                })
                .collect();

            return OperationResult::WouldBlock { blockers };
        }

        // No blocking operations from earlier transactions
        let size = self.storage.size_at_timestamp(read_timestamp);
        OperationResult::Complete(QueueResponse::Size(size))
    }

    /// Execute an is_empty operation without locking
    fn execute_is_empty_without_locking(
        &self,
        read_timestamp: HlcTimestamp,
    ) -> OperationResult<QueueResponse> {
        // Check for pending operations from earlier transactions
        let pending_writers = self.storage.has_pending_operations(read_timestamp);

        if !pending_writers.is_empty() {
            // There are pending operations from earlier transactions
            // We must wait to see the final queue state
            let blockers = pending_writers
                .into_iter()
                .map(|txn| BlockingInfo {
                    txn,
                    retry_on: RetryOn::CommitOrAbort,
                })
                .collect();

            return OperationResult::WouldBlock { blockers };
        }

        // No blocking operations from earlier transactions
        let is_empty = self.storage.is_empty_at_timestamp(read_timestamp);
        OperationResult::Complete(QueueResponse::IsEmpty(is_empty))
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
        read_timestamp: HlcTimestamp,
    ) -> OperationResult<Self::Response> {
        match operation {
            QueueOperation::Peek => self.execute_peek_without_locking(read_timestamp),
            QueueOperation::Size => self.execute_size_without_locking(read_timestamp),
            QueueOperation::IsEmpty => self.execute_is_empty_without_locking(read_timestamp),
            _ => panic!("Must be read-only operation"),
        }
    }

    fn begin(&mut self, txn_id: HlcTimestamp) {
        self.storage.begin_transaction(txn_id);
    }

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: HlcTimestamp,
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
            QueueOperation::Enqueue { value } => {
                self.storage.enqueue(value, txn_id, txn_id);
                OperationResult::Complete(QueueResponse::Enqueued)
            }
            QueueOperation::Dequeue => {
                let value = self.storage.dequeue(txn_id);
                OperationResult::Complete(QueueResponse::Dequeued(value))
            }
            QueueOperation::Peek => {
                let value = self.storage.peek(txn_id).map(|arc| (*arc).clone());
                OperationResult::Complete(QueueResponse::Peeked(value))
            }
            QueueOperation::Size => {
                let size = self.storage.size(txn_id);
                OperationResult::Complete(QueueResponse::Size(size))
            }
            QueueOperation::IsEmpty => {
                let is_empty = self.storage.is_empty(txn_id);
                OperationResult::Complete(QueueResponse::IsEmpty(is_empty))
            }
            QueueOperation::Clear => {
                self.storage.clear(txn_id);
                OperationResult::Complete(QueueResponse::Cleared)
            }
        }
    }

    fn prepare(&mut self, txn_id: HlcTimestamp) {
        // Get all locks held by this transaction
        let held_locks = self.lock_manager.locks_held_by(txn_id);

        // Release only the read locks
        if held_locks.contains(&LockMode::Shared) {
            self.lock_manager.release(txn_id);
        }
    }

    fn commit(&mut self, txn_id: HlcTimestamp) {
        // Release all locks
        self.lock_manager.release_all(txn_id);

        // Commit storage changes
        self.storage.commit_transaction(txn_id);
    }

    fn abort(&mut self, txn_id: HlcTimestamp) {
        // Release all locks
        self.lock_manager.release_all(txn_id);

        // Rollback storage changes
        self.storage.abort_transaction(txn_id);
    }

    fn engine_name(&self) -> &'static str {
        "queue"
    }

    fn snapshot(&self) -> Result<Vec<u8>, String> {
        // The processor ensures no active transactions before calling snapshot

        // Define the snapshot structure
        #[derive(serde::Serialize, serde::Deserialize)]
        struct QueueSnapshot {
            // Compacted queue data - only committed state
            entries: VecDeque<QueueEntry>,
        }

        // Get compacted data from storage
        let entries = self.storage.get_compacted_data();

        let snapshot = QueueSnapshot { entries };

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
        struct QueueSnapshot {
            entries: VecDeque<QueueEntry>,
        }

        // Deserialize snapshot
        let snapshot: QueueSnapshot = ciborium::from_reader(&decompressed[..])
            .map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;

        // Clear existing state
        self.storage = MvccStorage::new();
        self.lock_manager = LockManager::new();

        // Restore compacted data
        self.storage.restore_from_compacted(snapshot.entries);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::QueueValue;
    use proven_hlc::NodeId;

    fn create_timestamp(seconds: u64) -> HlcTimestamp {
        HlcTimestamp::new(seconds, 0, NodeId::new(1))
    }

    #[test]
    fn test_engine_basic_operations() {
        let mut engine = QueueTransactionEngine::new();
        let tx1 = create_timestamp(100);

        engine.begin(tx1);

        // Test enqueue
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::Integer(42),
        };

        let result = engine.apply_operation(enqueue_op, tx1);
        assert!(matches!(
            result,
            OperationResult::Complete(QueueResponse::Enqueued)
        ));

        // Test peek
        let peek_op = QueueOperation::Peek;

        let result = engine.apply_operation(peek_op, tx1);
        assert!(matches!(
            result,
            OperationResult::Complete(QueueResponse::Peeked(Some(QueueValue::Integer(42))))
        ));

        // Test commit
        engine.commit(tx1);
    }

    #[test]
    fn test_engine_blocking() {
        let mut engine = QueueTransactionEngine::new();
        let tx1 = create_timestamp(100);
        let tx2 = create_timestamp(200);

        engine.begin(tx1);
        engine.begin(tx2);

        // tx1 gets exclusive lock
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::String("tx1".to_string()),
        };

        let result = engine.apply_operation(enqueue_op, tx1);
        assert!(matches!(result, OperationResult::Complete(_)));

        // tx2 should be blocked
        let dequeue_op = QueueOperation::Dequeue;

        let result = engine.apply_operation(dequeue_op, tx2);
        assert!(matches!(result, OperationResult::WouldBlock { .. }));
    }

    #[test]
    fn test_engine_abort() {
        let mut engine = QueueTransactionEngine::new();
        let tx1 = create_timestamp(100);

        engine.begin(tx1);

        // Execute operation
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::Boolean(true),
        };

        engine.apply_operation(enqueue_op, tx1);

        // Abort
        engine.abort(tx1);
    }
}
