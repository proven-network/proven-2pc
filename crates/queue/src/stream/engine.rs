//! Queue engine implementation for stream processing
//!
//! Integrates with the proven-stream processor to handle distributed
//! queue operations with MVCC and eager locking.

use crate::storage::lock::LockMode;
use crate::storage::mvcc::QueueEntry;
use crate::stream::transaction::QueueTransactionManager;
use crate::stream::{QueueOperation, QueueResponse};
use proven_hlc::HlcTimestamp;
use proven_stream::engine::{BlockingInfo, OperationResult, RetryOn, TransactionEngine};
use std::collections::{HashSet, VecDeque};

/// Queue engine that implements the TransactionEngine trait
pub struct QueueTransactionEngine {
    /// Transaction manager handles MVCC and locking
    manager: QueueTransactionManager,
    /// Track active transactions
    active_transactions: HashSet<HlcTimestamp>,
}

impl QueueTransactionEngine {
    /// Create a new queue engine
    pub fn new() -> Self {
        Self {
            manager: QueueTransactionManager::new(),
            active_transactions: HashSet::new(),
        }
    }

    /// Execute a peek operation without locking
    fn execute_peek_without_locking(
        &self,
        read_timestamp: HlcTimestamp,
    ) -> OperationResult<QueueResponse> {
        // Check for pending operations from earlier transactions
        let pending_writers = self.manager.get_pending_operations(read_timestamp);

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
        let value = self.manager.peek_at_timestamp(read_timestamp);
        OperationResult::Complete(QueueResponse::Peeked(value.map(|arc| (*arc).clone())))
    }

    /// Execute a size operation without locking
    fn execute_size_without_locking(
        &self,
        read_timestamp: HlcTimestamp,
    ) -> OperationResult<QueueResponse> {
        // Check for pending operations from earlier transactions
        let pending_writers = self.manager.get_pending_operations(read_timestamp);

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
        let size = self.manager.size_at_timestamp(read_timestamp);
        OperationResult::Complete(QueueResponse::Size(size))
    }

    /// Execute an is_empty operation without locking
    fn execute_is_empty_without_locking(
        &self,
        read_timestamp: HlcTimestamp,
    ) -> OperationResult<QueueResponse> {
        // Check for pending operations from earlier transactions
        let pending_writers = self.manager.get_pending_operations(read_timestamp);

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
        let is_empty = self.manager.is_empty_at_timestamp(read_timestamp);
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
        &self,
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
        self.manager.begin_transaction(txn_id, txn_id);
        self.active_transactions.insert(txn_id);
    }

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: HlcTimestamp,
    ) -> OperationResult<Self::Response> {
        match self.manager.execute_operation(txn_id, &operation, txn_id) {
            Ok(response) => OperationResult::Complete(response),
            Err(crate::stream::transaction::TransactionError::LockConflict { holder, mode }) => {
                // Check what operation we're trying to do
                let our_mode = match &operation {
                    QueueOperation::Enqueue { .. } => LockMode::Append,
                    QueueOperation::Dequeue | QueueOperation::Clear => LockMode::Exclusive,
                    QueueOperation::Peek | QueueOperation::Size | QueueOperation::IsEmpty => {
                        LockMode::Shared
                    }
                };

                // Determine retry timing based on conflict type
                let retry_on = match (our_mode, mode) {
                    // Exclusive blocked by read - can retry after prepare
                    (LockMode::Exclusive, LockMode::Shared) => RetryOn::Prepare,
                    // Append blocked by read - can continue (they're compatible!)
                    // This shouldn't happen due to compatibility check
                    (LockMode::Append, LockMode::Shared) => RetryOn::Prepare,
                    // All other conflicts need commit/abort
                    _ => RetryOn::CommitOrAbort,
                };

                OperationResult::WouldBlock {
                    blockers: vec![BlockingInfo {
                        txn: holder,
                        retry_on,
                    }],
                }
            }
            Err(err) => OperationResult::Complete(QueueResponse::Error(format!("{:?}", err))),
        }
    }

    fn prepare(&mut self, txn_id: HlcTimestamp) {
        if !self.is_transaction_active(&txn_id) {
            // If transaction doesn't exist, that's fine - it may have been aborted already
            return;
        }

        // Release read locks on prepare (ignore errors - best effort)
        let _ = self.manager.prepare_transaction(txn_id);
    }

    fn commit(&mut self, txn_id: HlcTimestamp) {
        let _ = self.manager.commit_transaction(txn_id);
        self.active_transactions.remove(&txn_id);
    }

    fn abort(&mut self, txn_id: HlcTimestamp) {
        let _ = self.manager.abort_transaction(txn_id);
        self.active_transactions.remove(&txn_id);
    }

    fn is_transaction_active(&self, txn_id: &HlcTimestamp) -> bool {
        self.active_transactions.contains(txn_id)
    }

    fn engine_name(&self) -> &'static str {
        "queue"
    }

    fn snapshot(&self) -> Result<Vec<u8>, String> {
        // Only snapshot when no active transactions
        if !self.active_transactions.is_empty() {
            return Err("Cannot snapshot with active transactions".to_string());
        }

        // Define the snapshot structure
        #[derive(serde::Serialize, serde::Deserialize)]
        struct QueueSnapshot {
            // Compacted queue data - only committed state
            entries: VecDeque<QueueEntry>,
        }

        // Get compacted data from the transaction manager
        let entries = self.manager.get_compacted_data();

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

        // Create a fresh transaction manager and restore data
        let mut new_manager = QueueTransactionManager::new();
        new_manager.restore_from_compacted(snapshot.entries);

        // Replace the current manager
        self.manager = new_manager;

        // Clear active transactions
        self.active_transactions.clear();

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
        assert!(engine.is_transaction_active(&tx1));

        // Abort
        engine.abort(tx1);
        assert!(!engine.is_transaction_active(&tx1));
    }
}
