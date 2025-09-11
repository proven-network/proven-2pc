//! Queue engine implementation for stream processing
//!
//! Integrates with the proven-stream processor to handle distributed
//! queue operations with MVCC and eager locking.

use crate::storage::lock::LockMode;
use crate::storage::mvcc::QueueEntry;
use crate::stream::transaction::QueueTransactionManager;
use crate::stream::{QueueOperation, QueueResponse};
use proven_hlc::HlcTimestamp;
use proven_stream::engine::{OperationResult, RetryOn, TransactionEngine};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

/// Queue engine that implements the TransactionEngine trait
pub struct QueueTransactionEngine {
    /// Transaction manager handles MVCC and locking
    manager: Arc<Mutex<QueueTransactionManager>>,
    /// Track active transactions
    active_transactions: Arc<Mutex<HashSet<HlcTimestamp>>>,
}

impl QueueTransactionEngine {
    /// Create a new queue engine
    pub fn new() -> Self {
        Self {
            manager: Arc::new(Mutex::new(QueueTransactionManager::new())),
            active_transactions: Arc::new(Mutex::new(HashSet::new())),
        }
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

    fn begin_transaction(&mut self, txn_id: HlcTimestamp) {
        let mut manager = self.manager.lock().unwrap();
        manager.begin_transaction(txn_id, txn_id);
        self.active_transactions.lock().unwrap().insert(txn_id);
    }

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: HlcTimestamp,
    ) -> OperationResult<Self::Response> {
        let mut manager = self.manager.lock().unwrap();

        match manager.execute_operation(txn_id, &operation, txn_id) {
            Ok(response) => OperationResult::Success(response),
            Err(crate::stream::transaction::TransactionError::LockConflict { holder, mode }) => {
                // Check what operation we're trying to do
                let our_mode = match &operation {
                    QueueOperation::Enqueue { .. }
                    | QueueOperation::Dequeue { .. }
                    | QueueOperation::Clear { .. } => LockMode::Exclusive,
                    QueueOperation::Peek { .. }
                    | QueueOperation::Size { .. }
                    | QueueOperation::IsEmpty { .. } => LockMode::Shared,
                };

                // Determine retry timing based on conflict type
                let retry_on = if our_mode == LockMode::Exclusive && mode == LockMode::Shared {
                    // Write blocked by read - can retry after prepare
                    RetryOn::Prepare
                } else {
                    // All other conflicts need commit/abort
                    RetryOn::CommitOrAbort
                };

                OperationResult::WouldBlock {
                    blocking_txn: holder,
                    retry_on,
                }
            }
            Err(err) => OperationResult::Error(format!("{:?}", err)),
        }
    }

    fn prepare(&mut self, txn_id: HlcTimestamp) -> Result<(), String> {
        if !self.is_transaction_active(&txn_id) {
            return Err(format!("Transaction {} not active", txn_id));
        }

        // Release read locks on prepare
        let mut manager = self.manager.lock().unwrap();
        manager.prepare_transaction(txn_id)?;
        Ok(())
    }

    fn commit(&mut self, txn_id: HlcTimestamp) -> Result<(), String> {
        let mut manager = self.manager.lock().unwrap();
        let result = manager.commit_transaction(txn_id);
        self.active_transactions.lock().unwrap().remove(&txn_id);
        result
    }

    fn abort(&mut self, txn_id: HlcTimestamp) -> Result<(), String> {
        let mut manager = self.manager.lock().unwrap();
        let result = manager.abort_transaction(txn_id);
        self.active_transactions.lock().unwrap().remove(&txn_id);
        result
    }

    fn is_transaction_active(&self, txn_id: &HlcTimestamp) -> bool {
        self.active_transactions.lock().unwrap().contains(txn_id)
    }

    fn engine_name(&self) -> &str {
        "queue"
    }

    fn snapshot(&self) -> Result<Vec<u8>, String> {
        // Only snapshot when no active transactions
        if !self.active_transactions.lock().unwrap().is_empty() {
            return Err("Cannot snapshot with active transactions".to_string());
        }

        // Define the snapshot structure
        #[derive(serde::Serialize, serde::Deserialize)]
        struct QueueSnapshot {
            // Compacted queue data - only committed state
            queues: HashMap<String, VecDeque<QueueEntry>>,
        }

        // Get compacted data from the transaction manager
        let manager = self.manager.lock().unwrap();
        let compacted = manager.get_compacted_data();

        let snapshot = QueueSnapshot { queues: compacted };

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
            queues: HashMap<String, VecDeque<QueueEntry>>,
        }

        // Deserialize snapshot
        let snapshot: QueueSnapshot = ciborium::from_reader(&decompressed[..])
            .map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;

        // Create a fresh transaction manager and restore data
        let mut new_manager = QueueTransactionManager::new();
        new_manager.restore_from_compacted(snapshot.queues);

        // Replace the current manager
        *self.manager.lock().unwrap() = new_manager;

        // Clear active transactions
        self.active_transactions.lock().unwrap().clear();

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

        engine.begin_transaction(tx1);

        // Test enqueue
        let enqueue_op = QueueOperation::Enqueue {
            queue_name: "test_queue".to_string(),
            value: QueueValue::Integer(42),
        };

        let result = engine.apply_operation(enqueue_op, tx1);
        assert!(matches!(
            result,
            OperationResult::Success(QueueResponse::Enqueued)
        ));

        // Test peek
        let peek_op = QueueOperation::Peek {
            queue_name: "test_queue".to_string(),
        };

        let result = engine.apply_operation(peek_op, tx1);
        assert!(matches!(
            result,
            OperationResult::Success(QueueResponse::Peeked(Some(QueueValue::Integer(42))))
        ));

        // Test commit
        assert!(engine.commit(tx1).is_ok());
    }

    #[test]
    fn test_engine_blocking() {
        let mut engine = QueueTransactionEngine::new();
        let tx1 = create_timestamp(100);
        let tx2 = create_timestamp(200);

        engine.begin_transaction(tx1);
        engine.begin_transaction(tx2);

        // tx1 gets exclusive lock
        let enqueue_op = QueueOperation::Enqueue {
            queue_name: "test_queue".to_string(),
            value: QueueValue::String("tx1".to_string()),
        };

        let result = engine.apply_operation(enqueue_op, tx1);
        assert!(matches!(result, OperationResult::Success(_)));

        // tx2 should be blocked
        let dequeue_op = QueueOperation::Dequeue {
            queue_name: "test_queue".to_string(),
        };

        let result = engine.apply_operation(dequeue_op, tx2);
        assert!(matches!(result, OperationResult::WouldBlock { .. }));
    }

    #[test]
    fn test_engine_abort() {
        let mut engine = QueueTransactionEngine::new();
        let tx1 = create_timestamp(100);

        engine.begin_transaction(tx1);

        // Execute operation
        let enqueue_op = QueueOperation::Enqueue {
            queue_name: "test_queue".to_string(),
            value: QueueValue::Boolean(true),
        };

        engine.apply_operation(enqueue_op, tx1);
        assert!(engine.is_transaction_active(&tx1));

        // Abort
        assert!(engine.abort(tx1).is_ok());
        assert!(!engine.is_transaction_active(&tx1));
    }
}
