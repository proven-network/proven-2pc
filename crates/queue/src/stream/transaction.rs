//! Queue transaction handling
//!
//! Manages transactional operations on queues with eager locking and MVCC.

use crate::storage::{LockAttemptResult, LockManager, LockMode, MvccStorage};
use crate::stream::{QueueOperation, QueueResponse};
use proven_hlc::HlcTimestamp;
use std::collections::HashSet;

/// Transaction error types
#[derive(Debug)]
pub enum TransactionError {
    /// Transaction not found
    NotFound(HlcTimestamp),
    /// Transaction was aborted
    Aborted(HlcTimestamp),
    /// Lock conflict with another transaction
    LockConflict {
        holder: HlcTimestamp,
        mode: LockMode,
    },
    /// Other error
    Other(String),
}

/// Transaction state for queue operations
pub struct QueueTransaction {
    /// Transaction ID
    pub tx_id: HlcTimestamp,

    /// Locks acquired by this transaction
    pub acquired_locks: HashSet<String>,

    /// Whether this transaction has been aborted
    pub aborted: bool,
}

impl QueueTransaction {
    /// Create a new transaction
    pub fn new(tx_id: HlcTimestamp) -> Self {
        Self {
            tx_id,
            acquired_locks: HashSet::new(),
            aborted: false,
        }
    }

    /// Mark transaction as aborted
    pub fn abort(&mut self) {
        self.aborted = true;
    }
}

/// Main queue transaction coordinator
pub struct QueueTransactionManager {
    /// MVCC storage for queue data
    storage: MvccStorage,

    /// Lock manager for concurrency control
    lock_manager: LockManager,

    /// Active transactions
    transactions: std::collections::HashMap<HlcTimestamp, QueueTransaction>,
}

impl QueueTransactionManager {
    /// Create a new transaction manager
    pub fn new() -> Self {
        Self {
            storage: MvccStorage::new(),
            lock_manager: LockManager::new(),
            transactions: std::collections::HashMap::new(),
        }
    }

    /// Begin a new transaction
    pub fn begin_transaction(&mut self, tx_id: HlcTimestamp, start_time: HlcTimestamp) {
        self.storage.register_transaction(tx_id, start_time);
        self.transactions
            .insert(tx_id, QueueTransaction::new(tx_id));
    }

    /// Execute a queue operation within a transaction
    pub fn execute_operation(
        &mut self,
        tx_id: HlcTimestamp,
        operation: &QueueOperation,
        timestamp: HlcTimestamp,
    ) -> Result<QueueResponse, TransactionError> {
        // Check if transaction exists and is not aborted
        let tx = self
            .transactions
            .get(&tx_id)
            .ok_or(TransactionError::NotFound(tx_id))?;

        if tx.aborted {
            return Err(TransactionError::Aborted(tx_id));
        }

        // Determine required lock mode
        let (queue_name, lock_mode) = match operation {
            QueueOperation::Enqueue { queue_name, .. } => (queue_name.clone(), LockMode::Exclusive),
            QueueOperation::Dequeue { queue_name } => (queue_name.clone(), LockMode::Exclusive),
            QueueOperation::Clear { queue_name } => (queue_name.clone(), LockMode::Exclusive),
            QueueOperation::Peek { queue_name } => (queue_name.clone(), LockMode::Shared),
            QueueOperation::Size { queue_name } => (queue_name.clone(), LockMode::Shared),
            QueueOperation::IsEmpty { queue_name } => (queue_name.clone(), LockMode::Shared),
        };

        // Try to acquire lock if not already held
        if !self
            .transactions
            .get(&tx_id)
            .unwrap()
            .acquired_locks
            .contains(&queue_name)
        {
            match self.lock_manager.check(tx_id, &queue_name, lock_mode) {
                LockAttemptResult::WouldGrant => {
                    self.lock_manager
                        .grant(tx_id, queue_name.clone(), lock_mode);
                    self.transactions
                        .get_mut(&tx_id)
                        .unwrap()
                        .acquired_locks
                        .insert(queue_name.clone());
                }
                LockAttemptResult::Conflict { holder, mode } => {
                    return Err(TransactionError::LockConflict { holder, mode });
                }
            }
        }

        // Execute the operation
        match operation {
            QueueOperation::Enqueue { queue_name, value } => {
                self.storage
                    .enqueue(queue_name.clone(), value.clone(), tx_id, timestamp);
                Ok(QueueResponse::Enqueued)
            }
            QueueOperation::Dequeue { queue_name } => {
                let value = self.storage.dequeue(queue_name, tx_id);
                Ok(QueueResponse::Dequeued(value))
            }
            QueueOperation::Peek { queue_name } => {
                let value = self
                    .storage
                    .peek(queue_name, tx_id)
                    .map(|arc| (*arc).clone());
                Ok(QueueResponse::Peeked(value))
            }
            QueueOperation::Size { queue_name } => {
                let size = self.storage.size(queue_name, tx_id);
                Ok(QueueResponse::Size(size))
            }
            QueueOperation::IsEmpty { queue_name } => {
                let is_empty = self.storage.is_empty(queue_name, tx_id);
                Ok(QueueResponse::IsEmpty(is_empty))
            }
            QueueOperation::Clear { queue_name } => {
                self.storage.clear(queue_name, tx_id);
                Ok(QueueResponse::Cleared)
            }
        }
    }

    /// Prepare a transaction - releases read locks
    pub fn prepare_transaction(&mut self, tx_id: HlcTimestamp) -> Result<(), String> {
        let tx = self
            .transactions
            .get_mut(&tx_id)
            .ok_or_else(|| format!("Transaction {} not found", tx_id))?;

        if tx.aborted {
            return Err(format!("Cannot prepare aborted transaction {}", tx_id));
        }

        // Get all locks held by this transaction
        let held_locks = self.lock_manager.locks_held_by(tx_id);

        // Release only the read locks
        for (queue_name, mode) in held_locks {
            if mode == LockMode::Shared {
                self.lock_manager.release(tx_id, &queue_name);
                // Remove from transaction's acquired locks
                tx.acquired_locks.remove(&queue_name);
            }
        }

        Ok(())
    }

    /// Commit a transaction
    pub fn commit_transaction(&mut self, tx_id: HlcTimestamp) -> Result<(), String> {
        let tx = self
            .transactions
            .remove(&tx_id)
            .ok_or_else(|| format!("Transaction {} not found", tx_id))?;

        if tx.aborted {
            return Err(format!("Cannot commit aborted transaction {}", tx_id));
        }

        // Release all locks
        self.lock_manager.release_all(tx_id);

        // Commit storage changes
        self.storage.commit_transaction(tx_id);

        Ok(())
    }

    /// Abort a transaction
    pub fn abort_transaction(&mut self, tx_id: HlcTimestamp) -> Result<(), String> {
        let mut tx = self
            .transactions
            .remove(&tx_id)
            .ok_or_else(|| format!("Transaction {} not found", tx_id))?;

        tx.abort();

        // Release all locks
        self.lock_manager.release_all(tx_id);

        // Rollback storage changes
        self.storage.abort_transaction(tx_id);

        Ok(())
    }

    /// Check if a transaction holds any locks
    pub fn has_locks(&self, tx_id: HlcTimestamp) -> bool {
        self.lock_manager.has_locks(tx_id)
    }

    /// Get storage statistics
    pub fn stats(&self) -> crate::storage::StorageStats {
        self.storage.stats()
    }

    /// Get compacted data for snapshots
    pub fn get_compacted_data(
        &self,
    ) -> std::collections::HashMap<
        String,
        std::collections::VecDeque<crate::storage::mvcc::QueueEntry>,
    > {
        self.storage.get_compacted_data()
    }

    /// Restore from compacted data
    pub fn restore_from_compacted(
        &mut self,
        data: std::collections::HashMap<
            String,
            std::collections::VecDeque<crate::storage::mvcc::QueueEntry>,
        >,
    ) {
        self.storage.restore_from_compacted(data);
    }
}

impl Default for QueueTransactionManager {
    fn default() -> Self {
        Self::new()
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
    fn test_transaction_enqueue_dequeue() {
        let mut manager = QueueTransactionManager::new();
        let tx1 = create_timestamp(100);

        manager.begin_transaction(tx1, tx1);

        // Enqueue operation
        let enqueue_op = QueueOperation::Enqueue {
            queue_name: "test_queue".to_string(),
            value: QueueValue::String("test_value".to_string()),
        };

        let result = manager.execute_operation(tx1, &enqueue_op, tx1);
        assert!(matches!(result, Ok(QueueResponse::Enqueued)));

        // Dequeue operation
        let dequeue_op = QueueOperation::Dequeue {
            queue_name: "test_queue".to_string(),
        };

        let result = manager.execute_operation(tx1, &dequeue_op, tx1);
        assert!(matches!(
            result,
            Ok(QueueResponse::Dequeued(Some(QueueValue::String(s)))) if s == "test_value"
        ));

        // Commit
        assert!(manager.commit_transaction(tx1).is_ok());
    }

    #[test]
    fn test_lock_conflict() {
        let mut manager = QueueTransactionManager::new();
        let tx1 = create_timestamp(100);
        let tx2 = create_timestamp(200);

        manager.begin_transaction(tx1, tx1);
        manager.begin_transaction(tx2, tx2);

        // tx1 acquires exclusive lock
        let enqueue_op = QueueOperation::Enqueue {
            queue_name: "test_queue".to_string(),
            value: QueueValue::String("value1".to_string()),
        };

        assert!(manager.execute_operation(tx1, &enqueue_op, tx1).is_ok());

        // tx2 should fail to acquire conflicting lock
        let dequeue_op = QueueOperation::Dequeue {
            queue_name: "test_queue".to_string(),
        };

        let result = manager.execute_operation(tx2, &dequeue_op, tx2);
        assert!(matches!(result, Err(TransactionError::LockConflict { .. })));
    }

    #[test]
    fn test_shared_locks() {
        let mut manager = QueueTransactionManager::new();
        let tx1 = create_timestamp(100);
        let tx2 = create_timestamp(200);

        manager.begin_transaction(tx1, tx1);
        manager.begin_transaction(tx2, tx2);

        // Both transactions should be able to acquire shared locks
        let peek_op = QueueOperation::Peek {
            queue_name: "test_queue".to_string(),
        };

        assert!(manager.execute_operation(tx1, &peek_op, tx1).is_ok());
        assert!(manager.execute_operation(tx2, &peek_op, tx2).is_ok());
    }

    #[test]
    fn test_abort_transaction() {
        let mut manager = QueueTransactionManager::new();
        let tx1 = create_timestamp(100);

        manager.begin_transaction(tx1, tx1);

        // Execute some operations
        let enqueue_op = QueueOperation::Enqueue {
            queue_name: "test_queue".to_string(),
            value: QueueValue::String("value1".to_string()),
        };

        assert!(manager.execute_operation(tx1, &enqueue_op, tx1).is_ok());

        // Abort the transaction
        assert!(manager.abort_transaction(tx1).is_ok());

        // Start new transaction - should not see aborted changes
        let tx2 = create_timestamp(200);
        manager.begin_transaction(tx2, tx2);

        let size_op = QueueOperation::Size {
            queue_name: "test_queue".to_string(),
        };

        let result = manager.execute_operation(tx2, &size_op, tx2);
        assert!(matches!(result, Ok(QueueResponse::Size(0))));
    }
}
