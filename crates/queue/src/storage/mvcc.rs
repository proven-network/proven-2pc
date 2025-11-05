//! Multi-Version Concurrency Control (MVCC) for Queue storage
//!
//! Provides transaction isolation through operation logging instead of full queue cloning,
//! enabling quick aborts and consistent reads with minimal memory overhead.

use proven_common::TransactionId;
use proven_value::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// A single entry in the queue
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueueEntry {
    /// The actual value (wrapped in Arc for cheap clones)
    pub value: Arc<Value>,
    /// Transaction that created this entry
    pub created_by: TransactionId,
    /// When this entry was created
    pub created_at: TransactionId,
    /// Unique ID for this entry (for ordering)
    pub entry_id: u64,
}

/// Operations that can be performed on a queue
#[derive(Debug, Clone)]
enum QueueOp {
    /// Enqueue an entry to the back
    Enqueue { entry: Arc<QueueEntry> },
    /// Dequeue from the front (stores the dequeued entry for reconstruction)
    Dequeue,
    /// Clear all entries (stores cleared entries for reconstruction)
    Clear,
}

/// A committed operation with its timestamp
#[derive(Debug, Clone)]
struct CommittedOp {
    /// The operation
    op: QueueOp,
    /// When this operation was committed
    committed_at: TransactionId,
}

/// MVCC storage for a single queue using operation logging
#[derive(Debug)]
pub struct MvccStorage {
    /// Committed operations (ordered by commit time)
    /// Used to reconstruct queue state at any timestamp
    committed_operations: Vec<CommittedOp>,

    /// Current committed state of the queue (optimization for current reads)
    committed_queue: VecDeque<Arc<QueueEntry>>,

    /// Next entry ID counter
    next_entry_id: u64,

    /// Pending operations by transaction (not yet committed)
    /// Maps transaction ID -> list of operations
    pending_operations: HashMap<TransactionId, Vec<QueueOp>>,
}

impl MvccStorage {
    /// Create a new MVCC storage
    pub fn new() -> Self {
        Self {
            committed_operations: Vec::new(),
            committed_queue: VecDeque::new(),
            next_entry_id: 1,
            pending_operations: HashMap::new(),
        }
    }

    /// Begin a new transaction
    pub fn begin_transaction(&mut self, tx_id: TransactionId) {
        self.pending_operations.insert(tx_id, Vec::new());
    }

    /// Mark a transaction as committed
    pub fn commit_transaction(&mut self, tx_id: TransactionId) {
        // Move pending operations to committed with timestamp
        if let Some(ops) = self.pending_operations.remove(&tx_id) {
            for op in ops {
                // Record the committed operation with timestamp
                let committed_op = CommittedOp {
                    op: op.clone(),
                    committed_at: tx_id, // Using tx_id as commit timestamp
                };
                self.committed_operations.push(committed_op);

                // Apply to committed state
                match op {
                    QueueOp::Enqueue { entry } => {
                        self.committed_queue.push_back(entry);
                    }
                    QueueOp::Dequeue => {
                        // Now remove from committed_queue during commit
                        self.committed_queue.pop_front();
                    }
                    QueueOp::Clear => {
                        self.committed_queue.clear();
                    }
                }
            }
        }

        // Clean up transaction data
    }

    /// Abort a transaction by discarding all its operations
    pub fn abort_transaction(&mut self, tx_id: TransactionId) {
        // Simply discard all pending operations
        self.pending_operations.remove(&tx_id);
    }

    /// Get a materialized view of the queue for a transaction
    fn get_queue_view(&self, tx_id: TransactionId) -> VecDeque<Arc<QueueEntry>> {
        // Start with committed queue
        let mut view = self.committed_queue.clone();

        // Apply transaction's pending operations if any
        if let Some(ops) = self.pending_operations.get(&tx_id) {
            for op in ops {
                match op {
                    QueueOp::Enqueue { entry } => {
                        view.push_back(entry.clone());
                    }
                    QueueOp::Dequeue => {
                        // Remove from front for each dequeue operation
                        view.pop_front();
                    }
                    QueueOp::Clear => {
                        view.clear();
                    }
                }
            }
        }

        view
    }

    /// Reconstruct queue state at a specific timestamp (for snapshot reads)
    fn get_queue_at_timestamp(&self, read_timestamp: TransactionId) -> VecDeque<Arc<QueueEntry>> {
        let mut queue = VecDeque::new();

        // Apply all committed operations up to the read timestamp
        for committed_op in &self.committed_operations {
            // Only include operations committed before or at the read timestamp
            if committed_op.committed_at <= read_timestamp {
                match &committed_op.op {
                    QueueOp::Enqueue { entry } => {
                        queue.push_back(entry.clone());
                    }
                    QueueOp::Dequeue => {
                        queue.pop_front();
                    }
                    QueueOp::Clear => {
                        queue.clear();
                    }
                }
            }
        }

        queue
    }

    /// Check if there are pending operations from transactions that would affect a read
    pub fn has_pending_operations(&self, before_timestamp: TransactionId) -> Vec<TransactionId> {
        let mut pending_txns = Vec::new();

        for (tx_id, ops) in &self.pending_operations {
            // Only consider transactions that started before our read timestamp
            // and have pending operations
            if *tx_id < before_timestamp && !ops.is_empty() {
                pending_txns.push(*tx_id);
            }
        }

        pending_txns
    }

    /// Get next entry ID
    fn get_next_entry_id(&mut self) -> u64 {
        let id = self.next_entry_id;
        self.next_entry_id += 1;
        id
    }

    /// Enqueue a value to the back of the queue
    pub fn enqueue(&mut self, value: Value, tx_id: TransactionId, timestamp: TransactionId) {
        let entry_id = self.get_next_entry_id();

        let entry = Arc::new(QueueEntry {
            value: Arc::new(value),
            created_by: tx_id,
            created_at: timestamp,
            entry_id,
        });

        // Record the operation
        self.pending_operations
            .entry(tx_id)
            .or_default()
            .push(QueueOp::Enqueue { entry });
    }

    /// Dequeue a value from the front of the queue
    pub fn dequeue(&mut self, tx_id: TransactionId) -> Option<Value> {
        // Get a view with all operations applied
        let view = self.get_queue_view(tx_id);

        // Check if there's anything to dequeue
        if let Some(entry) = view.front() {
            // Record dequeue operation
            self.pending_operations
                .entry(tx_id)
                .or_default()
                .push(QueueOp::Dequeue);

            return Some((*entry.value).clone());
        }

        None
    }

    /// Peek at the front value without removing it
    pub fn peek(&self, tx_id: TransactionId) -> Option<Arc<Value>> {
        let view = self.get_queue_view(tx_id);
        view.front().map(|entry| entry.value.clone())
    }

    /// Get the size of the queue
    pub fn size(&self, tx_id: TransactionId) -> usize {
        self.get_queue_view(tx_id).len()
    }

    /// Check if the queue is empty
    pub fn is_empty(&self, tx_id: TransactionId) -> bool {
        self.size(tx_id) == 0
    }

    /// Peek at the front value at a specific timestamp (snapshot read)
    pub fn peek_at_timestamp(&self, read_timestamp: TransactionId) -> Option<Arc<Value>> {
        let queue = self.get_queue_at_timestamp(read_timestamp);
        queue.front().map(|entry| entry.value.clone())
    }

    /// Get the size of the queue at a specific timestamp (snapshot read)
    pub fn size_at_timestamp(&self, read_timestamp: TransactionId) -> usize {
        self.get_queue_at_timestamp(read_timestamp).len()
    }

    /// Check if the queue is empty at a specific timestamp (snapshot read)
    pub fn is_empty_at_timestamp(&self, read_timestamp: TransactionId) -> bool {
        self.get_queue_at_timestamp(read_timestamp).is_empty()
    }

    /// Clear all values from the queue
    pub fn clear(&mut self, tx_id: TransactionId) {
        // Just record the clear operation
        self.pending_operations
            .entry(tx_id)
            .or_default()
            .push(QueueOp::Clear);
    }

    /// Get statistics about the storage
    pub fn stats(&self) -> StorageStats {
        let total_entries = self.committed_queue.len();
        let committed_txns = self.committed_operations.len();
        let active_txns = self.pending_operations.len();

        StorageStats {
            total_entries,
            committed_txns,
            active_txns,
        }
    }

    /// Get a compacted view of the storage (only committed state)
    /// Used for creating snapshots when no transactions are active
    pub fn get_compacted_data(&self) -> VecDeque<QueueEntry> {
        // Convert Arc<QueueEntry> back to QueueEntry for serialization
        self.committed_queue
            .iter()
            .map(|arc_entry| QueueEntry {
                value: arc_entry.value.clone(),
                created_by: arc_entry.created_by,
                created_at: arc_entry.created_at,
                entry_id: arc_entry.entry_id,
            })
            .collect()
    }

    /// Restore from compacted data
    /// Should only be called on a fresh MVCC storage instance
    pub fn restore_from_compacted(&mut self, data: VecDeque<QueueEntry>) {
        // Clear any existing data
        self.committed_queue.clear();
        self.committed_operations.clear();
        self.pending_operations.clear();

        // Find the max entry_id to set next_entry_id correctly
        let max_id = data.iter().map(|e| e.entry_id).max().unwrap_or(0);
        self.next_entry_id = max_id + 1;

        // Convert QueueEntry to Arc<QueueEntry> for storage
        self.committed_queue = data.into_iter().map(Arc::new).collect();
    }
}

impl Default for MvccStorage {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the MVCC storage
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub total_entries: usize,
    pub committed_txns: usize,
    pub active_txns: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_timestamp() -> TransactionId {
        TransactionId::new()
    }

    #[test]
    fn test_basic_enqueue_dequeue() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp();

        storage.begin_transaction(tx1);

        // Enqueue some values
        storage.enqueue(Value::Str("first".to_string()), tx1, tx1);
        storage.enqueue(Value::Str("second".to_string()), tx1, create_timestamp());

        // Should be able to peek without removing
        assert_eq!(
            storage.peek(tx1).map(|arc| (*arc).clone()),
            Some(Value::Str("first".to_string()))
        );

        // Dequeue should return FIFO order
        assert_eq!(storage.dequeue(tx1), Some(Value::Str("first".to_string())));
        assert_eq!(storage.dequeue(tx1), Some(Value::Str("second".to_string())));
        assert_eq!(storage.dequeue(tx1), None);
    }

    #[test]
    fn test_transaction_isolation() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp();
        let tx2 = create_timestamp();

        storage.begin_transaction(tx1);
        storage.begin_transaction(tx2);

        // tx1 enqueues some values
        storage.enqueue(Value::Str("tx1_value".to_string()), tx1, tx1);

        // tx2 shouldn't see uncommitted values
        assert_eq!(storage.size(tx2), 0);
        assert!(storage.is_empty(tx2));

        // After commit, tx2 should see the values
        storage.commit_transaction(tx1);

        assert_eq!(storage.size(tx2), 1);
        assert_eq!(
            storage.peek(tx2).map(|arc| (*arc).clone()),
            Some(Value::Str("tx1_value".to_string()))
        );
    }

    #[test]
    fn test_abort_rollback() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp();
        let tx2 = create_timestamp();

        // tx1 creates a queue and commits
        storage.begin_transaction(tx1);
        storage.enqueue(Value::Str("committed".to_string()), tx1, tx1);
        storage.commit_transaction(tx1);

        // tx2 modifies the queue but aborts
        storage.begin_transaction(tx2);
        storage.enqueue(Value::Str("aborted".to_string()), tx2, tx2);
        assert_eq!(storage.size(tx2), 2);

        // Abort tx2
        storage.abort_transaction(tx2);

        // New transaction should only see committed value
        let tx3 = create_timestamp();
        storage.begin_transaction(tx3);
        assert_eq!(storage.size(tx3), 1);
        assert_eq!(
            storage.peek(tx3).map(|arc| (*arc).clone()),
            Some(Value::Str("committed".to_string()))
        );
    }

    #[test]
    fn test_clear_operation() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp();

        storage.begin_transaction(tx1);

        // Add some values
        for i in 0..5 {
            storage.enqueue(Value::I64(i), tx1, create_timestamp());
        }

        assert_eq!(storage.size(tx1), 5);

        // Clear the queue
        storage.clear(tx1);
        assert_eq!(storage.size(tx1), 0);
        assert!(storage.is_empty(tx1));
    }
}
