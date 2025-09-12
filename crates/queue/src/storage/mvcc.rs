//! Multi-Version Concurrency Control (MVCC) for Queue storage
//!
//! Provides transaction isolation through operation logging instead of full queue cloning,
//! enabling quick aborts and consistent reads with minimal memory overhead.

use crate::types::QueueValue;
use proven_hlc::HlcTimestamp;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// A single entry in the queue
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueueEntry {
    /// The actual value (wrapped in Arc for cheap clones)
    pub value: Arc<QueueValue>,
    /// Transaction that created this entry
    pub created_by: HlcTimestamp,
    /// When this entry was created
    pub created_at: HlcTimestamp,
    /// Unique ID for this entry (for ordering)
    pub entry_id: u64,
}

/// Operations that can be performed on a queue
#[derive(Debug, Clone)]
enum QueueOp {
    /// Enqueue an entry to the back
    Enqueue { entry: Arc<QueueEntry> },
    /// Dequeue from the front
    Dequeue,
    /// Clear all entries
    Clear,
}

/// MVCC storage for queues using operation logging
#[derive(Debug)]
pub struct MvccStorage {
    /// Base committed state of all queues
    base_queues: HashMap<String, VecDeque<Arc<QueueEntry>>>,

    /// Next entry ID counter for each queue
    next_entry_ids: HashMap<String, u64>,

    /// Operations performed by each transaction (not yet committed)
    /// Maps transaction ID -> queue name -> list of operations
    tx_operations: HashMap<HlcTimestamp, HashMap<String, Vec<QueueOp>>>,

    /// Dequeued entries by transaction (for rollback on abort)
    tx_dequeued: HashMap<HlcTimestamp, HashMap<String, Vec<Arc<QueueEntry>>>>,

    /// Set of committed transactions
    committed_transactions: HashSet<HlcTimestamp>,

    /// Transaction start times for visibility checks
    transaction_start_times: HashMap<HlcTimestamp, HlcTimestamp>,
}

impl MvccStorage {
    /// Create a new MVCC storage
    pub fn new() -> Self {
        Self {
            base_queues: HashMap::new(),
            next_entry_ids: HashMap::new(),
            tx_operations: HashMap::new(),
            tx_dequeued: HashMap::new(),
            committed_transactions: HashSet::new(),
            transaction_start_times: HashMap::new(),
        }
    }

    /// Register a new transaction
    pub fn register_transaction(&mut self, tx_id: HlcTimestamp, start_time: HlcTimestamp) {
        self.transaction_start_times.insert(tx_id, start_time);
        self.tx_operations.insert(tx_id, HashMap::new());
        self.tx_dequeued.insert(tx_id, HashMap::new());
    }

    /// Mark a transaction as committed
    pub fn commit_transaction(&mut self, tx_id: HlcTimestamp) {
        // Apply all operations from this transaction to base state
        if let Some(tx_ops) = self.tx_operations.remove(&tx_id) {
            for (queue_name, ops) in tx_ops {
                let base_queue = self.base_queues.entry(queue_name.clone()).or_default();

                for op in ops {
                    match op {
                        QueueOp::Enqueue { entry } => {
                            base_queue.push_back(entry);
                        }
                        QueueOp::Dequeue => {
                            // Already removed from base during operation
                        }
                        QueueOp::Clear => {
                            base_queue.clear();
                        }
                    }
                }
            }
        }

        // Clean up transaction data
        self.tx_dequeued.remove(&tx_id);
        self.committed_transactions.insert(tx_id);
        self.transaction_start_times.remove(&tx_id);
    }

    /// Abort a transaction by discarding all its operations
    pub fn abort_transaction(&mut self, tx_id: HlcTimestamp) {
        // Restore any dequeued entries back to their queues
        if let Some(dequeued_by_queue) = self.tx_dequeued.remove(&tx_id) {
            for (queue_name, entries) in dequeued_by_queue {
                let base_queue = self.base_queues.entry(queue_name).or_default();
                // Re-insert at front in reverse order to maintain original order
                for entry in entries.into_iter().rev() {
                    base_queue.push_front(entry);
                }
            }
        }

        // Discard all operations
        self.tx_operations.remove(&tx_id);
        self.transaction_start_times.remove(&tx_id);
    }

    /// Get a materialized view of a queue for a transaction
    fn get_queue_view(&self, queue_name: &str, tx_id: HlcTimestamp) -> VecDeque<Arc<QueueEntry>> {
        // Start with base queue
        let mut view = self
            .base_queues
            .get(queue_name)
            .cloned()
            .unwrap_or_default();

        // Apply transaction's operations if any
        if let Some(tx_ops) = self.tx_operations.get(&tx_id)
            && let Some(ops) = tx_ops.get(queue_name)
        {
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

    /// Get next entry ID for a queue
    fn get_next_entry_id(&mut self, queue_name: &str) -> u64 {
        let entry = self
            .next_entry_ids
            .entry(queue_name.to_string())
            .or_insert(1);
        let id = *entry;
        *entry += 1;
        id
    }

    /// Enqueue a value to the back of a queue
    pub fn enqueue(
        &mut self,
        queue_name: String,
        value: QueueValue,
        tx_id: HlcTimestamp,
        timestamp: HlcTimestamp,
    ) {
        let entry_id = self.get_next_entry_id(&queue_name);

        let entry = Arc::new(QueueEntry {
            value: Arc::new(value),
            created_by: tx_id,
            created_at: timestamp,
            entry_id,
        });

        // Record the operation
        self.tx_operations
            .entry(tx_id)
            .or_default()
            .entry(queue_name)
            .or_default()
            .push(QueueOp::Enqueue { entry });
    }

    /// Dequeue a value from the front of a queue
    pub fn dequeue(&mut self, queue_name: &str, tx_id: HlcTimestamp) -> Option<QueueValue> {
        // Get a view with all operations applied
        let mut view = self.get_queue_view(queue_name, tx_id);

        // Check if there's anything to dequeue
        if let Some(entry) = view.pop_front() {
            // Determine if this entry came from base or from our transaction
            let from_base = self
                .base_queues
                .get(queue_name)
                .and_then(|q| q.front())
                .map(|e| e.entry_id == entry.entry_id)
                .unwrap_or(false);

            if from_base {
                // Remove from base queue and record for rollback
                if let Some(base_queue) = self.base_queues.get_mut(queue_name)
                    && let Some(base_entry) = base_queue.pop_front()
                {
                    self.tx_dequeued
                        .entry(tx_id)
                        .or_default()
                        .entry(queue_name.to_string())
                        .or_default()
                        .push(base_entry);
                }
            } else {
                // Only record dequeue operation for items from our own transaction
                // Base queue items are already removed, so we don't need to record the operation
                self.tx_operations
                    .entry(tx_id)
                    .or_default()
                    .entry(queue_name.to_string())
                    .or_default()
                    .push(QueueOp::Dequeue);
            }

            return Some((*entry.value).clone());
        }

        None
    }

    /// Peek at the front value without removing it
    pub fn peek(&self, queue_name: &str, tx_id: HlcTimestamp) -> Option<Arc<QueueValue>> {
        let view = self.get_queue_view(queue_name, tx_id);
        view.front().map(|entry| entry.value.clone())
    }

    /// Get the size of a queue
    pub fn size(&self, queue_name: &str, tx_id: HlcTimestamp) -> usize {
        self.get_queue_view(queue_name, tx_id).len()
    }

    /// Check if a queue is empty
    pub fn is_empty(&self, queue_name: &str, tx_id: HlcTimestamp) -> bool {
        self.size(queue_name, tx_id) == 0
    }

    /// Clear all values from a queue
    pub fn clear(&mut self, queue_name: &str, tx_id: HlcTimestamp) {
        // For clear, we need to track what was in the base queue for rollback
        if let Some(base_queue) = self.base_queues.get_mut(queue_name) {
            // Move all base entries to dequeued for potential rollback
            let entries: Vec<Arc<QueueEntry>> = base_queue.drain(..).collect();
            if !entries.is_empty() {
                self.tx_dequeued
                    .entry(tx_id)
                    .or_default()
                    .entry(queue_name.to_string())
                    .or_default()
                    .extend(entries);
            }
        }

        // Record the clear operation
        self.tx_operations
            .entry(tx_id)
            .or_default()
            .entry(queue_name.to_string())
            .or_default()
            .push(QueueOp::Clear);
    }

    /// Get all queue names visible to a transaction
    pub fn list_queues(&self, tx_id: HlcTimestamp) -> Vec<String> {
        let mut queues = HashSet::new();

        // Add base queues
        for queue_name in self.base_queues.keys() {
            queues.insert(queue_name.clone());
        }

        // Add queues modified by this transaction
        if let Some(tx_ops) = self.tx_operations.get(&tx_id) {
            for queue_name in tx_ops.keys() {
                queues.insert(queue_name.clone());
            }
        }

        let mut result: Vec<String> = queues.into_iter().collect();
        result.sort();
        result
    }

    /// Get statistics about the storage
    pub fn stats(&self) -> StorageStats {
        let total_queues = self.base_queues.len();
        let total_entries: usize = self.base_queues.values().map(|q| q.len()).sum();
        let committed_txns = self.committed_transactions.len();
        let active_txns = self.transaction_start_times.len();

        StorageStats {
            total_queues,
            total_entries,
            committed_txns,
            active_txns,
        }
    }

    /// Get a compacted view of the storage (only committed state)
    /// Used for creating snapshots when no transactions are active
    pub fn get_compacted_data(&self) -> HashMap<String, VecDeque<QueueEntry>> {
        let mut result = HashMap::new();

        // Convert Arc<QueueEntry> back to QueueEntry for serialization
        for (queue_name, queue) in &self.base_queues {
            if !queue.is_empty() {
                let entries: VecDeque<QueueEntry> = queue
                    .iter()
                    .map(|arc_entry| QueueEntry {
                        value: arc_entry.value.clone(),
                        created_by: arc_entry.created_by,
                        created_at: arc_entry.created_at,
                        entry_id: arc_entry.entry_id,
                    })
                    .collect();
                result.insert(queue_name.clone(), entries);
            }
        }

        result
    }

    /// Restore from compacted data
    /// Should only be called on a fresh MVCC storage instance
    pub fn restore_from_compacted(&mut self, data: HashMap<String, VecDeque<QueueEntry>>) {
        use proven_hlc::NodeId;

        // Clear any existing data
        self.base_queues.clear();
        self.next_entry_ids.clear();
        self.tx_operations.clear();
        self.tx_dequeued.clear();
        self.committed_transactions.clear();
        self.transaction_start_times.clear();

        // Create a special "restore" transaction that's already committed
        let restore_txn = HlcTimestamp::new(0, 0, NodeId::new(0));
        self.committed_transactions.insert(restore_txn);

        // Restore all queues
        for (queue_name, entries) in data {
            // Find the max entry_id to set next_entry_id correctly
            let max_id = entries.iter().map(|e| e.entry_id).max().unwrap_or(0);
            self.next_entry_ids.insert(queue_name.clone(), max_id + 1);

            // Convert QueueEntry to Arc<QueueEntry> for storage
            let arc_entries: VecDeque<Arc<QueueEntry>> =
                entries.into_iter().map(Arc::new).collect();

            self.base_queues.insert(queue_name, arc_entries);
        }
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
    pub total_queues: usize,
    pub total_entries: usize,
    pub committed_txns: usize,
    pub active_txns: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_timestamp(seconds: u64) -> HlcTimestamp {
        use proven_hlc::NodeId;
        HlcTimestamp::new(seconds, 0, NodeId::new(1))
    }

    #[test]
    fn test_basic_enqueue_dequeue() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp(100);

        storage.register_transaction(tx1, tx1);

        // Enqueue some values
        storage.enqueue(
            "queue1".to_string(),
            QueueValue::String("first".to_string()),
            tx1,
            tx1,
        );
        storage.enqueue(
            "queue1".to_string(),
            QueueValue::String("second".to_string()),
            tx1,
            create_timestamp(101),
        );

        // Should be able to peek without removing
        assert_eq!(
            storage.peek("queue1", tx1).map(|arc| (*arc).clone()),
            Some(QueueValue::String("first".to_string()))
        );

        // Dequeue should return FIFO order
        assert_eq!(
            storage.dequeue("queue1", tx1),
            Some(QueueValue::String("first".to_string()))
        );
        assert_eq!(
            storage.dequeue("queue1", tx1),
            Some(QueueValue::String("second".to_string()))
        );
        assert_eq!(storage.dequeue("queue1", tx1), None);
    }

    #[test]
    fn test_transaction_isolation() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp(100);
        let tx2 = create_timestamp(200);

        storage.register_transaction(tx1, tx1);
        storage.register_transaction(tx2, tx2);

        // tx1 enqueues some values
        storage.enqueue(
            "queue1".to_string(),
            QueueValue::String("tx1_value".to_string()),
            tx1,
            tx1,
        );

        // tx2 shouldn't see uncommitted values
        assert_eq!(storage.size("queue1", tx2), 0);
        assert!(storage.is_empty("queue1", tx2));

        // After commit, tx2 should see the values
        storage.commit_transaction(tx1);

        assert_eq!(storage.size("queue1", tx2), 1);
        assert_eq!(
            storage.peek("queue1", tx2).map(|arc| (*arc).clone()),
            Some(QueueValue::String("tx1_value".to_string()))
        );
    }

    #[test]
    fn test_abort_rollback() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp(100);
        let tx2 = create_timestamp(200);

        // tx1 creates a queue and commits
        storage.register_transaction(tx1, tx1);
        storage.enqueue(
            "queue1".to_string(),
            QueueValue::String("committed".to_string()),
            tx1,
            tx1,
        );
        storage.commit_transaction(tx1);

        // tx2 modifies the queue but aborts
        storage.register_transaction(tx2, tx2);
        storage.enqueue(
            "queue1".to_string(),
            QueueValue::String("aborted".to_string()),
            tx2,
            tx2,
        );
        assert_eq!(storage.size("queue1", tx2), 2);

        // Abort tx2
        storage.abort_transaction(tx2);

        // New transaction should only see committed value
        let tx3 = create_timestamp(300);
        storage.register_transaction(tx3, tx3);
        assert_eq!(storage.size("queue1", tx3), 1);
        assert_eq!(
            storage.peek("queue1", tx3).map(|arc| (*arc).clone()),
            Some(QueueValue::String("committed".to_string()))
        );
    }

    #[test]
    fn test_clear_operation() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp(100);

        storage.register_transaction(tx1, tx1);

        // Add some values
        for i in 0..5 {
            storage.enqueue(
                "queue1".to_string(),
                QueueValue::Integer(i),
                tx1,
                create_timestamp(100 + i as u64),
            );
        }

        assert_eq!(storage.size("queue1", tx1), 5);

        // Clear the queue
        storage.clear("queue1", tx1);
        assert_eq!(storage.size("queue1", tx1), 0);
        assert!(storage.is_empty("queue1", tx1));
    }
}
