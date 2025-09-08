//! Multi-Version Concurrency Control (MVCC) for Queue storage
//!
//! Provides transaction isolation through versioning of queue entries,
//! enabling quick aborts and consistent reads.

use crate::types::QueueValue;
use proven_hlc::HlcTimestamp;
use std::collections::{HashMap, HashSet, VecDeque};

/// A single entry in the queue
#[derive(Debug, Clone)]
pub struct QueueEntry {
    /// The actual value
    pub value: QueueValue,
    /// Transaction that created this entry
    pub created_by: HlcTimestamp,
    /// When this entry was created
    pub created_at: HlcTimestamp,
    /// Unique ID for this entry (for ordering)
    pub entry_id: u64,
}

/// A versioned queue state
#[derive(Debug, Clone)]
pub struct VersionedQueue {
    /// Queue entries in FIFO order
    entries: VecDeque<QueueEntry>,
    /// Next entry ID to assign
    next_entry_id: u64,
}

impl VersionedQueue {
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            next_entry_id: 1,
        }
    }
}

/// MVCC storage for queues
#[derive(Debug)]
pub struct MvccStorage {
    /// All versions of all queues (queue_name -> versions)
    /// Each transaction has its own version when it modifies a queue
    queue_versions: HashMap<String, HashMap<HlcTimestamp, VersionedQueue>>,

    /// Base versions of queues (committed state)
    base_queues: HashMap<String, VersionedQueue>,

    /// Set of committed transactions
    committed_transactions: HashSet<HlcTimestamp>,

    /// Transaction start times for visibility checks
    transaction_start_times: HashMap<HlcTimestamp, HlcTimestamp>,

    /// Track which queues each transaction has modified
    transaction_modified_queues: HashMap<HlcTimestamp, HashSet<String>>,
}

impl MvccStorage {
    /// Create a new MVCC storage
    pub fn new() -> Self {
        Self {
            queue_versions: HashMap::new(),
            base_queues: HashMap::new(),
            committed_transactions: HashSet::new(),
            transaction_start_times: HashMap::new(),
            transaction_modified_queues: HashMap::new(),
        }
    }

    /// Register a new transaction
    pub fn register_transaction(&mut self, tx_id: HlcTimestamp, start_time: HlcTimestamp) {
        self.transaction_start_times.insert(tx_id, start_time);
        self.transaction_modified_queues
            .insert(tx_id, HashSet::new());
    }

    /// Mark a transaction as committed
    pub fn commit_transaction(&mut self, tx_id: HlcTimestamp) {
        // Move transaction's queue versions to base state
        if let Some(modified_queues) = self.transaction_modified_queues.remove(&tx_id) {
            for queue_name in modified_queues {
                if let Some(versions) = self.queue_versions.get_mut(&queue_name)
                    && let Some(tx_version) = versions.remove(&tx_id)
                {
                    self.base_queues.insert(queue_name.clone(), tx_version);
                }
            }
        }

        self.committed_transactions.insert(tx_id);
        self.transaction_start_times.remove(&tx_id);
    }

    /// Abort a transaction by removing all its versions
    pub fn abort_transaction(&mut self, tx_id: HlcTimestamp) {
        // Remove all queue versions created by this transaction
        if let Some(modified_queues) = self.transaction_modified_queues.remove(&tx_id) {
            for queue_name in modified_queues {
                if let Some(versions) = self.queue_versions.get_mut(&queue_name) {
                    versions.remove(&tx_id);
                }
                // Clean up empty version maps
                if self
                    .queue_versions
                    .get(&queue_name)
                    .is_some_and(|v| v.is_empty())
                {
                    self.queue_versions.remove(&queue_name);
                }
            }
        }

        // Remove transaction metadata
        self.transaction_start_times.remove(&tx_id);
    }

    /// Get the visible queue version for a transaction (for writes)
    fn get_queue_for_write(
        &mut self,
        queue_name: &str,
        tx_id: HlcTimestamp,
    ) -> &mut VersionedQueue {
        // Check if this transaction already has a version
        if let Some(versions) = self.queue_versions.get(queue_name)
            && versions.contains_key(&tx_id)
        {
            return self
                .queue_versions
                .get_mut(queue_name)
                .unwrap()
                .get_mut(&tx_id)
                .unwrap();
        }

        // Create a new version for this transaction by copying base state
        let base_queue = self
            .base_queues
            .get(queue_name)
            .cloned()
            .unwrap_or_else(VersionedQueue::new);

        self.queue_versions
            .entry(queue_name.to_string())
            .or_default()
            .insert(tx_id, base_queue);

        self.transaction_modified_queues
            .entry(tx_id)
            .or_default()
            .insert(queue_name.to_string());

        self.queue_versions
            .get_mut(queue_name)
            .unwrap()
            .get_mut(&tx_id)
            .unwrap()
    }

    /// Get the visible queue version for a transaction (for reads)
    fn get_queue_for_read(&self, queue_name: &str, tx_id: HlcTimestamp) -> Option<&VersionedQueue> {
        // First check if this transaction has its own version
        if let Some(versions) = self.queue_versions.get(queue_name)
            && let Some(queue) = versions.get(&tx_id)
        {
            return Some(queue);
        }

        // Otherwise return the base (committed) version
        self.base_queues.get(queue_name)
    }

    /// Enqueue a value to the back of a queue
    pub fn enqueue(
        &mut self,
        queue_name: String,
        value: QueueValue,
        tx_id: HlcTimestamp,
        timestamp: HlcTimestamp,
    ) {
        let queue = self.get_queue_for_write(&queue_name, tx_id);

        let entry = QueueEntry {
            value,
            created_by: tx_id,
            created_at: timestamp,
            entry_id: queue.next_entry_id,
        };

        queue.next_entry_id += 1;
        queue.entries.push_back(entry);
    }

    /// Dequeue a value from the front of a queue
    pub fn dequeue(&mut self, queue_name: &str, tx_id: HlcTimestamp) -> Option<QueueValue> {
        let queue = self.get_queue_for_write(queue_name, tx_id);
        queue.entries.pop_front().map(|entry| entry.value)
    }

    /// Peek at the front value without removing it
    pub fn peek(&self, queue_name: &str, tx_id: HlcTimestamp) -> Option<&QueueValue> {
        self.get_queue_for_read(queue_name, tx_id)
            .and_then(|queue| queue.entries.front().map(|entry| &entry.value))
    }

    /// Get the size of a queue
    pub fn size(&self, queue_name: &str, tx_id: HlcTimestamp) -> usize {
        self.get_queue_for_read(queue_name, tx_id)
            .map(|queue| queue.entries.len())
            .unwrap_or(0)
    }

    /// Check if a queue is empty
    pub fn is_empty(&self, queue_name: &str, tx_id: HlcTimestamp) -> bool {
        self.size(queue_name, tx_id) == 0
    }

    /// Clear all values from a queue
    pub fn clear(&mut self, queue_name: &str, tx_id: HlcTimestamp) {
        let queue = self.get_queue_for_write(queue_name, tx_id);
        queue.entries.clear();
    }

    /// Get all queue names visible to a transaction
    pub fn list_queues(&self, tx_id: HlcTimestamp) -> Vec<String> {
        let mut queues = HashSet::new();

        // Add base queues
        for queue_name in self.base_queues.keys() {
            queues.insert(queue_name.clone());
        }

        // Add queues modified by this transaction
        if let Some(modified) = self.transaction_modified_queues.get(&tx_id) {
            for queue_name in modified {
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
        let total_entries: usize = self.base_queues.values().map(|q| q.entries.len()).sum();
        let committed_txns = self.committed_transactions.len();
        let active_txns = self.transaction_start_times.len();

        StorageStats {
            total_queues,
            total_entries,
            committed_txns,
            active_txns,
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
            storage.peek("queue1", tx1),
            Some(&QueueValue::String("first".to_string()))
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
            storage.peek("queue1", tx2),
            Some(&QueueValue::String("tx1_value".to_string()))
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
            storage.peek("queue1", tx3),
            Some(&QueueValue::String("committed".to_string()))
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
