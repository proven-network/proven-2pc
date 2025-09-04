//! Deferred operations management for lock conflicts
//!
//! This module handles operations that must wait for locks to be released,
//! manages the wait graph for deadlock detection, and implements wound-wait logic.

use crate::storage::lock::LockMode;
use crate::stream::message::KvOperation;
use proven_hlc::HlcTimestamp;
use std::collections::{HashMap, HashSet};

/// A KV operation that is waiting for locks
#[derive(Debug, Clone)]
pub struct DeferredOperation {
    /// The original KV operation
    pub operation: KvOperation,
    /// Coordinator to send response to
    pub coordinator_id: String,
    /// Key and mode that was requested
    pub lock_requested: (String, LockMode),
    /// Transaction holding the conflicting lock
    pub waiting_for: HlcTimestamp,
    /// Number of times this operation has been attempted
    pub attempt_count: u32,
    /// When this operation was first deferred
    pub created_at: HlcTimestamp,
}

/// Manages deferred operations and deadlock detection
pub struct DeferredOperationsManager {
    /// Operations waiting for locks, grouped by transaction
    operations: HashMap<HlcTimestamp, Vec<DeferredOperation>>,

    /// Wait graph: waiter -> holder
    /// Used for deadlock detection and wound-wait decisions
    wait_graph: HashMap<HlcTimestamp, HlcTimestamp>,

    /// Reverse wait graph: holder -> set of waiters
    /// Used for efficient wake-up when locks are released
    reverse_wait_graph: HashMap<HlcTimestamp, HashSet<HlcTimestamp>>,
}

impl DeferredOperationsManager {
    /// Create a new deferred operations manager
    pub fn new() -> Self {
        Self {
            operations: HashMap::new(),
            wait_graph: HashMap::new(),
            reverse_wait_graph: HashMap::new(),
        }
    }

    /// Add a deferred operation
    pub fn add_deferred(
        &mut self,
        tx_id: HlcTimestamp,
        operation: DeferredOperation,
        waiting_for: HlcTimestamp,
    ) {
        // Add to operations list
        self.operations
            .entry(tx_id)
            .or_insert_with(Vec::new)
            .push(operation);

        // Update wait graph
        self.wait_graph.insert(tx_id, waiting_for);

        // Update reverse wait graph
        self.reverse_wait_graph
            .entry(waiting_for)
            .or_insert_with(HashSet::new)
            .insert(tx_id);
    }

    /// Check if a transaction should wound another based on wound-wait policy
    /// Returns true if requester should wound holder (requester is older)
    pub fn should_wound(&self, requester: HlcTimestamp, holder: HlcTimestamp) -> bool {
        // Wound-wait: older transaction (smaller timestamp) wounds younger
        requester < holder
    }

    /// Remove and return all deferred operations for a transaction
    pub fn remove_operations(&mut self, tx_id: HlcTimestamp) -> Option<Vec<DeferredOperation>> {
        // Remove from wait graph
        if let Some(waiting_for) = self.wait_graph.remove(&tx_id) {
            // Remove from reverse graph
            if let Some(waiters) = self.reverse_wait_graph.get_mut(&waiting_for) {
                waiters.remove(&tx_id);
                if waiters.is_empty() {
                    self.reverse_wait_graph.remove(&waiting_for);
                }
            }
        }

        // Return operations
        self.operations.remove(&tx_id)
    }

    /// Get all transactions waiting for a specific transaction
    pub fn get_waiters(&self, holder: HlcTimestamp) -> Vec<HlcTimestamp> {
        self.reverse_wait_graph
            .get(&holder)
            .map(|waiters| {
                let mut sorted: Vec<_> = waiters.iter().copied().collect();
                sorted.sort(); // Deterministic order
                sorted
            })
            .unwrap_or_default()
    }

    /// Remove a transaction that is being wounded
    /// Returns the victim's deferred operations
    pub fn wound_transaction(&mut self, victim: HlcTimestamp) -> Vec<DeferredOperation> {
        // Remove from all wait graphs
        if let Some(waiting_for) = self.wait_graph.remove(&victim) {
            if let Some(waiters) = self.reverse_wait_graph.get_mut(&waiting_for) {
                waiters.remove(&victim);
                if waiters.is_empty() {
                    self.reverse_wait_graph.remove(&waiting_for);
                }
            }
        }

        // Also remove as a holder (if transaction was holding locks others are waiting for)
        if let Some(waiters) = self.reverse_wait_graph.remove(&victim) {
            for waiter in waiters {
                self.wait_graph.remove(&waiter);
            }
        }

        // Return victim's operations
        self.operations.remove(&victim).unwrap_or_default()
    }

    /// Check if adding a wait edge would create a cycle (deadlock)
    pub fn would_create_cycle(&self, waiter: HlcTimestamp, holder: HlcTimestamp) -> bool {
        // Simple cycle detection: follow the chain from holder
        let mut current = holder;
        let mut visited = HashSet::new();

        while let Some(&next) = self.wait_graph.get(&current) {
            if next == waiter {
                return true; // Found cycle
            }
            if !visited.insert(current) {
                break; // Already visited, no cycle through this path
            }
            current = next;
        }

        false
    }

    /// Get the number of deferred operations for a transaction
    pub fn operation_count(&self, tx_id: HlcTimestamp) -> usize {
        self.operations
            .get(&tx_id)
            .map(|ops| ops.len())
            .unwrap_or(0)
    }

    /// Check if a transaction has deferred operations
    pub fn has_deferred(&self, tx_id: HlcTimestamp) -> bool {
        self.operations.contains_key(&tx_id)
    }
}

impl Default for DeferredOperationsManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::NodeId;

    fn create_timestamp(seconds: u64) -> HlcTimestamp {
        HlcTimestamp::new(seconds, 0, NodeId::new(1))
    }

    #[test]
    fn test_should_wound() {
        let manager = DeferredOperationsManager::new();

        let older_tx = create_timestamp(100);
        let younger_tx = create_timestamp(200);

        // Older should wound younger
        assert!(manager.should_wound(older_tx, younger_tx));

        // Younger should not wound older
        assert!(!manager.should_wound(younger_tx, older_tx));

        // Same age should not wound
        assert!(!manager.should_wound(older_tx, older_tx));
    }

    #[test]
    fn test_wait_graph_management() {
        let mut manager = DeferredOperationsManager::new();

        let tx1 = create_timestamp(100);
        let tx2 = create_timestamp(200);
        let tx3 = create_timestamp(300);

        // Create a simple deferred operation
        let op = DeferredOperation {
            operation: KvOperation::Get {
                key: "test".to_string(),
            },
            coordinator_id: "coord1".to_string(),
            lock_requested: ("test".to_string(), LockMode::Shared),
            waiting_for: tx1,
            attempt_count: 1,
            created_at: tx2,
        };

        // tx2 waits for tx1
        manager.add_deferred(tx2, op.clone(), tx1);

        // Check wait graph
        assert_eq!(manager.get_waiters(tx1), vec![tx2]);

        // tx3 also waits for tx1
        let op2 = DeferredOperation {
            waiting_for: tx1,
            ..op.clone()
        };
        manager.add_deferred(tx3, op2, tx1);

        // Should have both waiters in deterministic order
        let waiters = manager.get_waiters(tx1);
        assert_eq!(waiters.len(), 2);
        assert_eq!(waiters, vec![tx2, tx3]); // Sorted order
    }

    #[test]
    fn test_cycle_detection() {
        let mut manager = DeferredOperationsManager::new();

        let tx1 = create_timestamp(100);
        let tx2 = create_timestamp(200);
        let tx3 = create_timestamp(300);

        // Create chain: tx2 -> tx1
        manager.wait_graph.insert(tx2, tx1);

        // Check if tx1 -> tx2 would create cycle (it would)
        assert!(manager.would_create_cycle(tx1, tx2));

        // Check if tx3 -> tx1 would create cycle (it wouldn't)
        assert!(!manager.would_create_cycle(tx3, tx1));

        // Create longer chain: tx3 -> tx2 -> tx1
        manager.wait_graph.insert(tx3, tx2);

        // Check if tx1 -> tx3 would create cycle (it would)
        assert!(manager.would_create_cycle(tx1, tx3));
    }
}
