//! Deferred operations management for handling blocked transactions
//!
//! When a transaction would block on a lock, we defer its operation
//! and retry it when the blocking transaction completes.

use proven_hlc::HlcTimestamp;
use std::collections::{HashMap, VecDeque};

/// Information about a deferred operation
#[derive(Debug, Clone)]
pub struct DeferredOperation<O> {
    /// The operation to retry
    pub operation: O,

    /// The transaction that wants to execute this operation
    pub txn_id: HlcTimestamp,

    /// Coordinator ID for sending responses
    pub coordinator_id: String,

    /// Request ID for matching responses
    pub request_id: Option<String>,
}

/// Manages operations that are deferred due to lock conflicts
pub struct DeferredOperationsManager<O> {
    /// Operations waiting for each transaction to complete
    /// Key is the blocking transaction, value is queue of waiting operations
    waiting_on: HashMap<HlcTimestamp, VecDeque<DeferredOperation<O>>>,
}

impl<O: Clone> DeferredOperationsManager<O> {
    /// Create a new deferred operations manager
    pub fn new() -> Self {
        Self {
            waiting_on: HashMap::new(),
        }
    }

    /// Defer an operation until a blocking transaction completes
    pub fn defer_operation(
        &mut self,
        operation: O,
        txn_id: HlcTimestamp,
        blocking_txn: HlcTimestamp,
        coordinator_id: String,
        request_id: Option<String>,
    ) {
        let deferred = DeferredOperation {
            operation,
            txn_id,
            coordinator_id,
            request_id,
        };

        self.waiting_on
            .entry(blocking_txn)
            .or_insert_with(VecDeque::new)
            .push_back(deferred);
    }

    /// Get all operations that were waiting on a completed transaction
    ///
    /// Called when a transaction commits or aborts to retry waiting operations.
    pub fn take_waiting_operations(
        &mut self,
        completed_txn: &HlcTimestamp,
    ) -> Vec<DeferredOperation<O>> {
        self.waiting_on
            .remove(completed_txn)
            .map(|queue| queue.into_iter().collect())
            .unwrap_or_default()
    }

    /// Remove all deferred operations for an aborted transaction
    ///
    /// When a transaction is aborted, we need to clean up any operations
    /// it had deferred.
    pub fn remove_operations_for_transaction(&mut self, aborted_txn: &HlcTimestamp) {
        for queue in self.waiting_on.values_mut() {
            queue.retain(|op| op.txn_id != *aborted_txn);
        }
    }

    /// Check if there are any operations waiting on a transaction
    pub fn has_waiting_operations(&self, txn: &HlcTimestamp) -> bool {
        self.waiting_on
            .get(txn)
            .map(|queue| !queue.is_empty())
            .unwrap_or(false)
    }

    /// Get count of total deferred operations (for debugging/metrics)
    pub fn total_deferred_count(&self) -> usize {
        self.waiting_on.values().map(|q| q.len()).sum()
    }
}

impl<O: Clone> Default for DeferredOperationsManager<O> {
    fn default() -> Self {
        Self::new()
    }
}
