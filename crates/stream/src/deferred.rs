//! Deferred operations management for handling blocked transactions
//!
//! When a transaction would block on a lock, we defer its operation
//! and retry it when the blocking transaction completes.

use crate::engine::RetryOn;
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

    /// When this operation can be retried
    pub retry_on: RetryOn,
}

/// Manages operations that are deferred due to lock conflicts
pub struct DeferredOperationsManager<O> {
    /// Operations waiting for each transaction to complete
    /// Key is the blocking transaction, value is queue of waiting operations
    waiting_on_commit: HashMap<HlcTimestamp, VecDeque<DeferredOperation<O>>>,

    /// Operations that can retry after prepare (read lock release)
    /// Key is the blocking transaction, value is queue of waiting operations
    waiting_on_prepare: HashMap<HlcTimestamp, VecDeque<DeferredOperation<O>>>,
}

impl<O: Clone> DeferredOperationsManager<O> {
    /// Create a new deferred operations manager
    pub fn new() -> Self {
        Self {
            waiting_on_commit: HashMap::new(),
            waiting_on_prepare: HashMap::new(),
        }
    }

    /// Defer an operation until a blocking transaction reaches the appropriate state
    pub fn defer_operation(
        &mut self,
        operation: O,
        txn_id: HlcTimestamp,
        blocking_txn: HlcTimestamp,
        retry_on: RetryOn,
        coordinator_id: String,
        request_id: Option<String>,
    ) {
        let deferred = DeferredOperation {
            operation,
            txn_id,
            coordinator_id,
            request_id,
            retry_on,
        };

        match retry_on {
            RetryOn::Prepare => {
                self.waiting_on_prepare
                    .entry(blocking_txn)
                    .or_default()
                    .push_back(deferred);
            }
            RetryOn::CommitOrAbort => {
                self.waiting_on_commit
                    .entry(blocking_txn)
                    .or_default()
                    .push_back(deferred);
            }
        }
    }

    /// Get operations that can retry after a transaction prepares
    ///
    /// Called when a transaction prepares (releases read locks).
    pub fn take_prepare_waiting_operations(
        &mut self,
        prepared_txn: &HlcTimestamp,
    ) -> Vec<DeferredOperation<O>> {
        self.waiting_on_prepare
            .remove(prepared_txn)
            .map(|queue| queue.into_iter().collect())
            .unwrap_or_default()
    }

    /// Get all operations that were waiting on a completed transaction
    ///
    /// Called when a transaction commits or aborts to retry waiting operations.
    pub fn take_commit_waiting_operations(
        &mut self,
        completed_txn: &HlcTimestamp,
    ) -> Vec<DeferredOperation<O>> {
        // Take both prepare and commit waiters when transaction completes
        let mut operations: Vec<DeferredOperation<O>> = self
            .waiting_on_commit
            .remove(completed_txn)
            .map(|queue| queue.into_iter().collect())
            .unwrap_or_default();

        // Also take any prepare waiters that haven't been taken yet
        if let Some(prepare_queue) = self.waiting_on_prepare.remove(completed_txn) {
            operations.extend(prepare_queue);
        }

        operations
    }

    /// Remove all deferred operations for an aborted transaction
    ///
    /// When a transaction is aborted, we need to clean up any operations
    /// it had deferred.
    pub fn remove_operations_for_transaction(&mut self, aborted_txn: &HlcTimestamp) {
        for queue in self.waiting_on_commit.values_mut() {
            queue.retain(|op| op.txn_id != *aborted_txn);
        }
        for queue in self.waiting_on_prepare.values_mut() {
            queue.retain(|op| op.txn_id != *aborted_txn);
        }
    }

    /// Check if there are any operations waiting on a transaction
    pub fn has_waiting_operations(&self, txn: &HlcTimestamp) -> bool {
        let has_commit_waiters = self
            .waiting_on_commit
            .get(txn)
            .map(|queue| !queue.is_empty())
            .unwrap_or(false);

        let has_prepare_waiters = self
            .waiting_on_prepare
            .get(txn)
            .map(|queue| !queue.is_empty())
            .unwrap_or(false);

        has_commit_waiters || has_prepare_waiters
    }

    /// Get count of total deferred operations (for debugging/metrics)
    pub fn total_deferred_count(&self) -> usize {
        let commit_count: usize = self.waiting_on_commit.values().map(|q| q.len()).sum();
        let prepare_count: usize = self.waiting_on_prepare.values().map(|q| q.len()).sum();
        commit_count + prepare_count
    }
}

impl<O: Clone> Default for DeferredOperationsManager<O> {
    fn default() -> Self {
        Self::new()
    }
}
