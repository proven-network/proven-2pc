//! Deferred operations management for handling blocked transactions
//!
//! When a transaction would block on a lock, we defer its operation
//! and retry it when the blocking transaction completes.

use crate::engine::{BlockingInfo, RetryOn};
use proven_common::TransactionId;
use std::collections::{HashMap, HashSet, VecDeque};

/// Information about a deferred operation
#[derive(Debug, Clone)]
pub struct DeferredOperation<O> {
    /// The operation to retry
    pub operation: O,

    /// The transaction that wants to execute this operation
    pub txn_id: TransactionId,

    /// Coordinator ID for sending responses
    pub coordinator_id: String,

    /// Request ID for matching responses
    pub request_id: Option<String>,

    /// All transactions that are blocking this operation
    /// We can only retry when ALL of these have reached the appropriate state
    pub blockers: Vec<BlockingInfo>,

    /// Remaining blockers that haven't resolved yet
    pub remaining_blockers: HashSet<TransactionId>,
}

/// Manages operations that are deferred due to lock conflicts
pub struct DeferredOperationsManager<O> {
    /// Operations waiting for each transaction to complete
    /// Key is the blocking transaction, value is queue of waiting operations
    waiting_on_commit: HashMap<TransactionId, VecDeque<DeferredOperation<O>>>,

    /// Operations that can retry after prepare (read lock release)
    /// Key is the blocking transaction, value is queue of waiting operations
    waiting_on_prepare: HashMap<TransactionId, VecDeque<DeferredOperation<O>>>,
}

impl<O: Clone> DeferredOperationsManager<O> {
    /// Create a new deferred operations manager
    pub fn new() -> Self {
        Self {
            waiting_on_commit: HashMap::new(),
            waiting_on_prepare: HashMap::new(),
        }
    }

    /// Defer an operation until ALL blocking transactions reach the appropriate state
    pub fn defer_operation(
        &mut self,
        operation: O,
        txn_id: TransactionId,
        blockers: Vec<BlockingInfo>,
        coordinator_id: String,
        request_id: Option<String>,
    ) {
        if blockers.is_empty() {
            return; // Nothing to wait for
        }

        // Create a set of all blocker transaction IDs
        let remaining_blockers: HashSet<TransactionId> = blockers.iter().map(|b| b.txn).collect();

        let deferred = DeferredOperation {
            operation,
            txn_id,
            coordinator_id,
            request_id,
            blockers: blockers.clone(),
            remaining_blockers,
        };

        // Register with each blocker based on their retry_on type
        for blocker in blockers {
            match blocker.retry_on {
                RetryOn::Prepare => {
                    self.waiting_on_prepare
                        .entry(blocker.txn)
                        .or_default()
                        .push_back(deferred.clone());
                }
                RetryOn::CommitOrAbort => {
                    self.waiting_on_commit
                        .entry(blocker.txn)
                        .or_default()
                        .push_back(deferred.clone());
                }
            }
        }
    }

    /// Get operations that can retry after a transaction prepares
    ///
    /// Called when a transaction prepares (releases read locks).
    /// Only returns operations where ALL blockers have been resolved.
    pub fn take_prepare_waiting_operations(
        &mut self,
        prepared_txn: &TransactionId,
    ) -> Vec<DeferredOperation<O>> {
        let mut ready_operations = Vec::new();

        if let Some(mut waiting) = self.waiting_on_prepare.remove(prepared_txn) {
            let mut still_waiting = VecDeque::new();

            for mut op in waiting.drain(..) {
                // Remove this transaction from remaining blockers
                op.remaining_blockers.remove(prepared_txn);

                if op.remaining_blockers.is_empty() {
                    // All blockers resolved - can retry
                    ready_operations.push(op);
                } else {
                    // Still waiting on other blockers
                    still_waiting.push_back(op);
                }
            }

            // Put back operations still waiting
            if !still_waiting.is_empty() {
                self.waiting_on_prepare.insert(*prepared_txn, still_waiting);
            }
        }

        ready_operations
    }

    /// Get all operations that were waiting on a completed transaction
    ///
    /// Called when a transaction commits or aborts to retry waiting operations.
    /// Only returns operations where ALL blockers have been resolved.
    pub fn take_commit_waiting_operations(
        &mut self,
        completed_txn: &TransactionId,
    ) -> Vec<DeferredOperation<O>> {
        let mut ready_operations = Vec::new();

        // Check commit waiting queue
        if let Some(mut waiting) = self.waiting_on_commit.remove(completed_txn) {
            let mut still_waiting = VecDeque::new();

            for mut op in waiting.drain(..) {
                // Remove this transaction from remaining blockers
                op.remaining_blockers.remove(completed_txn);

                if op.remaining_blockers.is_empty() {
                    // All blockers resolved - can retry
                    ready_operations.push(op);
                } else {
                    // Still waiting on other blockers
                    still_waiting.push_back(op);
                }
            }

            // Put back operations still waiting
            if !still_waiting.is_empty() {
                self.waiting_on_commit.insert(*completed_txn, still_waiting);
            }
        }

        // Also check prepare queue (in case transaction aborted before prepare)
        if let Some(mut waiting) = self.waiting_on_prepare.remove(completed_txn) {
            let mut still_waiting = VecDeque::new();

            for mut op in waiting.drain(..) {
                // Remove this transaction from remaining blockers
                op.remaining_blockers.remove(completed_txn);

                if op.remaining_blockers.is_empty() {
                    // All blockers resolved - can retry
                    ready_operations.push(op);
                } else {
                    // Still waiting on other blockers
                    still_waiting.push_back(op);
                }
            }

            // Put back operations still waiting
            if !still_waiting.is_empty() {
                self.waiting_on_prepare
                    .insert(*completed_txn, still_waiting);
            }
        }

        ready_operations
    }

    /// Remove all deferred operations for an aborted transaction
    ///
    /// When a transaction is aborted, we need to clean up any operations
    /// it had deferred.
    pub fn remove_operations_for_transaction(&mut self, aborted_txn: &TransactionId) {
        for queue in self.waiting_on_commit.values_mut() {
            queue.retain(|op| op.txn_id != *aborted_txn);
        }
        for queue in self.waiting_on_prepare.values_mut() {
            queue.retain(|op| op.txn_id != *aborted_txn);
        }
    }

    /// Check if there are any operations waiting on a transaction
    pub fn has_waiting_operations(&self, txn: &TransactionId) -> bool {
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
