//! Deferred operations management
//!
//! This module handles operations that are blocked on locks and need to be retried
//! when the blocking transactions complete.

use crate::engine::{BlockingInfo, RetryOn};
use proven_common::TransactionId;
use std::collections::{HashMap, HashSet};

/// Manages deferred operations
pub struct DeferralManager<O> {
    /// Operations owned by each transaction
    /// txn_id -> HashMap of (op_id -> deferred op)
    deferred: HashMap<TransactionId, HashMap<u64, DeferredOp<O>>>,

    /// Reverse index: which transactions are waiting on this blocker?
    /// blocker_txn -> [(waiter_txn, op_id, WaitType)]
    waiting_on: HashMap<TransactionId, Vec<Waiter>>,

    /// Next operation ID
    next_op_id: u64,
}

/// A deferred operation that's waiting to retry
#[derive(Debug, Clone)]
pub struct DeferredOp<O> {
    pub operation: O,
    pub coordinator_id: String,
    pub request_id: String,

    /// Transaction that owns this operation
    pub owner_txn_id: TransactionId,

    /// What this op is still waiting for
    pub waiting_for: WaitingFor,

    /// Whether this is an atomic operation (AdHoc)
    ///
    /// When true, this operation must be retried using the sequence:
    /// begin → apply_operation → commit (all in same batch)
    pub is_atomic: bool,

    /// Whether this operation has been taken (ready for retry)
    taken: bool,
}

/// What a deferred operation is waiting for
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitingFor {
    /// Blockers that need to prepare (release read locks)
    pub prepare: HashSet<TransactionId>,

    /// Blockers that need to complete (release all locks)
    pub complete: HashSet<TransactionId>,
}

impl WaitingFor {
    /// Check if there are no more blockers
    pub fn is_empty(&self) -> bool {
        self.prepare.is_empty() && self.complete.is_empty()
    }

    /// Create from a list of blocking transactions
    pub fn from_blockers(blockers: &[BlockingInfo]) -> Self {
        let mut prepare = HashSet::new();
        let mut complete = HashSet::new();

        for blocker in blockers {
            match blocker.retry_on {
                RetryOn::Prepare => {
                    prepare.insert(blocker.txn);
                }
                RetryOn::CommitOrAbort => {
                    complete.insert(blocker.txn);
                }
            }
        }

        Self { prepare, complete }
    }
}

#[derive(Debug, Clone)]
struct Waiter {
    txn_id: TransactionId,
    op_id: u64,
    wait_type: WaitType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitType {
    Prepare,
    Complete,
}

impl<O: Clone> DeferralManager<O> {
    /// Create a new deferral manager
    pub fn new() -> Self {
        Self {
            deferred: HashMap::new(),
            waiting_on: HashMap::new(),
            next_op_id: 0,
        }
    }

    /// Defer an operation until blockers are resolved
    pub fn defer(
        &mut self,
        txn_id: TransactionId,
        operation: O,
        blockers: Vec<BlockingInfo>,
        coordinator_id: String,
        request_id: String,
        is_atomic: bool,
    ) {
        let waiting_for = WaitingFor::from_blockers(&blockers);

        // Get a unique operation ID
        let op_id = self.next_op_id;
        self.next_op_id += 1;

        // Add to deferred ops for this transaction
        let ops = self.deferred.entry(txn_id).or_default();
        ops.insert(
            op_id,
            DeferredOp {
                operation,
                coordinator_id,
                request_id,
                owner_txn_id: txn_id,
                waiting_for: waiting_for.clone(),
                is_atomic,
                taken: false,
            },
        );

        // Update reverse index for prepare waiters
        for blocker_id in &waiting_for.prepare {
            self.waiting_on
                .entry(*blocker_id)
                .or_default()
                .push(Waiter {
                    txn_id,
                    op_id,
                    wait_type: WaitType::Prepare,
                });
        }

        // Update reverse index for complete waiters
        for blocker_id in &waiting_for.complete {
            self.waiting_on
                .entry(*blocker_id)
                .or_default()
                .push(Waiter {
                    txn_id,
                    op_id,
                    wait_type: WaitType::Complete,
                });
        }
    }

    /// Take operations that are ready to retry after a transaction prepares
    pub fn take_ready_on_prepare(&mut self, prepared: TransactionId) -> Vec<DeferredOp<O>> {
        let mut ready = Vec::new();

        if let Some(waiters) = self.waiting_on.get_mut(&prepared) {
            // Process only prepare waiters
            for waiter in waiters.iter() {
                if waiter.wait_type != WaitType::Prepare {
                    continue;
                }

                if let Some(ops) = self.deferred.get_mut(&waiter.txn_id)
                    && let Some(op) = ops.get_mut(&waiter.op_id)
                {
                    // Skip if already taken
                    if op.taken {
                        continue;
                    }

                    // Remove this blocker
                    op.waiting_for.prepare.remove(&prepared);

                    // Check if ready
                    if op.waiting_for.is_empty() {
                        op.taken = true;
                        ready.push(op.clone());
                    }
                }
            }

            // Remove processed prepare waiters
            waiters.retain(|w| w.wait_type != WaitType::Prepare);
        }

        // Clean up empty entries
        if let Some(waiters) = self.waiting_on.get(&prepared)
            && waiters.is_empty()
        {
            self.waiting_on.remove(&prepared);
        }

        // Clean up taken operations
        self.remove_taken_operations();

        ready
    }

    /// Take operations that are ready to retry after a transaction completes
    pub fn take_ready_on_complete(&mut self, completed: TransactionId) -> Vec<DeferredOp<O>> {
        let mut ready = Vec::new();

        if let Some(waiters) = self.waiting_on.remove(&completed) {
            for waiter in waiters {
                if let Some(ops) = self.deferred.get_mut(&waiter.txn_id)
                    && let Some(op) = ops.get_mut(&waiter.op_id)
                {
                    // Skip if already taken
                    if op.taken {
                        continue;
                    }

                    // Remove this blocker (from both sets since it completed)
                    op.waiting_for.prepare.remove(&completed);
                    op.waiting_for.complete.remove(&completed);

                    // Check if ready
                    if op.waiting_for.is_empty() {
                        op.taken = true;
                        ready.push(op.clone());
                    }
                }
            }
        }

        // Clean up taken operations
        self.remove_taken_operations();

        ready
    }

    /// Remove all deferred operations for a transaction (e.g., when it's aborted)
    pub fn remove_for_transaction(&mut self, txn_id: TransactionId) {
        self.deferred.remove(&txn_id);
        // Note: leaving entries in waiting_on is OK, they'll be cleaned up
        // when the blocker completes or via remove_taken_operations
    }

    /// Get count of deferred operations for a transaction
    pub fn count_for_transaction(&self, txn_id: TransactionId) -> usize {
        self.deferred.get(&txn_id).map(|ops| ops.len()).unwrap_or(0)
    }

    /// Get total count of deferred operations
    pub fn total_count(&self) -> usize {
        self.deferred.values().map(|ops| ops.len()).sum()
    }

    /// Clean up operations that have been taken
    fn remove_taken_operations(&mut self) {
        self.deferred.retain(|_, ops| {
            ops.retain(|_, op| !op.taken);
            !ops.is_empty()
        });
    }
}

impl<O: Clone> Default for DeferralManager<O> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct TestOp(String);

    fn blocker(txn: TransactionId, retry_on: RetryOn) -> BlockingInfo {
        BlockingInfo { txn, retry_on }
    }

    #[test]
    fn test_defer_and_take_on_prepare() {
        let mut mgr = DeferralManager::new();

        let txn1 = TransactionId::new();
        let blocker_txn = TransactionId::new();

        mgr.defer(
            txn1,
            TestOp("op1".to_string()),
            vec![blocker(blocker_txn, RetryOn::Prepare)],
            "coord1".to_string(),
            "req1".to_string(),
            false,
        );

        assert_eq!(mgr.count_for_transaction(txn1), 1);
        assert_eq!(mgr.total_count(), 1);

        // Take ready operations when blocker prepares
        let ready = mgr.take_ready_on_prepare(blocker_txn);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].operation, TestOp("op1".to_string()));
        assert_eq!(ready[0].coordinator_id, "coord1");

        // Should be cleaned up
        assert_eq!(mgr.count_for_transaction(txn1), 0);
        assert_eq!(mgr.total_count(), 0);
    }

    #[test]
    fn test_defer_and_take_on_complete() {
        let mut mgr = DeferralManager::new();

        let txn1 = TransactionId::new();
        let blocker_txn = TransactionId::new();

        mgr.defer(
            txn1,
            TestOp("op1".to_string()),
            vec![blocker(blocker_txn, RetryOn::CommitOrAbort)],
            "coord1".to_string(),
            "req1".to_string(),
            false,
        );

        // Should not be ready on prepare
        let ready = mgr.take_ready_on_prepare(blocker_txn);
        assert_eq!(ready.len(), 0);
        assert_eq!(mgr.count_for_transaction(txn1), 1);

        // Should be ready on complete
        let ready = mgr.take_ready_on_complete(blocker_txn);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].operation, TestOp("op1".to_string()));
    }

    #[test]
    fn test_multiple_blockers_all_must_resolve() {
        let mut mgr = DeferralManager::new();

        let txn1 = TransactionId::new();
        let blocker1 = TransactionId::new();
        let blocker2 = TransactionId::new();

        mgr.defer(
            txn1,
            TestOp("op1".to_string()),
            vec![
                blocker(blocker1, RetryOn::Prepare),
                blocker(blocker2, RetryOn::CommitOrAbort),
            ],
            "coord1".to_string(),
            "req1".to_string(),
            false,
        );

        // After blocker1 prepares, should not be ready yet
        let ready = mgr.take_ready_on_prepare(blocker1);
        assert_eq!(ready.len(), 0);
        assert_eq!(mgr.count_for_transaction(txn1), 1);

        // After blocker2 completes, should be ready
        let ready = mgr.take_ready_on_complete(blocker2);
        assert_eq!(ready.len(), 1);
        assert_eq!(mgr.count_for_transaction(txn1), 0);
    }

    #[test]
    fn test_complete_resolves_prepare_waiters_too() {
        let mut mgr = DeferralManager::new();

        let txn1 = TransactionId::new();
        let blocker_txn = TransactionId::new();

        // Operation waiting on prepare
        mgr.defer(
            txn1,
            TestOp("op1".to_string()),
            vec![blocker(blocker_txn, RetryOn::Prepare)],
            "coord1".to_string(),
            "req1".to_string(),
            false,
        );

        // Blocker completes (aborts before prepare)
        let ready = mgr.take_ready_on_complete(blocker_txn);
        assert_eq!(ready.len(), 1);
    }

    #[test]
    fn test_remove_for_transaction() {
        let mut mgr = DeferralManager::new();

        let txn1 = TransactionId::new();
        let blocker_txn = TransactionId::new();

        mgr.defer(
            txn1,
            TestOp("op1".to_string()),
            vec![blocker(blocker_txn, RetryOn::CommitOrAbort)],
            "coord1".to_string(),
            "req1".to_string(),
            false,
        );

        assert_eq!(mgr.count_for_transaction(txn1), 1);

        mgr.remove_for_transaction(txn1);

        assert_eq!(mgr.count_for_transaction(txn1), 0);
        assert_eq!(mgr.total_count(), 0);
    }

    #[test]
    fn test_multiple_operations_for_same_transaction() {
        let mut mgr = DeferralManager::new();

        let txn1 = TransactionId::new();
        let blocker1 = TransactionId::new();
        let blocker2 = TransactionId::new();

        // Two operations for same transaction, different blockers
        mgr.defer(
            txn1,
            TestOp("op1".to_string()),
            vec![blocker(blocker1, RetryOn::CommitOrAbort)],
            "coord1".to_string(),
            "req1".to_string(),
            false,
        );

        mgr.defer(
            txn1,
            TestOp("op2".to_string()),
            vec![blocker(blocker2, RetryOn::CommitOrAbort)],
            "coord1".to_string(),
            "req1".to_string(),
            false,
        );

        assert_eq!(mgr.count_for_transaction(txn1), 2);

        // Complete blocker1 - should get op1
        let ready = mgr.take_ready_on_complete(blocker1);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].operation, TestOp("op1".to_string()));

        // Still have op2 waiting
        assert_eq!(mgr.count_for_transaction(txn1), 1);

        // Complete blocker2 - should get op2
        let ready = mgr.take_ready_on_complete(blocker2);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].operation, TestOp("op2".to_string()));

        assert_eq!(mgr.count_for_transaction(txn1), 0);
    }

    #[test]
    fn test_waiting_for_from_blockers() {
        let blocker1 = TransactionId::new();
        let blocker2 = TransactionId::new();

        let waiting = WaitingFor::from_blockers(&[
            blocker(blocker1, RetryOn::Prepare),
            blocker(blocker2, RetryOn::CommitOrAbort),
        ]);

        assert!(waiting.prepare.contains(&blocker1));
        assert!(waiting.complete.contains(&blocker2));
        assert!(!waiting.is_empty());
    }

    #[test]
    fn test_waiting_for_is_empty() {
        let waiting = WaitingFor {
            prepare: HashSet::new(),
            complete: HashSet::new(),
        };

        assert!(waiting.is_empty());
    }
}
