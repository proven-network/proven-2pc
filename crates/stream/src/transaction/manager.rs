//! Transaction manager - central state management for all transactions
//!
//! This module owns all transaction state and provides clean lifecycle management.

use super::deferral::{DeferralManager, DeferredOp};
use super::state::{AbortReason, CompletedInfo, TransactionPhase, TransactionState};
use crate::engine::{BlockingInfo, TransactionEngine};
use crate::error::{ProcessorError, Result};
use proven_common::{Timestamp, TransactionId};
use std::collections::HashMap;
use std::marker::PhantomData;

/// Manages all transaction state and lifecycle
pub struct TransactionManager<E: TransactionEngine> {
    /// Active transactions (not yet prepared)
    active: HashMap<TransactionId, TransactionState>,

    /// Prepared transactions (eligible for recovery)
    prepared: HashMap<TransactionId, TransactionState>,

    /// Recently completed (for late message handling, GC'd after 5 min)
    completed: HashMap<TransactionId, CompletedInfo>,

    /// Deferral manager
    deferral: DeferralManager<E::Operation>,

    /// Phantom data for engine type
    _engine: PhantomData<E>,
}

impl<E: TransactionEngine> TransactionManager<E> {
    /// Create a new transaction manager
    pub fn new() -> Self {
        Self {
            active: HashMap::new(),
            prepared: HashMap::new(),
            completed: HashMap::new(),
            deferral: DeferralManager::new(),
            _engine: PhantomData,
        }
    }

    // === LIFECYCLE METHODS ===

    /// Begin a new transaction
    pub fn begin(
        &mut self,
        txn_id: TransactionId,
        coordinator_id: String,
        deadline: Timestamp,
        participants: HashMap<String, u64>,
    ) {
        let state = TransactionState::new(coordinator_id, deadline, participants);
        self.active.insert(txn_id, state);
    }

    /// Transition a transaction from active to prepared
    ///
    /// Returns operations that are now ready to retry
    pub fn transition_to_prepared(
        &mut self,
        txn_id: TransactionId,
    ) -> Result<Vec<DeferredOp<E::Operation>>> {
        self.transition_to_prepared_with_participants(txn_id, HashMap::new())
    }

    /// Transition a transaction from active to prepared with participant information
    ///
    /// Returns operations that are now ready to retry
    pub fn transition_to_prepared_with_participants(
        &mut self,
        txn_id: TransactionId,
        participants: HashMap<String, u64>,
    ) -> Result<Vec<DeferredOp<E::Operation>>> {
        let mut state = self
            .active
            .remove(&txn_id)
            .ok_or_else(|| ProcessorError::TransactionNotFound(txn_id.to_string()))?;

        state.phase = TransactionPhase::Prepared;
        state.participants = participants; // Store participants for recovery
        self.prepared.insert(txn_id, state);

        Ok(self.deferral.take_ready_on_prepare(txn_id))
    }

    /// Transition a transaction to committed
    ///
    /// Returns operations that are now ready to retry
    pub fn transition_to_committed(
        &mut self,
        txn_id: TransactionId,
    ) -> Result<Vec<DeferredOp<E::Operation>>> {
        let state = self
            .active
            .remove(&txn_id)
            .or_else(|| self.prepared.remove(&txn_id))
            .ok_or_else(|| ProcessorError::TransactionNotFound(txn_id.to_string()))?;

        self.completed.insert(
            txn_id,
            CompletedInfo {
                coordinator_id: state.coordinator_id,
                phase: TransactionPhase::Committed,
                completed_at: Timestamp::now(),
            },
        );

        Ok(self.deferral.take_ready_on_complete(txn_id))
    }

    /// Transition a transaction to aborted
    ///
    /// Returns operations that are now ready to retry
    pub fn transition_to_aborted(
        &mut self,
        txn_id: TransactionId,
        reason: AbortReason,
    ) -> Result<Vec<DeferredOp<E::Operation>>> {
        let state = self
            .active
            .remove(&txn_id)
            .or_else(|| self.prepared.remove(&txn_id))
            .ok_or_else(|| ProcessorError::TransactionNotFound(txn_id.to_string()))?;

        self.completed.insert(
            txn_id,
            CompletedInfo {
                coordinator_id: state.coordinator_id,
                phase: TransactionPhase::Aborted {
                    aborted_at: Timestamp::now(),
                    reason,
                },
                completed_at: Timestamp::now(),
            },
        );

        // Remove any deferred ops owned by this transaction
        self.deferral.remove_for_transaction(txn_id);

        // Return ops that were waiting on this transaction
        Ok(self.deferral.take_ready_on_complete(txn_id))
    }

    /// Mark a transaction as wounded (but keep it in active state for now)
    pub fn mark_wounded(&mut self, victim: TransactionId, wounded_by: TransactionId) -> Result<()> {
        let state = self
            .active
            .get_mut(&victim)
            .ok_or_else(|| ProcessorError::TransactionNotFound(victim.to_string()))?;

        state.phase = TransactionPhase::Aborted {
            aborted_at: Timestamp::now(),
            reason: AbortReason::Wounded { by: wounded_by },
        };

        Ok(())
    }

    // === QUERY METHODS ===

    /// Check if a transaction exists (in any state)
    pub fn exists(&self, txn_id: TransactionId) -> bool {
        self.active.contains_key(&txn_id)
            || self.prepared.contains_key(&txn_id)
            || self.completed.contains_key(&txn_id)
    }

    /// Get transaction state (only if active or prepared)
    pub fn get(&self, txn_id: TransactionId) -> Result<&TransactionState> {
        self.active
            .get(&txn_id)
            .or_else(|| self.prepared.get(&txn_id))
            .ok_or_else(|| ProcessorError::TransactionNotFound(txn_id.to_string()))
    }

    /// Get transaction state mutably (only if active or prepared)
    pub fn get_mut(&mut self, txn_id: TransactionId) -> Result<&mut TransactionState> {
        self.active
            .get_mut(&txn_id)
            .or_else(|| self.prepared.get_mut(&txn_id))
            .ok_or_else(|| ProcessorError::TransactionNotFound(txn_id.to_string()))
    }

    /// Get completed transaction info
    pub fn get_completed(&self, txn_id: TransactionId) -> Option<&CompletedInfo> {
        self.completed.get(&txn_id)
    }

    /// Get coordinator ID for a transaction (in any state)
    pub fn get_coordinator(&self, txn_id: TransactionId) -> Option<String> {
        self.get(txn_id)
            .ok()
            .map(|s| s.coordinator_id.clone())
            .or_else(|| {
                self.completed
                    .get(&txn_id)
                    .map(|c| c.coordinator_id.clone())
            })
    }

    /// Get all prepared transactions past their deadline
    pub fn get_expired_prepared(&self, now: Timestamp) -> Vec<TransactionId> {
        self.prepared
            .iter()
            .filter(|(_, state)| now > state.deadline)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get all active transactions past their deadline
    pub fn get_expired_active(&self, now: Timestamp) -> Vec<TransactionId> {
        self.active
            .iter()
            .filter(|(id, state)| {
                let expired = now > state.deadline;
                if expired {
                    println!(
                        "  Active transaction {} expired: now={} > deadline={}",
                        id,
                        now.as_micros(),
                        state.deadline.as_micros()
                    );
                }
                expired
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get participants for a transaction
    pub fn get_participants(&self, txn_id: TransactionId) -> HashMap<String, u64> {
        self.active
            .get(&txn_id)
            .or_else(|| self.prepared.get(&txn_id))
            .map(|state| state.participants.clone())
            .unwrap_or_default()
    }

    /// Check if there are any active transactions
    pub fn has_active_transactions(&self) -> bool {
        !self.active.is_empty() || !self.prepared.is_empty()
    }

    /// Get count of active transactions
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    /// Get count of prepared transactions
    pub fn prepared_count(&self) -> usize {
        self.prepared.len()
    }

    // === DEFERRAL METHODS ===

    /// Defer an operation until blockers are resolved
    pub fn defer_operation(
        &mut self,
        txn_id: TransactionId,
        operation: E::Operation,
        blockers: Vec<BlockingInfo>,
        coordinator_id: String,
        request_id: String,
        is_atomic: bool,
    ) {
        self.deferral.defer(
            txn_id,
            operation,
            blockers,
            coordinator_id,
            request_id,
            is_atomic,
        );
    }

    /// Get count of deferred operations for a transaction
    pub fn deferred_count(&self, txn_id: TransactionId) -> usize {
        self.deferral.count_for_transaction(txn_id)
    }

    /// Get total count of deferred operations
    pub fn total_deferred_count(&self) -> usize {
        self.deferral.total_count()
    }

    // === GARBAGE COLLECTION ===

    /// Garbage collect completed transactions older than cutoff
    pub fn gc_completed(&mut self, cutoff: Timestamp) {
        self.completed.retain(|_, info| info.completed_at > cutoff);
    }
}

impl<E: TransactionEngine> Default for TransactionManager<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::RetryOn;
    use proven_common::{Operation, Response};
    use serde::{Deserialize, Serialize};

    // Mock types for testing
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestOperation(String);
    impl Operation for TestOperation {
        fn operation_type(&self) -> proven_common::OperationType {
            proven_common::OperationType::Read
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestResponse(String);
    impl Response for TestResponse {}

    struct TestBatch;
    impl crate::engine::BatchOperations for TestBatch {
        fn insert_metadata(&mut self, _key: Vec<u8>, _value: Vec<u8>) {}
        fn remove_metadata(&mut self, _key: Vec<u8>) {}
    }

    struct TestEngine;
    impl TransactionEngine for TestEngine {
        type Operation = TestOperation;
        type Response = TestResponse;
        type Batch = TestBatch;

        fn start_batch(&mut self) -> Self::Batch {
            TestBatch
        }

        fn commit_batch(&mut self, _batch: Self::Batch, _log_index: u64) {}

        fn read_at_timestamp(
            &mut self,
            _operation: Self::Operation,
            _read_txn_id: TransactionId,
        ) -> crate::engine::OperationResult<Self::Response> {
            unimplemented!()
        }

        fn apply_operation(
            &mut self,
            _batch: &mut Self::Batch,
            _operation: Self::Operation,
            _txn_id: TransactionId,
        ) -> crate::engine::OperationResult<Self::Response> {
            unimplemented!()
        }

        fn begin(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
        fn prepare(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
        fn commit(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
        fn abort(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {}
        fn get_log_index(&self) -> Option<u64> {
            None
        }
        fn scan_transaction_metadata(&self) -> Vec<(TransactionId, Vec<u8>)> {
            vec![]
        }
        fn engine_name(&self) -> &str {
            "test"
        }
    }

    fn blocker(txn: TransactionId, retry_on: RetryOn) -> BlockingInfo {
        BlockingInfo { txn, retry_on }
    }

    #[test]
    fn test_begin_transaction() {
        let mut mgr = TransactionManager::<TestEngine>::new();
        let txn_id = TransactionId::new();

        mgr.begin(
            txn_id,
            "coord-1".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        );

        assert!(mgr.exists(txn_id));
        assert_eq!(mgr.active_count(), 1);
        assert_eq!(mgr.prepared_count(), 0);

        let state = mgr.get(txn_id).unwrap();
        assert_eq!(state.coordinator_id, "coord-1");
        assert_eq!(state.phase, TransactionPhase::Active);
    }

    #[test]
    fn test_transition_to_prepared() {
        let mut mgr = TransactionManager::<TestEngine>::new();
        let txn_id = TransactionId::new();

        mgr.begin(
            txn_id,
            "coord-1".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        );

        let ready = mgr.transition_to_prepared(txn_id).unwrap();
        assert_eq!(ready.len(), 0);

        assert_eq!(mgr.active_count(), 0);
        assert_eq!(mgr.prepared_count(), 1);

        let state = mgr.get(txn_id).unwrap();
        assert_eq!(state.phase, TransactionPhase::Prepared);
    }

    #[test]
    fn test_transition_to_committed() {
        let mut mgr = TransactionManager::<TestEngine>::new();
        let txn_id = TransactionId::new();

        mgr.begin(
            txn_id,
            "coord-1".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        );

        let ready = mgr.transition_to_committed(txn_id).unwrap();
        assert_eq!(ready.len(), 0);

        assert_eq!(mgr.active_count(), 0);
        assert_eq!(mgr.prepared_count(), 0);

        // Should be in completed
        let info = mgr.get_completed(txn_id).unwrap();
        assert_eq!(info.phase, TransactionPhase::Committed);
    }

    #[test]
    fn test_transition_to_aborted() {
        let mut mgr = TransactionManager::<TestEngine>::new();
        let txn_id = TransactionId::new();

        mgr.begin(
            txn_id,
            "coord-1".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        );

        let ready = mgr
            .transition_to_aborted(txn_id, AbortReason::Explicit)
            .unwrap();
        assert_eq!(ready.len(), 0);

        assert_eq!(mgr.active_count(), 0);

        // Should be in completed
        let info = mgr.get_completed(txn_id).unwrap();
        match info.phase {
            TransactionPhase::Aborted {
                reason: AbortReason::Explicit,
                ..
            } => {}
            _ => panic!("Expected aborted phase"),
        }
    }

    #[test]
    fn test_mark_wounded() {
        let mut mgr = TransactionManager::<TestEngine>::new();
        let victim = TransactionId::new();
        let attacker = TransactionId::new();

        mgr.begin(
            victim,
            "coord-1".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        );

        mgr.mark_wounded(victim, attacker).unwrap();

        let state = mgr.get(victim).unwrap();
        assert_eq!(state.is_wounded(), Some(attacker));
    }

    #[test]
    fn test_defer_and_retry_on_prepare() {
        let mut mgr = TransactionManager::<TestEngine>::new();
        let waiter = TransactionId::new();
        let blocker_txn = TransactionId::new();

        mgr.begin(
            waiter,
            "coord-1".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        );

        mgr.begin(
            blocker_txn,
            "coord-2".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        );

        mgr.defer_operation(
            waiter,
            TestOperation("op1".to_string()),
            vec![blocker(blocker_txn, RetryOn::Prepare)],
            "coord-1".to_string(),
            "req1".to_string(),
            false,
        );

        assert_eq!(mgr.deferred_count(waiter), 1);

        // Prepare blocker
        let ready = mgr.transition_to_prepared(blocker_txn).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].operation, TestOperation("op1".to_string()));
    }

    #[test]
    fn test_defer_and_retry_on_complete() {
        let mut mgr = TransactionManager::<TestEngine>::new();
        let waiter = TransactionId::new();
        let blocker_txn = TransactionId::new();

        mgr.begin(
            waiter,
            "coord-1".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        );

        mgr.begin(
            blocker_txn,
            "coord-2".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        );

        mgr.defer_operation(
            waiter,
            TestOperation("op1".to_string()),
            vec![blocker(blocker_txn, RetryOn::CommitOrAbort)],
            "coord-1".to_string(),
            "req1".to_string(),
            false,
        );

        // Commit blocker
        let ready = mgr.transition_to_committed(blocker_txn).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].operation, TestOperation("op1".to_string()));
    }

    #[test]
    fn test_get_expired_prepared() {
        let mut mgr = TransactionManager::<TestEngine>::new();
        let txn1 = TransactionId::new();
        let txn2 = TransactionId::new();

        mgr.begin(
            txn1,
            "coord-1".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        );

        mgr.begin(
            txn2,
            "coord-2".to_string(),
            Timestamp::from_micros(2000),
            HashMap::new(),
        );

        mgr.transition_to_prepared(txn1).unwrap();
        mgr.transition_to_prepared(txn2).unwrap();

        // At time 1500, only txn1 should be expired
        let expired = mgr.get_expired_prepared(Timestamp::from_micros(1500));
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], txn1);

        // At time 3000, both should be expired
        let expired = mgr.get_expired_prepared(Timestamp::from_micros(3000));
        assert_eq!(expired.len(), 2);
    }

    #[test]
    fn test_gc_completed() {
        let mut mgr = TransactionManager::<TestEngine>::new();
        let txn1 = TransactionId::new();

        mgr.begin(
            txn1,
            "coord-1".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        );

        mgr.transition_to_committed(txn1).unwrap();

        assert!(mgr.get_completed(txn1).is_some());

        // GC with cutoff far in the future (removes everything completed before now + 1 year)
        let far_future = Timestamp::now().add_micros(365 * 24 * 60 * 60 * 1_000_000);
        mgr.gc_completed(far_future);

        assert!(mgr.get_completed(txn1).is_none());
    }

    #[test]
    fn test_has_active_transactions() {
        let mut mgr = TransactionManager::<TestEngine>::new();
        assert!(!mgr.has_active_transactions());

        let txn1 = TransactionId::new();
        mgr.begin(
            txn1,
            "coord-1".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        );

        assert!(mgr.has_active_transactions());

        mgr.transition_to_committed(txn1).unwrap();
        assert!(!mgr.has_active_transactions());
    }

    #[test]
    fn test_aborted_transaction_removes_deferred_ops() {
        let mut mgr = TransactionManager::<TestEngine>::new();
        let waiter = TransactionId::new();
        let blocker_txn = TransactionId::new();

        mgr.begin(
            waiter,
            "coord-1".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        );

        mgr.defer_operation(
            waiter,
            TestOperation("op1".to_string()),
            vec![blocker(blocker_txn, RetryOn::CommitOrAbort)],
            "coord-1".to_string(),
            "req1".to_string(),
            false,
        );

        assert_eq!(mgr.deferred_count(waiter), 1);

        // Abort the waiter
        mgr.transition_to_aborted(waiter, AbortReason::Explicit)
            .unwrap();

        // Deferred ops should be removed
        assert_eq!(mgr.deferred_count(waiter), 0);
    }
}
