//! Read-write execution path for 2PC transactions
//!
//! This executor handles transactions that:
//! - Participate in two-phase commit (2PC)
//! - Can block and wait on locks (wound-wait for deadlock prevention)
//! - Must be prepared before commit
//! - Require coordinator approval to commit

use crate::engine::{OperationResult, TransactionEngine};
use crate::error::Result;
use crate::executor::context::ExecutionContext;
use crate::kernel::ResponseMode;
use proven_common::{Response, TransactionId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Acknowledgment response for begin/prepare/commit/abort operations
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AckResponse {
    pub success: bool,
}

impl Response for AckResponse {}

impl AckResponse {
    pub fn success() -> Self {
        Self { success: true }
    }
}

/// Read-write execution - stateless functions for 2PC operations
pub struct ReadWriteExecution;

impl ReadWriteExecution {
    /// Execute an operation within a 2PC transaction
    ///
    /// Uses wound-wait protocol: wounds younger transactions, defers to older ones.
    pub fn execute<E: TransactionEngine>(
        ctx: &mut ExecutionContext<E>,
        batch: &mut E::Batch,
        operation: E::Operation,
        txn_id: TransactionId,
        coordinator_id: String,
        request_id: String,
        phase: ResponseMode,
    ) -> Result<()> {
        // Ensure transaction is registered (needed for wound-wait)
        if !ctx.tx_manager.exists(txn_id) {
            tracing::warn!(
                "Transaction {} not registered, cannot wound-wait properly",
                txn_id
            );
        }

        // Try to execute operation
        match ctx.engine.apply_operation(batch, operation.clone(), txn_id) {
            OperationResult::Complete(response) => {
                // Success - mark dirty for lazy persistence
                ctx.mark_dirty(txn_id);

                if phase == ResponseMode::Send {
                    ctx.response.send_success(
                        &coordinator_id,
                        Some(&txn_id.to_string()),
                        request_id,
                        response,
                    );
                }
                Ok(())
            }
            OperationResult::WouldBlock { blockers } => {
                // Wound-wait protocol: wound younger, defer to older

                // Find younger victims to wound
                let younger_victims: Vec<TransactionId> = blockers
                    .iter()
                    .filter(|b| b.txn > txn_id)
                    .map(|b| b.txn)
                    .collect();

                if !younger_victims.is_empty() {
                    tracing::debug!(
                        "Transaction {} wounding younger transactions: {:?}",
                        txn_id,
                        younger_victims
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                    );

                    // Wound each younger transaction
                    for victim in younger_victims {
                        ctx.wound_transaction(batch, victim, txn_id, phase)?;
                    }

                    // Retry operation after wounding (use same batch)
                    match ctx.engine.apply_operation(batch, operation.clone(), txn_id) {
                        OperationResult::Complete(response) => {
                            // Success after wounding - mark dirty for lazy persistence
                            ctx.mark_dirty(txn_id);

                            if phase == ResponseMode::Send {
                                ctx.response.send_success(
                                    &coordinator_id,
                                    Some(&txn_id.to_string()),
                                    request_id,
                                    response,
                                );
                            }
                            return Ok(());
                        }
                        OperationResult::WouldBlock {
                            blockers: new_blockers,
                        } => {
                            // Still blocked (by older transactions) - defer
                            tracing::debug!(
                                "Transaction {} still blocked after wounding, deferring",
                                txn_id
                            );

                            ctx.tx_manager.defer_operation(
                                txn_id,
                                operation,
                                new_blockers,
                                coordinator_id,
                                request_id,
                                false,
                            );

                            ctx.mark_dirty(txn_id);
                            return Ok(());
                        }
                    }
                }

                // All blockers are older - defer
                tracing::debug!(
                    "Transaction {} deferring to older transactions: {:?}",
                    txn_id,
                    blockers
                        .iter()
                        .map(|b| b.txn.to_string())
                        .collect::<Vec<_>>()
                );

                ctx.tx_manager.defer_operation(
                    txn_id,
                    operation,
                    blockers,
                    coordinator_id,
                    request_id,
                    false,
                );

                ctx.mark_dirty(txn_id);
                Ok(())
            }
        }
    }

    /// Prepare a transaction for commit (Phase 1 of 2PC)
    pub fn prepare<E: TransactionEngine>(
        ctx: &mut ExecutionContext<E>,
        batch: &mut E::Batch,
        txn_id: TransactionId,
        coordinator_id: String,
        request_id: String,
        participants: HashMap<String, u64>,
        phase: ResponseMode,
    ) -> Result<()> {
        // Prepare in engine (adds to batch)
        ctx.engine.prepare(batch, txn_id);

        // Transition to prepared state - this releases operations waiting on prepare
        ctx.tx_manager
            .transition_to_prepared_with_participants(txn_id, participants)?;

        // Mark dirty for lazy persistence
        ctx.mark_dirty(txn_id);

        // Send prepared response
        if phase == ResponseMode::Send {
            ctx.response
                .send_prepared(&coordinator_id, &txn_id.to_string(), Some(request_id));
        }

        Ok(())
    }

    /// Commit a prepared transaction (Phase 2 of 2PC)
    pub fn commit<E: TransactionEngine>(
        ctx: &mut ExecutionContext<E>,
        batch: &mut E::Batch,
        txn_id: TransactionId,
        coordinator_id: String,
        request_id: String,
        phase: ResponseMode,
    ) -> Result<()> {
        ctx.commit_transaction(batch, txn_id, coordinator_id, request_id, phase)
    }

    /// Abort a transaction
    pub fn abort<E: TransactionEngine>(
        ctx: &mut ExecutionContext<E>,
        batch: &mut E::Batch,
        txn_id: TransactionId,
        coordinator_id: String,
        request_id: String,
        phase: ResponseMode,
    ) -> Result<()> {
        // Abort in engine
        ctx.engine.abort(batch, txn_id);

        // Transition to aborted
        ctx.tx_manager
            .transition_to_aborted(txn_id, crate::transaction::AbortReason::Explicit)?;

        // Mark dirty for lazy persistence
        ctx.mark_dirty(txn_id);

        // Send response
        if phase == ResponseMode::Send {
            ctx.response.send_success(
                &coordinator_id,
                Some(&txn_id.to_string()),
                request_id,
                AckResponse::success(),
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{BlockingInfo, OperationResult, RetryOn, TransactionEngine};
    use crate::support::ResponseSender;
    use crate::transaction::TransactionManager;
    use proven_common::{
        Operation, OperationType, ProcessorType, Response, Timestamp, TransactionId,
    };
    use proven_engine::MockClient;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestOp(String);
    impl Operation for TestOp {
        fn operation_type(&self) -> OperationType {
            OperationType::Write
        }

        fn processor_type(&self) -> ProcessorType {
            ProcessorType::Kv
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestResponse(String);
    impl Response for TestResponse {}

    struct TestBatch;
    impl crate::engine::BatchOperations for TestBatch {
        fn insert_transaction_metadata(&mut self, _txn_id: TransactionId, _value: Vec<u8>) {}
        fn remove_transaction_metadata(&mut self, _txn_id: TransactionId) {}
    }

    struct TestEngine {
        should_block: bool,
        operations_executed: Vec<String>,
        blocker_txn: Option<TransactionId>,
        begun: Vec<TransactionId>,
        prepared: Vec<TransactionId>,
        committed: Vec<TransactionId>,
        aborted: Vec<TransactionId>,
    }

    impl TransactionEngine for TestEngine {
        type Operation = TestOp;
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
        ) -> OperationResult<Self::Response> {
            unimplemented!()
        }

        fn apply_operation(
            &mut self,
            _batch: &mut Self::Batch,
            operation: Self::Operation,
            _txn_id: TransactionId,
        ) -> OperationResult<Self::Response> {
            self.operations_executed.push(operation.0.clone());

            if self.should_block {
                let blocker = self.blocker_txn.unwrap_or_default();
                OperationResult::WouldBlock {
                    blockers: vec![BlockingInfo {
                        txn: blocker,
                        retry_on: RetryOn::CommitOrAbort,
                    }],
                }
            } else {
                OperationResult::Complete(TestResponse(format!("executed: {}", operation.0)))
            }
        }

        fn begin(&mut self, _batch: &mut Self::Batch, txn_id: TransactionId) {
            self.begun.push(txn_id);
        }

        fn prepare(&mut self, _batch: &mut Self::Batch, txn_id: TransactionId) {
            self.prepared.push(txn_id);
        }

        fn commit(&mut self, _batch: &mut Self::Batch, txn_id: TransactionId) {
            self.committed.push(txn_id);
        }

        fn abort(&mut self, _batch: &mut Self::Batch, txn_id: TransactionId) {
            self.aborted.push(txn_id);
        }

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

    fn setup() -> (TestEngine, TransactionManager<TestEngine>, ResponseSender) {
        let mock_engine_for_client = Arc::new(proven_engine::MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            mock_engine_for_client,
        ));
        let engine = TestEngine {
            should_block: false,
            operations_executed: Vec::new(),
            blocker_txn: None,
            begun: Vec::new(),
            prepared: Vec::new(),
            committed: Vec::new(),
            aborted: Vec::new(),
        };
        let tx_manager = TransactionManager::new();
        let response = ResponseSender::new(client, "test-stream".to_string(), "test".to_string());

        (engine, tx_manager, response)
    }

    #[tokio::test]
    async fn test_execute_operation() {
        let (mut engine, mut tx_manager, response) = setup();
        let txn_id = TransactionId::new();

        // Begin transaction first
        {
            let mut batch = engine.start_batch();
            tx_manager.begin(
                txn_id,
                "coord-1".to_string(),
                Timestamp::from_micros(10000),
                HashMap::new(),
            );
            engine.begin(&mut batch, txn_id);
            engine.commit_batch(batch, 1);
        }

        // Execute operation
        {
            let mut ctx = ExecutionContext::new(&mut engine, &mut tx_manager, &response);
            let mut batch = ctx.engine.start_batch();

            let result = ReadWriteExecution::execute(
                &mut ctx,
                &mut batch,
                TestOp("write1".to_string()),
                txn_id,
                "coord-1".to_string(),
                "req-2".to_string(),
                ResponseMode::Send,
            );

            ctx.engine.commit_batch(batch, 2);
            assert!(result.is_ok());
        }

        assert_eq!(engine.operations_executed.len(), 1);
        assert_eq!(engine.operations_executed[0], "write1");

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_prepare_commit_flow() {
        let (mut engine, mut tx_manager, response) = setup();
        let txn_id = TransactionId::new();

        // Begin
        {
            let mut batch = engine.start_batch();
            tx_manager.begin(
                txn_id,
                "coord-1".to_string(),
                Timestamp::from_micros(10000),
                HashMap::new(),
            );
            engine.begin(&mut batch, txn_id);
            engine.commit_batch(batch, 1);
        }

        // Execute
        {
            let mut ctx = ExecutionContext::new(&mut engine, &mut tx_manager, &response);
            let mut batch = ctx.engine.start_batch();
            ReadWriteExecution::execute(
                &mut ctx,
                &mut batch,
                TestOp("write1".to_string()),
                txn_id,
                "coord-1".to_string(),
                "req-2".to_string(),
                ResponseMode::Send,
            )
            .unwrap();
            ctx.engine.commit_batch(batch, 2);
        }

        // Prepare
        {
            let mut ctx = ExecutionContext::new(&mut engine, &mut tx_manager, &response);
            let mut batch = ctx.engine.start_batch();
            ReadWriteExecution::prepare(
                &mut ctx,
                &mut batch,
                txn_id,
                "coord-1".to_string(),
                "req-3".to_string(),
                HashMap::new(),
                ResponseMode::Send,
            )
            .unwrap();
            ctx.engine.commit_batch(batch, 3);
        }

        // Commit
        {
            let mut ctx = ExecutionContext::new(&mut engine, &mut tx_manager, &response);
            let mut batch = ctx.engine.start_batch();
            ReadWriteExecution::commit(
                &mut ctx,
                &mut batch,
                txn_id,
                "coord-1".to_string(),
                "req-4".to_string(),
                ResponseMode::Send,
            )
            .unwrap();
            ctx.engine.commit_batch(batch, 4);
        }

        assert_eq!(engine.begun.len(), 1);
        assert_eq!(engine.operations_executed.len(), 1);
        assert_eq!(engine.prepared.len(), 1);
        assert_eq!(engine.committed.len(), 1);
        assert_eq!(engine.aborted.len(), 0);

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_abort_transaction() {
        let (mut engine, mut tx_manager, response) = setup();
        let txn_id = TransactionId::new();

        // Begin
        {
            let mut batch = engine.start_batch();
            tx_manager.begin(
                txn_id,
                "coord-1".to_string(),
                Timestamp::from_micros(10000),
                HashMap::new(),
            );
            engine.begin(&mut batch, txn_id);
            engine.commit_batch(batch, 1);
        }

        // Execute
        {
            let mut ctx = ExecutionContext::new(&mut engine, &mut tx_manager, &response);
            let mut batch = ctx.engine.start_batch();
            ReadWriteExecution::execute(
                &mut ctx,
                &mut batch,
                TestOp("write1".to_string()),
                txn_id,
                "coord-1".to_string(),
                "req-2".to_string(),
                ResponseMode::Send,
            )
            .unwrap();
            ctx.engine.commit_batch(batch, 2);
        }

        // Abort
        {
            let mut ctx = ExecutionContext::new(&mut engine, &mut tx_manager, &response);
            let mut batch = ctx.engine.start_batch();
            ReadWriteExecution::abort(
                &mut ctx,
                &mut batch,
                txn_id,
                "coord-1".to_string(),
                "req-3".to_string(),
                ResponseMode::Send,
            )
            .unwrap();
            ctx.engine.commit_batch(batch, 3);
        }

        assert_eq!(engine.begun.len(), 1);
        assert_eq!(engine.operations_executed.len(), 1);
        assert_eq!(engine.aborted.len(), 1);
        assert_eq!(engine.committed.len(), 0);

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_blocked_operation_defers() {
        let (mut engine, mut tx_manager, response) = setup();

        // Create an older blocker
        let blocker = TransactionId::new();
        std::thread::sleep(std::time::Duration::from_millis(10));

        engine.should_block = true;
        engine.blocker_txn = Some(blocker);

        let txn_id = TransactionId::new();
        {
            let mut batch = engine.start_batch();
            tx_manager.begin(
                txn_id,
                "coord-1".to_string(),
                Timestamp::from_micros(10000),
                HashMap::new(),
            );
            engine.begin(&mut batch, txn_id);
            engine.commit_batch(batch, 1);
        }

        let result = {
            let mut ctx = ExecutionContext::new(&mut engine, &mut tx_manager, &response);
            let mut batch = ctx.engine.start_batch();
            let res = ReadWriteExecution::execute(
                &mut ctx,
                &mut batch,
                TestOp("write1".to_string()),
                txn_id,
                "coord-1".to_string(),
                "req-2".to_string(),
                ResponseMode::Send,
            );
            ctx.engine.commit_batch(batch, 2);
            res
        };

        assert!(result.is_ok());
        assert_eq!(engine.operations_executed.len(), 1);

        // Should have deferred (blocked by older transaction)
        assert_eq!(tx_manager.deferred_count(txn_id), 1);

        tokio::task::yield_now().await;
    }
}
