//! Ad-hoc execution path for auto-commit operations
//!
//! This executor handles single operations that:
//! - Auto-begin and auto-commit
//! - Don't participate in 2PC
//! - Use wound-wait protocol if blocked

use crate::engine::{OperationResult, TransactionEngine};
use crate::error::Result;
use crate::executor::context::ExecutionContext;
use crate::processor::ProcessorPhase;
use proven_common::{Timestamp, TransactionId};
use std::collections::HashMap;

/// Ad-hoc execution - stateless functions
pub struct AdHocExecution;

impl AdHocExecution {
    /// Execute an ad-hoc operation (auto-begin, apply, auto-commit)
    ///
    /// Ad-hoc operations are atomic: begin → apply → commit in one batch.
    /// If blocked, they use wound-wait and defer.
    pub fn execute<E: TransactionEngine>(
        ctx: &mut ExecutionContext<E>,
        batch: &mut E::Batch,
        operation: E::Operation,
        coordinator_id: String,
        request_id: String,
        phase: ProcessorPhase,
    ) -> Result<()> {
        let txn_id = TransactionId::new();
        let deadline = Timestamp::now().add_micros(60_000_000); // 60 second deadline

        // Begin transaction
        ctx.engine.begin(batch, txn_id);
        ctx.tx_manager
            .begin(txn_id, coordinator_id.clone(), deadline, HashMap::new());

        // Try to apply operation
        match ctx.engine.apply_operation(batch, operation.clone(), txn_id) {
            OperationResult::Complete(response) => {
                // Success: commit in same batch
                ctx.engine.commit(batch, txn_id);
                ctx.tx_manager.transition_to_committed(txn_id)?;
                ctx.persist_completed_state(batch, txn_id)?;

                // Send response
                if phase == ProcessorPhase::Live {
                    ctx.response
                        .send_success(&coordinator_id, None, request_id, response);
                }
                Ok(())
            }
            OperationResult::WouldBlock { blockers } => {
                // Blocked: use wound-wait protocol
                tracing::debug!(
                    "Ad-hoc operation blocked by {:?}, applying wound-wait",
                    blockers
                        .iter()
                        .map(|b| b.txn.to_string())
                        .collect::<Vec<_>>()
                );

                ctx.handle_blocked(
                    batch,
                    txn_id,
                    operation,
                    blockers,
                    coordinator_id,
                    request_id,
                    true, // is_atomic
                    phase,
                )?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{BlockingInfo, OperationResult, RetryOn, TransactionEngine};
    use crate::support::ResponseSender;
    use crate::transaction::TransactionManager;
    use proven_common::{Operation, OperationType, Response, TransactionId};
    use proven_engine::MockClient;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestOp(String);
    impl Operation for TestOp {
        fn operation_type(&self) -> OperationType {
            OperationType::Write
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

    struct TestEngine {
        should_block: bool,
        operations_executed: Vec<String>,
        blocker_txn: Option<TransactionId>,
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
        };
        let tx_manager = TransactionManager::new();
        let response = ResponseSender::new(client, "test-stream".to_string(), "test".to_string());

        (engine, tx_manager, response)
    }

    #[tokio::test]
    async fn test_execute_successful_adhoc() {
        let (mut engine, mut tx_manager, response) = setup();
        let mut ctx = ExecutionContext::new(&mut engine, &mut tx_manager, &response);
        let mut batch = ctx.engine.start_batch();

        let result = AdHocExecution::execute(
            &mut ctx,
            &mut batch,
            TestOp("write1".to_string()),
            "coord-1".to_string(),
            "req-1".to_string(),
            ProcessorPhase::Live,
        );

        assert!(result.is_ok());
        ctx.engine.commit_batch(batch, 1);

        assert_eq!(ctx.engine.operations_executed.len(), 1);
        assert_eq!(ctx.engine.operations_executed[0], "write1");

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_execute_blocked_adhoc_defers() {
        let (mut engine, mut tx_manager, response) = setup();

        // Create an older blocker (wound-wait won't wound older transactions)
        let blocker = TransactionId::new();
        std::thread::sleep(std::time::Duration::from_millis(10)); // Ensure blocker is older

        engine.should_block = true;
        engine.blocker_txn = Some(blocker);

        let mut ctx = ExecutionContext::new(&mut engine, &mut tx_manager, &response);
        let mut batch = ctx.engine.start_batch();

        let result = AdHocExecution::execute(
            &mut ctx,
            &mut batch,
            TestOp("write1".to_string()),
            "coord-1".to_string(),
            "req-1".to_string(),
            ProcessorPhase::Live,
        );

        assert!(result.is_ok());
        ctx.engine.commit_batch(batch, 1);

        // Should have tried to execute (and been blocked)
        assert_eq!(ctx.engine.operations_executed.len(), 1);

        // Should have deferred (blocked by older transaction)
        assert_eq!(ctx.tx_manager.total_deferred_count(), 1);

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_adhoc_auto_commits_on_success() {
        let (mut engine, mut tx_manager, response) = setup();
        let mut ctx = ExecutionContext::new(&mut engine, &mut tx_manager, &response);
        let mut batch = ctx.engine.start_batch();

        AdHocExecution::execute(
            &mut ctx,
            &mut batch,
            TestOp("write1".to_string()),
            "coord-1".to_string(),
            "req-1".to_string(),
            ProcessorPhase::Live,
        )
        .unwrap();

        ctx.engine.commit_batch(batch, 1);

        // Should have executed
        assert_eq!(ctx.engine.operations_executed.len(), 1);

        // Should NOT have deferred (completed successfully)
        assert_eq!(ctx.tx_manager.total_deferred_count(), 0);

        tokio::task::yield_now().await;
    }
}
