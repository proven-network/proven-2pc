//! Ad-hoc execution path for auto-commit operations
//!
//! This executor handles single operations that:
//! - Auto-begin and auto-commit
//! - Don't participate in 2PC
//! - Abort immediately if blocked (no waiting)

use crate::engine::{BatchOperations, OperationResult, TransactionEngine};
use crate::error::Result;
use crate::support::ResponseSender;
use crate::transaction::TransactionManager;
use proven_common::{Timestamp, TransactionId};

/// Executes ad-hoc operations with auto-commit
pub struct AdHocExecutor<'a, E: TransactionEngine> {
    engine: &'a mut E,
    tx_manager: &'a mut TransactionManager<E>,
    response: &'a ResponseSender,
}

impl<'a, E: TransactionEngine> AdHocExecutor<'a, E> {
    /// Create a new ad-hoc executor
    pub fn new(
        engine: &'a mut E,
        tx_manager: &'a mut TransactionManager<E>,
        response: &'a ResponseSender,
    ) -> Self {
        Self {
            engine,
            tx_manager,
            response,
        }
    }

    /// Execute an ad-hoc operation
    ///
    /// # Arguments
    /// * `batch` - The batch to accumulate operations into
    /// * `operation` - The operation to execute
    /// * `coordinator_id` - Coordinator to send response to
    /// * `request_id` - Request ID for matching responses
    pub fn execute(
        &mut self,
        batch: &mut E::Batch,
        operation: E::Operation,
        coordinator_id: String,
        request_id: String,
        _timestamp: Timestamp,
    ) -> Result<()> {
        // Generate a transaction ID for this ad-hoc operation
        let txn_id = TransactionId::new();

        // ═══════════════════════════════════════════════════════════
        // ADHOC PATTERN: begin → apply → commit in ONE batch
        // ═══════════════════════════════════════════════════════════

        // Begin transaction
        self.engine.begin(batch, txn_id);
        self.tx_manager.begin(
            txn_id,
            coordinator_id.clone(),
            Timestamp::now().add_micros(60_000_000), // 60 second deadline
            std::collections::HashMap::new(),
        );

        // Apply operation
        match self
            .engine
            .apply_operation(batch, operation.clone(), txn_id)
        {
            OperationResult::Complete(response) => {
                // ═══════════════════════════════════════════════════════
                // SUCCESS: Commit in same batch
                // ═══════════════════════════════════════════════════════

                // Commit transaction (adds commit operations to batch)
                self.engine.commit(batch, txn_id);

                // Transition in manager
                self.tx_manager.transition_to_committed(txn_id)?;

                // Add transaction metadata to batch (for completed state)
                let completed_info = self.tx_manager.get_completed(txn_id).ok_or_else(|| {
                    crate::error::ProcessorError::TransactionNotFound(txn_id.to_string())
                })?;

                let state = crate::transaction::state::TransactionState {
                    coordinator_id: completed_info.coordinator_id.clone(),
                    deadline: Timestamp::now(),
                    participants: std::collections::HashMap::new(),
                    phase: completed_info.phase,
                };

                let metadata_key =
                    crate::transaction::state::TransactionState::metadata_key(txn_id);
                let metadata_value = state.to_bytes()?;
                batch.insert_metadata(metadata_key, metadata_value);

                // NOTE: Batch will be committed by dispatcher
                // All changes (begin + apply + commit + metadata) are in the batch

                // Send response
                self.response
                    .send_success(&coordinator_id, None, request_id, response);

                Ok(())
            }

            OperationResult::WouldBlock { blockers } => {
                // ═══════════════════════════════════════════════════════
                // BLOCKED: Defer the operation
                // ═══════════════════════════════════════════════════════

                tracing::debug!(
                    "Ad-hoc operation blocked by {:?}, deferring",
                    blockers
                        .iter()
                        .map(|b| b.txn.to_string())
                        .collect::<Vec<_>>()
                );

                // Defer in manager with is_atomic=true flag
                self.tx_manager.defer_operation(
                    txn_id,
                    operation,
                    blockers,
                    coordinator_id,
                    request_id,
                    true, // ← is_atomic=true (important!)
                );

                // Persist deferral state to batch (transaction metadata)
                let state = self.tx_manager.get(txn_id)?;
                let metadata_key =
                    crate::transaction::state::TransactionState::metadata_key(txn_id);
                let metadata_value = state.to_bytes()?;
                batch.insert_metadata(metadata_key, metadata_value);

                // NOTE: Batch will be committed by dispatcher even though operation was deferred
                // This ensures transaction metadata is persisted for crash recovery

                // Don't send response yet (operation deferred)
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{BlockingInfo, RetryOn};
    use proven_common::{Operation, OperationType, Response};
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

    struct TestEngine {
        should_block: bool,
        operations_executed: Vec<String>,
    }

    struct TestBatch;
    impl crate::engine::BatchOperations for TestBatch {
        fn insert_metadata(&mut self, _key: Vec<u8>, _value: Vec<u8>) {}
        fn remove_metadata(&mut self, _key: Vec<u8>) {}
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
                OperationResult::WouldBlock {
                    blockers: vec![BlockingInfo {
                        txn: TransactionId::new(),
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
        };
        let tx_manager = TransactionManager::new();
        let response = ResponseSender::new(client, "test-stream".to_string(), "test".to_string());

        (engine, tx_manager, response)
    }

    #[tokio::test]
    async fn test_execute_successful_adhoc() {
        let (mut engine, mut tx_manager, response) = setup();
        let mut batch = engine.start_batch();
        {
            let mut executor = AdHocExecutor::new(&mut engine, &mut tx_manager, &response);
            let result = executor.execute(
                &mut batch,
                TestOp("write1".to_string()),
                "coord-1".to_string(),
                "req-1".to_string(),
                Timestamp::now(),
            );
            assert!(result.is_ok());
        }
        engine.commit_batch(batch, 1);

        assert_eq!(engine.operations_executed.len(), 1);
        assert_eq!(engine.operations_executed[0], "write1");

        // Yield to allow spawned tasks to complete
        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_execute_blocked_adhoc_defers() {
        let (mut engine, mut tx_manager, response) = setup();

        // Make engine block
        engine.should_block = true;

        let mut batch = engine.start_batch();
        {
            let mut executor = AdHocExecutor::new(&mut engine, &mut tx_manager, &response);
            let result = executor.execute(
                &mut batch,
                TestOp("write1".to_string()),
                "coord-1".to_string(),
                "req-1".to_string(),
                Timestamp::now(),
            );
            assert!(result.is_ok());
        }
        engine.commit_batch(batch, 1);

        // Should have tried to execute (and been blocked)
        assert_eq!(engine.operations_executed.len(), 1);

        // Should have deferred (better UX than aborting)
        assert_eq!(tx_manager.total_deferred_count(), 1);

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_adhoc_auto_commits_on_success() {
        let (mut engine, mut tx_manager, response) = setup();

        let mut batch = engine.start_batch();
        {
            let mut executor = AdHocExecutor::new(&mut engine, &mut tx_manager, &response);
            executor
                .execute(
                    &mut batch,
                    TestOp("write1".to_string()),
                    "coord-1".to_string(),
                    "req-1".to_string(),
                    Timestamp::now(),
                )
                .unwrap();
        }
        engine.commit_batch(batch, 1);

        // Should have executed
        assert_eq!(engine.operations_executed.len(), 1);

        // Should NOT have deferred (completed successfully)
        assert_eq!(tx_manager.total_deferred_count(), 0);

        tokio::task::yield_now().await;
    }
}
