//! Ad-hoc execution path for auto-commit operations
//!
//! This executor handles single operations that:
//! - Auto-begin and auto-commit
//! - Don't participate in 2PC
//! - Abort immediately if blocked (no waiting)

use crate::engine::{OperationResult, TransactionEngine};
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
    /// * `operation` - The operation to execute
    /// * `coordinator_id` - Coordinator to send response to
    /// * `request_id` - Request ID for matching responses
    /// * `log_index` - Log index for persistence
    pub fn execute(
        &mut self,
        operation: E::Operation,
        coordinator_id: String,
        request_id: String,
        _timestamp: Timestamp,
        log_index: u64,
    ) -> Result<()> {
        // Generate a transaction ID for this ad-hoc operation
        let txn_id = TransactionId::new();

        // Begin transaction
        self.engine.begin(txn_id, log_index);

        // Execute the operation
        match self
            .engine
            .apply_operation(operation.clone(), txn_id, log_index)
        {
            OperationResult::Complete(response) => {
                // Immediately commit
                self.engine.commit(txn_id, log_index);

                // Send successful response
                self.response
                    .send_success(&coordinator_id, None, request_id, response);

                // Retry any deferred operations that were waiting on this transaction

                Ok(())
            }
            OperationResult::WouldBlock { blockers } => {
                // Ad-hoc operations can defer and wait for blockers
                // Better UX than forcing clients to retry
                tracing::debug!(
                    "Ad-hoc operation blocked by {:?}, deferring",
                    blockers
                        .iter()
                        .map(|b| b.txn.to_string())
                        .collect::<Vec<_>>()
                );

                self.tx_manager.defer_operation(
                    txn_id,
                    operation,
                    blockers,
                    coordinator_id,
                    request_id,
                );

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

    impl TransactionEngine for TestEngine {
        type Operation = TestOp;
        type Response = TestResponse;

        fn read_at_timestamp(
            &mut self,
            _operation: Self::Operation,
            _read_txn_id: TransactionId,
        ) -> OperationResult<Self::Response> {
            unimplemented!()
        }

        fn apply_operation(
            &mut self,
            operation: Self::Operation,
            _txn_id: TransactionId,
            _log_index: u64,
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

        fn begin(&mut self, _txn_id: TransactionId, _log_index: u64) {}
        fn prepare(&mut self, _txn_id: TransactionId, _log_index: u64) {}
        fn commit(&mut self, _txn_id: TransactionId, _log_index: u64) {}
        fn abort(&mut self, _txn_id: TransactionId, _log_index: u64) {}
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
        let mut executor = AdHocExecutor::new(&mut engine, &mut tx_manager, &response);

        let result = executor.execute(
            TestOp("write1".to_string()),
            "coord-1".to_string(),
            "req-1".to_string(),
            Timestamp::now(),
            1,
        );

        assert!(result.is_ok());
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

        let mut executor = AdHocExecutor::new(&mut engine, &mut tx_manager, &response);

        let result = executor.execute(
            TestOp("write1".to_string()),
            "coord-1".to_string(),
            "req-1".to_string(),
            Timestamp::now(),
            1,
        );

        assert!(result.is_ok());

        // Should have tried to execute (and been blocked)
        assert_eq!(engine.operations_executed.len(), 1);

        // Should have deferred (better UX than aborting)
        assert_eq!(tx_manager.total_deferred_count(), 1);

        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_adhoc_auto_commits_on_success() {
        let (mut engine, mut tx_manager, response) = setup();
        let mut executor = AdHocExecutor::new(&mut engine, &mut tx_manager, &response);

        executor
            .execute(
                TestOp("write1".to_string()),
                "coord-1".to_string(),
                "req-1".to_string(),
                Timestamp::now(),
                1,
            )
            .unwrap();

        // Should have executed
        assert_eq!(engine.operations_executed.len(), 1);

        // Should NOT have deferred (completed successfully)
        assert_eq!(tx_manager.total_deferred_count(), 0);

        tokio::task::yield_now().await;
    }
}
