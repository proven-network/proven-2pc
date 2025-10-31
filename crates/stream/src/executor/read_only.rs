//! Read-only execution path for snapshot isolation reads
//!
//! This executor handles read-only operations that:
//! - Use snapshot isolation (no locks needed)
//! - Can be blocked by exclusive write locks
//! - Bypass the ordered stream (use pubsub)
//! - Don't participate in 2PC

use crate::engine::{OperationResult, TransactionEngine};
use crate::error::Result;
use crate::support::ResponseSender;
use crate::transaction::TransactionManager;
use proven_common::TransactionId;

/// Executes read-only operations using snapshot isolation
pub struct ReadOnlyExecutor<'a, E: TransactionEngine> {
    engine: &'a mut E,
    tx_manager: &'a mut TransactionManager<E>,
    response: &'a ResponseSender,
}

impl<'a, E: TransactionEngine> ReadOnlyExecutor<'a, E> {
    /// Create a new read-only executor
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

    /// Execute a read-only operation
    ///
    /// # Arguments
    /// * `operation` - The operation to execute
    /// * `read_timestamp` - The transaction ID to read at (snapshot)
    /// * `coordinator_id` - Coordinator to send response to
    /// * `request_id` - Request ID for matching responses
    pub fn execute(
        &mut self,
        operation: E::Operation,
        read_timestamp: TransactionId,
        coordinator_id: String,
        request_id: String,
    ) -> Result<()> {
        match self
            .engine
            .read_at_timestamp(operation.clone(), read_timestamp)
        {
            OperationResult::Complete(response) => {
                self.response
                    .send_success(&coordinator_id, None, request_id, response);
                Ok(())
            }
            OperationResult::WouldBlock { blockers } => {
                // Defer until blockers complete
                self.tx_manager.defer_operation(
                    read_timestamp, // Use read timestamp as "transaction ID" for tracking
                    operation,
                    blockers,
                    coordinator_id,
                    request_id,
                    false, // ReadOnly operations are not atomic
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
            OperationType::Read
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestResponse(String);
    impl Response for TestResponse {}

    struct TestEngine {
        should_block: bool,
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
            operation: Self::Operation,
            _read_txn_id: TransactionId,
        ) -> OperationResult<Self::Response> {
            if self.should_block {
                OperationResult::WouldBlock {
                    blockers: vec![BlockingInfo {
                        txn: TransactionId::new(),
                        retry_on: RetryOn::CommitOrAbort,
                    }],
                }
            } else {
                OperationResult::Complete(TestResponse(format!("result: {}", operation.0)))
            }
        }

        fn apply_operation(
            &mut self,
            _batch: &mut Self::Batch,
            _operation: Self::Operation,
            _txn_id: TransactionId,
        ) -> OperationResult<Self::Response> {
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

    fn setup() -> (TestEngine, TransactionManager<TestEngine>, ResponseSender) {
        let mock_engine_for_client = Arc::new(proven_engine::MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            mock_engine_for_client,
        ));
        let engine = TestEngine {
            should_block: false,
        };
        let tx_manager = TransactionManager::new();
        let response = ResponseSender::new(client, "test-stream".to_string(), "test".to_string());

        (engine, tx_manager, response)
    }

    #[tokio::test]
    async fn test_execute_successful_read() {
        let (mut engine, mut tx_manager, response) = setup();
        let mut executor = ReadOnlyExecutor::new(&mut engine, &mut tx_manager, &response);

        let result = executor.execute(
            TestOp("key1".to_string()),
            TransactionId::new(),
            "coord-1".to_string(),
            "req-1".to_string(),
        );

        assert!(result.is_ok());
        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_execute_blocked_read() {
        let (mut engine, mut tx_manager, response) = setup();

        // Make engine block
        engine.should_block = true;

        let mut executor = ReadOnlyExecutor::new(&mut engine, &mut tx_manager, &response);

        let read_timestamp = TransactionId::new();
        let result = executor.execute(
            TestOp("key1".to_string()),
            read_timestamp,
            "coord-1".to_string(),
            "req-1".to_string(),
        );

        assert!(result.is_ok());

        // Should have deferred the operation
        assert_eq!(tx_manager.deferred_count(read_timestamp), 1);
        tokio::task::yield_now().await;
    }
}
