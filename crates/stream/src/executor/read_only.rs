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

/// Read-only execution - stateless functions
pub struct ReadOnlyExecution;

impl ReadOnlyExecution {
    /// Execute a read-only operation using snapshot isolation
    ///
    /// Read-only operations bypass batching and execute immediately.
    /// If blocked by a write lock, they defer until the blocker completes.
    pub fn execute<E: TransactionEngine>(
        engine: &mut E,
        tx_manager: &mut TransactionManager<E>,
        response: &ResponseSender,
        operation: E::Operation,
        read_timestamp: TransactionId,
        coordinator_id: String,
        request_id: String,
    ) -> Result<()> {
        match engine.read_at_timestamp(operation.clone(), read_timestamp) {
            OperationResult::Complete(resp) => {
                // Success - send response immediately
                response.send_success(&coordinator_id, None, request_id, resp);
                Ok(())
            }
            OperationResult::WouldBlock { blockers } => {
                // Blocked by write lock - defer until blocker completes
                // Note: Read-only doesn't use wound-wait (no transaction to wound)
                tx_manager.defer_operation(
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
        fn insert_transaction_metadata(&mut self, _txn_id: TransactionId, _value: Vec<u8>) {}
        fn remove_transaction_metadata(&mut self, _txn_id: TransactionId) {}
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

        let result = ReadOnlyExecution::execute(
            &mut engine,
            &mut tx_manager,
            &response,
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
        engine.should_block = true;

        let read_timestamp = TransactionId::new();
        let result = ReadOnlyExecution::execute(
            &mut engine,
            &mut tx_manager,
            &response,
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
