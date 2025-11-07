//! Read-only execution path for snapshot isolation reads
//!
//! This executor handles read-only operations that:
//! - Use snapshot isolation (no locks needed)
//! - Can be blocked by exclusive write locks
//! - Bypass the ordered stream (use pubsub)
//! - Don't participate in 2PC

use crate::engine::TransactionEngine;
use crate::error::Result;
use crate::support::ResponseSender;
use proven_common::TransactionId;

/// Read-only execution - stateless functions
pub struct ReadOnlyExecution;

impl ReadOnlyExecution {
    /// Execute a read-only operation using snapshot isolation
    ///
    /// Read-only operations bypass batching and execute immediately.
    /// They never block - MVCC handles isolation via snapshot reads.
    pub fn execute<E: TransactionEngine>(
        engine: &E,
        response: &ResponseSender,
        operation: E::Operation,
        read_timestamp: TransactionId,
        coordinator_id: String,
        request_id: String,
    ) -> Result<()> {
        let resp = engine.read_at_timestamp(operation, read_timestamp);
        response.send_success(&coordinator_id, None, request_id, resp);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::OperationResult;

    use super::*;
    use proven_common::{ChangeData, Operation, OperationType, ProcessorType, Response};
    use proven_engine::MockClient;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestOp(String);
    impl Operation for TestOp {
        fn operation_type(&self) -> OperationType {
            OperationType::Read
        }

        fn processor_type(&self) -> ProcessorType {
            ProcessorType::Kv
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestResponse(String);
    impl Response for TestResponse {}

    struct TestEngine;

    struct TestBatch;
    impl crate::engine::BatchOperations for TestBatch {
        fn insert_transaction_metadata(&mut self, _txn_id: TransactionId, _value: Vec<u8>) {}
        fn remove_transaction_metadata(&mut self, _txn_id: TransactionId) {}
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct TestChangeData;
    impl ChangeData for TestChangeData {
        fn merge(self, _other: Self) -> Self {
            self
        }
    }

    impl TransactionEngine for TestEngine {
        type Operation = TestOp;
        type Response = TestResponse;
        type ChangeData = TestChangeData;
        type Batch = TestBatch;

        fn start_batch(&mut self) -> Self::Batch {
            TestBatch
        }

        fn commit_batch(&mut self, _batch: Self::Batch, _log_index: u64) {}

        fn read_at_timestamp(
            &self,
            operation: Self::Operation,
            _read_txn_id: TransactionId,
        ) -> Self::Response {
            TestResponse(format!("result: {}", operation.0))
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
        fn commit(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) -> Self::ChangeData {
            TestChangeData
        }
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

    fn setup() -> (TestEngine, ResponseSender) {
        let mock_engine_for_client = Arc::new(proven_engine::MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            mock_engine_for_client,
        ));
        let engine = TestEngine;
        let response = ResponseSender::new(client, "test-stream".to_string(), "test".to_string());

        (engine, response)
    }

    #[tokio::test]
    async fn test_execute_successful_read() {
        let (engine, response) = setup();

        let result = ReadOnlyExecution::execute(
            &engine,
            &response,
            TestOp("key1".to_string()),
            TransactionId::new(),
            "coord-1".to_string(),
            "req-1".to_string(),
        );

        assert!(result.is_ok());
        tokio::task::yield_now().await;
    }
}
