//! Ad-hoc executor for auto-commit operations

use super::{Executor as ExecutorTrait, common::ExecutorInfra};
use crate::error::Result;
use async_trait::async_trait;
use proven_common::{Operation, OperationType, TransactionId};
use std::sync::Arc;

/// Ad-hoc executor with auto-commit operations
pub struct AdHocExecutor {
    /// Shared infrastructure
    infra: Arc<ExecutorInfra>,
}

impl AdHocExecutor {
    /// Create a new ad-hoc executor
    pub fn new(infra: Arc<ExecutorInfra>) -> Self {
        Self { infra }
    }

    /// Execute a single auto-commit operation
    async fn execute_adhoc<O: Operation>(&self, stream: &str, operation: &O) -> Result<Vec<u8>> {
        // Generate a fresh transaction ID for this operation (contains timestamp for MVCC)
        let txn_id = TransactionId::new();

        // Set deadline for this operation (30 seconds from now)
        let deadline = proven_common::Timestamp::now().add_micros(30_000_000);
        let timeout = std::time::Duration::from_secs(30);

        // Route based on operation type
        match operation.operation_type() {
            OperationType::Read => {
                // Read operations use snapshot isolation via pubsub
                let response = self
                    .infra
                    .send_readonly_operation(stream, txn_id, operation.clone(), timeout)
                    .await?;

                ExecutorInfra::handle_response(response)
            }
            OperationType::Write => {
                // Write operations use auto-commit via ordered stream
                let (_, response) = self
                    .infra
                    .send_auto_commit_operation(
                        stream,
                        txn_id,
                        deadline,
                        operation.clone(),
                        timeout,
                    )
                    .await?;

                ExecutorInfra::handle_response(response)
            }
        }
    }
}

#[async_trait]
impl ExecutorTrait for AdHocExecutor {
    async fn execute<O: Operation + Send + Sync>(
        &self,
        stream: String,
        operation: &O,
    ) -> Result<Vec<u8>> {
        // Each operation is independent with auto-commit
        self.execute_adhoc(&stream, operation).await
    }

    async fn finish(&self) -> Result<()> {
        // No-op for ad-hoc operations
        Ok(())
    }

    async fn cancel(&self) -> Result<()> {
        // No-op for ad-hoc operations
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestReadOperation {
        query: String,
    }

    impl Operation for TestReadOperation {
        fn operation_type(&self) -> OperationType {
            OperationType::Read
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestWriteOperation {
        data: String,
    }

    impl Operation for TestWriteOperation {
        fn operation_type(&self) -> OperationType {
            OperationType::Write
        }
    }

    #[test]
    fn test_operation_type_classification() {
        let read_op = TestReadOperation {
            query: "SELECT * FROM users".to_string(),
        };
        assert_eq!(read_op.operation_type(), OperationType::Read);

        let write_op = TestWriteOperation {
            data: "INSERT INTO users VALUES (1)".to_string(),
        };
        assert_eq!(write_op.operation_type(), OperationType::Write);
    }
}
