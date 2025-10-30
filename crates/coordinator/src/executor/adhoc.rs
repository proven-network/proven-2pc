//! Ad-hoc executor for auto-commit operations

use super::{Executor as ExecutorTrait, common::ExecutorInfra};
use crate::error::Result;
use async_trait::async_trait;
use proven_common::{Operation, OperationType};
use proven_common::TransactionId;
use std::collections::HashMap;
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
        // Generate a fresh timestamp for this operation
        let timestamp = TransactionId::new();

        // Use a reasonable timeout
        let timeout = std::time::Duration::from_secs(30);

        // Generate request ID
        let request_id = self.infra.generate_request_id();

        // Ensure processor is running
        self.infra.ensure_processor(stream, timeout).await?;

        // Serialize operation
        let operation_bytes = serde_json::to_vec(operation)
            .map_err(crate::error::CoordinatorError::SerializationError)?;

        // Route based on operation type
        match operation.operation_type() {
            OperationType::Read => {
                // Read operations go through pubsub for better performance
                let mut headers = HashMap::new();
                headers.insert("request_id".to_string(), request_id.clone());
                headers.insert(
                    "coordinator_id".to_string(),
                    self.infra.coordinator_id.clone(),
                );
                headers.insert("read_timestamp".to_string(), timestamp.to_string());
                headers.insert("txn_mode".to_string(), "adhoc".to_string());
                headers.insert("auto_commit".to_string(), "true".to_string());

                // Use readonly pubsub subject pattern
                let subject = format!("stream.{}.readonly", stream);
                let response = self
                    .infra
                    .send_pubsub_and_wait(&subject, headers, operation_bytes, timeout)
                    .await?;

                ExecutorInfra::handle_response(response)
            }
            OperationType::Write => {
                // Write operations go through streams for ordering
                let mut headers = HashMap::new();
                headers.insert("request_id".to_string(), request_id.clone());
                headers.insert(
                    "coordinator_id".to_string(),
                    self.infra.coordinator_id.clone(),
                );
                headers.insert("operation_timestamp".to_string(), timestamp.to_string());
                headers.insert("txn_mode".to_string(), "adhoc".to_string());
                headers.insert("auto_commit".to_string(), "true".to_string());

                let (_, response) = self
                    .infra
                    .send_and_wait(stream, headers, operation_bytes, timeout)
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
