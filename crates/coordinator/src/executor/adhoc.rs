//! Ad-hoc executor for auto-commit operations

use super::{Executor as ExecutorTrait, common::ExecutorInfra};
use crate::error::Result;
use async_trait::async_trait;
use proven_common::Operation;
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

        // Build headers for ad-hoc operation
        let mut headers = HashMap::new();
        headers.insert("request_id".to_string(), request_id.clone());
        headers.insert(
            "coordinator_id".to_string(),
            self.infra.coordinator_id.clone(),
        );
        headers.insert("operation_timestamp".to_string(), timestamp.to_string());
        headers.insert("txn_mode".to_string(), "adhoc".to_string());
        headers.insert("auto_commit".to_string(), "true".to_string());

        // Ensure processor is running
        self.infra.ensure_processor(stream, timeout).await?;

        // Serialize operation
        let operation_bytes = serde_json::to_vec(operation)
            .map_err(crate::error::CoordinatorError::SerializationError)?;

        // Send and wait for response
        let (_, response) = self
            .infra
            .send_and_wait(stream, headers, operation_bytes, timeout)
            .await?;

        // Handle response
        ExecutorInfra::handle_response(response)
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
