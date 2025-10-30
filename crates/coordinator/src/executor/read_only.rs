//! Read-only executor with snapshot isolation

use super::{Executor as ExecutorTrait, common::ExecutorInfra};
use crate::error::{CoordinatorError, Result};
use crate::speculation::{CheckResult, PredictionContext};
use async_trait::async_trait;
use proven_common::TransactionId;
use proven_common::{Operation, OperationType};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, oneshot};

/// Type alias for speculation response receivers
type SpeculationReceivers = Arc<AsyncMutex<HashMap<usize, oneshot::Receiver<Result<Vec<u8>>>>>>;

/// Read-only executor using consistent snapshot
pub struct ReadOnlyExecutor {
    /// Consistent read timestamp for snapshot isolation
    read_timestamp: TransactionId,

    /// Shared infrastructure
    infra: Arc<ExecutorInfra>,

    /// Speculative execution context for pattern learning
    prediction_context: Arc<AsyncMutex<PredictionContext>>,

    /// Response receivers for speculated operations
    speculation_receivers: SpeculationReceivers,
}

impl ReadOnlyExecutor {
    /// Create a new read-only executor
    pub async fn new(
        read_timestamp: TransactionId,
        infra: Arc<ExecutorInfra>,
        prediction_context: PredictionContext,
    ) -> Self {
        // Execute predictions with read-only headers via pubsub
        let speculation_result = if !prediction_context.predictions().is_empty() {
            let timeout = std::time::Duration::from_secs(30); // Reasonable timeout for reads

            // Build headers for predictions
            let coordinator_id = infra.coordinator_id.clone();
            let read_timestamp_str = read_timestamp.to_string();

            let client = infra.client.clone();
            infra
                .execute_predictions(
                    prediction_context.predictions(),
                    timeout,
                    || {
                        let mut headers = HashMap::new();
                        headers.insert("coordinator_id".to_string(), coordinator_id.clone());
                        headers.insert("read_timestamp".to_string(), read_timestamp_str.clone());
                        headers.insert("txn_mode".to_string(), "read_only".to_string());
                        headers
                    },
                    |streams_and_messages| {
                        let client = client.clone();
                        async move {
                            // Pubsub doesn't have a batched API, so send to each stream separately
                            for (stream, messages) in streams_and_messages {
                                let subject = format!("stream.{}.readonly", stream);
                                client
                                    .publish(&subject, messages)
                                    .await
                                    .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;
                            }
                            Ok(())
                        }
                    },
                )
                .await
        } else {
            Ok(HashMap::new())
        };

        // Store receivers if predictions were executed
        let speculation_receivers = match speculation_result {
            Ok(receivers) => Arc::new(AsyncMutex::new(receivers)),
            Err(e) => {
                tracing::debug!("Failed to execute read predictions: {}", e);
                Arc::new(AsyncMutex::new(HashMap::new()))
            }
        };

        Self {
            read_timestamp,
            infra,
            prediction_context: Arc::new(AsyncMutex::new(prediction_context)),
            speculation_receivers,
        }
    }

    /// Get the read timestamp
    pub fn read_timestamp(&self) -> TransactionId {
        self.read_timestamp
    }

    /// Execute with speculation check
    async fn execute_with_speculation<O: Operation>(
        &self,
        stream: &str,
        operation: &O,
    ) -> Result<Vec<u8>> {
        // First check if this is a read operation
        if operation.operation_type() != OperationType::Read {
            return Err(CoordinatorError::Other(
                "Write operation attempted in read-only transaction. Retry with read-write transaction.".to_string()
            ));
        }

        // Check with predictions
        let op_value =
            serde_json::to_value(operation).map_err(CoordinatorError::SerializationError)?;

        let check_result = {
            let mut pred_context = self.prediction_context.lock().await;
            pred_context.check(stream, &op_value, false)
        };

        match check_result {
            CheckResult::Match { index, .. } => {
                // Get the speculated response from our receivers
                let mut receivers = self.speculation_receivers.lock().await;
                if let Some(receiver) = receivers.remove(&index) {
                    // Wait for the speculated response
                    match receiver.await {
                        Ok(Ok(response)) => Ok(response),
                        Ok(Err(e)) => {
                            // Speculation execution failed - for reads, just execute normally
                            tracing::debug!("Read speculation failed at {}: {}", index, e);
                            self.execute_normal(stream, operation).await
                        }
                        Err(_) => {
                            // Channel closed - execute normally
                            tracing::debug!("Read speculation channel closed for {}", index);
                            self.execute_normal(stream, operation).await
                        }
                    }
                } else {
                    // No receiver available - execute normally
                    self.execute_normal(stream, operation).await
                }
            }
            CheckResult::NoPrediction => {
                // Execute normally
                self.execute_normal(stream, operation).await
            }
            CheckResult::SpeculationMismatch {
                expected,
                actual,
                position,
            } => {
                // For read-only, we can just ignore mismatches and execute normally
                // since we're not modifying state
                tracing::debug!(
                    "Read speculation mismatch at {}: expected {}, got {}",
                    position,
                    expected,
                    actual
                );
                self.execute_normal(stream, operation).await
            }
        }
    }

    /// Normal execution at snapshot timestamp
    async fn execute_normal<O: Operation>(&self, stream: &str, operation: &O) -> Result<Vec<u8>> {
        // Verify this is a read operation
        if operation.operation_type() != OperationType::Read {
            return Err(CoordinatorError::Other(
                "Write operation attempted in read-only transaction. Retry with read-write transaction.".to_string()
            ));
        }

        // Use a reasonable timeout for read operations
        let timeout = std::time::Duration::from_secs(30);

        // Generate request ID
        let request_id = self.infra.generate_request_id();

        // Build headers for snapshot read
        let mut headers = HashMap::new();
        headers.insert("request_id".to_string(), request_id.clone());
        headers.insert(
            "coordinator_id".to_string(),
            self.infra.coordinator_id.clone(),
        );
        headers.insert(
            "read_timestamp".to_string(),
            self.read_timestamp.to_string(),
        );
        headers.insert("txn_mode".to_string(), "read_only".to_string());

        // Ensure processor is running
        self.infra.ensure_processor(stream, timeout).await?;

        // Serialize operation
        let operation_bytes =
            serde_json::to_vec(operation).map_err(CoordinatorError::SerializationError)?;

        // Send via pubsub to stream.{stream_name}.readonly subject
        let subject = format!("stream.{}.readonly", stream);
        let response = self
            .infra
            .send_pubsub_and_wait(&subject, headers, operation_bytes, timeout)
            .await?;

        // Handle response
        ExecutorInfra::handle_response(response)
    }

    /// Report outcome to prediction context
    async fn report_outcome(&self, success: bool) {
        self.prediction_context.lock().await.report_outcome(success);
    }
}

#[async_trait]
impl ExecutorTrait for ReadOnlyExecutor {
    async fn execute<O: Operation + Send + Sync>(
        &self,
        stream: String,
        operation: &O,
    ) -> Result<Vec<u8>> {
        self.execute_with_speculation(&stream, operation).await
    }

    async fn finish(&self) -> Result<()> {
        // Report success for learning
        self.report_outcome(true).await;
        Ok(())
    }

    async fn cancel(&self) -> Result<()> {
        // No cleanup needed for read-only
        // Still report to prediction context for learning
        self.report_outcome(false).await;
        Ok(())
    }
}
