//! Common utilities shared by executor implementations

use crate::error::{CoordinatorError, Result};
use crate::responses::{ResponseCollector, ResponseMessage};
use crate::speculation::predictor::PredictedOperation;
use proven_engine::{Message, MockClient};
use proven_hlc::HlcTimestamp;
use proven_runner::Runner;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

/// Shared infrastructure for all executors
pub struct ExecutorInfra {
    /// Coordinator ID for routing
    pub coordinator_id: String,

    /// Client for sending messages
    pub client: Arc<MockClient>,

    /// Response collector
    pub response_collector: Arc<ResponseCollector>,

    /// Runner for ensuring processors are running
    pub runner: Arc<Runner>,
}

impl ExecutorInfra {
    /// Create new infrastructure context
    pub fn new(
        coordinator_id: String,
        client: Arc<MockClient>,
        response_collector: Arc<ResponseCollector>,
        runner: Arc<Runner>,
    ) -> Self {
        Self {
            coordinator_id,
            client,
            response_collector,
            runner,
        }
    }

    /// Generate a unique request ID
    pub fn generate_request_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    /// Ensure processor is running for a stream
    pub async fn ensure_processor(&self, stream: &str, timeout: Duration) -> Result<()> {
        self.runner
            .ensure_processor(stream, timeout)
            .await
            .map_err(|e| {
                CoordinatorError::EngineError(format!("Failed to ensure processor: {}", e))
            })?;
        Ok(())
    }

    /// Send a message and wait for response
    pub async fn send_and_wait(
        &self,
        stream: &str,
        headers: HashMap<String, String>,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<ResponseMessage> {
        // Get request ID from headers
        let request_id = headers
            .get("request_id")
            .ok_or_else(|| CoordinatorError::Other("Missing request_id in headers".to_string()))?
            .clone();

        // Send message
        let message = Message::new(body, headers);
        self.client
            .publish_to_stream(stream.to_string(), vec![message])
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        // Wait for response
        self.response_collector
            .wait_for_response(request_id, timeout)
            .await
    }

    /// Send messages to multiple streams (fire and forget)
    pub async fn send_messages_batch(
        &self,
        messages: Vec<(String, HashMap<String, String>, Vec<u8>)>,
    ) -> Result<()> {
        let mut streams_and_messages = HashMap::new();

        for (stream, headers, body) in messages {
            streams_and_messages
                .entry(stream)
                .or_insert_with(Vec::new)
                .push(Message::new(body, headers));
        }

        self.client
            .publish_to_streams(streams_and_messages)
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        Ok(())
    }

    /// Execute predictions speculatively
    ///
    /// Takes predictions and a function to build headers for each stream/operation pair
    /// Returns receivers for each prediction's result
    pub async fn execute_predictions<F>(
        &self,
        predictions: &[PredictedOperation],
        timeout: Duration,
        mut build_headers: F,
    ) -> Result<HashMap<usize, oneshot::Receiver<Result<Vec<u8>>>>>
    where
        F: FnMut(&str) -> HashMap<String, String>,
    {
        if predictions.is_empty() {
            return Ok(HashMap::new());
        }

        // Group operations by stream while maintaining order
        let mut stream_operations: HashMap<String, Vec<(usize, Vec<u8>)>> = HashMap::new();

        for (index, pred_op) in predictions.iter().enumerate() {
            // Serialize the operation
            let operation_bytes = serde_json::to_vec(&pred_op.operation)
                .map_err(CoordinatorError::SerializationError)?;

            stream_operations
                .entry(pred_op.stream.clone())
                .or_default()
                .push((index, operation_bytes));
        }

        // Create channels for all operations
        let mut receivers = HashMap::new();
        let mut senders = HashMap::new();

        for index in 0..predictions.len() {
            let (tx, rx) = oneshot::channel();
            receivers.insert(index, rx);
            senders.insert(index, tx);
        }

        // Execute operations for each stream concurrently
        let mut handles = Vec::new();

        for (stream, operations) in stream_operations {
            // Build headers for this stream
            let headers = build_headers(&stream);

            // Clone what we need for the async task
            let client = self.client.clone();
            let response_collector = self.response_collector.clone();
            let runner = self.runner.clone();
            let stream_senders: Vec<_> = operations
                .iter()
                .map(|(idx, _)| (*idx, senders.remove(idx).unwrap()))
                .collect();

            let handle = tokio::spawn(async move {
                // Ensure processor is running
                if let Err(e) = runner.ensure_processor(&stream, timeout).await {
                    for (_, sender) in stream_senders {
                        let _ = sender.send(Err(CoordinatorError::EngineError(format!(
                            "Failed to ensure processor: {}",
                            e
                        ))));
                    }
                    return;
                }

                // Execute each operation in order for this stream
                for ((_, body), (_, sender)) in operations.into_iter().zip(stream_senders) {
                    let request_id = uuid::Uuid::new_v4().to_string();
                    let mut op_headers = headers.clone();
                    op_headers.insert("request_id".to_string(), request_id.clone());

                    // Send message
                    let message = Message::new(body, op_headers);
                    if let Err(e) = client
                        .publish_to_stream(stream.clone(), vec![message])
                        .await
                    {
                        let _ = sender.send(Err(CoordinatorError::EngineError(e.to_string())));
                        continue;
                    }

                    // Wait for response
                    let result = response_collector
                        .wait_for_response(request_id, timeout)
                        .await
                        .and_then(Self::handle_response);

                    let _ = sender.send(result);
                }
            });

            handles.push(handle);
        }

        // Wait for all to complete
        for handle in handles {
            let _ = handle.await;
        }

        Ok(receivers)
    }

    /// Handle response from engine
    pub fn handle_response(response: ResponseMessage) -> Result<Vec<u8>> {
        match response {
            ResponseMessage::Complete { body, .. } => Ok(body),
            ResponseMessage::Wounded { wounded_by, .. } => {
                Err(CoordinatorError::TransactionWounded {
                    wounded_by: wounded_by.to_string(),
                })
            }
            ResponseMessage::Error { kind, .. } => {
                use crate::responses::ErrorKind;
                match kind {
                    ErrorKind::DeadlineExceeded => Err(CoordinatorError::DeadlineExceeded),
                    ErrorKind::InvalidDeadlineFormat => Err(CoordinatorError::Other(
                        "Invalid deadline format".to_string(),
                    )),
                    ErrorKind::DeadlineRequired => {
                        Err(CoordinatorError::Other("Deadline required".to_string()))
                    }
                    ErrorKind::PrepareAfterDeadline => Err(CoordinatorError::Other(
                        "Prepare after deadline".to_string(),
                    )),
                    ErrorKind::SerializationFailed(msg) => Err(CoordinatorError::Other(format!(
                        "Serialization failed: {}",
                        msg
                    ))),
                    ErrorKind::EngineError(msg) => Err(CoordinatorError::EngineError(msg)),
                    ErrorKind::Unknown(msg) => Err(CoordinatorError::Other(msg)),
                }
            }
            ResponseMessage::Prepared { .. } => Err(CoordinatorError::Other(
                "Unexpected Prepared response in execute".to_string(),
            )),
        }
    }
}

/// Calculate timeout from deadline
pub fn calculate_timeout(deadline: HlcTimestamp) -> Result<Duration> {
    // Get current time as microseconds
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;

    if deadline.physical <= now_us {
        return Err(CoordinatorError::DeadlineExceeded);
    }
    let remaining_us = deadline.physical.saturating_sub(now_us);
    Ok(Duration::from_micros(remaining_us))
}
