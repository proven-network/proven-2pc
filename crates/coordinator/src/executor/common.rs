//! Common utilities shared by executor implementations

use crate::error::{CoordinatorError, Result};
use crate::responses::{ResponseCollector, ResponseMessage};
use crate::speculation::predictor::PredictedOperation;
use proven_common::{Operation, ProcessorType, Timestamp, TransactionId};
use proven_engine::{Message, MockClient};
use proven_protocol::{OrderedMessage, ReadOnlyMessage, TransactionPhase};
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
    pub async fn ensure_processor(
        &self,
        stream: &str,
        processor_type: ProcessorType,
        timeout: Duration,
    ) -> Result<()> {
        self.runner
            .ensure_processor(stream, processor_type, timeout)
            .await
            .map_err(|e| {
                CoordinatorError::EngineError(format!("Failed to ensure processor: {}", e))
            })?;
        Ok(())
    }

    /// Send a message and wait for response, returning both offset and response
    pub async fn send_and_wait(
        &self,
        stream: &str,
        headers: HashMap<String, String>,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<(u64, ResponseMessage)> {
        // Get request ID from headers
        let request_id = headers
            .get("request_id")
            .ok_or_else(|| CoordinatorError::Other("Missing request_id in headers".to_string()))?
            .clone();

        // Send message and capture offset
        let message = Message::new(body, headers);
        let offset = self
            .client
            .publish_to_stream(stream.to_string(), vec![message])
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        // Wait for response
        let response = self
            .response_collector
            .wait_for_response(request_id, timeout)
            .await?;

        Ok((offset, response))
    }

    /// Send a message via pubsub and wait for response
    pub async fn send_pubsub_and_wait(
        &self,
        subject: &str,
        headers: HashMap<String, String>,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<ResponseMessage> {
        // Get request ID from headers
        let request_id = headers
            .get("request_id")
            .ok_or_else(|| CoordinatorError::Other("Missing request_id in headers".to_string()))?
            .clone();

        // Send message via pubsub
        let message = Message::new(body, headers);
        self.client
            .publish(subject, vec![message])
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        // Wait for response (same response channel)
        self.response_collector
            .wait_for_response(request_id, timeout)
            .await
    }

    /// Send messages to multiple streams in parallel (fire and forget)
    ///
    /// This publishes to each stream independently and in parallel, allowing
    /// streams to be in different consensus groups without artificial constraints.
    pub async fn send_messages_batch(
        &self,
        messages: Vec<(String, HashMap<String, String>, Vec<u8>)>,
    ) -> Result<()> {
        let mut streams_and_messages: HashMap<String, Vec<Message>> = HashMap::new();

        for (stream, headers, body) in messages {
            streams_and_messages
                .entry(stream)
                .or_default()
                .push(Message::new(body, headers));
        }

        // Publish to each stream in parallel
        let mut tasks = tokio::task::JoinSet::new();

        for (stream, messages) in streams_and_messages {
            let client = self.client.clone();
            tasks.spawn(async move {
                client
                    .publish_to_stream(stream, messages)
                    .await
                    .map_err(|e| CoordinatorError::EngineError(e.to_string()))
            });
        }

        // Wait for all publishes to complete
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(e),
                Err(_join_err) => {
                    return Err(CoordinatorError::EngineError(
                        "Task failed during batch send".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Send a transaction operation message (typed, safe)
    pub async fn send_transaction_operation<O: Operation>(
        &self,
        stream: &str,
        txn_id: TransactionId,
        txn_deadline: Timestamp,
        operation: O,
        timeout: Duration,
    ) -> Result<(u64, ResponseMessage)> {
        let request_id = self.generate_request_id();

        // Ensure processor is running
        self.ensure_processor(stream, operation.processor_type(), timeout)
            .await?;

        // Build typed message
        let message = OrderedMessage::TransactionOperation {
            txn_id,
            coordinator_id: self.coordinator_id.clone(),
            request_id: request_id.clone(),
            txn_deadline,
            operation,
        };

        // Send and wait
        let offset = self
            .client
            .publish_to_stream(stream.to_string(), vec![message.into_message()])
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        let response = self
            .response_collector
            .wait_for_response(request_id, timeout)
            .await?;

        Ok((offset, response))
    }

    /// Send an auto-commit operation message (typed, safe)
    pub async fn send_auto_commit_operation<O: Operation>(
        &self,
        stream: &str,
        txn_id: TransactionId,
        txn_deadline: Timestamp,
        operation: O,
        timeout: Duration,
    ) -> Result<(u64, ResponseMessage)> {
        let request_id = self.generate_request_id();

        // Ensure processor is running
        self.ensure_processor(stream, operation.processor_type(), timeout)
            .await?;

        // Build typed message
        let message = OrderedMessage::AutoCommitOperation {
            txn_id,
            coordinator_id: self.coordinator_id.clone(),
            request_id: request_id.clone(),
            txn_deadline,
            operation,
        };

        // Send and wait
        let offset = self
            .client
            .publish_to_stream(stream.to_string(), vec![message.into_message()])
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        let response = self
            .response_collector
            .wait_for_response(request_id, timeout)
            .await?;

        Ok((offset, response))
    }

    /// Send a read-only operation message via pubsub (typed, safe)
    pub async fn send_readonly_operation<O: Operation>(
        &self,
        stream: &str,
        read_timestamp: TransactionId,
        operation: O,
        timeout: Duration,
    ) -> Result<ResponseMessage> {
        let request_id = self.generate_request_id();

        // Ensure processor is running
        self.ensure_processor(stream, operation.processor_type(), timeout)
            .await?;

        // Build typed message
        let message = ReadOnlyMessage {
            read_timestamp,
            coordinator_id: self.coordinator_id.clone(),
            request_id: request_id.clone(),
            operation,
        };

        // Send via pubsub
        let subject = format!("stream.{}.readonly", stream);
        self.client
            .publish(&subject, vec![message.into_message()])
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        // Wait for response
        self.response_collector
            .wait_for_response(request_id, timeout)
            .await
    }

    /// Send a transaction control message (prepare/commit/abort)
    ///
    /// If request_id and timeout are provided, waits for a response.
    /// Otherwise, sends fire-and-forget.
    pub async fn send_control_message(
        &self,
        stream: &str,
        txn_id: TransactionId,
        phase: TransactionPhase,
        request_id: Option<String>,
        timeout: Option<Duration>,
        txn_deadline: Timestamp,
    ) -> Result<Option<ResponseMessage>> {
        // Build control message manually (no operation needed)
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("txn_phase".to_string(), phase.phase_name().to_string());
        headers.insert("coordinator_id".to_string(), self.coordinator_id.clone());
        headers.insert("txn_deadline".to_string(), txn_deadline.to_string());

        // Add request_id if provided (for prepare votes)
        if let Some(ref req_id) = request_id {
            headers.insert("request_id".to_string(), req_id.clone());
        }

        // Add participants if this is a prepare phase
        if let Some(participants) = phase.participants() {
            headers.insert(
                "participants".to_string(),
                serde_json::to_string(&participants).unwrap(),
            );
        }

        let message = Message::new(Vec::new(), headers);

        let _ = self
            .client
            .publish_to_stream(stream.to_string(), vec![message])
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        // Wait for response if request_id and timeout provided
        if let (Some(req_id), Some(timeout)) = (request_id, timeout) {
            let response = self
                .response_collector
                .wait_for_response(req_id, timeout)
                .await?;
            Ok(Some(response))
        } else {
            Ok(None)
        }
    }

    /// Execute predictions speculatively
    ///
    /// Takes predictions, a function to build headers, and a function to send batched messages
    /// Returns receivers for each prediction's result
    ///
    /// The send_messages closure receives a HashMap<String, Vec<Message>> where the key is the
    /// stream name, allowing for parallel sending to multiple independent streams
    pub async fn execute_predictions<H, S, Fut>(
        &self,
        predictions: &[PredictedOperation],
        timeout: Duration,
        mut build_headers: H,
        send_messages: S,
    ) -> Result<HashMap<usize, oneshot::Receiver<Result<Vec<u8>>>>>
    where
        H: FnMut() -> HashMap<String, String>,
        S: FnOnce(HashMap<String, Vec<Message>>) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
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

        // Ensure all processors are running (using processor_type from predictions)
        let mut stream_processor_types: HashMap<String, ProcessorType> = HashMap::new();
        for pred_op in predictions.iter() {
            stream_processor_types.insert(pred_op.stream.clone(), pred_op.processor_type);
        }

        for (stream, processor_type) in stream_processor_types {
            if let Err(e) = self
                .runner
                .ensure_processor(&stream, processor_type, timeout)
                .await
            {
                // Mark all operations for this stream as failed
                for (idx, _) in stream_operations.get(&stream).unwrap_or(&vec![]) {
                    if let Some(sender) = senders.remove(idx) {
                        let _ = sender.send(Err(CoordinatorError::EngineError(format!(
                            "Failed to ensure processor: {}",
                            e
                        ))));
                    }
                }
            }
        }

        // Build headers once
        let headers = build_headers();

        // Build all messages for all streams with unique request IDs
        let mut all_messages: HashMap<String, Vec<Message>> = HashMap::new();
        let mut request_ids = Vec::new();

        for (stream, operations) in &stream_operations {
            let stream_messages = all_messages.entry(stream.clone()).or_default();

            for (idx, body) in operations {
                let request_id = uuid::Uuid::new_v4().to_string();
                let mut op_headers = headers.clone();
                op_headers.insert("request_id".to_string(), request_id.clone());

                stream_messages.push(Message::new(body.clone(), op_headers));
                request_ids.push((*idx, request_id));
            }
        }

        // Send all messages at once via caller-provided closure
        if let Err(e) = send_messages(all_messages).await {
            let error_msg = format!("{}", e);
            for (idx, _) in request_ids {
                if let Some(sender) = senders.remove(&idx) {
                    let _ = sender.send(Err(CoordinatorError::EngineError(error_msg.clone())));
                }
            }
            return Ok(receivers);
        }

        // Wait for all responses
        for (idx, request_id) in request_ids {
            let result = self
                .response_collector
                .wait_for_response(request_id, timeout)
                .await
                .and_then(Self::handle_response);

            if let Some(sender) = senders.remove(&idx) {
                let _ = sender.send(result);
            }
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
pub fn calculate_timeout(deadline: Timestamp) -> Result<Duration> {
    let now = Timestamp::now();

    if deadline <= now {
        return Err(CoordinatorError::DeadlineExceeded);
    }

    // Calculate remaining duration in microseconds
    let remaining_micros = deadline.duration_since(&now);
    Ok(Duration::from_micros(remaining_micros))
}
