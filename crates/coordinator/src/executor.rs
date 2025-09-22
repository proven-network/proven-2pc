//! Transaction-scoped executor that handles all message-level concerns
//!
//! The Executor is created per-transaction and holds all transaction context
//! including deadline, participants, and infrastructure connections. It provides
//! a unified execution path for both normal and speculative operations.

use crate::error::{CoordinatorError, Result};
use crate::responses::{ResponseCollector, ResponseMessage};
use parking_lot::Mutex;
use proven_common::Operation;
use proven_engine::{Message, MockClient};
use proven_hlc::HlcTimestamp;
use proven_runner::Runner;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;

/// Transaction-scoped executor with all context needed for execution
pub struct Executor {
    /// Transaction ID
    pub txn_id: String,

    /// Transaction timestamp
    pub timestamp: HlcTimestamp,

    /// Transaction deadline
    pub deadline: HlcTimestamp,

    /// Coordinator ID for routing
    coordinator_id: String,

    /// Participants in this transaction
    participants: Arc<Mutex<Vec<String>>>,

    /// Streams that have received the deadline
    streams_with_deadline: Arc<Mutex<HashSet<String>>>,

    /// Which participants each stream knows about
    participant_awareness: Arc<Mutex<HashMap<String, HashSet<String>>>>,

    /// Offset tracking for participants
    participant_offsets: Arc<Mutex<HashMap<String, u64>>>,

    /// Client for sending messages
    client: Arc<MockClient>,

    /// Response collector
    response_collector: Arc<ResponseCollector>,

    /// Runner for ensuring processors are running
    runner: Arc<Runner>,

    /// Request counter for generating unique request IDs
    request_counter: Arc<AtomicU64>,
}

impl Executor {
    /// Create a new transaction-scoped executor
    pub fn new(
        txn_id: String,
        timestamp: HlcTimestamp,
        deadline: HlcTimestamp,
        coordinator_id: String,
        client: Arc<MockClient>,
        response_collector: Arc<ResponseCollector>,
        runner: Arc<Runner>,
    ) -> Self {
        Self {
            txn_id,
            timestamp,
            deadline,
            coordinator_id,
            participants: Arc::new(Mutex::new(Vec::new())),
            streams_with_deadline: Arc::new(Mutex::new(HashSet::new())),
            participant_awareness: Arc::new(Mutex::new(HashMap::new())),
            participant_offsets: Arc::new(Mutex::new(HashMap::new())),
            client,
            response_collector,
            runner,
            request_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Execute an operation on a stream
    ///
    /// This method handles all protocol-level concerns:
    /// - Deadline checking and propagation
    /// - Participant tracking
    /// - Header formatting
    /// - Response collection
    pub async fn execute(&self, stream: &str, operation_bytes: &[u8]) -> Result<Vec<u8>> {
        // Calculate timeout from deadline
        let timeout = self.calculate_timeout()?;

        // Check if this is first contact with stream and get deadline string if needed
        let deadline_str = self.track_participant(stream);

        // Generate request ID
        let request_id = self.generate_request_id(stream);

        // Build headers
        let mut headers = self.build_base_headers(&request_id);
        if let Some(deadline) = deadline_str {
            headers.insert("txn_deadline".to_string(), deadline);
        }

        // Ensure processor is running
        self.runner
            .ensure_processor(stream, timeout)
            .await
            .map_err(|e| {
                CoordinatorError::EngineError(format!("Failed to ensure processor: {}", e))
            })?;

        // Send message
        let message = Message::new(operation_bytes.to_vec(), headers);
        self.client
            .publish_to_stream(stream.to_string(), vec![message])
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        // Wait for response
        let response = self
            .response_collector
            .wait_for_response(request_id, timeout)
            .await?;

        // Handle response
        self.handle_response(response)
    }

    /// Execute an operation from a typed Operation trait object
    pub async fn execute_operation<O: Operation>(
        &self,
        stream: &str,
        operation: &O,
    ) -> Result<Vec<u8>> {
        let operation_bytes =
            serde_json::to_vec(operation).map_err(CoordinatorError::SerializationError)?;
        self.execute(stream, &operation_bytes).await
    }

    /// Execute an operation asynchronously and return a receiver for the response
    ///
    /// This method sends the operation immediately but returns a oneshot receiver
    /// that can be awaited later. This enables parallel execution of multiple operations.
    pub async fn execute_async(
        &self,
        stream: &str,
        operation_bytes: &[u8],
    ) -> Result<oneshot::Receiver<Result<Vec<u8>>>> {
        // Generate request_id and determine timeout based on stream
        let timeout = if self.streams_with_deadline.lock().contains(stream) {
            Duration::from_secs(30)
        } else {
            Duration::from_secs(5)
        };

        let deadline_str = self
            .streams_with_deadline
            .lock()
            .contains(stream)
            .then(|| self.deadline.to_string());
        let request_id = self.generate_request_id(stream);

        // Build headers
        let mut headers = self.build_base_headers(&request_id);
        if let Some(deadline) = deadline_str {
            headers.insert("txn_deadline".to_string(), deadline);
        }

        // Ensure processor is running
        self.runner
            .ensure_processor(stream, timeout)
            .await
            .map_err(|e| {
                CoordinatorError::EngineError(format!("Failed to ensure processor: {}", e))
            })?;

        // Create oneshot channel for the response
        let (tx, rx) = oneshot::channel();

        // Send message immediately
        let message = Message::new(operation_bytes.to_vec(), headers);
        self.client
            .publish_to_stream(stream.to_string(), vec![message])
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        // Spawn a task to wait for the response
        let response_collector = self.response_collector.clone();
        let request_id_clone = request_id.clone();
        tokio::spawn(async move {
            // Wait for response and send through channel
            let result = response_collector
                .wait_for_response(request_id_clone, timeout)
                .await
                .and_then(Self::handle_response_static);

            // Send result through channel (ignore if receiver dropped)
            let _ = tx.send(result);
        });

        Ok(rx)
    }

    /// Execute operations across multiple streams in a single batch
    ///
    /// This method sends operations to multiple streams in a single network round-trip,
    /// amortizing latency when all streams are in the same consensus group.
    /// Returns a map of (stream, operation_index) -> receiver
    pub async fn execute_multi_stream_batch(
        &self,
        stream_operations: HashMap<String, Vec<Vec<u8>>>,
    ) -> Result<HashMap<(String, usize), oneshot::Receiver<Result<Vec<u8>>>>> {
        use std::collections::HashMap;

        if stream_operations.is_empty() {
            return Ok(HashMap::new());
        }

        // Determine timeout (use max timeout if any stream has deadline)
        let has_deadline = stream_operations
            .keys()
            .any(|s| self.streams_with_deadline.lock().contains(s));
        let timeout = if has_deadline {
            Duration::from_secs(30)
        } else {
            Duration::from_secs(5)
        };

        let deadline_str = has_deadline.then(|| self.deadline.to_string());

        // Ensure all processors are running
        for stream in stream_operations.keys() {
            self.runner
                .ensure_processor(stream, timeout)
                .await
                .map_err(|e| {
                    CoordinatorError::EngineError(format!("Failed to ensure processor: {}", e))
                })?;
        }

        // Build all messages and track mappings
        let mut streams_and_messages = HashMap::new();
        let mut all_receivers = HashMap::new();
        let mut request_mappings = Vec::new();

        for (stream, operations) in stream_operations {
            let mut messages = Vec::with_capacity(operations.len());

            for (op_idx, operation_bytes) in operations.into_iter().enumerate() {
                let request_id = self.generate_request_id(&stream);

                // Build headers
                let mut headers = self.build_base_headers(&request_id);
                if let Some(deadline) = &deadline_str {
                    headers.insert("txn_deadline".to_string(), deadline.clone());
                }

                // Create message
                messages.push(Message::new(operation_bytes, headers));

                // Create oneshot channel
                let (tx, rx) = oneshot::channel();
                all_receivers.insert((stream.clone(), op_idx), rx);
                request_mappings.push((request_id, tx));
            }

            streams_and_messages.insert(stream, messages);
        }

        // Send all messages in a single batched call (amortizes network latency)
        self.client
            .publish_to_streams(streams_and_messages)
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        // Spawn tasks to wait for each response
        let response_collector = self.response_collector.clone();
        for (request_id, tx) in request_mappings {
            let collector = response_collector.clone();
            tokio::spawn(async move {
                let result = collector
                    .wait_for_response(request_id, timeout)
                    .await
                    .and_then(Self::handle_response_static);

                // Send result through channel (ignore if receiver dropped)
                let _ = tx.send(result);
            });
        }

        Ok(all_receivers)
    }

    /// Execute multiple operations on the same stream as a batch
    ///
    /// This ensures all operations for a stream are sent together in order,
    /// which is critical for maintaining consistency. Returns receivers for each operation.
    pub async fn execute_batch_async(
        &self,
        stream: &str,
        operations: &[Vec<u8>],
    ) -> Result<Vec<oneshot::Receiver<Result<Vec<u8>>>>> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        // Determine timeout based on stream
        let timeout = if self.streams_with_deadline.lock().contains(stream) {
            Duration::from_secs(30)
        } else {
            Duration::from_secs(5)
        };

        let deadline_str = self
            .streams_with_deadline
            .lock()
            .contains(stream)
            .then(|| self.deadline.to_string());

        // Ensure processor is running
        self.runner
            .ensure_processor(stream, timeout)
            .await
            .map_err(|e| {
                CoordinatorError::EngineError(format!("Failed to ensure processor: {}", e))
            })?;

        // Create messages and channels for all operations
        let mut messages = Vec::with_capacity(operations.len());
        let mut receivers = Vec::with_capacity(operations.len());
        let mut request_mappings = Vec::with_capacity(operations.len());

        for operation_bytes in operations {
            let request_id = self.generate_request_id(stream);

            // Build headers
            let mut headers = self.build_base_headers(&request_id);
            if let Some(deadline) = &deadline_str {
                headers.insert("txn_deadline".to_string(), deadline.clone());
            }

            // Create message
            messages.push(Message::new(operation_bytes.clone(), headers));

            // Create oneshot channel
            let (tx, rx) = oneshot::channel();
            receivers.push(rx);
            request_mappings.push((request_id, tx));
        }

        // Send all messages as a batch
        self.client
            .publish_to_stream(stream.to_string(), messages)
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        // Spawn tasks to wait for each response
        let response_collector = self.response_collector.clone();
        for (request_id, tx) in request_mappings {
            let collector = response_collector.clone();
            tokio::spawn(async move {
                let result = collector
                    .wait_for_response(request_id, timeout)
                    .await
                    .and_then(Self::handle_response_static);

                // Send result through channel (ignore if receiver dropped)
                let _ = tx.send(result);
            });
        }

        Ok(receivers)
    }

    /// Static version of handle_response for use in async tasks
    fn handle_response_static(response: ResponseMessage) -> Result<Vec<u8>> {
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
            ResponseMessage::Prepared { .. } => {
                // Prepared is handled differently in Transaction
                Err(CoordinatorError::Other(
                    "Unexpected Prepared response in execute".to_string(),
                ))
            }
        }
    }

    /// Send a message to a stream without waiting for response
    pub async fn send_message(
        &self,
        stream: &str,
        headers: HashMap<String, String>,
        body: Vec<u8>,
    ) -> Result<()> {
        let message = Message::new(body, headers);
        self.client
            .publish_to_stream(stream.to_string(), vec![message])
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;
        Ok(())
    }

    /// Send messages to multiple streams in a single batch (fire and forget)
    /// This amortizes network latency for 2PC phase messages
    pub async fn send_messages_batch(
        &self,
        messages: Vec<(String, HashMap<String, String>, Vec<u8>)>, // (stream, headers, body)
    ) -> Result<()> {
        use std::collections::HashMap;

        if messages.is_empty() {
            return Ok(());
        }

        // Group messages by stream
        let mut stream_messages: HashMap<String, Vec<Message>> = HashMap::new();
        for (stream, headers, body) in messages {
            stream_messages
                .entry(stream)
                .or_default()
                .push(Message::new(body, headers));
        }

        // Send all messages in a single batch
        self.client
            .publish_to_streams(stream_messages)
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

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

    /// Generate a unique request ID for a phase
    pub fn generate_phase_request_id(&self, phase: &str) -> String {
        format!(
            "{}-{}-{}",
            phase,
            self.txn_id,
            self.request_counter.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Build headers for a phase message
    pub fn build_phase_headers(&self, phase: &str, request_id: &str) -> HashMap<String, String> {
        let mut headers = self.build_base_headers(request_id);
        headers.insert("txn_phase".to_string(), phase.to_string());
        headers
    }

    /// Get the current deadline timeout
    pub fn get_timeout(&self) -> Result<Duration> {
        self.calculate_timeout()
    }

    /// Get access to response collector for direct use
    pub fn response_collector(&self) -> &Arc<ResponseCollector> {
        &self.response_collector
    }

    /// Get access to client for direct publishing
    pub fn client(&self) -> &Arc<MockClient> {
        &self.client
    }

    /// Get the current list of participants
    pub fn participants(&self) -> Vec<String> {
        self.participants.lock().clone()
    }

    /// Calculate new participants that a stream doesn't know about
    pub fn calculate_new_participants(&self, stream: &str) -> HashMap<String, u64> {
        let participants = self.participants.lock();

        // Fast path: single-stream transactions have no other participants
        if participants.len() <= 1 {
            return HashMap::new();
        }

        let mut new_participants = HashMap::new();
        let awareness = self.participant_awareness.lock();
        let offsets = self.participant_offsets.lock();

        let stream_awareness = awareness.get(stream).cloned().unwrap_or_default();

        for participant in participants.iter() {
            if participant != stream && !stream_awareness.contains(participant) {
                let offset = offsets.get(participant).copied().unwrap_or(0);
                new_participants.insert(participant.clone(), offset);
            }
        }

        new_participants
    }

    /// Update participant awareness after informing a stream about new participants
    pub fn update_participant_awareness(
        &self,
        stream: &str,
        new_participants: &HashMap<String, u64>,
    ) {
        if !new_participants.is_empty() {
            let mut awareness = self.participant_awareness.lock();
            if let Some(stream_awareness) = awareness.get_mut(stream) {
                for participant in new_participants.keys() {
                    stream_awareness.insert(participant.clone());
                }
            }
        }
    }

    /// Calculate timeout from deadline
    fn calculate_timeout(&self) -> Result<Duration> {
        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        if now_micros >= self.deadline.physical {
            return Err(CoordinatorError::DeadlineExceeded);
        }

        let remaining_micros = self.deadline.physical - now_micros;
        Ok(Duration::from_micros(remaining_micros))
    }

    /// Track a participant and return deadline string if first contact
    fn track_participant(&self, stream: &str) -> Option<String> {
        let mut participants = self.participants.lock();
        let mut streams_with_deadline = self.streams_with_deadline.lock();

        if !participants.contains(&stream.to_string()) {
            let is_first_participant = participants.is_empty();
            participants.push(stream.to_string());

            // For multi-stream transactions, track awareness
            if !is_first_participant {
                self.participant_offsets
                    .lock()
                    .insert(stream.to_string(), 0);
                self.participant_awareness
                    .lock()
                    .insert(stream.to_string(), Default::default());
            }

            // Mark as having received deadline
            streams_with_deadline.insert(stream.to_string());

            // Return deadline to include in headers
            Some(self.deadline.to_string())
        } else {
            None
        }
    }

    /// Generate a unique request ID
    fn generate_request_id(&self, stream: &str) -> String {
        format!(
            "op-{}-{}-{}",
            self.txn_id,
            stream,
            self.request_counter.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Build base headers for a message
    fn build_base_headers(&self, request_id: &str) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), self.txn_id.clone());
        headers.insert("coordinator_id".to_string(), self.coordinator_id.clone());
        headers.insert("request_id".to_string(), request_id.to_string());
        headers
    }

    /// Handle a response message
    fn handle_response(&self, response: ResponseMessage) -> Result<Vec<u8>> {
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
                    ErrorKind::DeadlineExceeded | ErrorKind::PrepareAfterDeadline => {
                        Err(CoordinatorError::DeadlineExceeded)
                    }
                    ErrorKind::InvalidDeadlineFormat | ErrorKind::DeadlineRequired => Err(
                        CoordinatorError::OperationFailed(format!("Protocol error: {:?}", kind)),
                    ),
                    ErrorKind::SerializationFailed(msg)
                    | ErrorKind::EngineError(msg)
                    | ErrorKind::Unknown(msg) => Err(CoordinatorError::OperationFailed(msg)),
                }
            }
            ResponseMessage::Prepared { .. } => {
                // Prepared responses shouldn't come here (only in 2PC)
                Err(CoordinatorError::OperationFailed(
                    "Unexpected prepared response".to_string(),
                ))
            }
        }
    }
}
