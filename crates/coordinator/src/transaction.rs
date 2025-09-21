//! Transaction implementation with active methods

use crate::error::{CoordinatorError, Result};
use crate::responses::{ResponseCollector, ResponseMessage};
use crate::speculation::SpeculativeContext;
use parking_lot::Mutex;
use proven_engine::{Message, MockClient};
use proven_hlc::HlcTimestamp;
use proven_runner::Runner;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Transaction state in the coordinator
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    /// Transaction is active and accepting operations
    Active,
    /// Prepare phase has started
    Preparing,
    /// All participants have voted to prepare
    Prepared,
    /// Commit phase has started
    Committing,
    /// Transaction has been committed
    Committed,
    /// Abort phase has started
    Aborting,
    /// Transaction has been aborted
    Aborted,
}

/// Prepare vote from a participant
#[derive(Debug, Clone)]
pub enum PrepareVote {
    /// Participant successfully prepared
    Prepared,
    /// Participant was wounded
    Wounded { wounded_by: String },
    /// Participant encountered an error
    Error(String),
}

/// Transaction metadata (internal)
#[derive(Debug)]
pub(crate) struct TransactionMetadata {
    pub(crate) state: TransactionState,
    pub(crate) participants: Vec<String>,
    pub(crate) timestamp: HlcTimestamp,
    pub(crate) deadline: HlcTimestamp,
    pub(crate) prepare_votes: HashMap<String, PrepareVote>,
    pub(crate) streams_with_deadline: HashSet<String>,
    pub(crate) participant_awareness: HashMap<String, HashSet<String>>,
    pub(crate) participant_offsets: HashMap<String, u64>,
    /// Pending speculated writes organized by stream for ordering enforcement
    pub(crate) pending_speculated_writes: HashMap<String, VecDeque<Value>>,
    /// Cache of speculated operation results
    pub(crate) speculation_cache: HashMap<Value, Value>,
}

impl TransactionMetadata {
    pub(crate) fn new(
        timestamp: HlcTimestamp,
        deadline: HlcTimestamp,
        pending_speculated_writes: HashMap<String, VecDeque<Value>>,
        speculation_cache: HashMap<Value, Value>,
    ) -> Self {
        Self {
            state: TransactionState::Active,
            participants: Vec::new(),
            timestamp,
            deadline,
            prepare_votes: HashMap::new(),
            streams_with_deadline: HashSet::new(),
            participant_awareness: HashMap::new(),
            participant_offsets: HashMap::new(),
            pending_speculated_writes,
            speculation_cache,
        }
    }
}

/// Active transaction object with methods
#[derive(Clone)]
pub struct Transaction {
    /// Transaction ID
    id: String,

    /// Transaction metadata
    metadata: Arc<Mutex<TransactionMetadata>>,

    /// Client for sending messages
    client: Arc<MockClient>,

    /// Response collector
    response_collector: Arc<ResponseCollector>,

    /// Coordinator ID for routing
    coordinator_id: String,

    /// Runner for ensuring processors are running
    runner: Arc<Runner>,

    /// Request counter for generating unique request IDs
    request_counter: Arc<AtomicU64>,

    /// Speculative execution context for pattern learning
    speculative_context: SpeculativeContext,
}

impl Transaction {
    /// Create a new transaction
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: String,
        timestamp: HlcTimestamp,
        deadline: HlcTimestamp,
        client: Arc<MockClient>,
        response_collector: Arc<ResponseCollector>,
        coordinator_id: String,
        runner: Arc<Runner>,
        pending_speculated_writes: HashMap<String, VecDeque<Value>>,
        speculation_cache: HashMap<Value, Value>,
        speculative_context: SpeculativeContext,
    ) -> Self {
        Self {
            id,
            metadata: Arc::new(Mutex::new(TransactionMetadata::new(
                timestamp,
                deadline,
                pending_speculated_writes,
                speculation_cache,
            ))),
            client,
            response_collector,
            coordinator_id,
            runner,
            request_counter: Arc::new(AtomicU64::new(0)),
            speculative_context,
        }
    }

    /// Get the transaction ID
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Get the current transaction state
    pub fn state(&self) -> TransactionState {
        self.metadata.lock().state.clone()
    }

    /// Get the transaction timestamp
    pub fn timestamp(&self) -> HlcTimestamp {
        self.metadata.lock().timestamp
    }

    /// Execute an operation on a stream
    ///
    /// This method:
    /// 1. Sends the operation to the specified stream
    /// 2. Waits for and returns the response
    /// 3. Handles protocol-level concerns (deferred, wounded, etc.)
    ///
    /// The operation bytes and response bytes are opaque to the coordinator.
    /// Clients handle serialization/deserialization.
    pub async fn execute(&self, stream: String, operation: Vec<u8>) -> Result<Vec<u8>> {
        // Batch metadata operations under a single lock
        let (timeout, deadline_str, _is_first_participant) = {
            let mut meta = self.metadata.lock();

            // Get current physical time in microseconds
            let now_micros = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;

            // Check if we've exceeded the deadline
            if now_micros >= meta.deadline.physical {
                return Err(CoordinatorError::DeadlineExceeded);
            }

            // Calculate remaining time and convert to Duration
            let remaining_micros = meta.deadline.physical - now_micros;
            let timeout = Duration::from_micros(remaining_micros);

            // Check if this is first contact with stream
            let is_first_participant = meta.participants.is_empty();
            let deadline_str = if !meta.participants.contains(&stream) {
                meta.participants.push(stream.clone());

                // For single-stream transactions, skip the extra tracking overhead
                if !is_first_participant {
                    // Only do complex tracking for multi-stream transactions
                    meta.participant_offsets.insert(stream.clone(), 0);
                    meta.participant_awareness
                        .insert(stream.clone(), Default::default());
                }

                // Mark this stream as having received the deadline
                meta.streams_with_deadline.insert(stream.clone());

                // Return deadline to include in headers
                Some(meta.deadline.to_string())
            } else {
                None
            };

            (timeout, deadline_str, is_first_participant)
        };

        // Generate request ID for correlation using atomic counter
        let request_id = format!(
            "op-{}-{}-{}",
            self.id,
            stream,
            self.request_counter.fetch_add(1, Ordering::Relaxed)
        );

        // Build message headers
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), self.id.clone());
        headers.insert("coordinator_id".to_string(), self.coordinator_id.clone());
        headers.insert("request_id".to_string(), request_id.clone());

        // Include deadline if this is first contact with stream
        if let Some(deadline) = deadline_str {
            headers.insert("txn_deadline".to_string(), deadline);
        }

        // Ensure processor is running for this stream
        // Use the transaction timeout as the minimum duration for the processor
        self.runner
            .ensure_processor(&stream, timeout)
            .await
            .map_err(|e| {
                CoordinatorError::EngineError(format!("Failed to ensure processor: {}", e))
            })?;

        // Send operation to stream
        let message = Message::new(operation, headers);
        self.client
            .publish_to_stream(stream, vec![message])
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        // Wait for response
        let response = self
            .response_collector
            .wait_for_response(request_id, timeout)
            .await?;

        // Handle protocol-level response status
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

    /// Commit this transaction
    pub async fn commit(&self) -> Result<()> {
        // Check for unexecuted speculated writes
        {
            let meta = self.metadata.lock();
            for (stream, pending_writes) in &meta.pending_speculated_writes {
                if !pending_writes.is_empty() {
                    return Err(CoordinatorError::InvalidState(format!(
                        "Cannot commit: {} unexecuted speculated writes on stream '{}'",
                        pending_writes.len(),
                        stream
                    )));
                }
            }
        }

        let participants = {
            let meta = self.metadata.lock();
            if !matches!(meta.state, TransactionState::Active) {
                return Err(CoordinatorError::InvalidState(format!(
                    "Cannot commit transaction in state {:?}",
                    meta.state
                )));
            }
            meta.participants.clone()
        };

        // Update patterns based on success
        let result = match participants.len() {
            0 => self.commit_empty(),
            1 => self.commit_single(&participants[0]).await,
            _ => self.commit_multiple(&participants).await,
        };

        // Update speculation patterns based on commit outcome
        if result.is_ok() {
            self.speculative_context.update_patterns(&self.id, true);
        } else {
            self.speculative_context.update_patterns(&self.id, false);
        }

        result
    }

    /// Abort this transaction
    pub async fn abort(&self) -> Result<()> {
        let participants = {
            let mut meta = self.metadata.lock();
            if matches!(
                meta.state,
                TransactionState::Committed | TransactionState::Aborted
            ) {
                return Ok(()); // Already terminated
            }
            meta.state = TransactionState::Aborting;
            meta.participants.clone()
        };

        // Send abort to all participants
        for stream in participants {
            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), self.id.clone());
            headers.insert("txn_phase".to_string(), "abort".to_string());

            let message = Message::new(Vec::new(), headers);
            self.client
                .publish_to_stream(stream, vec![message])
                .await
                .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;
        }

        self.metadata.lock().state = TransactionState::Aborted;
        Ok(())
    }

    /// Commit empty transaction (no participants)
    fn commit_empty(&self) -> Result<()> {
        let mut meta = self.metadata.lock();
        meta.state = TransactionState::Committed;
        Ok(())
    }

    /// Commit with single participant optimization
    async fn commit_single(&self, participant: &str) -> Result<()> {
        // Update state
        {
            let mut meta = self.metadata.lock();
            meta.state = TransactionState::Preparing;
        }

        // Send prepare_and_commit using atomic counter
        let request_id = format!(
            "prepare_commit-{}-{}",
            self.id,
            self.request_counter.fetch_add(1, Ordering::Relaxed)
        );

        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), self.id.clone());
        headers.insert("txn_phase".to_string(), "prepare_and_commit".to_string());
        headers.insert("coordinator_id".to_string(), self.coordinator_id.clone());
        headers.insert("request_id".to_string(), request_id.clone());

        let message = Message::new(Vec::new(), headers);
        self.client
            .publish_to_stream(participant.to_string(), vec![message])
            .await
            .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;

        // Wait for response
        let success = match self
            .collect_prepare_votes(&[participant.to_string()], &request_id)
            .await
        {
            Ok(prepared) => prepared,
            Err(e) => {
                // Always abort on any prepare error (including timeout)
                // For single participant, the prepare_and_commit already told them to abort
                let mut meta = self.metadata.lock();
                meta.state = TransactionState::Aborted;
                return Err(e);
            }
        };

        if !success {
            let mut meta = self.metadata.lock();
            meta.state = TransactionState::Aborted;
            return Ok(()); // Participant handled abort
        }

        // Mark as committed
        {
            let mut meta = self.metadata.lock();
            meta.state = TransactionState::Committed;
        }

        Ok(())
    }

    /// Commit with standard 2PC
    async fn commit_multiple(&self, participants: &[String]) -> Result<()> {
        // Phase 1: Prepare
        {
            let mut meta = self.metadata.lock();
            meta.state = TransactionState::Preparing;
        }

        // Generate request ID for this prepare round using atomic counter
        let request_id = format!(
            "prepare-{}-{}",
            self.id,
            self.request_counter.fetch_add(1, Ordering::Relaxed)
        );

        // Send prepare to all participants
        for stream in participants {
            // Calculate any remaining participant awareness updates
            let new_participant_offsets = self.calculate_new_participants(stream);

            // Update awareness
            if !new_participant_offsets.is_empty() {
                let mut meta = self.metadata.lock();
                if let Some(awareness) = meta.participant_awareness.get_mut(stream) {
                    for participant in new_participant_offsets.keys() {
                        awareness.insert(participant.clone());
                    }
                }
            }

            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), self.id.clone());
            headers.insert("txn_phase".to_string(), "prepare".to_string());
            headers.insert("coordinator_id".to_string(), self.coordinator_id.clone());
            headers.insert("request_id".to_string(), request_id.clone());

            // Include any new participants
            if !new_participant_offsets.is_empty() {
                headers.insert(
                    "new_participants".to_string(),
                    serde_json::to_string(&new_participant_offsets).unwrap(),
                );
            }

            let message = Message::new(Vec::new(), headers);
            self.client
                .publish_to_stream(stream.clone(), vec![message])
                .await
                .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;
        }

        // Wait for prepare votes
        let all_prepared = match self.collect_prepare_votes(participants, &request_id).await {
            Ok(prepared) => prepared,
            Err(e) => {
                // Check if we're past the deadline
                let past_deadline = {
                    let meta = self.metadata.lock();
                    let now_micros = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as u64;
                    now_micros >= meta.deadline.physical
                };

                if past_deadline {
                    // Transaction is already effectively aborted by deadline
                    // No need to send explicit abort messages
                } else {
                    // Still within deadline, explicitly abort
                    let _ = self.abort().await;
                }
                return Err(e);
            }
        };

        if !all_prepared {
            // Abort if any participant couldn't prepare
            self.abort().await?;
            return Err(CoordinatorError::TransactionAborted);
        }

        // Update state to prepared
        {
            let mut meta = self.metadata.lock();
            meta.state = TransactionState::Prepared;
        }

        // Phase 2: Commit
        {
            let mut meta = self.metadata.lock();
            meta.state = TransactionState::Committing;
        }

        // Send commit to all participants
        for stream in participants {
            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), self.id.clone());
            headers.insert("txn_phase".to_string(), "commit".to_string());

            let message = Message::new(Vec::new(), headers);
            self.client
                .publish_to_stream(stream.clone(), vec![message])
                .await
                .map_err(|e| CoordinatorError::EngineError(e.to_string()))?;
        }

        // Mark as committed
        {
            let mut meta = self.metadata.lock();
            meta.state = TransactionState::Committed;
        }

        Ok(())
    }

    /// Calculate new participants that a stream doesn't know about
    fn calculate_new_participants(&self, stream: &str) -> HashMap<String, u64> {
        let meta = self.metadata.lock();

        // Fast path: single-stream transactions have no other participants
        if meta.participants.len() <= 1 {
            return HashMap::new();
        }

        let mut new_participants = HashMap::new();

        let stream_awareness = meta
            .participant_awareness
            .get(stream)
            .cloned()
            .unwrap_or_default();

        for participant in &meta.participants {
            if participant != stream && !stream_awareness.contains(participant) {
                let offset = meta
                    .participant_offsets
                    .get(participant)
                    .copied()
                    .unwrap_or(0);
                new_participants.insert(participant.clone(), offset);
            }
        }

        new_participants
    }

    /// Collect prepare votes from participants
    async fn collect_prepare_votes(
        &self,
        participants: &[String],
        request_id: &str,
    ) -> Result<bool> {
        let mut pending_participants: HashSet<String> = participants.iter().cloned().collect();

        // Use remaining time until transaction deadline for prepare timeout
        let timeout_duration = {
            let meta = self.metadata.lock();
            let now_micros = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;

            if now_micros >= meta.deadline.physical {
                // Already past deadline
                return Err(CoordinatorError::DeadlineExceeded);
            }

            let remaining_micros = meta.deadline.physical - now_micros;
            // Cap at 2 seconds to avoid waiting too long for unresponsive participants
            // This helps prevent slowdowns when a processor is unresponsive
            Duration::from_micros(remaining_micros).min(Duration::from_secs(2))
        };

        let deadline = tokio::time::Instant::now() + timeout_duration;

        while !pending_participants.is_empty() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(CoordinatorError::PrepareTimeout);
            }

            // Try to get a response
            match self
                .response_collector
                .wait_for_response(request_id.to_string(), remaining)
                .await
            {
                Ok(response) => {
                    // Get participant from response
                    if let Some(participant_str) = response.participant()
                        && pending_participants.contains(participant_str)
                    {
                        let participant = participant_str.to_string();
                        let vote = match response {
                            ResponseMessage::Complete { .. } | ResponseMessage::Prepared { .. } => {
                                PrepareVote::Prepared
                            }
                            ResponseMessage::Wounded { wounded_by, .. } => PrepareVote::Wounded {
                                wounded_by: wounded_by.to_string(),
                            },
                            ResponseMessage::Error { kind, .. } => {
                                // Convert error kind to string for PrepareVote
                                PrepareVote::Error(format!("{:?}", kind))
                            }
                        };

                        // Record vote
                        {
                            let mut meta = self.metadata.lock();
                            meta.prepare_votes.insert(participant.clone(), vote.clone());
                        }

                        pending_participants.remove(&participant);

                        // Check if this is a negative vote
                        if !matches!(vote, PrepareVote::Prepared) {
                            return Ok(false); // Abort if any participant can't prepare
                        }
                    }
                }
                Err(CoordinatorError::ResponseTimeout) => {
                    return Err(CoordinatorError::PrepareTimeout);
                }
                Err(e) => return Err(e),
            }
        }

        // All participants voted to prepare
        Ok(true)
    }
}
