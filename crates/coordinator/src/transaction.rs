//! Transaction implementation with active methods

use crate::error::{CoordinatorError, Result};
use crate::responses::{ResponseCollector, ResponseStatus};
use parking_lot::Mutex;
use proven_engine::{Message, MockClient};
use proven_hlc::HlcTimestamp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
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
}

impl TransactionMetadata {
    pub(crate) fn new(timestamp: HlcTimestamp, deadline: HlcTimestamp) -> Self {
        Self {
            state: TransactionState::Active,
            participants: Vec::new(),
            timestamp,
            deadline,
            prepare_votes: HashMap::new(),
            streams_with_deadline: HashSet::new(),
            participant_awareness: HashMap::new(),
            participant_offsets: HashMap::new(),
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
}

impl Transaction {
    /// Create a new transaction
    pub(crate) fn new(
        id: String,
        timestamp: HlcTimestamp,
        deadline: HlcTimestamp,
        client: Arc<MockClient>,
        response_collector: Arc<ResponseCollector>,
        coordinator_id: String,
    ) -> Self {
        Self {
            id,
            metadata: Arc::new(Mutex::new(TransactionMetadata::new(timestamp, deadline))),
            client,
            response_collector,
            coordinator_id,
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
        // Calculate timeout based on remaining time until deadline
        let timeout = {
            let meta = self.metadata.lock();
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
            Duration::from_micros(remaining_micros)
        };

        // Check if this is first contact with stream and include deadline if so
        let deadline_str = {
            let mut meta = self.metadata.lock();
            if !meta.participants.contains(&stream) {
                meta.participants.push(stream.clone());

                // Track initial offset for this participant (0 for now, could be enhanced)
                meta.participant_offsets.insert(stream.clone(), 0);

                // Initialize awareness tracking
                meta.participant_awareness
                    .insert(stream.clone(), Default::default());

                // Mark this stream as having received the deadline
                meta.streams_with_deadline.insert(stream.clone());

                // Return deadline to include in headers
                Some(meta.deadline.to_string())
            } else {
                None
            }
        };

        // Generate request ID for correlation
        let request_id = format!(
            "op-{}-{}-{}",
            self.id,
            stream,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
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
        match response.status {
            ResponseStatus::Success => Ok(response.body),
            ResponseStatus::Wounded { wounded_by } => Err(CoordinatorError::TransactionWounded {
                wounded_by: wounded_by.to_string(),
            }),
            ResponseStatus::Error(e) => Err(CoordinatorError::OperationFailed(e)),
        }
    }

    /// Commit this transaction
    pub async fn commit(&self) -> Result<()> {
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

        match participants.len() {
            0 => self.commit_empty(),
            1 => self.commit_single(&participants[0]).await,
            _ => self.commit_multiple(&participants).await,
        }
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

        // Send prepare_and_commit
        let request_id = format!(
            "prepare_commit-{}-{}",
            self.id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
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
        let success = self
            .collect_prepare_votes(&[participant.to_string()], &request_id)
            .await?;

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

        // Generate request ID for this prepare round
        let request_id = format!(
            "prepare-{}-{}",
            self.id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
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
        let all_prepared = self
            .collect_prepare_votes(participants, &request_id)
            .await?;

        if !all_prepared {
            // Abort if any participant couldn't prepare
            return self.abort().await;
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
        let timeout_duration = Duration::from_secs(5);
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
                    if let Some(participant) = response.participant
                        && pending_participants.contains(&participant)
                    {
                        let vote = match response.status {
                            ResponseStatus::Success => PrepareVote::Prepared,
                            ResponseStatus::Wounded { wounded_by } => PrepareVote::Wounded {
                                wounded_by: wounded_by.to_string(),
                            },
                            ResponseStatus::Error(e) => PrepareVote::Error(e),
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
