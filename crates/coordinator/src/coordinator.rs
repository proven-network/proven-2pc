//! Core coordinator implementation
//!
//! This module contains the MockCoordinator which orchestrates distributed
//! transactions across multiple participants using two-phase commit (2PC).
//!
//! ## Current Implementation
//! - Participants must be declared upfront when beginning a transaction
//! - Standard 2PC with prepare and commit phases
//! - Supports prepare vote collection for testing
//!
//! ## Future Improvements (TODO)
//! - Phase 2: Dynamic participant discovery (add participants as operations are sent)
//! - Phase 3: Single participant optimization (skip prepare for single participant)
//! - Phase 4: Progressive participant awareness (include participant info in messages)
//! - Phase 5: Recovery protocol for coordinator failures

use crate::error::{CoordinatorError, Result};
use crate::transaction::{PrepareVote, Transaction, TransactionState};
use parking_lot::Mutex;
use proven_engine::{Message, MockClient, MockEngine};
use proven_hlc::{HlcClock, NodeId};
use serde_json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

/// Mock coordinator for distributed transactions
#[derive(Clone)]
pub struct MockCoordinator {
    /// Coordinator ID
    coordinator_id: String,

    /// Client for engine communication
    client: MockClient,

    /// Active transactions
    transactions: Arc<Mutex<HashMap<String, Transaction>>>,

    /// HLC clock for timestamps
    hlc: Arc<Mutex<HlcClock>>,

    /// Whether to wait for actual prepare votes (vs mock mode for testing)
    wait_for_prepare_votes: bool,
}

impl MockCoordinator {
    /// Create a new mock coordinator
    pub fn new(coordinator_id: String, engine: Arc<MockEngine>) -> Self {
        Self::new_with_options(coordinator_id, engine, false)
    }

    /// Create a new coordinator with prepare vote collection enabled
    pub fn new_with_prepare_votes(coordinator_id: String, engine: Arc<MockEngine>) -> Self {
        Self::new_with_options(coordinator_id, engine, true)
    }

    fn new_with_options(
        coordinator_id: String,
        engine: Arc<MockEngine>,
        wait_for_prepare_votes: bool,
    ) -> Self {
        let client = MockClient::new(format!("coordinator-{}", coordinator_id), engine);

        // Create HLC clock with node ID based on coordinator ID hash
        // Use wrapping arithmetic to avoid overflow
        let seed = coordinator_id
            .bytes()
            .fold(0u8, |acc, b| acc.wrapping_add(b));
        let node_id = NodeId::from_seed(seed);
        let hlc = Arc::new(Mutex::new(HlcClock::new(node_id)));

        Self {
            coordinator_id,
            client,
            transactions: Arc::new(Mutex::new(HashMap::new())),
            hlc,
            wait_for_prepare_votes,
        }
    }

    /// Begin a new distributed transaction
    ///
    /// Participants will be discovered dynamically as operations are sent to streams
    pub async fn begin_transaction(&self) -> Result<String> {
        let timestamp = self.hlc.lock().now();
        let txn_id = format!("{}:{}", self.coordinator_id, timestamp);

        let transaction = Transaction {
            id: txn_id.clone(),
            state: TransactionState::Active,
            participants: Vec::new(), // Start with empty participants
            timestamp,
            prepare_votes: HashMap::new(),
            participant_awareness: HashMap::new(),
            participant_offsets: HashMap::new(),
        };

        self.transactions.lock().insert(txn_id.clone(), transaction);

        // No need to send begin messages - transactions are created on first operation
        Ok(txn_id)
    }

    /// Execute an operation within a transaction
    ///
    /// Adds the stream as a participant if not already present and
    /// includes information about other participants this stream hasn't been told about yet
    pub async fn execute_operation(
        &self,
        txn_id: &str,
        stream: &str,
        operation: Vec<u8>,
    ) -> Result<()> {
        // Check transaction exists and is active, add participant if new, get unknown participants
        let (is_new_participant, new_participant_offsets) = {
            let mut txns = self.transactions.lock();
            let txn = txns
                .get_mut(txn_id)
                .ok_or_else(|| CoordinatorError::TransactionNotFound(txn_id.to_string()))?;

            if !matches!(txn.state, TransactionState::Active) {
                return Err(CoordinatorError::InvalidState(format!(
                    "Transaction {} is not active",
                    txn_id
                )));
            }

            // Add participant if not already present
            let is_new_participant = !txn.participants.contains(&stream.to_string());
            if is_new_participant {
                txn.participants.push(stream.to_string());
                txn.participant_awareness
                    .insert(stream.to_string(), HashSet::new());
                // We'll set the actual offset after we publish the message
            }

            // Build map of participants this stream doesn't know about yet
            let mut new_participants = HashMap::new();
            let stream_awareness = txn
                .participant_awareness
                .get(&stream.to_string())
                .cloned()
                .unwrap_or_default();

            for participant in &txn.participants {
                // Don't tell a stream about itself
                if participant != stream && !stream_awareness.contains(participant) {
                    // Get the offset when this participant joined
                    let offset = txn
                        .participant_offsets
                        .get(participant)
                        .copied()
                        .unwrap_or(0);
                    new_participants.insert(participant.clone(), offset);
                }
            }

            // Update this stream's awareness with the participants we're about to tell it about
            if let Some(awareness) = txn.participant_awareness.get_mut(stream) {
                for participant in new_participants.keys() {
                    awareness.insert(participant.clone());
                }
            }

            (is_new_participant, new_participants)
        };

        // Create message with transaction headers and unique request ID
        let request_id = format!(
            "req-{}-{}",
            txn_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("coordinator_id".to_string(), self.coordinator_id.clone());
        headers.insert("request_id".to_string(), request_id);

        // Include new participants if there are any
        if !new_participant_offsets.is_empty() {
            headers.insert(
                "new_participants".to_string(),
                serde_json::to_string(&new_participant_offsets).unwrap(),
            );
        }

        let message = Message::new(operation, headers);

        // Send to the appropriate stream and get the actual offset
        let offset = self
            .client
            .publish_to_stream(stream.to_string(), vec![message])
            .await?;

        // If this is a new participant, store its actual offset
        if is_new_participant {
            let mut txns = self.transactions.lock();
            if let Some(txn) = txns.get_mut(txn_id) {
                txn.participant_offsets.insert(stream.to_string(), offset);
            }
        }

        Ok(())
    }

    /// Collect prepare votes from participants
    async fn collect_prepare_votes(
        &self,
        txn_id: &str,
        participants: &[String],
        request_id: &str,
    ) -> Result<bool> {
        // Subscribe to response channels for each participant
        let response_subject = format!("coordinator.{}.response", self.coordinator_id);
        let mut response_stream = self.client.subscribe(&response_subject, None).await?;

        // Track which participants we're waiting for
        let mut pending_participants: HashSet<String> = participants.iter().cloned().collect();

        // Wait for responses with timeout
        let timeout_duration = Duration::from_secs(5);
        let deadline = tokio::time::Instant::now() + timeout_duration;

        while !pending_participants.is_empty() {
            // Check for timeout
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(CoordinatorError::PrepareTimeout);
            }

            // Wait for next message with timeout
            match timeout(remaining, response_stream.recv()).await {
                Ok(Some(msg)) => {
                    // Parse response and check request_id matches
                    if let (Some(resp_txn_id), Some(participant)) =
                        (msg.headers.get("txn_id"), msg.headers.get("participant"))
                    {
                        let resp_request_id = msg.headers.get("request_id");
                        // Only process if this response is for our current prepare request
                        if resp_request_id.map(String::as_str) == Some(&request_id[..])
                            && resp_txn_id == txn_id
                            && pending_participants.contains(participant)
                        {
                            // Check headers first for protocol status (from generic processor)
                            let vote = if let Some(status) = msg.headers.get("status") {
                                match status.as_str() {
                                    "prepared" => PrepareVote::Prepared,
                                    "wounded" => {
                                        let wounded_by = msg.headers.get("wounded_by")
                                            .map(|s| s.to_string())
                                            .unwrap_or_else(|| "unknown".to_string());
                                        PrepareVote::Wounded { wounded_by }
                                    }
                                    "error" => {
                                        let error = msg.headers.get("error")
                                            .map(|s| s.to_string())
                                            .unwrap_or_else(|| "Unknown error".to_string());
                                        PrepareVote::Error(error)
                                    }
                                    _ => PrepareVote::Error(format!("Unknown status: {}", status))
                                }
                            } else if !msg.body.is_empty() {
                                // Fallback to deserializing body for backward compatibility
                                // Try SQL response first, then KV response
                                if let Ok(sql_response) =
                                    serde_json::from_slice::<proven_sql::stream::response::SqlResponse>(
                                        &msg.body,
                                    ) {
                                    match sql_response {
                                        proven_sql::stream::response::SqlResponse::Prepared => {
                                            PrepareVote::Prepared
                                        }
                                        proven_sql::stream::response::SqlResponse::Wounded {
                                            wounded_by,
                                            ..
                                        } => PrepareVote::Wounded {
                                            wounded_by: wounded_by.to_string(),
                                        },
                                        proven_sql::stream::response::SqlResponse::Error(e) => {
                                            PrepareVote::Error(e)
                                        }
                                        _ => PrepareVote::Error("Unexpected response type".to_string()),
                                    }
                                } else if let Ok(kv_response) = serde_json::from_slice::<
                                    proven_kv::stream::response::KvResponse,
                                >(&msg.body)
                                {
                                    match kv_response {
                                        proven_kv::stream::response::KvResponse::Error(e) => {
                                            PrepareVote::Error(e)
                                        }
                                        _ => PrepareVote::Error("Unexpected KV response type for prepare phase".to_string()),
                                    }
                                } else {
                                    PrepareVote::Error("Failed to deserialize response".to_string())
                                }
                            } else {
                                PrepareVote::Error("Empty response with no status header".to_string())
                            };

                            // Record vote
                            {
                                let mut txns = self.transactions.lock();
                                if let Some(txn) = txns.get_mut(txn_id) {
                                    txn.prepare_votes.insert(participant.clone(), vote.clone());
                                }
                            }

                            pending_participants.remove(participant);

                            // Check if this is a negative vote
                            if !matches!(vote, PrepareVote::Prepared) {
                                return Ok(false); // Abort if any participant can't prepare
                            }
                        }
                    }
                }
                Ok(None) => {
                    // Stream ended
                    return Err(CoordinatorError::PrepareFailed(
                        "Response stream ended unexpectedly".to_string(),
                    ));
                }
                Err(_) => {
                    // Timeout
                    return Err(CoordinatorError::PrepareTimeout);
                }
            }
        }

        // All participants voted to prepare
        Ok(true)
    }

    /// Commit a distributed transaction using 2PC
    ///
    /// Optimizes for single participant by sending combined prepare_and_commit
    pub async fn commit_transaction(&self, txn_id: &str) -> Result<()> {
        // Get participants and check count
        let participants = {
            let txns = self.transactions.lock();
            let txn = txns
                .get(txn_id)
                .ok_or_else(|| CoordinatorError::TransactionNotFound(txn_id.to_string()))?;
            txn.participants.clone()
        };

        match participants.len() {
            0 => {
                // Empty transaction - just mark as committed
                let mut txns = self.transactions.lock();
                let txn = txns.get_mut(txn_id).unwrap();
                txn.state = TransactionState::Committed;
                Ok(())
            }
            1 => {
                // Single participant optimization - use prepare_and_commit
                self.commit_single_participant(txn_id, &participants[0])
                    .await
            }
            _ => {
                // Multiple participants - use standard 2PC
                self.commit_multiple_participants(txn_id, &participants)
                    .await
            }
        }
    }

    /// Commit with single participant optimization
    async fn commit_single_participant(&self, txn_id: &str, participant: &str) -> Result<()> {
        // Update state to preparing
        {
            let mut txns = self.transactions.lock();
            let txn = txns.get_mut(txn_id).unwrap();
            txn.state = TransactionState::Preparing;
        }

        // Send prepare_and_commit message
        let request_id = format!(
            "prepare_commit-{}-{}",
            txn_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("txn_phase".to_string(), "prepare_and_commit".to_string());
        headers.insert("coordinator_id".to_string(), self.coordinator_id.clone());
        headers.insert("request_id".to_string(), request_id.clone());

        let message = Message::new(Vec::new(), headers);
        self.client
            .publish_to_stream(participant.to_string(), vec![message])
            .await?;

        // Wait for response if enabled
        if self.wait_for_prepare_votes {
            // For prepare_and_commit, we expect either success or failure
            let success = self
                .collect_prepare_votes(txn_id, &[participant.to_string()], &request_id)
                .await?;

            if !success {
                // Single participant couldn't prepare_and_commit, mark as aborted
                let mut txns = self.transactions.lock();
                let txn = txns.get_mut(txn_id).unwrap();
                txn.state = TransactionState::Aborted;
                return Ok(()); // Already handled by participant
            }
        }

        // Mark as committed
        {
            let mut txns = self.transactions.lock();
            let txn = txns.get_mut(txn_id).unwrap();
            txn.state = TransactionState::Committed;
        }

        Ok(())
    }

    /// Commit with standard 2PC for multiple participants
    async fn commit_multiple_participants(
        &self,
        txn_id: &str,
        participants: &[String],
    ) -> Result<()> {
        // Phase 1: Prepare
        {
            let mut txns = self.transactions.lock();
            let txn = txns.get_mut(txn_id).unwrap();
            txn.state = TransactionState::Preparing;
        }

        // Generate a unique request ID for this prepare round
        let request_id = format!(
            "prepare-{}-{}",
            txn_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        for stream in participants {
            // Calculate any final participant deltas for this stream
            let new_participant_offsets = {
                let txns = self.transactions.lock();
                let txn = txns.get(txn_id).unwrap();

                let mut new_participants = HashMap::new();
                let stream_awareness = txn
                    .participant_awareness
                    .get(stream)
                    .cloned()
                    .unwrap_or_default();

                for participant in &txn.participants {
                    // Don't tell a stream about itself, and only include participants it doesn't know about
                    if participant != stream && !stream_awareness.contains(participant) {
                        let offset = txn
                            .participant_offsets
                            .get(participant)
                            .copied()
                            .unwrap_or(0);
                        new_participants.insert(participant.clone(), offset);
                    }
                }

                new_participants
            };

            // Update awareness for this prepare message
            if !new_participant_offsets.is_empty() {
                let mut txns = self.transactions.lock();
                let txn = txns.get_mut(txn_id).unwrap();
                if let Some(awareness) = txn.participant_awareness.get_mut(stream) {
                    for participant in new_participant_offsets.keys() {
                        awareness.insert(participant.clone());
                    }
                }
            }

            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), txn_id.to_string());
            headers.insert("txn_phase".to_string(), "prepare".to_string());
            headers.insert("coordinator_id".to_string(), self.coordinator_id.clone());
            headers.insert("request_id".to_string(), request_id.clone());

            // Include any final participant deltas
            if !new_participant_offsets.is_empty() {
                headers.insert(
                    "new_participants".to_string(),
                    serde_json::to_string(&new_participant_offsets).unwrap(),
                );
            }

            let message = Message::new(Vec::new(), headers);
            self.client
                .publish_to_stream(stream.clone(), vec![message])
                .await?;
        }

        // Wait for prepare votes if enabled, otherwise mock success
        if self.wait_for_prepare_votes {
            let all_prepared = self
                .collect_prepare_votes(txn_id, participants, &request_id)
                .await?;

            if !all_prepared {
                // If any participant couldn't prepare, abort the transaction
                return self.abort_transaction(txn_id).await;
            }
        }
        // else: mock mode - assume all participants prepared successfully

        // All participants prepared successfully
        {
            let mut txns = self.transactions.lock();
            let txn = txns.get_mut(txn_id).unwrap();
            txn.state = TransactionState::Prepared;
        }

        // Phase 2: Commit
        {
            let mut txns = self.transactions.lock();
            let txn = txns.get_mut(txn_id).unwrap();
            txn.state = TransactionState::Committing;
        }

        // Send commit messages to all participants
        for stream in participants {
            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), txn_id.to_string());
            headers.insert("txn_phase".to_string(), "commit".to_string());

            let message = Message::new(Vec::new(), headers);
            self.client
                .publish_to_stream(stream.clone(), vec![message])
                .await?;
        }

        // Mark as committed
        {
            let mut txns = self.transactions.lock();
            let txn = txns.get_mut(txn_id).unwrap();
            txn.state = TransactionState::Committed;
        }

        Ok(())
    }

    /// Abort a distributed transaction
    ///
    /// TODO (Phase 6): Include participant info for recovery in abort messages
    pub async fn abort_transaction(&self, txn_id: &str) -> Result<()> {
        // Update state
        let participants = {
            let mut txns = self.transactions.lock();
            let txn = txns
                .get_mut(txn_id)
                .ok_or_else(|| CoordinatorError::TransactionNotFound(txn_id.to_string()))?;

            txn.state = TransactionState::Aborting;
            txn.participants.clone()
        };

        // Send abort messages to all participants
        for stream in &participants {
            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), txn_id.to_string());
            headers.insert("txn_phase".to_string(), "abort".to_string());

            // TODO (Phase 4): Include participant list for early abort recovery

            let message = Message::new(Vec::new(), headers);
            self.client
                .publish_to_stream(stream.clone(), vec![message])
                .await?;
        }

        // Mark as aborted
        {
            let mut txns = self.transactions.lock();
            let txn = txns.get_mut(txn_id).unwrap();
            txn.state = TransactionState::Aborted;
        }

        Ok(())
    }

    /// Get transaction state
    pub fn get_transaction_state(&self, txn_id: &str) -> Option<TransactionState> {
        self.transactions
            .lock()
            .get(txn_id)
            .map(|t| t.state.clone())
    }
}
