//! Read-write executor with full 2PC and speculation support

use super::{
    Executor as ExecutorTrait,
    common::{self, ExecutorInfra},
};
use crate::error::{CoordinatorError, Result};
use crate::responses::ResponseMessage;
use crate::speculation::{CheckResult, PredictionContext};
use async_trait::async_trait;
use parking_lot::Mutex;
use proven_common::Operation;
use proven_hlc::HlcTimestamp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, oneshot};

/// Type alias for speculation response receivers
type SpeculationReceivers = Arc<AsyncMutex<HashMap<usize, oneshot::Receiver<Result<Vec<u8>>>>>>;

/// Transaction state
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Preparing,
    Prepared,
    Committing,
    Committed,
    Aborting,
    Aborted,
}

/// Prepare vote from a participant
#[derive(Debug, Clone)]
pub enum PrepareVote {
    Prepared,
    Wounded,
    Error,
}

/// Read-write executor with full 2PC coordination
pub struct ReadWriteExecutor {
    /// Transaction ID (which is the timestamp)
    txn_id: HlcTimestamp,

    /// Transaction deadline
    deadline: HlcTimestamp,

    /// Transaction state
    state: Arc<Mutex<TransactionState>>,

    /// Prepare votes from participants
    prepare_votes: Arc<Mutex<HashMap<String, PrepareVote>>>,

    /// Participants in this transaction
    participants: Arc<Mutex<Vec<String>>>,

    /// Streams that have received the deadline
    streams_with_deadline: Arc<Mutex<HashSet<String>>>,

    /// Which participants each stream knows about
    participant_awareness: Arc<Mutex<HashMap<String, HashSet<String>>>>,

    /// Shared infrastructure
    infra: Arc<ExecutorInfra>,

    /// Speculative execution context for pattern learning
    prediction_context: Arc<AsyncMutex<PredictionContext>>,

    /// Response receivers for speculated operations
    speculation_receivers: SpeculationReceivers,
}

impl ReadWriteExecutor {
    /// Create a new read-write executor
    pub async fn new(
        txn_id: HlcTimestamp,
        deadline: HlcTimestamp,
        infra: Arc<ExecutorInfra>,
        prediction_context: PredictionContext,
    ) -> Self {
        // Execute predictions with transaction headers
        let speculation_result = if !prediction_context.predictions().is_empty() {
            let timeout = match common::calculate_timeout(deadline) {
                Ok(t) => t,
                Err(e) => {
                    tracing::debug!("Failed to calculate timeout for predictions: {}", e);
                    std::time::Duration::from_secs(30)
                }
            };

            // Build headers for predictions
            let coordinator_id = infra.coordinator_id.clone();
            let txn_id_str = txn_id.to_string();
            let deadline_str = deadline.to_string();

            infra
                .execute_predictions(prediction_context.predictions(), timeout, |_stream| {
                    let mut headers = HashMap::new();
                    headers.insert("txn_id".to_string(), txn_id_str.clone());
                    headers.insert("coordinator_id".to_string(), coordinator_id.clone());
                    headers.insert("txn_deadline".to_string(), deadline_str.clone());
                    headers
                })
                .await
        } else {
            Ok(HashMap::new())
        };

        // Store receivers if predictions were executed
        let speculation_receivers = match speculation_result {
            Ok(receivers) => Arc::new(AsyncMutex::new(receivers)),
            Err(e) => {
                tracing::debug!("Failed to execute predictions: {}", e);
                Arc::new(AsyncMutex::new(HashMap::new()))
            }
        };

        // Track all streams that we're sending speculative operations to
        let mut initial_participants = Vec::new();
        let mut initial_streams_with_deadline = HashSet::new();
        for pred_op in prediction_context.predictions() {
            if !initial_participants.contains(&pred_op.stream) {
                initial_participants.push(pred_op.stream.clone());
                initial_streams_with_deadline.insert(pred_op.stream.clone());
            }
        }

        Self {
            txn_id,
            deadline,
            state: Arc::new(Mutex::new(TransactionState::Active)),
            prepare_votes: Arc::new(Mutex::new(HashMap::new())),
            participants: Arc::new(Mutex::new(initial_participants)),
            streams_with_deadline: Arc::new(Mutex::new(initial_streams_with_deadline)),
            participant_awareness: Arc::new(Mutex::new(HashMap::new())),
            infra,
            prediction_context: Arc::new(AsyncMutex::new(prediction_context)),
            speculation_receivers,
        }
    }

    /// Get transaction ID as string
    pub fn txn_id(&self) -> String {
        self.txn_id.to_string()
    }

    /// Get transaction timestamp
    pub fn timestamp(&self) -> HlcTimestamp {
        self.txn_id
    }

    /// Track participant and return deadline string if needed
    fn track_participant(&self, stream: &str) -> Option<String> {
        let mut participants = self.participants.lock();
        let mut streams_with_deadline = self.streams_with_deadline.lock();

        // Check if this is first contact with stream
        let needs_deadline = !streams_with_deadline.contains(stream);

        if needs_deadline {
            streams_with_deadline.insert(stream.to_string());
        }

        // Add to participants if not already present
        if !participants.contains(&stream.to_string()) {
            participants.push(stream.to_string());
        }

        if needs_deadline {
            Some(self.deadline.to_string())
        } else {
            None
        }
    }

    /// Build base headers for a message
    fn build_base_headers(&self, request_id: &str) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), self.txn_id.to_string());
        headers.insert(
            "coordinator_id".to_string(),
            self.infra.coordinator_id.clone(),
        );
        headers.insert("request_id".to_string(), request_id.to_string());
        headers
    }

    /// Execute operation with speculation check
    async fn execute_with_speculation<O: Operation>(
        &self,
        stream: &str,
        operation: &O,
    ) -> Result<Vec<u8>> {
        // Check if this is a write operation
        let is_write = operation.operation_type() == proven_common::OperationType::Write;

        // Check with predictions first
        let op_value =
            serde_json::to_value(operation).map_err(CoordinatorError::SerializationError)?;

        let check_result = {
            let mut pred_context = self.prediction_context.lock().await;
            pred_context.check(stream, &op_value, is_write)
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
                            // Speculation execution failed
                            Err(CoordinatorError::SpeculationFailed(format!(
                                "Speculated operation {} failed: {}",
                                index, e
                            )))
                        }
                        Err(_) => {
                            // Channel closed
                            Err(CoordinatorError::SpeculationFailed(format!(
                                "Speculation channel closed for operation {}",
                                index
                            )))
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
                // Speculation failed - abort transaction
                Err(CoordinatorError::SpeculationFailed(format!(
                    "Operation mismatch at position {}: expected {}, got {}",
                    position, expected, actual
                )))
            }
        }
    }

    /// Normal execution without speculation
    async fn execute_normal<O: Operation>(&self, stream: &str, operation: &O) -> Result<Vec<u8>> {
        // Calculate timeout
        let timeout = common::calculate_timeout(self.deadline)?;

        // Track participant and get deadline if needed
        let deadline_str = self.track_participant(stream);

        // Generate request ID
        let request_id = self.infra.generate_request_id();

        // Build headers
        let mut headers = self.build_base_headers(&request_id);
        if let Some(deadline) = deadline_str {
            headers.insert("txn_deadline".to_string(), deadline);
        }

        // Ensure processor is running
        self.infra.ensure_processor(stream, timeout).await?;

        // Serialize operation
        let operation_bytes =
            serde_json::to_vec(operation).map_err(CoordinatorError::SerializationError)?;

        // Send and wait for response
        let response = self
            .infra
            .send_and_wait(stream, headers, operation_bytes, timeout)
            .await?;

        // Handle response
        ExecutorInfra::handle_response(response)
    }

    /// Prepare phase of 2PC
    async fn prepare(&self) -> Result<()> {
        let participants = self.participants.lock().clone();

        match participants.len() {
            0 => Ok(()), // No participants to prepare
            1 => self.prepare_single(&participants[0]).await,
            _ => self.prepare_multiple(&participants).await,
        }
    }

    /// Prepare with single participant optimization
    async fn prepare_single(&self, participant: &str) -> Result<()> {
        *self.state.lock() = TransactionState::Preparing;

        let request_id = self.infra.generate_request_id();
        let mut headers = self.build_base_headers(&request_id);
        headers.insert("txn_phase".to_string(), "prepare_and_commit".to_string());

        let timeout = common::calculate_timeout(self.deadline)?;

        let response = match self
            .infra
            .send_and_wait(participant, headers, Vec::new(), timeout)
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                // On error (e.g., timeout), abort the transaction
                let _ = self.abort().await;
                return Err(e);
            }
        };

        let vote = self.parse_prepare_vote(&response);

        if matches!(vote, PrepareVote::Prepared) {
            *self.state.lock() = TransactionState::Committed;
            Ok(())
        } else {
            let _ = self.abort().await;
            Err(CoordinatorError::TransactionAborted)
        }
    }

    /// Standard 2PC prepare
    async fn prepare_multiple(&self, participants: &[String]) -> Result<()> {
        *self.state.lock() = TransactionState::Preparing;

        let request_id = self.infra.generate_request_id();

        // Build prepare messages
        let mut prepare_messages = Vec::new();
        for participant in participants {
            let mut headers = self.build_base_headers(&request_id);
            headers.insert("txn_phase".to_string(), "prepare".to_string());

            // Add participant awareness updates
            let new_participants = self.calculate_new_participants(participant);
            if !new_participants.is_empty() {
                self.update_participant_awareness(participant, &new_participants);
                headers.insert(
                    "new_participants".to_string(),
                    serde_json::to_string(&new_participants).unwrap(),
                );
            }

            prepare_messages.push((participant.clone(), headers, Vec::new()));
        }

        // Send all prepare messages
        if let Err(e) = self.infra.send_messages_batch(prepare_messages).await {
            // Failed to send prepare messages - abort the transaction
            let _ = self.abort().await;
            return Err(e);
        }

        // Collect votes
        let all_prepared = match self.collect_prepare_votes(participants, &request_id).await {
            Ok(prepared) => prepared,
            Err(e) => {
                // On error (e.g., timeout), abort the transaction
                let _ = self.abort().await;
                return Err(e);
            }
        };

        if all_prepared {
            *self.state.lock() = TransactionState::Prepared;
            Ok(())
        } else {
            self.abort().await?;
            Err(CoordinatorError::TransactionAborted)
        }
    }

    /// Commit phase
    async fn commit(&self) -> Result<()> {
        // Check if we need to prepare first
        let state = self.state.lock().clone();
        if state == TransactionState::Active {
            // If prepare fails, the transaction is already aborted
            self.prepare().await?;
        }

        let participants = self.participants.lock().clone();
        if participants.is_empty() {
            *self.state.lock() = TransactionState::Committed;
            return Ok(());
        }

        *self.state.lock() = TransactionState::Committing;

        // Build commit messages
        let mut commit_messages = Vec::new();
        for participant in participants {
            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), self.txn_id.to_string());
            headers.insert("txn_phase".to_string(), "commit".to_string());
            commit_messages.push((participant, headers, Vec::new()));
        }

        // Send all commit messages
        let _ = self.infra.send_messages_batch(commit_messages).await;

        *self.state.lock() = TransactionState::Committed;
        Ok(())
    }

    /// Abort transaction
    async fn abort(&self) -> Result<()> {
        {
            let mut state = self.state.lock();
            if matches!(
                *state,
                TransactionState::Committed | TransactionState::Aborted
            ) {
                return Ok(());
            }
            *state = TransactionState::Aborting;
        }

        let participants = self.participants.lock().clone();

        // Build abort messages
        let mut abort_messages = Vec::new();
        for participant in participants {
            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), self.txn_id.to_string());
            headers.insert("txn_phase".to_string(), "abort".to_string());
            abort_messages.push((participant, headers, Vec::new()));
        }

        // Send all abort messages
        if !abort_messages.is_empty() {
            let _ = self.infra.send_messages_batch(abort_messages).await;
        }

        *self.state.lock() = TransactionState::Aborted;
        Ok(())
    }

    /// Parse response into prepare vote
    fn parse_prepare_vote(&self, response: &ResponseMessage) -> PrepareVote {
        match response {
            ResponseMessage::Complete { .. } | ResponseMessage::Prepared { .. } => {
                PrepareVote::Prepared
            }
            ResponseMessage::Wounded { .. } => PrepareVote::Wounded,
            ResponseMessage::Error { .. } => PrepareVote::Error,
        }
    }

    /// Calculate new participants this stream doesn't know about
    fn calculate_new_participants(&self, participant: &str) -> Vec<String> {
        let awareness = self.participant_awareness.lock();
        let participants = self.participants.lock();

        let known = awareness.get(participant).cloned().unwrap_or_default();
        participants
            .iter()
            .filter(|p| *p != participant && !known.contains(*p))
            .cloned()
            .collect()
    }

    /// Update participant awareness tracking
    fn update_participant_awareness(&self, participant: &str, new_participants: &[String]) {
        let mut awareness = self.participant_awareness.lock();
        awareness
            .entry(participant.to_string())
            .or_default()
            .extend(new_participants.iter().cloned());
    }

    /// Collect prepare votes from all participants
    async fn collect_prepare_votes(
        &self,
        participants: &[String],
        request_id: &str,
    ) -> Result<bool> {
        let mut pending: HashSet<String> = participants.iter().cloned().collect();
        let timeout = common::calculate_timeout(self.deadline)?;
        let deadline = tokio::time::Instant::now() + timeout;

        while !pending.is_empty() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(CoordinatorError::PrepareTimeout);
            }

            match self
                .infra
                .response_collector
                .wait_for_response(request_id.to_string(), remaining)
                .await
            {
                Ok(response) => {
                    if let Some(participant) = response.participant() {
                        let participant = participant.to_string();
                        if pending.contains(&participant) {
                            let vote = self.parse_prepare_vote(&response);

                            self.prepare_votes
                                .lock()
                                .insert(participant.clone(), vote.clone());

                            pending.remove(&participant);

                            if !matches!(vote, PrepareVote::Prepared) {
                                return Ok(false);
                            }
                        }
                    }
                }
                Err(CoordinatorError::ResponseTimeout) => {
                    return Err(CoordinatorError::PrepareTimeout);
                }
                Err(e) => return Err(e),
            }
        }

        Ok(true)
    }

    /// Report outcome to prediction context
    async fn report_outcome(&self, success: bool) {
        let prediction_context = self.prediction_context.clone();
        tokio::spawn(async move {
            prediction_context.lock().await.report_outcome(success);
        });
    }
}

#[async_trait]
impl ExecutorTrait for ReadWriteExecutor {
    async fn execute<O: Operation + Send + Sync>(
        &self,
        stream: String,
        operation: &O,
    ) -> Result<Vec<u8>> {
        // Check state
        {
            let state = self.state.lock();
            if !matches!(*state, TransactionState::Active) {
                return Err(CoordinatorError::InvalidState(format!(
                    "Cannot execute operations in state {:?}",
                    *state
                )));
            }
        }

        // Try with speculation first
        match self.execute_with_speculation(&stream, operation).await {
            Ok(result) => Ok(result),
            Err(CoordinatorError::SpeculationFailed(_)) => {
                // Speculation failed - abort and let caller retry
                self.abort().await?;
                Err(CoordinatorError::SpeculationFailed(
                    "Speculation failed, transaction aborted".to_string(),
                ))
            }
            Err(e) => {
                // On any error, abort the transaction to avoid leaving it in an inconsistent state
                let _ = self.abort().await;
                Err(e)
            }
        }
    }

    async fn finish(&self) -> Result<()> {
        // Check if all predictions were used
        let should_abort = {
            let pred_context = self.prediction_context.lock().await;
            pred_context.prediction_count() > 0 && !pred_context.all_predictions_used()
        };

        if should_abort {
            self.abort().await?;
            return Err(CoordinatorError::SpeculationFailed(
                "Transaction did not use all predicted operations".to_string(),
            ));
        }

        // Commit transaction
        match self.commit().await {
            Ok(()) => {
                // Report success to prediction context
                self.report_outcome(true).await;
                Ok(())
            }
            Err(e) => {
                // If commit fails, abort to clean up
                let _ = self.abort().await;
                // Report failure to prediction context
                self.report_outcome(false).await;
                Err(e)
            }
        }
    }

    async fn cancel(&self) -> Result<()> {
        // Abort transaction
        self.abort().await?;

        // Report failure to prediction context
        self.report_outcome(false).await;

        Ok(())
    }
}
