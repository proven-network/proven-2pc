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
use proven_common::{Timestamp, TransactionId};
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
    txn_id: TransactionId,

    /// Transaction deadline
    deadline: Timestamp,

    /// Transaction state
    state: Arc<Mutex<TransactionState>>,

    /// Prepare votes from participants
    prepare_votes: Arc<Mutex<HashMap<String, PrepareVote>>>,

    /// Participant offsets (stream name -> log offset where first operation was sent)
    /// This serves as both the participant list (keys) and recovery metadata (offsets)
    participant_offsets: Arc<Mutex<HashMap<String, u64>>>,

    /// Streams that have received the deadline
    streams_with_deadline: Arc<Mutex<HashSet<String>>>,

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
        txn_id: TransactionId,
        deadline: Timestamp,
        infra: Arc<ExecutorInfra>,
        prediction_context: PredictionContext,
    ) -> Self {
        // Create participant_offsets early so speculation can populate it
        let participant_offsets = Arc::new(Mutex::new(HashMap::new()));

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

            let offsets_for_closure = participant_offsets.clone();

            let client = infra.client.clone();
            infra
                .execute_predictions(
                    prediction_context.predictions(),
                    timeout,
                    || {
                        let mut headers = HashMap::new();
                        headers.insert("txn_id".to_string(), txn_id_str.clone());
                        headers.insert("coordinator_id".to_string(), coordinator_id.clone());
                        headers.insert("txn_deadline".to_string(), deadline_str.clone());
                        headers
                    },
                    |streams_and_messages| {
                        let client = client.clone();
                        let offsets = offsets_for_closure.clone();
                        async move {
                            // Publish to each stream in parallel (no consensus group requirement)
                            let mut tasks = tokio::task::JoinSet::new();

                            for (stream, messages) in streams_and_messages {
                                let client = client.clone();
                                tasks.spawn(async move {
                                    let offset = client
                                        .publish_to_stream(stream.clone(), messages)
                                        .await
                                        .map_err(|e| {
                                            CoordinatorError::EngineError(e.to_string())
                                        })?;
                                    Ok::<_, CoordinatorError>((stream, offset))
                                });
                            }

                            // Wait for all publishes and collect offsets
                            while let Some(result) = tasks.join_next().await {
                                match result {
                                    Ok(Ok((stream, offset))) => {
                                        offsets.lock().insert(stream, offset);
                                    }
                                    Ok(Err(e)) => return Err(e),
                                    Err(_join_err) => {
                                        return Err(CoordinatorError::EngineError(
                                            "Task failed during speculation publish".to_string(),
                                        ));
                                    }
                                }
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
                tracing::debug!("Failed to execute predictions: {}", e);
                Arc::new(AsyncMutex::new(HashMap::new()))
            }
        };

        // Track all streams that we're sending speculative operations to
        let mut initial_streams_with_deadline = HashSet::new();
        for pred_op in prediction_context.predictions() {
            initial_streams_with_deadline.insert(pred_op.stream.clone());
        }

        Self {
            txn_id,
            deadline,
            state: Arc::new(Mutex::new(TransactionState::Active)),
            prepare_votes: Arc::new(Mutex::new(HashMap::new())),
            participant_offsets,
            streams_with_deadline: Arc::new(Mutex::new(initial_streams_with_deadline)),
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
    pub fn timestamp(&self) -> TransactionId {
        self.txn_id
    }

    /// Track participant and return deadline string if needed
    fn track_participant(&self, stream: &str) {
        // Note: We always send deadline now (required by protocol)
        // Just track that we've seen this participant
        self.streams_with_deadline.lock().insert(stream.to_string());
        // Note: participant_offsets will be populated when we get the offset back from send_transaction_operation
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
        let processor_type = operation.processor_type();

        let check_result = {
            let mut pred_context = self.prediction_context.lock().await;
            pred_context.check(stream, &op_value, processor_type, is_write)
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

        // Track participant (always send deadline now)
        self.track_participant(stream);

        // Use typed helper to send transaction operation
        let (offset, response) = self
            .infra
            .send_transaction_operation(
                stream,
                self.txn_id,
                self.deadline,
                operation.clone(),
                timeout,
            )
            .await?;

        // Store the offset for this participant (if not already stored)
        {
            let mut offsets = self.participant_offsets.lock();
            offsets.entry(stream.to_string()).or_insert(offset);
        }

        // Handle response
        ExecutorInfra::handle_response(response)
    }

    /// Prepare phase of 2PC
    async fn prepare(&self) -> Result<()> {
        let participants: Vec<String> = self.participant_offsets.lock().keys().cloned().collect();

        match participants.len() {
            0 => Ok(()), // No participants to prepare
            1 => self.prepare_single(&participants[0]).await,
            _ => self.prepare_multiple(&participants).await,
        }
    }

    /// Prepare with single participant optimization
    async fn prepare_single(&self, participant: &str) -> Result<()> {
        *self.state.lock() = TransactionState::Preparing;

        // Use PrepareAndCommit for single participant optimization
        use proven_protocol::TransactionPhase;
        let request_id = self.infra.generate_request_id();
        let timeout = common::calculate_timeout(self.deadline)?;

        match self
            .infra
            .send_control_message(
                participant,
                self.txn_id,
                TransactionPhase::PrepareAndCommit,
                Some(request_id),
                Some(timeout),
            )
            .await
        {
            Ok(Some(response)) => {
                let vote = self.parse_prepare_vote(&response);
                if matches!(vote, PrepareVote::Prepared) {
                    *self.state.lock() = TransactionState::Committed;
                    Ok(())
                } else {
                    let _ = self.abort().await;
                    Err(CoordinatorError::TransactionAborted)
                }
            }
            Ok(None) => {
                // Shouldn't happen since we provided request_id
                let _ = self.abort().await;
                Err(CoordinatorError::Other(
                    "No response from prepare".to_string(),
                ))
            }
            Err(e) => {
                // On error, abort the transaction
                let _ = self.abort().await;
                Err(e)
            }
        }
    }

    /// Standard 2PC prepare
    async fn prepare_multiple(&self, participants: &[String]) -> Result<()> {
        *self.state.lock() = TransactionState::Preparing;

        let request_id = self.infra.generate_request_id();

        // Get all participant offsets to send with PREPARE messages
        let all_participants = self.participant_offsets.lock().clone();

        // Send prepare messages and collect votes
        use proven_protocol::TransactionPhase;

        // Send all prepare messages in parallel (without waiting for responses)
        let mut tasks = tokio::task::JoinSet::new();

        for participant in participants {
            let infra = self.infra.clone();
            let txn_id = self.txn_id;
            let participants_clone = all_participants.clone();
            let req_id = request_id.clone();
            let participant = participant.clone(); // Clone the string to move into task

            tasks.spawn(async move {
                infra
                    .send_control_message(
                        &participant,
                        txn_id,
                        TransactionPhase::Prepare(participants_clone),
                        Some(req_id),
                        None, // Don't wait - we'll collect all responses in parallel
                    )
                    .await
            });
        }

        // Wait for all sends to complete and check for errors
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(_)) => continue, // Successfully sent (ignore response option)
                Ok(Err(e)) => {
                    // Failed to send prepare message - abort the transaction
                    let _ = self.abort().await;
                    return Err(e);
                }
                Err(_join_err) => {
                    // Task panic - abort the transaction
                    let _ = self.abort().await;
                    return Err(CoordinatorError::EngineError(
                        "Task failed during prepare send".to_string(),
                    ));
                }
            }
        }

        // Now collect all votes in parallel
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

        let participants: Vec<String> = self.participant_offsets.lock().keys().cloned().collect();
        if participants.is_empty() {
            *self.state.lock() = TransactionState::Committed;
            return Ok(());
        }

        *self.state.lock() = TransactionState::Committing;

        // Send commit messages in parallel (fire-and-forget)
        use proven_protocol::TransactionPhase;
        let mut tasks = tokio::task::JoinSet::new();

        for participant in participants {
            let infra = self.infra.clone();
            let txn_id = self.txn_id;
            tasks.spawn(async move {
                let _ = infra
                    .send_control_message(
                        &participant,
                        txn_id,
                        TransactionPhase::Commit,
                        None,
                        None,
                    )
                    .await;
            });
        }

        // Wait for all sends to complete (ignore individual results since fire-and-forget)
        while (tasks.join_next().await).is_some() {}

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

        let participants: Vec<String> = self.participant_offsets.lock().keys().cloned().collect();

        // Send abort messages in parallel (fire-and-forget)
        use proven_protocol::TransactionPhase;
        let mut tasks = tokio::task::JoinSet::new();

        for participant in participants {
            let infra = self.infra.clone();
            let txn_id = self.txn_id;
            tasks.spawn(async move {
                let _ = infra
                    .send_control_message(&participant, txn_id, TransactionPhase::Abort, None, None)
                    .await;
            });
        }

        // Wait for all sends to complete (ignore individual results since fire-and-forget)
        while (tasks.join_next().await).is_some() {}

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
