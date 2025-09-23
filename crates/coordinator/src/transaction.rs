//! Transaction implementation with active methods
//!
//! The Transaction handles state management and 2PC coordination,
//! while delegating all execution to the transaction-scoped Executor.

use crate::error::{CoordinatorError, Result};
use crate::executor::Executor;
use crate::responses::ResponseMessage;
use crate::speculation::{CheckResult, PredictionContext};
use parking_lot::Mutex;
use proven_common::Operation;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

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

/// Active transaction object with methods
#[derive(Clone)]
pub struct Transaction {
    /// Transaction state
    state: Arc<Mutex<TransactionState>>,

    /// Prepare votes from participants
    prepare_votes: Arc<Mutex<HashMap<String, PrepareVote>>>,

    /// Transaction-scoped executor with all context
    executor: Arc<Executor>,

    /// Speculative execution context for pattern learning (async mutex to avoid deadlocks)
    prediction_context: Arc<AsyncMutex<PredictionContext>>,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(executor: Arc<Executor>, prediction_context: PredictionContext) -> Self {
        Self {
            state: Arc::new(Mutex::new(TransactionState::Active)),
            prepare_votes: Arc::new(Mutex::new(HashMap::new())),
            executor,
            prediction_context: Arc::new(AsyncMutex::new(prediction_context)),
        }
    }

    /// Get the transaction ID
    pub fn id(&self) -> &str {
        &self.executor.txn_id
    }

    /// Get the current transaction state
    pub fn state(&self) -> TransactionState {
        self.state.lock().clone()
    }

    /// Get the transaction timestamp
    pub fn timestamp(&self) -> proven_hlc::HlcTimestamp {
        self.executor.timestamp
    }

    /// Execute an operation on a stream
    ///
    /// This method:
    /// 1. Checks predictions first for speculated responses
    /// 2. Falls back to normal execution if no prediction
    /// 3. Aborts on speculation failure
    pub async fn execute<O: Operation>(&self, stream: String, operation: &O) -> Result<Vec<u8>> {
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

        // Determine if this is a write operation
        let is_write = operation.operation_type() == proven_common::OperationType::Write;

        // Check with predictions first
        let op_value =
            serde_json::to_value(operation).map_err(CoordinatorError::SerializationError)?;

        // Use async-aware mutex for prediction context
        let check_result = {
            let mut pred_context = self.prediction_context.lock().await;
            pred_context.check(&stream, &op_value, is_write).await
        };

        match check_result {
            CheckResult::Match { response, .. } => {
                // Use speculated response directly
                Ok(response)
            }
            CheckResult::SpeculativeExecutionFailed { position, reason } => {
                // Speculation failed - abort transaction
                // Client should retry with begin_without_speculation()
                self.abort().await?;
                Err(CoordinatorError::SpeculationFailed(format!(
                    "Speculated operation {} failed: {}",
                    position, reason
                )))
            }
            CheckResult::NoPrediction => {
                // Execute normally through executor
                self.executor.execute_operation(&stream, operation).await
            }
            CheckResult::SpeculationFailed {
                expected,
                actual,
                position,
            } => {
                // Speculation failed - abort transaction
                // Client should retry with begin_without_speculation()
                self.abort().await?;
                Err(CoordinatorError::SpeculationFailed(format!(
                    "Operation mismatch at position {}: expected {}, got {}",
                    position, expected, actual
                )))
            }
        }
    }

    /// Commit this transaction
    pub async fn commit(&self) -> Result<()> {
        // Check state
        {
            let state = self.state.lock();
            if !matches!(*state, TransactionState::Active) {
                return Err(CoordinatorError::InvalidState(format!(
                    "Cannot commit transaction in state {:?}",
                    *state
                )));
            }
        }

        // Check if all predictions were used
        let should_abort = {
            let pred_context = self.prediction_context.lock().await;
            pred_context.prediction_count() > 0 && !pred_context.all_predictions_used()
        }; // Lock is dropped here

        if should_abort {
            // Transaction didn't follow predicted pattern - abort
            self.abort().await?;
            return Err(CoordinatorError::SpeculationFailed(
                "Transaction did not use all predicted operations".to_string(),
            ));
        }

        let participants = self.executor.participants();

        match participants.len() {
            0 => self.commit_empty().await,
            1 => self.commit_single(&participants[0]).await,
            _ => self.commit_multiple(&participants).await,
        }
    }

    /// Abort this transaction
    pub async fn abort(&self) -> Result<()> {
        {
            let mut state = self.state.lock();
            if matches!(
                *state,
                TransactionState::Committed | TransactionState::Aborted
            ) {
                return Ok(()); // Already terminated
            }
            *state = TransactionState::Aborting;
        }

        let participants = self.executor.participants();

        // Build all abort messages
        let mut abort_messages = Vec::new();
        for participant in participants {
            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), self.executor.txn_id.clone());
            headers.insert("txn_phase".to_string(), "abort".to_string());

            abort_messages.push((participant.clone(), headers, Vec::new()));
        }

        // Send all abort messages in a single batch (fire and forget)
        if !abort_messages.is_empty() {
            let _ = self.executor.send_messages_batch(abort_messages).await;
        }

        *self.state.lock() = TransactionState::Aborted;

        // Report failed transaction to prediction context for learning (async)
        self.report_outcome_async(false).await;

        Ok(())
    }

    /// Commit empty transaction (no participants)
    async fn commit_empty(&self) -> Result<()> {
        *self.state.lock() = TransactionState::Committed;

        // Report successful commit to prediction context for learning (async)
        self.report_outcome_async(true).await;

        Ok(())
    }

    /// Commit with single participant optimization
    async fn commit_single(&self, participant: &str) -> Result<()> {
        // Update state
        *self.state.lock() = TransactionState::Preparing;

        // Send prepare_and_commit
        let request_id = self
            .executor
            .generate_phase_request_id("prepare_and_commit");
        let headers = self
            .executor
            .build_phase_headers("prepare_and_commit", &request_id);

        let timeout = self.executor.get_timeout()?;

        let response = self
            .executor
            .send_and_wait(participant, headers, Vec::new(), timeout)
            .await?;

        // Parse and record vote
        let vote = self.parse_prepare_vote(&response);
        {
            let mut votes = self.prepare_votes.lock();
            votes.insert(participant.to_string(), vote.clone());
        }

        if matches!(vote, PrepareVote::Prepared) {
            *self.state.lock() = TransactionState::Committed;

            // Report successful commit to prediction context for learning (async)
            self.report_outcome_async(true).await;

            Ok(())
        } else {
            *self.state.lock() = TransactionState::Aborted;
            Err(CoordinatorError::TransactionAborted)
        }
    }

    /// Commit with standard 2PC
    async fn commit_multiple(&self, participants: &[String]) -> Result<()> {
        // Phase 1: Prepare
        *self.state.lock() = TransactionState::Preparing;

        // Send prepare to all participants
        let request_id = self.executor.generate_phase_request_id("prepare");

        // Build all prepare messages
        let mut prepare_messages = Vec::new();
        for participant in participants {
            // Calculate new participants this one doesn't know about
            let new_participants = self.executor.calculate_new_participants(participant);

            // Build headers
            let mut headers = self.executor.build_phase_headers("prepare", &request_id);

            if !new_participants.is_empty() {
                // Update awareness tracking
                self.executor
                    .update_participant_awareness(participant, &new_participants);

                // Add new participants to headers
                headers.insert(
                    "new_participants".to_string(),
                    serde_json::to_string(&new_participants).unwrap(),
                );
            }

            prepare_messages.push((participant.clone(), headers, Vec::new()));
        }

        // Send all prepare messages in a single batch
        self.executor.send_messages_batch(prepare_messages).await?;

        // Collect prepare votes from all participants
        let all_prepared = self
            .collect_prepare_votes(participants, &request_id)
            .await?;

        if !all_prepared {
            // Abort if any participant couldn't prepare
            self.abort().await?;
            return Err(CoordinatorError::TransactionAborted);
        }

        // Update state to prepared
        *self.state.lock() = TransactionState::Prepared;

        // Phase 2: Commit
        *self.state.lock() = TransactionState::Committing;

        // Build all commit messages
        let mut commit_messages = Vec::new();
        for participant in participants {
            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), self.executor.txn_id.clone());
            headers.insert("txn_phase".to_string(), "commit".to_string());

            commit_messages.push((participant.clone(), headers, Vec::new()));
        }

        // Send all commit messages in a single batch (fire and forget)
        let _ = self.executor.send_messages_batch(commit_messages).await;

        // Mark as committed
        *self.state.lock() = TransactionState::Committed;

        // Report successful commit to prediction context for learning (async)
        self.report_outcome_async(true).await;

        Ok(())
    }

    /// Parse a response into a prepare vote
    fn parse_prepare_vote(&self, response: &ResponseMessage) -> PrepareVote {
        match response {
            ResponseMessage::Complete { .. } | ResponseMessage::Prepared { .. } => {
                PrepareVote::Prepared
            }
            ResponseMessage::Wounded { wounded_by, .. } => PrepareVote::Wounded {
                wounded_by: wounded_by.to_string(),
            },
            ResponseMessage::Error { kind, .. } => PrepareVote::Error(format!("{:?}", kind)),
        }
    }

    /// Collect prepare votes from participants
    async fn collect_prepare_votes(
        &self,
        participants: &[String],
        request_id: &str,
    ) -> Result<bool> {
        let mut pending_participants: HashSet<String> = participants.iter().cloned().collect();

        // Calculate timeout for collecting votes
        let timeout_duration = self.executor.get_timeout()?;

        let deadline = tokio::time::Instant::now() + timeout_duration;

        // Get response collector
        let response_collector = self.executor.response_collector();

        while !pending_participants.is_empty() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(CoordinatorError::PrepareTimeout);
            }

            // Try to get a response
            match response_collector
                .wait_for_response(request_id.to_string(), remaining)
                .await
            {
                Ok(response) => {
                    // Get participant from response
                    if let Some(participant_str) = response.participant() {
                        let participant = participant_str.to_string();

                        if pending_participants.contains(&participant) {
                            let vote = self.parse_prepare_vote(&response);

                            // Record vote
                            {
                                let mut votes = self.prepare_votes.lock();
                                votes.insert(participant.clone(), vote.clone());
                            }

                            pending_participants.remove(&participant);

                            // Check if this is a negative vote
                            if !matches!(vote, PrepareVote::Prepared) {
                                return Ok(false); // Abort if any participant can't prepare
                            }
                        }
                    }
                }
                Err(CoordinatorError::ResponseTimeout) => {
                    // Timeout waiting for a participant
                    return Err(CoordinatorError::PrepareTimeout);
                }
                Err(e) => return Err(e),
            }
        }

        // All participants voted to prepare
        Ok(true)
    }

    /// Report transaction outcome to prediction context asynchronously
    /// This spawns a background task to avoid blocking the commit path
    async fn report_outcome_async(&self, success: bool) {
        let prediction_context = self.prediction_context.clone();

        tokio::spawn(async move {
            prediction_context.lock().await.report_outcome(success);
        });
    }
}
