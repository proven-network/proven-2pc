//! Message routing for different transaction modes
//!
//! The router is responsible for determining the transaction mode and
//! dispatching messages to the appropriate execution module.

use crate::engine::{OperationResult, TransactionEngine, TransactionMode};
use crate::error::{ProcessorError, Result};
use crate::execution;
use crate::response::ResponseSender;
use crate::transaction::TransactionContext;
use crate::transaction::recovery::TransactionDecision;
use proven_common::{Timestamp, TransactionId};
use proven_engine::{Message, MockClient};
use proven_protocol::{CoordinatorMessage, OperationMessage, TransactionControlMessage};
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;

/// Message router that dispatches to appropriate execution paths
pub struct MessageRouter<E: TransactionEngine> {
    /// The storage engine
    engine: E,

    /// Transaction context with all state
    context: TransactionContext<E>,

    /// Engine client for sending responses
    client: Arc<MockClient>,

    /// Name of this stream
    stream_name: String,

    /// Response sender for coordinator communication
    response_sender: ResponseSender,
}

impl<E: TransactionEngine> MessageRouter<E> {
    /// Create a new message router
    pub fn new(engine: E, client: Arc<MockClient>, stream_name: String) -> Self {
        let engine_name = engine.engine_name().to_string();
        let context = TransactionContext::new(client.clone(), stream_name.clone());
        let response_sender =
            ResponseSender::new(client.clone(), stream_name.clone(), engine_name.clone());

        Self {
            engine,
            context,
            client,
            stream_name,
            response_sender,
        }
    }

    /// Get mutable reference to the context (for processor access)
    pub fn context_mut(&mut self) -> &mut TransactionContext<E> {
        &mut self.context
    }

    /// Get reference to the context (for processor access)
    pub fn context(&self) -> &TransactionContext<E> {
        &self.context
    }

    /// Get mutable reference to the engine (for snapshots)
    pub fn engine_mut(&mut self) -> &mut E {
        &mut self.engine
    }

    /// Route a message based on transaction mode
    pub async fn route_message(
        &mut self,
        message: Message,
        msg_timestamp: Timestamp,
        log_index: u64,
    ) -> Result<()> {
        // Track transaction state (deadlines, coordinator, participants)
        // This is needed for recovery to work correctly
        self.track_transaction_state(&message, TransactionId::new())?;

        // Determine transaction mode
        let mode = execution::get_transaction_mode(&message);

        match mode {
            TransactionMode::ReadOnly => {
                // Read-only transactions should come via pubsub, not the ordered stream
                // This maintains the optimization where read-only ops bypass Raft consensus
                Err(ProcessorError::InvalidOperation(
                    "Read-only transaction received on ordered stream. \
                     Read-only operations should use pubsub (stream.{stream_name}.readonly) \
                     to bypass Raft consensus."
                        .to_string(),
                ))
            }
            TransactionMode::AdHoc => {
                // Ad-hoc needs deferred manager and response sender
                execution::adhoc::execute_adhoc(
                    &mut self.engine,
                    message,
                    msg_timestamp,
                    log_index,
                    &self.response_sender,
                    &mut self.context.deferred_manager,
                )
                .await
            }
            TransactionMode::ReadWrite => {
                // Read-write needs full routing through this router
                self.process_read_write_message(message, msg_timestamp, log_index)
                    .await
            }
        }
    }

    /// Route a readonly message from pubsub (no log index)
    pub async fn route_readonly_message(&mut self, message: Message) -> Result<()> {
        execution::read_only::execute_read_only(
            &mut self.engine,
            message,
            &self.response_sender,
            &mut self.context.deferred_manager,
        )
        .await
    }

    /// Track transaction state during replay phase
    pub fn track_transaction_state(
        &mut self,
        message: &Message,
        _timestamp: TransactionId,
    ) -> Result<()> {
        // Extract transaction ID if present
        if let Some(txn_id_str) = message.get_header("txn_id") {
            let txn_id =
                TransactionId::parse(txn_id_str).map_err(ProcessorError::InvalidTransactionId)?;

            // Track deadline
            if let Some(deadline_str) = message.get_header("txn_deadline")
                && self.context.get_deadline(&txn_id).is_none()
                && let Ok(deadline) = Timestamp::parse(deadline_str)
            {
                self.context.set_deadline(txn_id, deadline);
            }

            // Track coordinator
            if let Some(coord_id) = message.get_header("coordinator_id") {
                self.context.set_coordinator(txn_id, coord_id.to_string());
            }

            // Track participants
            if let Some(participants_str) = message.get_header("participants")
                && let Ok(participants) =
                    serde_json::from_str::<HashMap<String, u64>>(participants_str)
            {
                self.context
                    .transaction_participants
                    .entry(txn_id)
                    .or_default()
                    .extend(participants);
            }

            // Track transaction phases for recovery
            if let Some(phase) = message.get_header("txn_phase") {
                match phase {
                    "commit" => {
                        self.context.cleanup_committed(&txn_id);
                    }
                    "abort" => {
                        self.context.cleanup_aborted(&txn_id);
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    /// Process a read-write transaction message
    async fn process_read_write_message(
        &mut self,
        message: Message,
        msg_timestamp: Timestamp,
        log_index: u64,
    ) -> Result<()> {
        // Parse message into typed structure
        let coord_msg = CoordinatorMessage::from_message(message)
            .map_err(|e| ProcessorError::InvalidOperation(e.to_string()))?;

        let (txn_id, txn_id_str, coordinator_id, request_id) = match &coord_msg {
            CoordinatorMessage::Operation(op) => (
                op.txn_id,
                op.txn_id.to_string(),
                Some(op.coordinator_id.as_str()),
                Some(op.request_id.clone()),
            ),
            CoordinatorMessage::Control(ctrl) => (
                ctrl.txn_id,
                ctrl.txn_id.to_string(),
                ctrl.coordinator_id.as_deref(),
                ctrl.request_id.clone(),
            ),
            CoordinatorMessage::ReadOnly { .. } => {
                return Err(ProcessorError::InvalidOperation(
                    "Read-only message received on read-write path".to_string(),
                ));
            }
        };

        // Check if wounded first - wounded status takes precedence over deadline
        if let Some(wounded_by) = self.context.is_wounded(&txn_id) {
            if let Some(coord_id) = coordinator_id {
                self.send_wounded_response(coord_id, &txn_id_str, wounded_by, request_id);
            }
            return Ok(());
        }

        // Check if we're past the deadline
        if let Some(deadline) = self.context.get_deadline(&txn_id)
            && msg_timestamp > deadline
        {
            if let Some(coord_id) = coordinator_id {
                self.send_error_response(
                    coord_id,
                    &txn_id_str,
                    "Transaction deadline exceeded".to_string(),
                    request_id,
                );
            }
            return Ok(());
        }

        // Dispatch based on message type
        match coord_msg {
            CoordinatorMessage::Control(ctrl) => {
                self.handle_control_message(ctrl, txn_id, &txn_id_str, msg_timestamp, log_index)
                    .await
            }
            CoordinatorMessage::Operation(op) => {
                self.handle_operation_message(op, txn_id, &txn_id_str, log_index)
                    .await
            }
            CoordinatorMessage::ReadOnly { .. } => unreachable!("Already checked above"),
        }
    }

    /// Handle transaction control messages using typed message
    async fn handle_control_message(
        &mut self,
        ctrl: TransactionControlMessage,
        txn_id: TransactionId,
        txn_id_str: &str,
        msg_timestamp: Timestamp,
        log_index: u64,
    ) -> Result<()> {
        use proven_protocol::TransactionPhase;

        let coordinator_id = ctrl.coordinator_id.as_deref();
        let request_id = ctrl.request_id;

        match ctrl.phase {
            TransactionPhase::Prepare => {
                self.handle_prepare(
                    txn_id,
                    txn_id_str,
                    coordinator_id,
                    request_id,
                    msg_timestamp,
                    log_index,
                )
                .await
            }
            TransactionPhase::PrepareAndCommit => {
                self.handle_prepare_and_commit(
                    txn_id,
                    txn_id_str,
                    coordinator_id,
                    request_id,
                    msg_timestamp,
                    log_index,
                )
                .await
            }
            TransactionPhase::Commit => {
                self.handle_commit(txn_id, txn_id_str, coordinator_id, request_id, log_index)
                    .await
            }
            TransactionPhase::Abort => {
                self.handle_abort(txn_id, txn_id_str, coordinator_id, request_id, log_index)
                    .await
            }
        }
    }

    /// Handle a regular operation message using typed message
    async fn handle_operation_message(
        &mut self,
        op: OperationMessage,
        txn_id: TransactionId,
        txn_id_str: &str,
        log_index: u64,
    ) -> Result<()> {
        // Deserialize the operation
        let operation: E::Operation = serde_json::from_slice(&op.operation).map_err(|e| {
            ProcessorError::InvalidOperation(format!("Failed to deserialize: {}", e))
        })?;

        let coordinator_id = &op.coordinator_id;
        let request_id = Some(op.request_id.clone());

        // Check if this is the first time seeing this transaction
        if !self.context.is_begun(&txn_id) {
            // Ensure we have a deadline
            if self.context.get_deadline(&txn_id).is_none() {
                if let Some(deadline) = op.txn_deadline {
                    self.context.set_deadline(txn_id, deadline);
                } else {
                    self.send_error_response(
                        coordinator_id,
                        txn_id_str,
                        "Transaction deadline required".to_string(),
                        request_id,
                    );
                    return Ok(());
                }
            }

            // Begin transaction
            self.engine.begin(txn_id, log_index);
            self.context.mark_begun(txn_id);
        }

        // Store coordinator ID
        self.context
            .set_coordinator(txn_id, coordinator_id.to_string());

        // Execute the operation
        self.execute_operation(
            operation,
            txn_id,
            txn_id_str,
            coordinator_id,
            request_id,
            log_index,
        )
        .await
    }

    /// Execute an operation and handle the result
    async fn execute_operation(
        &mut self,
        operation: E::Operation,
        txn_id: TransactionId,
        txn_id_str: &str,
        coordinator_id: &str,
        request_id: Option<String>,
        log_index: u64,
    ) -> Result<()> {
        match self
            .engine
            .apply_operation(operation.clone(), txn_id, log_index)
        {
            OperationResult::Complete(response) => {
                self.send_response(coordinator_id, txn_id_str, response, request_id);
                Ok(())
            }

            OperationResult::WouldBlock { blockers } => {
                // Find younger blockers to wound
                let younger_blockers: Vec<_> = blockers
                    .iter()
                    .filter(|b| b.txn > txn_id)
                    .map(|b| b.txn)
                    .collect();

                if !younger_blockers.is_empty() {
                    // Wound younger transactions
                    for victim in younger_blockers {
                        self.wound_transaction(victim, txn_id).await;
                    }

                    // Retry after wounding
                    match self
                        .engine
                        .apply_operation(operation.clone(), txn_id, log_index)
                    {
                        OperationResult::Complete(response) => {
                            self.send_response(coordinator_id, txn_id_str, response, request_id);
                            Ok(())
                        }
                        OperationResult::WouldBlock {
                            blockers: new_blockers,
                        } => {
                            // Still blocked - defer with all blockers
                            self.context.deferred_manager.defer_operation(
                                operation,
                                txn_id,
                                new_blockers,
                                coordinator_id.to_string(),
                                request_id,
                            );
                            Ok(())
                        }
                    }
                } else {
                    // All blockers are older - must wait for all of them
                    self.context.deferred_manager.defer_operation(
                        operation,
                        txn_id,
                        blockers,
                        coordinator_id.to_string(),
                        request_id,
                    );
                    Ok(())
                }
            }
        }
    }

    /// Handle prepare phase
    async fn handle_prepare(
        &mut self,
        txn_id: TransactionId,
        txn_id_str: &str,
        coordinator_id: Option<&str>,
        request_id: Option<String>,
        msg_timestamp: Timestamp,
        log_index: u64,
    ) -> Result<()> {
        // Check deadline
        if let Some(deadline) = self.context.get_deadline(&txn_id)
            && msg_timestamp > deadline
        {
            if let Some(coord_id) = coordinator_id {
                self.send_error_response(
                    coord_id,
                    txn_id_str,
                    "Prepare received after deadline".to_string(),
                    request_id,
                );
            }
            return Ok(());
        }

        // Check if wounded
        if let Some(wounded_by) = self.context.is_wounded(&txn_id) {
            if let Some(coord_id) = coordinator_id {
                self.send_wounded_response(coord_id, txn_id_str, wounded_by, request_id);
            }
            return Ok(());
        }

        // Check if transaction exists
        if !self.context.is_begun(&txn_id) {
            if let Some(coord_id) = coordinator_id {
                self.send_error_response(
                    coord_id,
                    txn_id_str,
                    format!("Transaction {} not found", txn_id),
                    request_id,
                );
            }
            return Ok(());
        }

        // Prepare the transaction
        self.engine.prepare(txn_id, log_index);

        // Schedule recovery
        if let Some(deadline) = self.context.get_deadline(&txn_id) {
            let participants = self
                .context
                .transaction_participants
                .get(&txn_id)
                .cloned()
                .unwrap_or_default();
            self.context
                .recovery_manager
                .schedule_recovery(txn_id, deadline, participants);
        }

        // Send response
        if let Some(coord_id) = coordinator_id {
            self.send_prepared_response(coord_id, txn_id_str, request_id);
        }

        // Retry prepare-waiting operations
        self.retry_prepare_waiting_operations(txn_id).await;

        Ok(())
    }

    /// Handle prepare and commit
    async fn handle_prepare_and_commit(
        &mut self,
        txn_id: TransactionId,
        txn_id_str: &str,
        coordinator_id: Option<&str>,
        request_id: Option<String>,
        msg_timestamp: Timestamp,
        log_index: u64,
    ) -> Result<()> {
        // Check deadline
        if let Some(deadline) = self.context.get_deadline(&txn_id)
            && msg_timestamp > deadline
        {
            if let Some(coord_id) = coordinator_id {
                self.send_error_response(
                    coord_id,
                    txn_id_str,
                    "Prepare received after deadline".to_string(),
                    request_id,
                );
            }
            return Ok(());
        }

        // Check if wounded
        if let Some(wounded_by) = self.context.is_wounded(&txn_id) {
            if let Some(coord_id) = coordinator_id {
                self.send_wounded_response(coord_id, txn_id_str, wounded_by, request_id);
            }
            return Ok(());
        }

        // Check if transaction exists
        if !self.context.is_begun(&txn_id) {
            if let Some(coord_id) = coordinator_id {
                self.send_error_response(
                    coord_id,
                    txn_id_str,
                    format!("Transaction {} not found", txn_id),
                    request_id,
                );
            }
            return Ok(());
        }

        // Prepare and commit
        self.engine.prepare(txn_id, log_index);
        self.engine.commit(txn_id, log_index);

        // Send response
        if let Some(coord_id) = coordinator_id {
            self.send_prepared_response(coord_id, txn_id_str, request_id);
        }

        // Retry waiting operations
        self.retry_prepare_waiting_operations(txn_id).await;
        self.retry_deferred_operations(txn_id).await;

        Ok(())
    }

    /// Handle commit phase
    async fn handle_commit(
        &mut self,
        txn_id: TransactionId,
        _txn_id_str: &str,
        _coordinator_id: Option<&str>,
        _request_id: Option<String>,
        log_index: u64,
    ) -> Result<()> {
        // Commit the transaction
        self.engine.commit(txn_id, log_index);

        // Clean up state
        self.context.cleanup_committed(&txn_id);

        // Retry deferred operations
        self.retry_deferred_operations(txn_id).await;

        Ok(())
    }

    /// Handle abort phase
    async fn handle_abort(
        &mut self,
        txn_id: TransactionId,
        _txn_id_str: &str,
        _coordinator_id: Option<&str>,
        _request_id: Option<String>,
        log_index: u64,
    ) -> Result<()> {
        // Abort the transaction
        self.engine.abort(txn_id, log_index);

        // Clean up state
        self.context.cleanup_aborted(&txn_id);

        // Retry deferred operations
        self.retry_deferred_operations(txn_id).await;

        Ok(())
    }

    /// Wound a transaction
    async fn wound_transaction(&mut self, victim: TransactionId, wounded_by: TransactionId) {
        // Mark as wounded
        self.context.wound_transaction(victim, wounded_by);

        // Notify coordinator if known
        if let Some(victim_coord) = self.context.get_coordinator(&victim) {
            let victim_str = victim.to_string();
            self.send_wounded_response(&victim_coord, &victim_str, wounded_by, None);
        }

        // Abort the victim (with dummy log index since this is internally initiated)
        self.engine.abort(victim, 0);

        // Clean up
        self.context.cleanup_aborted(&victim);
    }

    /// Retry operations waiting on prepare
    async fn retry_prepare_waiting_operations(&mut self, prepared_txn: TransactionId) {
        let waiting_ops = self
            .context
            .deferred_manager
            .take_prepare_waiting_operations(&prepared_txn);

        for deferred in waiting_ops {
            if let Err(e) = self
                .execute_operation(
                    deferred.operation,
                    deferred.txn_id,
                    &deferred.txn_id.to_string(),
                    &deferred.coordinator_id,
                    deferred.request_id,
                    0, // Deferred operations use dummy log index
                )
                .await
            {
                tracing::error!(
                    "Failed to retry deferred operation for txn {}: {:?}",
                    deferred.txn_id,
                    e
                );
            }
        }
    }

    /// Retry operations waiting on commit/abort
    async fn retry_deferred_operations(&mut self, completed_txn: TransactionId) {
        let waiting_ops = self
            .context
            .deferred_manager
            .take_commit_waiting_operations(&completed_txn);

        for deferred in waiting_ops {
            // Check if this is a read-only operation (would have used read timestamp as txn_id)
            // For now, treat all deferred operations the same way
            // In the future, we might want to re-route read-only ops differently
            if let Err(e) = self
                .execute_operation(
                    deferred.operation,
                    deferred.txn_id,
                    &deferred.txn_id.to_string(),
                    &deferred.coordinator_id,
                    deferred.request_id,
                    0, // Deferred operations use dummy log index
                )
                .await
            {
                tracing::error!(
                    "Failed to retry deferred operation for txn {}: {:?}",
                    deferred.txn_id,
                    e
                );
            }
        }
    }

    /// Run recovery check for expired transactions
    pub async fn run_recovery_check(&mut self, current_time: Timestamp) -> Result<()> {
        let expired = self.context.get_expired_transactions(current_time);

        for txn_id in expired {
            let participants = self
                .context
                .transaction_participants
                .get(&txn_id)
                .cloned()
                .unwrap_or_default();

            let decision = self
                .context
                .recovery_manager
                .execute_recovery(txn_id, participants, current_time)
                .await;

            match decision {
                TransactionDecision::Commit => {
                    // Publish COMMIT message to our own stream so it gets processed normally
                    let mut headers = HashMap::new();
                    headers.insert("txn_id".to_string(), txn_id.to_string());
                    headers.insert("txn_phase".to_string(), "commit".to_string());

                    let message = Message::new(Vec::new(), headers);
                    if let Err(e) = self
                        .client
                        .publish_to_stream(self.stream_name.clone(), vec![message])
                        .await
                    {
                        tracing::error!(
                            "[{}] Failed to publish recovery COMMIT: {:?}",
                            self.stream_name,
                            e
                        );
                    } else {
                        // Mark as resolved immediately to prevent repeated recovery attempts
                        // The actual commit will be processed when the message comes through
                        self.context.resolved_transactions.insert(txn_id);
                        // Remove deadline to stop triggering recovery checks
                        self.context.transaction_deadlines.remove(&txn_id);
                    }
                }
                TransactionDecision::Abort => {
                    // Publish ABORT message to our own stream so it gets processed normally
                    let mut headers = HashMap::new();
                    headers.insert("txn_id".to_string(), txn_id.to_string());
                    headers.insert("txn_phase".to_string(), "abort".to_string());

                    let message = Message::new(Vec::new(), headers);
                    if let Err(e) = self
                        .client
                        .publish_to_stream(self.stream_name.clone(), vec![message])
                        .await
                    {
                        tracing::error!(
                            "[{}] Failed to publish recovery ABORT: {:?}",
                            self.stream_name,
                            e
                        );
                    } else {
                        // Mark as resolved immediately to prevent repeated recovery attempts
                        // The actual abort will be processed when the message comes through
                        self.context.resolved_transactions.insert(txn_id);
                        // Remove deadline to stop triggering recovery checks
                        self.context.transaction_deadlines.remove(&txn_id);
                    }
                }
                TransactionDecision::Unknown => {
                    // Leave for future recovery
                }
            }
        }

        Ok(())
    }

    // Response sending methods (delegated to ResponseSender)

    fn send_response(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        response: E::Response,
        request_id: Option<String>,
    ) {
        self.response_sender
            .send_success(coordinator_id, Some(txn_id), request_id, response);
    }

    fn send_prepared_response(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        request_id: Option<String>,
    ) {
        self.response_sender
            .send_prepared(coordinator_id, txn_id, request_id);
    }

    fn send_wounded_response(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        wounded_by: TransactionId,
        request_id: Option<String>,
    ) {
        self.response_sender
            .send_wounded(coordinator_id, txn_id, wounded_by, request_id);
    }

    fn send_error_response(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        error: String,
        request_id: Option<String>,
    ) {
        self.response_sender
            .send_error(coordinator_id, Some(txn_id), error, request_id);
    }
}
