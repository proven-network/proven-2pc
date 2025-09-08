//! Generic stream processor that handles distributed transaction coordination
//!
//! This processor consumes messages from a stream and delegates storage
//! operations to a TransactionEngine implementation. It handles all the
//! distributed systems concerns like retries, deferrals, and coordinator
//! communication.

use crate::deferred::DeferredOperationsManager;
use crate::engine::{OperationResult, TransactionEngine};
use crate::error::{ProcessorError, Result};
use crate::recovery::{RecoveryManager, TransactionDecision};
use proven_engine::{Message, MockClient};
use proven_hlc::HlcTimestamp;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

/// Processing phase for the stream processor
#[derive(Debug, Clone)]
pub enum ProcessorPhase {
    /// Replaying historical messages to rebuild state
    Replay,
    /// Checking for and executing recovery
    Recovery,
    /// Normal live processing
    Live,
}

/// Generic stream processor that works with any TransactionEngine
pub struct StreamProcessor<E: TransactionEngine> {
    /// The storage engine that executes operations
    engine: E,

    /// Manager for deferred operations (blocked on locks)
    deferred_manager: DeferredOperationsManager<E::Operation>,

    /// Map from transaction ID to coordinator ID (for responses)
    transaction_coordinators: HashMap<HlcTimestamp, String>,

    /// Track wounded transactions (txn_id -> wounded_by)
    wounded_transactions: HashMap<HlcTimestamp, HlcTimestamp>,

    /// Track transaction deadlines (txn_id -> deadline)
    transaction_deadlines: HashMap<HlcTimestamp, HlcTimestamp>,

    /// Track transaction participants (txn_id -> (participant -> offset))
    transaction_participants: HashMap<HlcTimestamp, HashMap<String, u64>>,

    /// Recovery manager for handling coordinator failures
    recovery_manager: RecoveryManager<E>,

    /// Engine client for sending responses
    client: Arc<MockClient>,

    /// Name of this stream for identification
    stream_name: String,

    /// Current processing phase
    phase: ProcessorPhase,

    /// Current stream offset (for phase transitions)
    current_offset: u64,
}

impl<E: TransactionEngine> StreamProcessor<E> {
    /// Create a new stream processor (starts in replay mode)
    pub fn new(engine: E, client: Arc<MockClient>, stream_name: String) -> Self {
        Self {
            engine,
            deferred_manager: DeferredOperationsManager::new(),
            transaction_coordinators: HashMap::new(),
            wounded_transactions: HashMap::new(),
            transaction_deadlines: HashMap::new(),
            transaction_participants: HashMap::new(),
            recovery_manager: RecoveryManager::new(client.clone(), stream_name.clone()),
            client,
            stream_name,
            phase: ProcessorPhase::Replay,
            current_offset: 0,
        }
    }

    /// Get the current processing phase
    pub fn phase(&self) -> &ProcessorPhase {
        &self.phase
    }

    /// Process a message from the stream with phase awareness
    async fn process_message(
        &mut self,
        message: Message,
        msg_timestamp: HlcTimestamp,
        msg_offset: u64,
    ) -> Result<()> {
        self.current_offset = msg_offset;

        // Always track state regardless of phase
        self.track_transaction_state(&message, msg_timestamp)?;

        match self.phase.clone() {
            ProcessorPhase::Replay => {
                // Just rebuild state, no responses or side effects
                // Transitions are handled by run() method
                Ok(())
            }
            ProcessorPhase::Recovery => {
                // Should not receive messages in this phase
                unreachable!("Should not process messages during recovery phase")
            }
            ProcessorPhase::Live => {
                // Normal processing
                self.process_message_live(message, msg_timestamp).await
            }
        }
    }

    /// Track transaction state without side effects (for replay phase)
    fn track_transaction_state(
        &mut self,
        message: &Message,
        _timestamp: HlcTimestamp,
    ) -> Result<()> {
        let txn_id_str = message
            .txn_id()
            .ok_or(ProcessorError::MissingHeader("txn_id"))?
            .to_string();
        let txn_id = HlcTimestamp::parse(&txn_id_str)
            .map_err(|e| ProcessorError::InvalidTransactionId(e))?;

        // Track deadline
        if let Some(deadline_str) = message.txn_deadline() {
            if !self.transaction_deadlines.contains_key(&txn_id) {
                if let Ok(deadline) = HlcTimestamp::parse(deadline_str) {
                    self.transaction_deadlines.insert(txn_id, deadline);
                }
            }
        }

        // Track coordinator
        if let Some(coord_id) = message.coordinator_id() {
            self.transaction_coordinators
                .insert(txn_id, coord_id.to_string());
        }

        // Track participants
        if let Some(participants_str) = message.get_header("new_participants") {
            if let Ok(participants) = serde_json::from_str::<HashMap<String, u64>>(participants_str)
            {
                self.transaction_participants
                    .entry(txn_id)
                    .or_insert_with(HashMap::new)
                    .extend(participants);
            }
        }

        // Track transaction phases for recovery
        if let Some(phase) = message.txn_phase() {
            match phase {
                "prepare" | "prepare_and_commit" => {
                    // Transaction is in prepared state
                    // We could track this explicitly if needed
                }
                "commit" => {
                    // Transaction committed, remove from tracking
                    self.transaction_coordinators.remove(&txn_id);
                    self.transaction_deadlines.remove(&txn_id);
                    self.transaction_participants.remove(&txn_id);
                }
                "abort" => {
                    // Transaction aborted, remove from tracking
                    self.transaction_coordinators.remove(&txn_id);
                    self.transaction_deadlines.remove(&txn_id);
                    self.transaction_participants.remove(&txn_id);
                    self.wounded_transactions.remove(&txn_id);
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Process a message normally (live phase)
    async fn process_message_live(
        &mut self,
        message: Message,
        msg_timestamp: HlcTimestamp,
    ) -> Result<()> {
        // Extract transaction ID
        let txn_id_str = message
            .txn_id()
            .ok_or(ProcessorError::MissingHeader("txn_id"))?
            .to_string();

        let txn_id = HlcTimestamp::parse(&txn_id_str)
            .map_err(|e| ProcessorError::InvalidTransactionId(e))?;

        // State tracking is already done in track_transaction_state()

        // Check if we're past the deadline
        if let Some(&deadline) = self.transaction_deadlines.get(&txn_id) {
            if msg_timestamp > deadline {
                // Past deadline - reject the operation
                if let Some(coordinator_id) = message.coordinator_id() {
                    let request_id = message.request_id().map(|s| s.to_string());
                    self.send_error_response(
                        coordinator_id,
                        &txn_id_str,
                        "Transaction deadline exceeded".to_string(),
                        request_id,
                    );
                }
                return Ok(());
            }
        }

        // Handle transaction control messages (empty body)
        if message.is_transaction_control() {
            return self
                .handle_control_message(message, txn_id, &txn_id_str, msg_timestamp)
                .await;
        }

        // Handle regular operations
        self.handle_operation_message(message, txn_id, &txn_id_str)
            .await
    }

    /// Handle transaction control messages (prepare, commit, abort)
    async fn handle_control_message(
        &mut self,
        message: Message,
        txn_id: HlcTimestamp,
        txn_id_str: &str,
        msg_timestamp: HlcTimestamp,
    ) -> Result<()> {
        let phase = message
            .txn_phase()
            .ok_or(ProcessorError::MissingHeader("txn_phase"))?;

        let coordinator_id = message.coordinator_id();
        let request_id = message.request_id().map(|s| s.to_string());

        match phase {
            "prepare" => {
                self.handle_prepare(
                    txn_id,
                    txn_id_str,
                    coordinator_id,
                    request_id,
                    msg_timestamp,
                )
                .await
            }
            "prepare_and_commit" => {
                self.handle_prepare_and_commit(
                    txn_id,
                    txn_id_str,
                    coordinator_id,
                    request_id,
                    msg_timestamp,
                )
                .await
            }
            "commit" => {
                self.handle_commit(txn_id, txn_id_str, coordinator_id, request_id)
                    .await
            }
            "abort" => {
                self.handle_abort(txn_id, txn_id_str, coordinator_id, request_id)
                    .await
            }
            unknown => Err(ProcessorError::UnknownPhase(unknown.to_string())),
        }
    }

    /// Handle a regular operation message
    async fn handle_operation_message(
        &mut self,
        message: Message,
        txn_id: HlcTimestamp,
        txn_id_str: &str,
    ) -> Result<()> {
        // Deserialize the operation
        let operation: E::Operation = serde_json::from_slice(&message.body).map_err(|e| {
            ProcessorError::InvalidOperation(format!("Failed to deserialize: {}", e))
        })?;

        // Get coordinator ID for responses
        let coordinator_id = message
            .coordinator_id()
            .ok_or(ProcessorError::MissingHeader("coordinator_id"))?;

        let request_id = message.request_id().map(|s| s.to_string());

        // Ensure transaction exists and has a deadline
        if !self.engine.is_transaction_active(&txn_id) {
            // For new transactions, deadline must be present
            if !self.transaction_deadlines.contains_key(&txn_id) {
                // Try to extract deadline from message
                if let Some(deadline_str) = message.txn_deadline() {
                    if let Ok(deadline) = HlcTimestamp::parse(deadline_str) {
                        self.transaction_deadlines.insert(txn_id, deadline);
                    } else {
                        // Invalid deadline format
                        self.send_error_response(
                            coordinator_id,
                            txn_id_str,
                            "Invalid transaction deadline format".to_string(),
                            request_id,
                        );
                        return Ok(());
                    }
                } else {
                    // No deadline provided for new transaction
                    self.send_error_response(
                        coordinator_id,
                        txn_id_str,
                        "Transaction deadline required".to_string(),
                        request_id,
                    );
                    return Ok(());
                }
            }
            self.engine.begin_transaction(txn_id);
        }

        // Store coordinator ID for this transaction
        self.transaction_coordinators
            .insert(txn_id, coordinator_id.to_string());

        // Execute the operation
        self.execute_operation(operation, txn_id, txn_id_str, coordinator_id, request_id)
            .await?;

        // Handle auto-commit if specified
        if message.is_auto_commit() {
            self.engine
                .commit(txn_id)
                .map_err(ProcessorError::EngineError)?;
            self.retry_deferred_operations(txn_id).await;
        }

        Ok(())
    }

    /// Execute an operation and handle the result
    async fn execute_operation(
        &mut self,
        operation: E::Operation,
        txn_id: HlcTimestamp,
        txn_id_str: &str,
        coordinator_id: &str,
        request_id: Option<String>,
    ) -> Result<()> {
        match self.engine.apply_operation(operation.clone(), txn_id) {
            OperationResult::Success(response) => {
                self.send_response(coordinator_id, txn_id_str, response, request_id);
                Ok(())
            }

            OperationResult::WouldBlock { blocking_txn } => {
                // Decide: wound or wait based on transaction ages
                if txn_id < blocking_txn {
                    // We're older - wound the younger blocking transaction
                    self.wound_transaction(blocking_txn, txn_id).await;

                    // Retry the operation after wounding
                    match self.engine.apply_operation(operation.clone(), txn_id) {
                        OperationResult::Success(response) => {
                            self.send_response(coordinator_id, txn_id_str, response, request_id);
                            Ok(())
                        }
                        OperationResult::WouldBlock {
                            blocking_txn: new_blocker,
                        } => {
                            // Still blocked (shouldn't happen after wounding, but handle it)
                            self.send_deferred_response(
                                coordinator_id,
                                txn_id_str,
                                new_blocker,
                                request_id.clone(),
                            );
                            self.deferred_manager.defer_operation(
                                operation,
                                txn_id,
                                new_blocker,
                                coordinator_id.to_string(),
                                request_id,
                            );
                            Ok(())
                        }
                        OperationResult::Error(msg) => {
                            self.send_error_response(
                                coordinator_id,
                                txn_id_str,
                                msg.clone(),
                                request_id,
                            );
                            Err(ProcessorError::EngineError(msg))
                        }
                    }
                } else {
                    // We're younger - must wait
                    self.send_deferred_response(
                        coordinator_id,
                        txn_id_str,
                        blocking_txn,
                        request_id.clone(),
                    );
                    self.deferred_manager.defer_operation(
                        operation,
                        txn_id,
                        blocking_txn,
                        coordinator_id.to_string(),
                        request_id,
                    );
                    Ok(())
                }
            }

            OperationResult::Error(msg) => {
                self.send_error_response(coordinator_id, txn_id_str, msg.clone(), request_id);
                Err(ProcessorError::EngineError(msg))
            }
        }
    }

    /// Handle prepare phase
    async fn handle_prepare(
        &mut self,
        txn_id: HlcTimestamp,
        txn_id_str: &str,
        coordinator_id: Option<&str>,
        request_id: Option<String>,
        msg_timestamp: HlcTimestamp,
    ) -> Result<()> {
        // Check if prepare message arrived after deadline
        if let Some(&deadline) = self.transaction_deadlines.get(&txn_id) {
            if msg_timestamp > deadline {
                // Past deadline - don't vote prepared
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
        }
        // Check if transaction was wounded
        if let Some(wounded_by) = self.wounded_transactions.get(&txn_id) {
            if let Some(coord_id) = coordinator_id {
                self.send_wounded_response(coord_id, txn_id_str, *wounded_by, request_id);
            }
            return Ok(());
        }

        match self.engine.prepare(txn_id) {
            Ok(()) => {
                // Schedule recovery after deadline
                if let Some(&deadline) = self.transaction_deadlines.get(&txn_id) {
                    let participants = self
                        .transaction_participants
                        .get(&txn_id)
                        .cloned()
                        .unwrap_or_default();
                    self.recovery_manager
                        .schedule_recovery(txn_id, deadline, participants);
                }

                if let Some(coord_id) = coordinator_id {
                    self.send_prepared_response(coord_id, txn_id_str, request_id);
                }
                Ok(())
            }
            Err(e) => {
                if let Some(coord_id) = coordinator_id {
                    self.send_error_response(coord_id, txn_id_str, e.clone(), request_id);
                }
                Err(ProcessorError::EngineError(e))
            }
        }
    }

    /// Handle combined prepare and commit (single participant optimization)
    async fn handle_prepare_and_commit(
        &mut self,
        txn_id: HlcTimestamp,
        txn_id_str: &str,
        coordinator_id: Option<&str>,
        request_id: Option<String>,
        msg_timestamp: HlcTimestamp,
    ) -> Result<()> {
        // Check if prepare_and_commit message arrived after deadline
        if let Some(&deadline) = self.transaction_deadlines.get(&txn_id) {
            if msg_timestamp > deadline {
                // Past deadline - don't vote prepared
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
        }
        // Check if transaction was wounded
        if let Some(wounded_by) = self.wounded_transactions.get(&txn_id) {
            if let Some(coord_id) = coordinator_id {
                self.send_wounded_response(coord_id, txn_id_str, *wounded_by, request_id);
            }
            return Ok(());
        }

        // Try to prepare
        match self.engine.prepare(txn_id) {
            Ok(()) => {
                // Prepare succeeded, try to commit
                match self.engine.commit(txn_id) {
                    Ok(()) => {
                        if let Some(coord_id) = coordinator_id {
                            self.send_prepared_response(coord_id, txn_id_str, request_id);
                        }
                        self.retry_deferred_operations(txn_id).await;
                        Ok(())
                    }
                    Err(e) => {
                        // Commit failed, abort
                        let _ = self.engine.abort(txn_id);
                        if let Some(coord_id) = coordinator_id {
                            self.send_error_response(coord_id, txn_id_str, e.clone(), request_id);
                        }
                        Err(ProcessorError::EngineError(e))
                    }
                }
            }
            Err(e) => {
                // Prepare failed, abort
                let _ = self.engine.abort(txn_id);
                if let Some(coord_id) = coordinator_id {
                    self.send_error_response(coord_id, txn_id_str, e.clone(), request_id);
                }
                Err(ProcessorError::EngineError(e))
            }
        }
    }

    /// Handle commit phase
    async fn handle_commit(
        &mut self,
        txn_id: HlcTimestamp,
        _txn_id_str: &str,
        _coordinator_id: Option<&str>,
        _request_id: Option<String>,
    ) -> Result<()> {
        self.engine
            .commit(txn_id)
            .map_err(ProcessorError::EngineError)?;

        // Clean up wounded tracking
        self.wounded_transactions.remove(&txn_id);

        self.retry_deferred_operations(txn_id).await;
        Ok(())
    }

    /// Handle abort phase
    async fn handle_abort(
        &mut self,
        txn_id: HlcTimestamp,
        _txn_id_str: &str,
        _coordinator_id: Option<&str>,
        _request_id: Option<String>,
    ) -> Result<()> {
        self.engine
            .abort(txn_id)
            .map_err(ProcessorError::EngineError)?;

        // Clean up wounded tracking
        self.wounded_transactions.remove(&txn_id);

        self.deferred_manager
            .remove_operations_for_transaction(&txn_id);
        self.retry_deferred_operations(txn_id).await;
        Ok(())
    }

    /// Run recovery check for transactions past their deadline
    async fn run_recovery_check(&mut self, current_time: HlcTimestamp) -> Result<()> {
        // Find all transactions past deadline that are in prepared state
        let mut transactions_to_recover = Vec::new();

        for (txn_id, deadline) in &self.transaction_deadlines {
            if current_time > *deadline {
                // Check if transaction is in prepared state
                // For now, we'll check if it has a coordinator (meaning it was active)
                // and is not wounded (wounded transactions abort themselves)
                if self.transaction_coordinators.contains_key(txn_id)
                    && !self.wounded_transactions.contains_key(txn_id)
                {
                    transactions_to_recover.push(*txn_id);
                }
            }
        }

        // Execute recovery for each transaction
        for txn_id in transactions_to_recover {
            let participants = self
                .transaction_participants
                .get(&txn_id)
                .cloned()
                .unwrap_or_default();

            // Use recovery manager to determine outcome
            let decision = self
                .recovery_manager
                .execute_recovery(txn_id, participants, current_time)
                .await;

            // Apply the decision locally
            match decision {
                TransactionDecision::Commit => {
                    // Apply commit locally
                    let _ = self.engine.commit(txn_id);
                    // Clean up state
                    self.transaction_coordinators.remove(&txn_id);
                    self.transaction_deadlines.remove(&txn_id);
                    self.transaction_participants.remove(&txn_id);
                }
                TransactionDecision::Abort => {
                    // Apply abort locally
                    let _ = self.engine.abort(txn_id);
                    // Clean up state
                    self.transaction_coordinators.remove(&txn_id);
                    self.transaction_deadlines.remove(&txn_id);
                    self.transaction_participants.remove(&txn_id);
                    self.wounded_transactions.remove(&txn_id);
                }
                TransactionDecision::Unknown => {
                    // No decision could be made, leave for future recovery
                }
            }
        }

        Ok(())
    }

    /// Wound a transaction (force it to abort due to deadlock prevention)
    async fn wound_transaction(&mut self, victim: HlcTimestamp, wounded_by: HlcTimestamp) {
        // Track that this transaction was wounded
        self.wounded_transactions.insert(victim, wounded_by);

        // Notify the victim's coordinator that it was wounded
        if let Some(victim_coord) = self.transaction_coordinators.get(&victim).cloned() {
            let victim_str = victim.to_string();
            self.send_wounded_response(&victim_coord, &victim_str, wounded_by, None);
        }

        // Abort the victim transaction
        let _ = self.engine.abort(victim);

        // Remove victim's deferred operations
        // Note: We already notified the main coordinator above
        self.deferred_manager
            .remove_operations_for_transaction(&victim);

        // Remove victim from coordinator tracking
        self.transaction_coordinators.remove(&victim);
    }

    /// Retry operations that were deferred waiting on this transaction
    async fn retry_deferred_operations(&mut self, completed_txn: HlcTimestamp) {
        let waiting_ops = self
            .deferred_manager
            .take_waiting_operations(&completed_txn);

        for deferred in waiting_ops {
            let _ = self
                .execute_operation(
                    deferred.operation,
                    deferred.txn_id,
                    &deferred.txn_id.to_string(),
                    &deferred.coordinator_id,
                    deferred.request_id,
                )
                .await;
        }
    }

    /// Send a response back to the coordinator
    fn send_response<R: Serialize>(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        response: R,
        request_id: Option<String>,
    ) {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("participant".to_string(), self.stream_name.clone());
        headers.insert("engine".to_string(), self.engine.engine_name().to_string());

        if let Some(req_id) = request_id {
            headers.insert("request_id".to_string(), req_id);
        }

        let body = serde_json::to_vec(&response).unwrap_or_else(|e| {
            tracing::error!("Failed to serialize response: {}", e);
            Vec::new()
        });

        let subject = format!("coordinator.{}.response", coordinator_id);
        let message = Message::new(body, headers);

        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish(&subject, vec![message]).await;
        });
    }

    /// Send a prepared response
    fn send_prepared_response(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        request_id: Option<String>,
    ) {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("participant".to_string(), self.stream_name.clone());
        headers.insert("engine".to_string(), self.engine.engine_name().to_string());
        headers.insert("status".to_string(), "prepared".to_string());

        if let Some(req_id) = request_id {
            headers.insert("request_id".to_string(), req_id);
        }

        let subject = format!("coordinator.{}.response", coordinator_id);
        let message = Message::new(Vec::new(), headers);

        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish(&subject, vec![message]).await;
        });
    }

    /// Send a wounded response
    fn send_wounded_response(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        wounded_by: HlcTimestamp,
        request_id: Option<String>,
    ) {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("participant".to_string(), self.stream_name.clone());
        headers.insert("engine".to_string(), self.engine.engine_name().to_string());
        headers.insert("status".to_string(), "wounded".to_string());
        headers.insert("wounded_by".to_string(), wounded_by.to_string());

        if let Some(req_id) = request_id {
            headers.insert("request_id".to_string(), req_id);
        }

        let subject = format!("coordinator.{}.response", coordinator_id);
        let message = Message::new(Vec::new(), headers);

        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish(&subject, vec![message]).await;
        });
    }

    /// Send a deferred response (operation blocked on lock)
    fn send_deferred_response(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        blocking_txn: HlcTimestamp,
        request_id: Option<String>,
    ) {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("participant".to_string(), self.stream_name.clone());
        headers.insert("engine".to_string(), self.engine.engine_name().to_string());
        headers.insert("status".to_string(), "deferred".to_string());
        headers.insert("blocking_txn".to_string(), blocking_txn.to_string());

        if let Some(req_id) = request_id {
            headers.insert("request_id".to_string(), req_id);
        }

        let subject = format!("coordinator.{}.response", coordinator_id);
        let message = Message::new(Vec::new(), headers);

        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish(&subject, vec![message]).await;
        });
    }

    /// Send an error response
    fn send_error_response(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        error: String,
        request_id: Option<String>,
    ) {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("participant".to_string(), self.stream_name.clone());
        headers.insert("engine".to_string(), self.engine.engine_name().to_string());
        headers.insert("status".to_string(), "error".to_string());
        headers.insert("error".to_string(), error);

        if let Some(req_id) = request_id {
            headers.insert("request_id".to_string(), req_id);
        }

        let subject = format!("coordinator.{}.response", coordinator_id);
        let message = Message::new(Vec::new(), headers);

        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish(&subject, vec![message]).await;
        });
    }

    /// Run the processor, consuming messages from its stream
    pub async fn run(&mut self) -> Result<()> {
        use proven_engine::DeadlineStreamItem;
        use proven_hlc::NodeId;
        use std::time::{SystemTime, UNIX_EPOCH};
        use tokio_stream::StreamExt;

        let mut last_offset = 0u64;

        // Phase 1: Replay historical messages up to current time
        if matches!(self.phase, ProcessorPhase::Replay) {
            // Get current time as HLC timestamp
            let physical = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;
            let current_time = HlcTimestamp::new(physical, 0, NodeId::new(1));

            // Clone what we need to avoid borrow issues
            let client = self.client.clone();
            let stream_name = self.stream_name.clone();

            // Stream messages until we reach current time
            let replay_stream = client
                .stream_messages_until_deadline(&stream_name, Some(0), current_time)
                .map_err(|e| ProcessorError::EngineError(e.to_string()))?;

            tokio::pin!(replay_stream);

            while let Some(item) = replay_stream.next().await {
                match item {
                    DeadlineStreamItem::Message(message, timestamp, offset) => {
                        last_offset = offset;
                        if let Err(e) = self.process_message(message, timestamp, offset).await {
                            tracing::error!("Error during replay at offset {}: {:?}", offset, e);
                        }
                    }
                    DeadlineStreamItem::DeadlineReached => {
                        // Replay complete, transition to recovery then live
                        self.phase = ProcessorPhase::Recovery;
                        self.run_recovery_check(current_time).await?;
                        self.phase = ProcessorPhase::Live;
                        break;
                    }
                }
            }
        }

        // Phase 2: Continue with live processing
        let start_offset = if last_offset > 0 {
            Some(last_offset + 1)
        } else {
            Some(self.current_offset)
        };

        let mut live_stream = self
            .client
            .stream_messages(self.stream_name.clone(), start_offset)
            .await
            .map_err(|e| ProcessorError::EngineError(e.to_string()))?;

        // Process live messages continuously
        while let Some((message, timestamp, offset)) = live_stream.recv().await {
            if let Err(e) = self.process_message(message, timestamp, offset).await {
                // Log error but continue processing
                tracing::error!("Error processing message at offset {}: {:?}", offset, e);
            }
        }

        Ok(())
    }
}
