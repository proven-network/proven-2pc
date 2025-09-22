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
use proven_engine::client::MessageStream;
use proven_engine::{Message, MockClient};
use proven_hlc::HlcTimestamp;
use proven_snapshot::SnapshotStore;
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

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

/// Configuration for snapshot behavior
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// No messages for this duration triggers snapshot consideration
    pub idle_duration: Duration,
    /// Minimum commits before allowing snapshot
    pub min_commits_between_snapshots: u64,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            idle_duration: Duration::from_secs(10),
            min_commits_between_snapshots: 100,
        }
    }
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

    /// Cached engine name to avoid repeated string allocation
    engine_name: String,

    /// Current processing phase
    phase: ProcessorPhase,

    /// Current stream offset (for phase transitions)
    pub current_offset: u64,

    /// Snapshot store (if configured)
    snapshot_store: Option<Arc<dyn SnapshotStore>>,

    /// Snapshot tracking
    last_snapshot_offset: u64,
    last_message_time: Option<Instant>,
    last_log_timestamp: HlcTimestamp,
    commits_since_snapshot: u64,

    /// Snapshot configuration
    pub snapshot_config: SnapshotConfig,

    /// Flag indicating if replay is complete and processor is ready
    is_ready: Arc<AtomicBool>,
}

impl<E: TransactionEngine + Send> StreamProcessor<E> {
    /// Create a new stream processor with snapshot store (starts in replay mode)
    pub fn new(
        mut engine: E,
        client: Arc<MockClient>,
        stream_name: String,
        snapshot_store: Arc<dyn SnapshotStore>,
    ) -> Self {
        use proven_hlc::NodeId;

        // Try to restore from latest snapshot
        let mut start_offset = 0u64;
        let mut last_snapshot_offset = 0u64;
        let mut last_timestamp = HlcTimestamp::new(0, 0, NodeId::new(0));

        if let Some((metadata, data)) = snapshot_store.get_latest_snapshot(&stream_name)
            && engine.restore_from_snapshot(&data).is_ok()
        {
            // Remember where we snapshotted
            last_snapshot_offset = metadata.log_offset;
            // Start replay from the next message after the snapshot
            start_offset = metadata.log_offset + 1;
            last_timestamp = metadata.log_timestamp;
            tracing::info!(
                "Restored from snapshot at offset {}, will replay from {} for stream {}",
                metadata.log_offset,
                start_offset,
                stream_name
            );
        }

        let engine_name = engine.engine_name().to_string();

        Self {
            engine,
            deferred_manager: DeferredOperationsManager::new(),
            transaction_coordinators: HashMap::new(),
            wounded_transactions: HashMap::new(),
            transaction_deadlines: HashMap::new(),
            transaction_participants: HashMap::new(),
            recovery_manager: RecoveryManager::new(client.clone(), stream_name.clone()),
            client,
            stream_name: stream_name.clone(),
            engine_name,
            phase: ProcessorPhase::Replay,
            current_offset: start_offset,
            snapshot_store: Some(snapshot_store),
            last_snapshot_offset,
            last_message_time: None,
            last_log_timestamp: last_timestamp,
            commits_since_snapshot: 0,
            snapshot_config: SnapshotConfig::default(),
            is_ready: Arc::new(AtomicBool::new(false)),
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
        self.last_log_timestamp = msg_timestamp;
        self.last_message_time = Some(Instant::now());

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
        let txn_id =
            HlcTimestamp::parse(&txn_id_str).map_err(ProcessorError::InvalidTransactionId)?;

        // Track deadline
        if let Some(deadline_str) = message.txn_deadline()
            && !self.transaction_deadlines.contains_key(&txn_id)
            && let Ok(deadline) = HlcTimestamp::parse(deadline_str)
        {
            self.transaction_deadlines.insert(txn_id, deadline);
        }

        // Track coordinator
        if let Some(coord_id) = message.coordinator_id() {
            self.transaction_coordinators
                .insert(txn_id, coord_id.to_string());
        }

        // Track participants
        if let Some(participants_str) = message.get_header("new_participants")
            && let Ok(participants) = serde_json::from_str::<HashMap<String, u64>>(participants_str)
        {
            self.transaction_participants
                .entry(txn_id)
                .or_default()
                .extend(participants);
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
                    self.commits_since_snapshot += 1;
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

        let txn_id =
            HlcTimestamp::parse(&txn_id_str).map_err(ProcessorError::InvalidTransactionId)?;

        // State tracking is already done in track_transaction_state()

        // Check if we're past the deadline
        if let Some(&deadline) = self.transaction_deadlines.get(&txn_id)
            && msg_timestamp > deadline
        {
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
            if let std::collections::hash_map::Entry::Vacant(e) =
                self.transaction_deadlines.entry(txn_id)
            {
                // Try to extract deadline from message
                if let Some(deadline_str) = message.txn_deadline() {
                    if let Ok(deadline) = HlcTimestamp::parse(deadline_str) {
                        e.insert(deadline);
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
            match self.engine.commit(txn_id) {
                Ok(()) => {
                    self.retry_deferred_operations(txn_id).await;
                }
                Err(e) => {
                    println!(
                        "[{}] ERROR: Auto-commit failed for txn {}: {}",
                        self.stream_name, txn_id, e
                    );
                    return Err(ProcessorError::EngineError(e));
                }
            }
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
            OperationResult::Complete(response) => {
                self.send_response(coordinator_id, txn_id_str, response, request_id);
                Ok(())
            }

            OperationResult::WouldBlock { blockers } => {
                // Log all blockers
                let blocker_list: Vec<String> =
                    blockers.iter().map(|b| b.txn.to_string()).collect();
                println!(
                    "[{}] DEFERRED: WouldBlock for txn {}: {:?}",
                    self.stream_name, txn_id, blocker_list
                );
                println!("Operation: {:?}", operation);

                // Find younger blockers that we should wound
                let younger_blockers: Vec<_> = blockers
                    .iter()
                    .filter(|b| b.txn > txn_id)
                    .map(|b| b.txn)
                    .collect();

                if !younger_blockers.is_empty() {
                    // We're older than some blockers - wound them all
                    for victim in younger_blockers {
                        self.wound_transaction(victim, txn_id).await;
                    }

                    // Retry the operation after wounding
                    match self.engine.apply_operation(operation.clone(), txn_id) {
                        OperationResult::Complete(response) => {
                            self.send_response(coordinator_id, txn_id_str, response, request_id);
                            Ok(())
                        }
                        OperationResult::WouldBlock {
                            blockers: new_blockers,
                        } => {
                            // Still blocked after wounding - should only be older transactions now
                            // For now, use the first blocker for backward compatibility
                            // TODO: Update deferred manager to track all blockers
                            if let Some(first_blocker) = new_blockers.first() {
                                self.deferred_manager.defer_operation(
                                    operation,
                                    txn_id,
                                    first_blocker.txn,
                                    first_blocker.retry_on,
                                    coordinator_id.to_string(),
                                    request_id,
                                );
                            }
                            Ok(())
                        }
                    }
                } else {
                    // All blockers are older - we must wait
                    // For now, use the first blocker for backward compatibility
                    // TODO: Update deferred manager to track all blockers
                    if let Some(first_blocker) = blockers.first() {
                        self.deferred_manager.defer_operation(
                            operation,
                            txn_id,
                            first_blocker.txn,
                            first_blocker.retry_on,
                            coordinator_id.to_string(),
                            request_id,
                        );
                    }
                    Ok(())
                }
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
        if let Some(&deadline) = self.transaction_deadlines.get(&txn_id)
            && msg_timestamp > deadline
        {
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

                // Send prepare vote response BEFORE retrying operations
                if let Some(coord_id) = coordinator_id {
                    self.send_prepared_response(coord_id, txn_id_str, request_id);
                }

                // Now retry operations that were waiting on prepare
                self.retry_prepare_waiting_operations(txn_id).await;

                Ok(())
            }
            Err(e) => {
                println!(
                    "[{}] ERROR: Prepare failed in handle_prepare for txn {}: {}",
                    self.stream_name, txn_id, e
                );
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
        if let Some(&deadline) = self.transaction_deadlines.get(&txn_id)
            && msg_timestamp > deadline
        {
            // Past deadline - don't vote prepared
            if let Some(coord_id) = coordinator_id {
                println!(
                    "[{}] ERROR: Prepare received after deadline for txn {}: {}",
                    self.stream_name, txn_id, deadline
                );
                self.send_error_response(
                    coord_id,
                    txn_id_str,
                    "Prepare received after deadline".to_string(),
                    request_id,
                );
            }
            return Ok(());
        }
        // Check if transaction was wounded
        if let Some(wounded_by) = self.wounded_transactions.get(&txn_id) {
            if let Some(coord_id) = coordinator_id {
                println!(
                    "[{}] ERROR: Cannot prepare wounded txn {}: {}",
                    self.stream_name, txn_id, *wounded_by
                );
                self.send_wounded_response(coord_id, txn_id_str, *wounded_by, request_id);
            }
            return Ok(());
        }

        // Try to prepare
        match self.engine.prepare(txn_id) {
            Ok(()) => {
                // Prepare succeeded, now try to commit
                match self.engine.commit(txn_id) {
                    Ok(()) => {
                        // Send response BEFORE retrying operations
                        if let Some(coord_id) = coordinator_id {
                            self.send_prepared_response(coord_id, txn_id_str, request_id);
                        }

                        // Now retry operations in order: prepare waiters first, then commit waiters
                        self.retry_prepare_waiting_operations(txn_id).await;
                        self.retry_deferred_operations(txn_id).await;
                        Ok(())
                    }
                    Err(e) => {
                        // Commit failed, abort
                        println!(
                            "[{}] ERROR: Commit failed for txn {}: {}",
                            self.stream_name, txn_id, e
                        );
                        let abort_result = self.engine.abort(txn_id);
                        if let Err(abort_err) = abort_result {
                            println!(
                                "[{}] ERROR: Abort failed after commit failure for txn {}: {}",
                                self.stream_name, txn_id, abort_err
                            );
                        }
                        if let Some(coord_id) = coordinator_id {
                            self.send_error_response(coord_id, txn_id_str, e.clone(), request_id);
                        }
                        Err(ProcessorError::EngineError(e))
                    }
                }
            }
            Err(e) => {
                // Prepare failed, abort
                println!(
                    "[{}] ERROR: Prepare failed for txn {}: {}",
                    self.stream_name, txn_id, e
                );
                let abort_result = self.engine.abort(txn_id);
                if let Err(abort_err) = abort_result {
                    println!(
                        "[{}] ERROR: Abort failed after prepare failure for txn {}: {}",
                        self.stream_name, txn_id, abort_err
                    );
                }
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
        match self.engine.commit(txn_id) {
            Ok(()) => {
                // Clean up wounded tracking
                self.wounded_transactions.remove(&txn_id);
                self.retry_deferred_operations(txn_id).await;
                Ok(())
            }
            Err(e) => {
                println!(
                    "[{}] ERROR: Commit failed in handle_commit for txn {}: {}",
                    self.stream_name, txn_id, e
                );
                Err(ProcessorError::EngineError(e))
            }
        }
    }

    /// Handle abort phase
    async fn handle_abort(
        &mut self,
        txn_id: HlcTimestamp,
        _txn_id_str: &str,
        _coordinator_id: Option<&str>,
        _request_id: Option<String>,
    ) -> Result<()> {
        println!(
            "[{}] ABORT: Received abort for txn {}",
            self.stream_name, txn_id
        );

        match self.engine.abort(txn_id) {
            Ok(()) => {
                println!(
                    "[{}] ABORT: Successfully aborted txn {}",
                    self.stream_name, txn_id
                );

                // Clean up wounded tracking
                self.wounded_transactions.remove(&txn_id);
                self.deferred_manager
                    .remove_operations_for_transaction(&txn_id);
                self.retry_deferred_operations(txn_id).await;
                Ok(())
            }
            Err(e) => {
                println!(
                    "[{}] ERROR: Abort failed in handle_abort for txn {}: {}",
                    self.stream_name, txn_id, e
                );
                Err(ProcessorError::EngineError(e))
            }
        }
    }

    /// Cleanup expired transactions that haven't been properly terminated
    async fn cleanup_expired_transactions(&mut self, current_time: HlcTimestamp) {
        // Find all transactions past deadline
        let mut expired_transactions = Vec::new();

        for (txn_id, deadline) in &self.transaction_deadlines {
            if current_time > *deadline {
                expired_transactions.push(*txn_id);
            }
        }

        // Abort expired transactions
        for txn_id in expired_transactions {
            println!(
                "[{}] CLEANUP: Aborting expired transaction {}",
                self.stream_name, txn_id
            );

            // Abort in the engine to release any resources/locks
            if let Err(e) = self.engine.abort(txn_id) {
                println!(
                    "[{}] ERROR: Failed to abort expired transaction {}: {}",
                    self.stream_name, txn_id, e
                );
            }

            // Clean up our tracking
            self.transaction_deadlines.remove(&txn_id);
            self.transaction_coordinators.remove(&txn_id);
            self.transaction_participants.remove(&txn_id);
            self.wounded_transactions.remove(&txn_id);
            self.deferred_manager
                .remove_operations_for_transaction(&txn_id);

            // Retry deferred operations
            self.retry_deferred_operations(txn_id).await;
        }
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

    /// Retry operations that were deferred waiting on this transaction to commit/abort
    async fn retry_deferred_operations(&mut self, completed_txn: HlcTimestamp) {
        let waiting_ops = self
            .deferred_manager
            .take_commit_waiting_operations(&completed_txn);

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

    /// Retry operations that were waiting on this transaction to prepare
    async fn retry_prepare_waiting_operations(&mut self, prepared_txn: HlcTimestamp) {
        let waiting_ops = self
            .deferred_manager
            .take_prepare_waiting_operations(&prepared_txn);

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
    fn send_response(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        response: E::Response,
        request_id: Option<String>,
    ) {
        // Pre-size HashMap to avoid reallocation (request_id is almost always present)
        let mut headers = HashMap::with_capacity(4);
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("participant".to_string(), self.stream_name.clone());
        headers.insert("engine".to_string(), self.engine_name.clone());

        if let Some(req_id) = &request_id {
            headers.insert("request_id".to_string(), req_id.clone());
        }

        let body = match serde_json::to_vec(&response) {
            Ok(bytes) => bytes,
            Err(e) => {
                // If we can't serialize the response, send an error instead
                tracing::error!("Failed to serialize response: {}", e);
                self.send_error_response(
                    coordinator_id,
                    txn_id,
                    format!("Failed to serialize response: {}", e),
                    request_id,
                );
                return;
            }
        };

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
        // Pre-size HashMap to avoid reallocation
        let mut headers = HashMap::with_capacity(5);
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("participant".to_string(), self.stream_name.clone());
        headers.insert("engine".to_string(), self.engine_name.clone());
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
        // Pre-size HashMap to avoid reallocation
        let mut headers = HashMap::with_capacity(6);
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("participant".to_string(), self.stream_name.clone());
        headers.insert("engine".to_string(), self.engine_name.clone());
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

    /// Send an infrastructure error response
    /// These are errors that occur outside of normal operation processing
    /// (e.g., deadline exceeded, prepare failures, etc.)
    fn send_error_response(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        error: String,
        request_id: Option<String>,
    ) {
        // Pre-size HashMap to avoid reallocation
        let mut headers = HashMap::with_capacity(6);
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("participant".to_string(), self.stream_name.clone());
        headers.insert("engine".to_string(), self.engine_name.clone());
        headers.insert("status".to_string(), "error".to_string());
        headers.insert("error".to_string(), error.clone());

        if let Some(req_id) = request_id {
            headers.insert("request_id".to_string(), req_id);
        }

        // Use a minimal body - coordinator uses headers for error detection
        // Only create JSON if absolutely necessary
        let body = Vec::new();

        let subject = format!("coordinator.{}.response", coordinator_id);
        let message = Message::new(body, headers);

        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish(&subject, vec![message]).await;
        });
    }

    /// Check if we should take a snapshot
    fn should_snapshot(&self) -> bool {
        // No active transactions
        if !self.transaction_coordinators.is_empty() {
            return false;
        }

        // Check idle time - must be idle
        if let Some(last_msg_time) = self.last_message_time
            && last_msg_time.elapsed() < self.snapshot_config.idle_duration
        {
            return false;
        }

        // Check minimum commit count
        if self.commits_since_snapshot < self.snapshot_config.min_commits_between_snapshots {
            return false;
        }

        // We're idle and have enough commits
        true
    }

    /// Try to take a snapshot if conditions are met
    async fn try_snapshot(&mut self) -> Result<()> {
        if !self.should_snapshot() {
            return Ok(());
        }

        if let Some(store) = &self.snapshot_store {
            match self.engine.snapshot() {
                Ok(data) => {
                    store
                        .save_snapshot(
                            &self.stream_name,
                            self.current_offset,
                            self.last_log_timestamp,
                            data,
                        )
                        .map_err(|e| {
                            ProcessorError::EngineError(format!("Snapshot save failed: {}", e))
                        })?;

                    self.last_snapshot_offset = self.current_offset;
                    self.commits_since_snapshot = 0;

                    tracing::info!(
                        "Created snapshot at offset {} for stream {}",
                        self.current_offset,
                        self.stream_name
                    );
                }
                Err(e) => {
                    tracing::debug!("Snapshot not taken: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Check if the processor has completed replay and is ready for operations
    pub fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::Relaxed)
    }

    /// Start the processor - performs replay then spawns live processing task
    /// Returns after replay is complete and live stream is subscribed
    pub async fn start_with_replay(mut self, shutdown_rx: oneshot::Receiver<()>) -> Result<()>
    where
        E: Send + 'static,
    {
        let stream_name = self.stream_name.clone();

        // Perform replay phase first (blocking)
        self.perform_replay().await?;

        // Mark as ready
        self.is_ready.store(true, Ordering::Relaxed);

        // Create the live stream subscription BEFORE spawning the task
        // This ensures we're subscribed before returning
        let start_offset = if self.current_offset > 0 {
            Some(self.current_offset + 1)
        } else {
            Some(0)
        };

        let live_stream = self
            .client
            .stream_messages(stream_name.clone(), start_offset)
            .await
            .map_err(|e| ProcessorError::EngineError(e.to_string()))?;

        tracing::info!(
            "Stream processor for {} is ready, starting live processing",
            stream_name
        );

        // Spawn live processing in background task (processor takes ownership)
        let stream_name_clone = stream_name.clone();
        let stream_name_task = stream_name.clone();
        let handle = tokio::spawn(async move {
            tracing::debug!("Live processing started for stream: {}", stream_name_task);
            match self
                .run_live_processing_with_stream(shutdown_rx, live_stream)
                .await
            {
                Ok(()) => {
                    tracing::debug!("Live processing for {} ended normally", stream_name_task);
                }
                Err(e) => {
                    tracing::error!("Live processing for {} failed: {:?}", stream_name_task, e);
                }
            }
            tracing::debug!(
                "Live processing task exited for stream: {}",
                stream_name_clone
            );
        });

        // Spawn a monitor task to detect if the processor task panics
        let stream_name_monitor = stream_name.clone();
        tokio::spawn(async move {
            match handle.await {
                Ok(()) => {
                    // Task completed normally
                }
                Err(e) if e.is_panic() => {
                    tracing::error!(
                        "Panic detected in processor for {}: {:?}",
                        stream_name_monitor,
                        e
                    );
                }
                Err(e) => {
                    tracing::warn!("Task join error for {}: {:?}", stream_name_monitor, e);
                }
            }
        });

        Ok(())
    }

    /// Perform the replay phase (blocking until complete)
    async fn perform_replay(&mut self) -> Result<()> {
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

        self.current_offset = last_offset;
        Ok(())
    }

    /// Run live processing with provided stream
    async fn run_live_processing_with_stream(
        mut self,
        mut shutdown_rx: oneshot::Receiver<()>,
        mut live_stream: MessageStream,
    ) -> Result<()> {
        // Track when we last ran cleanup
        let mut last_cleanup = std::time::Instant::now();

        // Process live messages continuously
        loop {
            // Check for shutdown signal
            if shutdown_rx.try_recv().is_ok() {
                tracing::info!("Stream processor for {} shutting down", self.stream_name);
                break;
            }

            // Periodically check for expired transactions (every 1 second)
            if last_cleanup.elapsed() > Duration::from_secs(1) {
                let current_time = proven_hlc::HlcTimestamp::new(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    0,
                    proven_hlc::NodeId::new(0),
                );
                self.cleanup_expired_transactions(current_time).await;
                last_cleanup = std::time::Instant::now();
            }

            // Check for messages with a timeout to allow periodic snapshot checks and shutdown checks
            let timeout =
                tokio::time::timeout(Duration::from_millis(100), live_stream.recv()).await;

            match timeout {
                Ok(Some((message, timestamp, offset))) => {
                    if let Err(e) = self.process_message(message, timestamp, offset).await {
                        // Log error but continue processing
                        tracing::error!("Error processing message at offset {}: {:?}", offset, e);
                    }
                }
                Ok(None) => {
                    // Stream ended
                    break;
                }
                Err(_) => {
                    // Timeout - check if we should snapshot
                    if let Err(e) = self.try_snapshot().await {
                        tracing::warn!("Failed to create snapshot: {:?}", e);
                    }
                }
            }
        }

        Ok(())
    }
}
