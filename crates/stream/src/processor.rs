//! Generic stream processor that handles distributed transaction coordination
//!
//! This processor consumes messages from a stream and delegates storage
//! operations to a TransactionEngine implementation. It handles all the
//! distributed systems concerns like retries, deferrals, and coordinator
//! communication.

use crate::deferred::DeferredOperationsManager;
use crate::engine::{OperationResult, TransactionEngine};
use proven_engine::{Message, MockClient};
use proven_hlc::HlcTimestamp;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProcessorError {
    #[error("Missing required header: {0}")]
    MissingHeader(&'static str),

    #[error("Invalid transaction ID: {0}")]
    InvalidTransactionId(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Unknown transaction phase: {0}")]
    UnknownPhase(String),

    #[error("Engine error: {0}")]
    EngineError(String),
}

type Result<T> = std::result::Result<T, ProcessorError>;

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

    /// Engine client for sending responses
    client: Arc<MockClient>,

    /// Name of this stream for identification
    stream_name: String,
}

impl<E: TransactionEngine> StreamProcessor<E> {
    /// Create a new stream processor
    pub fn new(engine: E, client: Arc<MockClient>, stream_name: String) -> Self {
        Self {
            engine,
            deferred_manager: DeferredOperationsManager::new(),
            transaction_coordinators: HashMap::new(),
            wounded_transactions: HashMap::new(),
            client,
            stream_name,
        }
    }

    /// Process a message from the stream
    pub async fn process_message(&mut self, message: Message) -> Result<()> {
        // Extract transaction ID
        let txn_id_str = message
            .txn_id()
            .ok_or(ProcessorError::MissingHeader("txn_id"))?
            .to_string();

        let txn_id = HlcTimestamp::parse(&txn_id_str)
            .map_err(|e| ProcessorError::InvalidTransactionId(e))?;

        // Handle transaction control messages (empty body)
        if message.is_transaction_control() {
            return self
                .handle_control_message(message, txn_id, &txn_id_str)
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
    ) -> Result<()> {
        let phase = message
            .txn_phase()
            .ok_or(ProcessorError::MissingHeader("txn_phase"))?;

        let coordinator_id = message.coordinator_id();
        let request_id = message.request_id().map(|s| s.to_string());

        match phase {
            "prepare" => {
                self.handle_prepare(txn_id, txn_id_str, coordinator_id, request_id)
                    .await
            }
            "prepare_and_commit" => {
                self.handle_prepare_and_commit(txn_id, txn_id_str, coordinator_id, request_id)
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

        // Ensure transaction exists
        if !self.engine.is_transaction_active(&txn_id) {
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
                        OperationResult::WouldBlock { blocking_txn: new_blocker } => {
                            // Still blocked (shouldn't happen after wounding, but handle it)
                            self.send_deferred_response(coordinator_id, txn_id_str, new_blocker, request_id.clone());
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
                            self.send_error_response(coordinator_id, txn_id_str, msg.clone(), request_id);
                            Err(ProcessorError::EngineError(msg))
                        }
                    }
                } else {
                    // We're younger - must wait
                    self.send_deferred_response(coordinator_id, txn_id_str, blocking_txn, request_id.clone());
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
    ) -> Result<()> {
        // Check if transaction was wounded
        if let Some(wounded_by) = self.wounded_transactions.get(&txn_id) {
            if let Some(coord_id) = coordinator_id {
                self.send_wounded_response(coord_id, txn_id_str, *wounded_by, request_id);
            }
            return Ok(());
        }
        
        match self.engine.prepare(txn_id) {
            Ok(()) => {
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
    ) -> Result<()> {
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

    /// Wound a transaction (force it to abort due to deadlock prevention)
    async fn wound_transaction(&mut self, victim: HlcTimestamp, wounded_by: HlcTimestamp) {
        // Track that this transaction was wounded
        self.wounded_transactions.insert(victim, wounded_by);
        
        // Notify the victim's coordinator that it was wounded
        if let Some(victim_coord) = self.transaction_coordinators.get(&victim).cloned() {
            let victim_str = format!(
                "txn_runtime1_{:010}",
                victim.physical * 1_000_000_000 + victim.logical as u64
            );
            self.send_wounded_response(&victim_coord, &victim_str, wounded_by, None);
        }
        
        // Abort the victim transaction
        let _ = self.engine.abort(victim);
        
        // Remove victim's deferred operations
        // Note: We already notified the main coordinator above
        self.deferred_manager.remove_operations_for_transaction(&victim);
        
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
}
