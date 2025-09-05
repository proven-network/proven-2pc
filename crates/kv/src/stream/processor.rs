//! Core KV stream processor implementation
//!
//! Consumes ordered messages from a Raft-ordered stream and processes KV operations
//! with proper transaction isolation using MVCC for rollback support and PCC for
//! conflict prevention.

use super::deferred::{DeferredOperation, DeferredOperationsManager};
use super::message::{KvOperation, StreamMessage};
use super::response::KvResponse;
use super::transaction::TransactionContext;
use crate::storage::lock::{LockAttemptResult, LockManager, LockMode};
use crate::storage::mvcc::MvccStorage;
use crate::types::Value;
use proven_engine::MockClient;
use proven_hlc::HlcTimestamp;
use std::collections::HashMap;
use std::sync::Arc;

/// Error type for KV operations
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Transaction {0} not active")]
    TransactionNotActive(HlcTimestamp),

    #[error("Lock conflict with transaction {holder:?} holding {mode:?}")]
    LockConflict {
        holder: HlcTimestamp,
        mode: LockMode,
    },

    #[error("Transaction wounded by {wounded_by:?}")]
    TransactionWounded { wounded_by: HlcTimestamp },

    #[error("Invalid value: {0}")]
    InvalidValue(String),
}

type Result<T> = std::result::Result<T, Error>;

/// KV stream processor with transaction isolation
pub struct KvStreamProcessor {
    /// MVCC storage
    pub storage: MvccStorage,

    /// Lock manager for PCC
    pub lock_manager: LockManager,

    /// Active transaction contexts
    active_transactions: HashMap<HlcTimestamp, TransactionContext>,

    /// Coordinator IDs for active transactions (for wounded notifications)
    transaction_coordinators: HashMap<HlcTimestamp, String>,

    /// Deferred operations manager
    deferred_manager: DeferredOperationsManager,

    /// Engine client for sending responses
    client: Arc<MockClient>,

    /// Stream name for identification in responses
    stream_name: String,
}

impl KvStreamProcessor {
    /// Create a processor for testing (creates a mock engine internally)
    #[cfg(test)]
    pub fn new_for_testing() -> (Self, Arc<proven_engine::MockEngine>) {
        use proven_engine::MockEngine;
        let engine = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-kv-processor".to_string(),
            engine.clone(),
        ));
        (Self::new(client, "kv-stream".to_string()), engine)
    }

    /// Create a new processor with empty storage
    pub fn new(client: Arc<MockClient>, stream_name: String) -> Self {
        Self {
            storage: MvccStorage::new(),
            lock_manager: LockManager::new(),
            active_transactions: HashMap::new(),
            transaction_coordinators: HashMap::new(),
            deferred_manager: DeferredOperationsManager::new(),
            client,
            stream_name,
        }
    }

    /// Send a response back to the coordinator via engine pub/sub with optional request_id
    fn send_response_with_request(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        response: KvResponse,
        request_id: Option<String>,
    ) {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("participant".to_string(), self.stream_name.clone());
        if let Some(req_id) = request_id {
            headers.insert("request_id".to_string(), req_id);
        }

        // Serialize the response to the body using serde_json
        let body = serde_json::to_vec(&response).unwrap_or_else(|e| {
            eprintln!("Failed to serialize KV response: {}", e);
            Vec::new()
        });

        let subject = format!("coordinator.{}.response", coordinator_id);
        let message = proven_engine::Message::new(body, headers);

        // Use tokio::spawn to avoid blocking
        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish(&subject, vec![message]).await;
        });
    }

    /// Send a response back to the coordinator via engine pub/sub
    fn send_response(&self, coordinator_id: &str, txn_id: &str, response: KvResponse) {
        self.send_response_with_request(coordinator_id, txn_id, response, None)
    }

    /// Process a message from the stream (matches SQL pattern)
    pub async fn process_message(&mut self, message: StreamMessage) -> Result<()> {
        let txn_id_str = message
            .txn_id()
            .ok_or_else(|| Error::InvalidValue("Missing txn_id header".into()))?;

        // Parse transaction ID to HlcTimestamp at the boundary
        let txn_id = HlcTimestamp::parse(txn_id_str).map_err(|e| Error::InvalidValue(e))?;

        // Check if this is a commit/abort/prepare message (empty body)
        if message.is_transaction_control() {
            match message.txn_phase() {
                Some("prepare") => {
                    return self
                        .prepare_transaction(
                            txn_id,
                            txn_id_str,
                            message.headers.get("request_id").cloned(),
                        )
                        .await;
                }
                Some("commit") => return self.commit_transaction(txn_id).await,
                Some("abort") => return self.abort_transaction(txn_id).await,
                Some(phase) => {
                    return Err(Error::InvalidValue(format!("Unknown txn_phase: {}", phase)));
                }
                None => {
                    return Err(Error::InvalidValue(
                        "Empty message without txn_phase".into(),
                    ));
                }
            }
        }

        // Deserialize the operation
        let operation: KvOperation = serde_json::from_slice(&message.body)
            .map_err(|e| Error::InvalidValue(format!("Failed to deserialize operation: {}", e)))?;

        // Ensure transaction exists (automatically create if needed)
        if !self.active_transactions.contains_key(&txn_id) {
            self.create_transaction(txn_id)?;
        }

        // Get coordinator ID - required for all operations
        let coordinator_id = message.coordinator_id().ok_or_else(|| {
            Error::InvalidValue("coordinator_id is required for all operations".to_string())
        })?;

        // Get request ID if present
        let request_id = message.headers.get("request_id").cloned();

        // Store coordinator ID for this transaction (for wounded notifications)
        self.transaction_coordinators
            .insert(txn_id, coordinator_id.to_string());

        // Execute the operation
        let result = self
            .execute_operation(operation, txn_id, coordinator_id)
            .await;

        // Send response to coordinator
        let response = match result {
            Ok(Some(res)) => res,
            Ok(None) => {
                // Operation was deferred, response already sent
                return Ok(());
            }
            Err(e) => KvResponse::Error(format!("{:?}", e)),
        };
        self.send_response_with_request(coordinator_id, txn_id_str, response, request_id);

        // Auto-commit if specified
        if message.is_auto_commit() {
            self.commit_transaction(txn_id).await?;
        }

        Ok(())
    }

    /// Create new transaction context
    fn create_transaction(&mut self, txn_id: HlcTimestamp) -> Result<()> {
        let tx_ctx = TransactionContext::new(txn_id);
        self.storage.register_transaction(txn_id, txn_id);
        self.active_transactions.insert(txn_id, tx_ctx);
        Ok(())
    }

    /// Execute a KV operation with lock conflict handling
    async fn execute_operation(
        &mut self,
        operation: KvOperation,
        txn_id: HlcTimestamp,
        coordinator_id: &str,
    ) -> Result<Option<KvResponse>> {
        match operation {
            KvOperation::Get { ref key } => {
                self.execute_get(key, txn_id, &operation, coordinator_id)
                    .await
            }
            KvOperation::Put { ref key, ref value } => {
                self.execute_put(key, value.clone(), txn_id, &operation, coordinator_id)
                    .await
            }
            KvOperation::Delete { ref key } => {
                self.execute_delete(key, txn_id, &operation, coordinator_id)
                    .await
            }
        }
    }

    /// Execute a Get operation
    async fn execute_get(
        &mut self,
        key: &str,
        txn_id: HlcTimestamp,
        operation: &KvOperation,
        coordinator_id: &str,
    ) -> Result<Option<KvResponse>> {
        // Try to acquire shared lock
        match self
            .try_lock_with_conflict_handling(
                key,
                LockMode::Shared,
                txn_id,
                operation.clone(),
                coordinator_id,
            )
            .await?
        {
            Some(_) => {
                // Lock acquired, perform the get
                let value = self.storage.get(key, txn_id).cloned();
                Ok(Some(KvResponse::GetResult {
                    key: key.to_string(),
                    value,
                }))
            }
            None => Ok(None), // Operation deferred
        }
    }

    /// Execute a Put operation
    async fn execute_put(
        &mut self,
        key: &str,
        value: Value,
        txn_id: HlcTimestamp,
        operation: &KvOperation,
        coordinator_id: &str,
    ) -> Result<Option<KvResponse>> {
        // Try to acquire exclusive lock
        match self
            .try_lock_with_conflict_handling(
                key,
                LockMode::Exclusive,
                txn_id,
                operation.clone(),
                coordinator_id,
            )
            .await?
        {
            Some(_) => {
                // Lock acquired, perform the put
                let previous = self.storage.get(key, txn_id).cloned();
                self.storage.put(key.to_string(), value, txn_id, txn_id);
                Ok(Some(KvResponse::PutResult {
                    key: key.to_string(),
                    previous,
                }))
            }
            None => Ok(None), // Operation deferred
        }
    }

    /// Execute a Delete operation
    async fn execute_delete(
        &mut self,
        key: &str,
        txn_id: HlcTimestamp,
        operation: &KvOperation,
        coordinator_id: &str,
    ) -> Result<Option<KvResponse>> {
        // Try to acquire exclusive lock
        match self
            .try_lock_with_conflict_handling(
                key,
                LockMode::Exclusive,
                txn_id,
                operation.clone(),
                coordinator_id,
            )
            .await?
        {
            Some(_) => {
                // Lock acquired, perform the delete
                let existed = self.storage.exists(key, txn_id);
                if existed {
                    self.storage.delete(key, txn_id);
                }
                Ok(Some(KvResponse::DeleteResult {
                    key: key.to_string(),
                    deleted: existed,
                }))
            }
            None => Ok(None), // Operation deferred
        }
    }

    /// Try to acquire a lock with wound-wait conflict handling
    async fn try_lock_with_conflict_handling(
        &mut self,
        key: &str,
        mode: LockMode,
        txn_id: HlcTimestamp,
        operation: KvOperation,
        coordinator_id: &str,
    ) -> Result<Option<()>> {
        // Check if lock can be acquired
        match self.lock_manager.check(txn_id, key, mode) {
            LockAttemptResult::WouldGrant => {
                // Grant the lock
                self.lock_manager.grant(txn_id, key.to_string(), mode);

                // Track in transaction context
                if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
                    tx_ctx.add_lock(key.to_string(), mode);
                }

                Ok(Some(()))
            }
            LockAttemptResult::Conflict {
                holder,
                mode: _held_mode,
            } => {
                // Determine wound-wait: older transaction wounds younger
                let should_wound = txn_id < holder;

                if should_wound {
                    // Check if holder is prepared first
                    let holder_prepared = self
                        .active_transactions
                        .get(&holder)
                        .map(|ctx| ctx.is_prepared())
                        .unwrap_or(false);

                    if holder_prepared {
                        // Cannot wound prepared transaction, must wait
                        let deferred_op = DeferredOperation {
                            operation,
                            coordinator_id: coordinator_id.to_string(),
                            lock_requested: (key.to_string(), mode),
                            waiting_for: holder,
                            attempt_count: 1,
                            created_at: txn_id,
                        };

                        self.deferred_manager
                            .add_deferred(txn_id, deferred_op, holder);

                        // Send deferred response
                        let response = KvResponse::Deferred {
                            waiting_for: holder,
                            lock_key: key.to_string(),
                        };
                        let txn_id_str = format!(
                            "txn_runtime1_{:010}",
                            txn_id.physical * 1_000_000_000 + txn_id.logical as u64
                        );
                        self.send_response(coordinator_id, &txn_id_str, response);

                        Ok(None) // Must wait even though we're older
                    } else {
                        // We're older and holder isn't prepared, wound it
                        self.wound_transaction(holder, txn_id).await?;

                        // Retry lock acquisition
                        self.lock_manager.grant(txn_id, key.to_string(), mode);
                        if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
                            tx_ctx.add_lock(key.to_string(), mode);
                        }
                        Ok(Some(()))
                    }
                } else {
                    // We're younger, must wait
                    let deferred_op = DeferredOperation {
                        operation,
                        coordinator_id: coordinator_id.to_string(),
                        lock_requested: (key.to_string(), mode),
                        waiting_for: holder,
                        attempt_count: 1,
                        created_at: txn_id,
                    };

                    // Add to deferred operations
                    self.deferred_manager
                        .add_deferred(txn_id, deferred_op, holder);

                    // Send deferred response
                    let response = KvResponse::Deferred {
                        waiting_for: holder,
                        lock_key: key.to_string(),
                    };
                    // Convert back to string for response channel
                    let txn_id_str = format!(
                        "txn_runtime1_{:010}",
                        txn_id.physical * 1_000_000_000 + txn_id.logical as u64
                    );
                    self.send_response(coordinator_id, &txn_id_str, response);

                    Ok(None) // Operation deferred
                }
            }
        }
    }

    /// Prepare a transaction for commit
    async fn prepare_transaction(
        &mut self,
        txn_id: HlcTimestamp,
        txn_id_str: &str,
        request_id: Option<String>,
    ) -> Result<()> {
        // Get coordinator ID for response
        let coordinator_id = self
            .transaction_coordinators
            .get(&txn_id)
            .cloned()
            .unwrap_or_default();

        if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
            if let Some(wounded_by) = tx_ctx.wounded_by {
                // Transaction was wounded - send wounded response
                let response = KvResponse::Wounded {
                    wounded_by,
                    reason: "Transaction wounded before prepare".to_string(),
                };
                self.send_response_with_request(
                    &coordinator_id,
                    txn_id_str,
                    response,
                    request_id.clone(),
                );
                Err(Error::TransactionWounded { wounded_by })
            } else if tx_ctx.prepare() {
                // Transaction successfully prepared - send prepared response
                self.send_response_with_request(
                    &coordinator_id,
                    txn_id_str,
                    KvResponse::Prepared,
                    request_id.clone(),
                );
                Ok(())
            } else {
                // Transaction cannot be prepared for other reasons
                let response =
                    KvResponse::Error("Cannot prepare: transaction not active".to_string());
                self.send_response_with_request(
                    &coordinator_id,
                    txn_id_str,
                    response,
                    request_id.clone(),
                );
                Err(Error::TransactionNotActive(txn_id))
            }
        } else {
            // Transaction doesn't exist
            let response = KvResponse::Error("Cannot prepare: transaction not found".to_string());
            self.send_response_with_request(&coordinator_id, txn_id_str, response, request_id);
            Err(Error::TransactionNotActive(txn_id))
        }
    }

    /// Commit a transaction
    async fn commit_transaction(&mut self, txn_id: HlcTimestamp) -> Result<()> {
        if let Some(mut tx_ctx) = self.active_transactions.remove(&txn_id) {
            // Commit in storage
            self.storage.commit_transaction(txn_id);

            // Release all locks
            self.lock_manager.release_all(txn_id);

            // Wake up first waiter (SQL does same - only retries first waiter)
            let waiters = self.deferred_manager.get_waiters(txn_id);
            if let Some(first_waiter) = waiters.first() {
                self.retry_deferred_operations(*first_waiter);
            }

            tx_ctx.commit();
        }

        // Clean up coordinator tracking
        self.transaction_coordinators.remove(&txn_id);
        Ok(())
    }

    /// Abort a transaction
    async fn abort_transaction(&mut self, txn_id: HlcTimestamp) -> Result<()> {
        if let Some(mut tx_ctx) = self.active_transactions.remove(&txn_id) {
            // Abort in storage
            self.storage.abort_transaction(txn_id);

            // Release all locks
            self.lock_manager.release_all(txn_id);

            // Wake up first waiter
            let waiters = self.deferred_manager.get_waiters(txn_id);
            if let Some(first_waiter) = waiters.first() {
                self.retry_deferred_operations(*first_waiter);
            }

            tx_ctx.abort();
        }

        // Clean up coordinator tracking
        self.transaction_coordinators.remove(&txn_id);
        // If transaction doesn't exist, ignore (might have been auto-committed)
        Ok(())
    }

    /// Wound a transaction (force it to abort)
    async fn wound_transaction(
        &mut self,
        victim: HlcTimestamp,
        wounded_by: HlcTimestamp,
    ) -> Result<()> {
        // Check if victim is prepared - prepared transactions cannot be wounded
        if let Some(victim_ctx) = self.active_transactions.get(&victim) {
            if victim_ctx.is_prepared() {
                // Cannot wound a prepared transaction
                // The older transaction must wait or abort itself
                return Err(Error::LockConflict {
                    holder: victim,
                    mode: LockMode::Exclusive, // Prepared transactions hold all their locks
                });
            }
        }

        // Remove victim's deferred operations
        let victim_ops = self.deferred_manager.wound_transaction(victim);

        // Convert victim timestamp to string format
        let victim_str = format!(
            "txn_runtime1_{:010}",
            victim.physical * 1_000_000_000 + victim.logical as u64
        );

        // Send wounded notification to victim's deferred operations
        for op in victim_ops {
            let response = KvResponse::Wounded {
                wounded_by,
                reason: "Wounded by older transaction to prevent deadlock".to_string(),
            };
            self.send_response(&op.coordinator_id, &victim_str, response);
        }

        // Send wounded notification if victim has a coordinator
        if let Some(coordinator_id) = self.transaction_coordinators.get(&victim) {
            let response = KvResponse::Wounded {
                wounded_by,
                reason: "Wounded by older transaction to prevent deadlock".to_string(),
            };
            self.send_response(coordinator_id, &victim_str, response);
        }

        // Mark the victim as wounded
        if let Some(tx_ctx) = self.active_transactions.get_mut(&victim) {
            tx_ctx.wounded_by = Some(wounded_by);
        }

        // Abort the victim
        self.abort_transaction(victim).await?;

        Ok(())
    }

    /// Retry deferred operations for a transaction (synchronous to avoid recursion)
    fn retry_deferred_operations(&mut self, tx_id: HlcTimestamp) {
        if let Some(operations) = self.deferred_manager.remove_operations(tx_id) {
            let txn_id_str = format!(
                "txn_runtime1_{:010}",
                tx_id.physical * 1_000_000_000 + tx_id.logical as u64
            );

            // For each deferred operation, we attempt to re-acquire the lock
            // This is synchronous - we just check if lock is available and execute if so
            for op in operations {
                // Try to execute based on operation type
                let result: Result<KvResponse> = match &op.operation {
                    KvOperation::Get { key } => {
                        // Check if we can get shared lock
                        match self.lock_manager.check(tx_id, key, LockMode::Shared) {
                            LockAttemptResult::WouldGrant => {
                                self.lock_manager
                                    .grant(tx_id, key.to_string(), LockMode::Shared);
                                let value = self.storage.get(key, tx_id).cloned();
                                Ok(KvResponse::GetResult {
                                    key: key.to_string(),
                                    value,
                                })
                            }
                            LockAttemptResult::Conflict { holder, .. } => {
                                // Still blocked, re-defer
                                self.deferred_manager
                                    .add_deferred(tx_id, op.clone(), holder);
                                continue;
                            }
                        }
                    }
                    KvOperation::Put { key, value } => {
                        // Check if we can get exclusive lock
                        match self.lock_manager.check(tx_id, key, LockMode::Exclusive) {
                            LockAttemptResult::WouldGrant => {
                                self.lock_manager.grant(
                                    tx_id,
                                    key.to_string(),
                                    LockMode::Exclusive,
                                );
                                let previous = self.storage.get(key, tx_id).cloned();
                                self.storage
                                    .put(key.to_string(), value.clone(), tx_id, tx_id);
                                Ok(KvResponse::PutResult {
                                    key: key.to_string(),
                                    previous,
                                })
                            }
                            LockAttemptResult::Conflict { holder, .. } => {
                                // Still blocked, re-defer
                                self.deferred_manager
                                    .add_deferred(tx_id, op.clone(), holder);
                                continue;
                            }
                        }
                    }
                    KvOperation::Delete { key } => {
                        // Check if we can get exclusive lock
                        match self.lock_manager.check(tx_id, key, LockMode::Exclusive) {
                            LockAttemptResult::WouldGrant => {
                                self.lock_manager.grant(
                                    tx_id,
                                    key.to_string(),
                                    LockMode::Exclusive,
                                );
                                let existed = self.storage.exists(key, tx_id);
                                if existed {
                                    self.storage.delete(key, tx_id);
                                }
                                Ok(KvResponse::DeleteResult {
                                    key: key.to_string(),
                                    deleted: existed,
                                })
                            }
                            LockAttemptResult::Conflict { holder, .. } => {
                                // Still blocked, re-defer
                                self.deferred_manager
                                    .add_deferred(tx_id, op.clone(), holder);
                                continue;
                            }
                        }
                    }
                };

                // Send response if we executed
                if let Ok(response) = result {
                    self.send_response(&op.coordinator_id, &txn_id_str, response);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_engine::MockClient;

    fn create_message(
        operation: Option<KvOperation>,
        txn_id: &str,
        coordinator_id: &str,
        auto_commit: bool,
        txn_phase: Option<&str>,
    ) -> StreamMessage {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());

        // Always include coordinator_id for operations (not needed for commit/abort)
        if operation.is_some() {
            headers.insert("coordinator_id".to_string(), coordinator_id.to_string());
        }

        if auto_commit {
            headers.insert("auto_commit".to_string(), "true".to_string());
        }

        if let Some(phase) = txn_phase {
            headers.insert("txn_phase".to_string(), phase.to_string());
        }

        let body = if let Some(op) = operation {
            serde_json::to_vec(&op).unwrap()
        } else {
            Vec::new()
        };

        StreamMessage::new(body, headers)
    }

    #[tokio::test]
    async fn test_basic_operations() {
        let (mut processor, engine) = KvStreamProcessor::new_for_testing();

        // Create a test client to subscribe to coordinator responses
        let test_client = MockClient::new("test-observer".to_string(), engine.clone());

        // Subscribe to coordinator response channels
        let mut coord1_responses = test_client
            .subscribe("coordinator.coord1.response", None)
            .await
            .unwrap();
        let mut coord2_responses = test_client
            .subscribe("coordinator.coord2.response", None)
            .await
            .unwrap();

        // Put operation with auto-commit
        let put_op = KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("value1".to_string()),
        };
        let msg = create_message(
            Some(put_op),
            "txn_runtime1_1000000000",
            "coord1",
            true,
            None,
        );

        processor.process_message(msg).await.unwrap();

        // Check Put response
        let put_response_msg = coord1_responses
            .recv()
            .await
            .expect("Should receive put response");
        let put_response: KvResponse = serde_json::from_slice(&put_response_msg.body)
            .expect("Should deserialize put response");
        assert!(
            matches!(put_response, KvResponse::PutResult { .. }),
            "Should receive PutResult"
        );

        // Get operation with auto-commit
        let get_op = KvOperation::Get {
            key: "key1".to_string(),
        };
        let msg = create_message(
            Some(get_op),
            "txn_runtime1_2000000000",
            "coord2",
            true,
            None,
        );

        processor.process_message(msg).await.unwrap();

        // Check Get response
        let get_response_msg = coord2_responses
            .recv()
            .await
            .expect("Should receive get response");
        let get_response: KvResponse = serde_json::from_slice(&get_response_msg.body)
            .expect("Should deserialize get response");

        if let KvResponse::GetResult { value, .. } = get_response {
            assert_eq!(
                value,
                Some(Value::String("value1".to_string())),
                "Should get the value we put"
            );
        } else {
            panic!("Expected GetResult response");
        }
    }

    #[tokio::test]
    async fn test_transaction_isolation() {
        let (mut processor, engine) = KvStreamProcessor::new_for_testing();

        // Create a test client to subscribe to coordinator responses
        let test_client = MockClient::new("test-observer".to_string(), engine.clone());

        // Subscribe to coordinator response channels
        let mut coord1_responses = test_client
            .subscribe("coordinator.coord1.response", None)
            .await
            .unwrap();
        let mut coord2_responses = test_client
            .subscribe("coordinator.coord2.response", None)
            .await
            .unwrap();
        let mut coord3_responses = test_client
            .subscribe("coordinator.coord3.response", None)
            .await
            .unwrap();

        // Transaction 1 (older): Put but don't commit yet
        let put_op = KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("value1".to_string()),
        };
        let msg = create_message(
            Some(put_op),
            "txn_runtime1_1000000000", // Older transaction
            "coord1",
            false, // No auto-commit - will hold lock
            None,
        );

        processor.process_message(msg).await.unwrap();

        // Check Put response for tx1
        let put_response_msg = coord1_responses
            .recv()
            .await
            .expect("Should receive put response");
        let put_response: KvResponse = serde_json::from_slice(&put_response_msg.body)
            .expect("Should deserialize put response");
        assert!(
            matches!(put_response, KvResponse::PutResult { .. }),
            "Should receive PutResult"
        );

        // Transaction 2 (younger): Try to get the same key - should defer since tx1 holds lock
        let get_op = KvOperation::Get {
            key: "key1".to_string(),
        };
        let msg = create_message(
            Some(get_op),
            "txn_runtime1_2000000000", // Younger transaction - will defer to older
            "coord2",
            true, // Auto-commit
            None,
        );

        processor.process_message(msg).await.unwrap();

        // Check that tx2 gets deferred (can't read while tx1 holds lock)
        let response_msg = coord2_responses
            .recv()
            .await
            .expect("Should receive response for tx2");
        let response: KvResponse =
            serde_json::from_slice(&response_msg.body).expect("Should deserialize response");

        match response {
            KvResponse::Deferred { .. } => {
                // Expected - younger transaction defers to older
            }
            other => panic!(
                "Expected Deferred response for younger transaction, got: {:?}",
                other
            ),
        }

        // Now commit tx1
        let msg = create_message(None, "txn_runtime1_1000000000", "", false, Some("commit"));
        processor.process_message(msg).await.unwrap();

        // Transaction 3: Should see committed data
        let get_op = KvOperation::Get {
            key: "key1".to_string(),
        };
        let msg = create_message(
            Some(get_op),
            "txn_runtime1_3000000000",
            "coord3",
            true,
            None,
        );

        processor.process_message(msg).await.unwrap();

        // Check that tx3 sees committed data
        let response_msg = coord3_responses
            .recv()
            .await
            .expect("Should receive response for tx3");
        let response: KvResponse =
            serde_json::from_slice(&response_msg.body).expect("Should deserialize response");

        if let KvResponse::GetResult { value, .. } = response {
            assert_eq!(
                value,
                Some(Value::String("value1".to_string())),
                "Should see committed data"
            );
        } else {
            panic!("Expected GetResult response");
        }
    }
}
