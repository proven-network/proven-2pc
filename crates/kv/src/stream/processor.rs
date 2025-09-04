//! Core KV stream processor implementation
//!
//! Consumes ordered messages from a Raft-ordered stream and processes KV operations
//! with proper transaction isolation using MVCC for rollback support and PCC for
//! conflict prevention.

use super::deferred::{DeferredOperation, DeferredOperationsManager};
use super::message::{KvOperation, StreamMessage};
use super::response::{KvResponse, ResponseChannel};
use super::transaction::TransactionContext;
use crate::storage::lock::{LockAttemptResult, LockManager, LockMode};
use crate::storage::mvcc::MvccStorage;
use crate::types::Value;
use proven_hlc::HlcTimestamp;
use std::collections::HashMap;

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

    /// Response channel for sending results back
    response_channel: Box<dyn ResponseChannel>,
}

impl KvStreamProcessor {
    /// Create a new processor with empty storage
    pub fn new(response_channel: Box<dyn ResponseChannel>) -> Self {
        Self {
            storage: MvccStorage::new(),
            lock_manager: LockManager::new(),
            active_transactions: HashMap::new(),
            transaction_coordinators: HashMap::new(),
            deferred_manager: DeferredOperationsManager::new(),
            response_channel,
        }
    }

    /// Process a message from the stream (matches SQL pattern)
    pub async fn process_message(&mut self, message: StreamMessage) -> Result<()> {
        let txn_id_str = message
            .txn_id()
            .ok_or_else(|| Error::InvalidValue("Missing txn_id header".into()))?;

        // Parse transaction ID to HlcTimestamp at the boundary
        let txn_id = HlcTimestamp::parse(txn_id_str).map_err(|e| Error::InvalidValue(e))?;

        // Check if this is a commit/abort message (empty body)
        if message.is_transaction_control() {
            match message.txn_phase() {
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
        let operation: KvOperation =
            bincode::decode_from_slice(&message.body, bincode::config::standard())
                .map_err(|e| {
                    Error::InvalidValue(format!("Failed to deserialize operation: {}", e))
                })?
                .0;

        // Ensure transaction exists (automatically create if needed)
        if !self.active_transactions.contains_key(&txn_id) {
            self.create_transaction(txn_id)?;
        }

        // Get coordinator ID - required for all operations
        let coordinator_id = message.coordinator_id().ok_or_else(|| {
            Error::InvalidValue("coordinator_id is required for all operations".to_string())
        })?;

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
        self.response_channel
            .send(coordinator_id, txn_id_str, response);

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
                    // We're older, wound the younger transaction
                    self.wound_transaction(holder, txn_id).await?;

                    // Retry lock acquisition
                    self.lock_manager.grant(txn_id, key.to_string(), mode);
                    if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
                        tx_ctx.add_lock(key.to_string(), mode);
                    }
                    Ok(Some(()))
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
                    self.response_channel
                        .send(coordinator_id, &txn_id_str, response);

                    Ok(None) // Operation deferred
                }
            }
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
            self.response_channel
                .send(&op.coordinator_id, &victim_str, response);
        }

        // Send wounded notification if victim has a coordinator
        if let Some(coordinator_id) = self.transaction_coordinators.get(&victim) {
            let response = KvResponse::Wounded {
                wounded_by,
                reason: "Wounded by older transaction to prevent deadlock".to_string(),
            };
            self.response_channel
                .send(coordinator_id, &victim_str, response);
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
                    self.response_channel
                        .send(&op.coordinator_id, &txn_id_str, response);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::response::MockResponseChannel;
    use std::sync::Arc;

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
            bincode::encode_to_vec(&op, bincode::config::standard()).unwrap()
        } else {
            Vec::new()
        };

        StreamMessage::new(body, headers)
    }

    #[tokio::test]
    async fn test_basic_operations() {
        let mock_channel = Arc::new(MockResponseChannel::new());
        let response_channel: Box<dyn ResponseChannel> = Box::new(mock_channel.clone());
        let mut processor = KvStreamProcessor::new(response_channel);

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

        // Check responses
        let responses = mock_channel.get_responses();
        assert_eq!(responses.len(), 2);

        // First response should be put result
        if let KvResponse::PutResult { key, previous } = &responses[0].2 {
            assert_eq!(key, "key1");
            assert!(previous.is_none());
        } else {
            panic!("Expected PutResult");
        }

        // Second response should be get result
        if let KvResponse::GetResult { key, value } = &responses[1].2 {
            assert_eq!(key, "key1");
            assert_eq!(value, &Some(Value::String("value1".to_string())));
        } else {
            panic!("Expected GetResult");
        }
    }

    #[tokio::test]
    async fn test_transaction_isolation() {
        let mock_channel = Arc::new(MockResponseChannel::new());
        let response_channel: Box<dyn ResponseChannel> = Box::new(mock_channel.clone());
        let mut processor = KvStreamProcessor::new(response_channel);

        // Transaction 1: Put but don't commit
        let put_op = KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("value1".to_string()),
        };
        let msg = create_message(
            Some(put_op),
            "txn_runtime1_1000000000",
            "coord1",
            false, // No auto-commit
            None,
        );

        processor.process_message(msg).await.unwrap();

        // Transaction 2: Try to get (should not see uncommitted data)
        let get_op = KvOperation::Get {
            key: "key1".to_string(),
        };
        let msg = create_message(
            Some(get_op),
            "txn_runtime1_2000000000",
            "coord2",
            true, // Auto-commit
            None,
        );

        processor.process_message(msg).await.unwrap();

        // Check that tx2 doesn't see uncommitted data
        let responses = mock_channel.get_responses();
        if let Some((_, _, KvResponse::GetResult { value, .. })) =
            responses.iter().find(|(coord, _, _)| coord == "coord2")
        {
            assert!(value.is_none(), "Should not see uncommitted data");
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
        let responses = mock_channel.get_responses();
        if let Some((_, _, KvResponse::GetResult { value, .. })) = responses
            .iter()
            .rev()
            .find(|(coord, _, _)| coord == "coord3")
        {
            assert_eq!(value, &Some(Value::String("value1".to_string())));
        } else {
            panic!("Expected GetResult for coord3");
        }
    }
}
