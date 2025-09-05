//! KV Transaction Engine implementation for the generic stream processor
//!
//! This module implements the TransactionEngine trait, providing KV-specific
//! operation execution while delegating message handling to the generic processor.

use proven_hlc::HlcTimestamp;
use proven_stream::{OperationResult, TransactionEngine};

use crate::storage::lock::{LockAttemptResult, LockManager, LockMode};
use crate::storage::mvcc::MvccStorage;
use crate::types::Value;

use super::operation::KvOperation;
use super::response::KvResponse;
use super::transaction::TransactionContext;

use std::collections::HashMap;

/// KV-specific transaction engine
pub struct KvTransactionEngine {
    /// MVCC storage for versioned data
    storage: MvccStorage,

    /// Lock manager for pessimistic concurrency control
    lock_manager: LockManager,

    /// Active transaction contexts
    active_transactions: HashMap<HlcTimestamp, TransactionContext>,
}

impl KvTransactionEngine {
    /// Create a new KV transaction engine
    pub fn new() -> Self {
        Self {
            storage: MvccStorage::new(),
            lock_manager: LockManager::new(),
            active_transactions: HashMap::new(),
        }
    }

    /// Execute a get operation
    fn execute_get(
        &mut self,
        key: &str,
        txn_id: HlcTimestamp,
    ) -> OperationResult<KvResponse> {
        // Check if we can acquire the lock
        match self.lock_manager.check(txn_id, key, LockMode::Shared) {
            LockAttemptResult::WouldGrant => {
                // Grant the lock
                self.lock_manager.grant(txn_id, key.to_string(), LockMode::Shared);
                
                // Perform the read
                let value = self.storage.get(key, txn_id);
                
                // Track lock in transaction context
                if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
                    tx_ctx.locks_held.push((key.to_string(), LockMode::Shared));
                }

                OperationResult::Success(KvResponse::GetResult {
                    key: key.to_string(),
                    value: value.cloned(),
                })
            }
            LockAttemptResult::Conflict { holder, .. } => {
                // Check for wound-wait
                if txn_id < holder {
                    // We're older, wound the younger transaction
                    if let Some(younger_ctx) = self.active_transactions.get_mut(&holder) {
                        younger_ctx.wounded_by = Some(txn_id);
                    }
                    // Still need to wait though
                    OperationResult::WouldBlock {
                        blocking_txn: holder,
                    }
                } else {
                    // We're younger, must wait
                    OperationResult::WouldBlock {
                        blocking_txn: holder,
                    }
                }
            }
        }
    }

    /// Execute a put operation
    fn execute_put(
        &mut self,
        key: &str,
        value: Value,
        txn_id: HlcTimestamp,
    ) -> OperationResult<KvResponse> {
        // Check if we can acquire the lock
        match self.lock_manager.check(txn_id, key, LockMode::Exclusive) {
            LockAttemptResult::WouldGrant => {
                // Grant the lock
                self.lock_manager.grant(txn_id, key.to_string(), LockMode::Exclusive);
                
                // Get the previous value for the response
                let previous = self.storage.get(key, txn_id).cloned();

                // Write to storage
                self.storage.put(key.to_string(), value.clone(), txn_id, txn_id);

                // Track lock in transaction context
                if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
                    tx_ctx.locks_held.push((key.to_string(), LockMode::Exclusive));
                }

                OperationResult::Success(KvResponse::PutResult {
                    key: key.to_string(),
                    previous,
                })
            }
            LockAttemptResult::Conflict { holder, .. } => {
                // Check for wound-wait
                if txn_id < holder {
                    // We're older, wound the younger transaction
                    if let Some(younger_ctx) = self.active_transactions.get_mut(&holder) {
                        younger_ctx.wounded_by = Some(txn_id);
                    }
                    // Still need to wait though
                    OperationResult::WouldBlock {
                        blocking_txn: holder,
                    }
                } else {
                    // We're younger, must wait
                    OperationResult::WouldBlock {
                        blocking_txn: holder,
                    }
                }
            }
        }
    }

    /// Execute a delete operation
    fn execute_delete(
        &mut self,
        key: &str,
        txn_id: HlcTimestamp,
    ) -> OperationResult<KvResponse> {
        // Check if we can acquire the lock
        match self.lock_manager.check(txn_id, key, LockMode::Exclusive) {
            LockAttemptResult::WouldGrant => {
                // Grant the lock
                self.lock_manager.grant(txn_id, key.to_string(), LockMode::Exclusive);
                
                // Check if key exists
                let existed = self.storage.get(key, txn_id).is_some();

                // Delete from storage
                if existed {
                    self.storage.delete(key, txn_id);
                }

                // Track lock in transaction context
                if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
                    tx_ctx.locks_held.push((key.to_string(), LockMode::Exclusive));
                }

                OperationResult::Success(KvResponse::DeleteResult {
                    key: key.to_string(),
                    deleted: existed,
                })
            }
            LockAttemptResult::Conflict { holder, .. } => {
                // Check for wound-wait
                if txn_id < holder {
                    // We're older, wound the younger transaction
                    if let Some(younger_ctx) = self.active_transactions.get_mut(&holder) {
                        younger_ctx.wounded_by = Some(txn_id);
                    }
                    // Still need to wait though
                    OperationResult::WouldBlock {
                        blocking_txn: holder,
                    }
                } else {
                    // We're younger, must wait
                    OperationResult::WouldBlock {
                        blocking_txn: holder,
                    }
                }
            }
        }
    }
}

impl TransactionEngine for KvTransactionEngine {
    type Operation = KvOperation;
    type Response = KvResponse;

    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: HlcTimestamp,
    ) -> OperationResult<Self::Response> {
        match operation {
            KvOperation::Get { ref key } => self.execute_get(key, txn_id),
            KvOperation::Put { ref key, ref value } => {
                self.execute_put(key, value.clone(), txn_id)
            }
            KvOperation::Delete { ref key } => self.execute_delete(key, txn_id),
        }
    }

    fn prepare(&mut self, txn_id: HlcTimestamp) -> Result<(), String> {
        // Get transaction context
        let tx_ctx = self
            .active_transactions
            .get_mut(&txn_id)
            .ok_or_else(|| format!("Transaction {} not found", txn_id))?;

        // Try to prepare the transaction
        if tx_ctx.prepare() {
            Ok(())
        } else {
            Err("Transaction cannot be prepared (may be wounded or aborted)".to_string())
        }
    }

    fn commit(&mut self, txn_id: HlcTimestamp) -> Result<(), String> {
        // Remove transaction context
        let tx_ctx = self
            .active_transactions
            .remove(&txn_id)
            .ok_or_else(|| format!("Transaction {} not found", txn_id))?;

        // For auto-commit, we don't require prepare
        // (The transaction is active and can be directly committed)
        // For 2PC, it should be prepared
        if tx_ctx.is_active() || tx_ctx.is_prepared() {
            // Commit to storage
            self.storage.commit_transaction(txn_id);

            // Release all locks held by this transaction
            self.lock_manager.release_all(txn_id);

            Ok(())
        } else {
            Err("Transaction is not in a committable state".to_string())
        }
    }

    fn abort(&mut self, txn_id: HlcTimestamp) -> Result<(), String> {
        // Remove transaction context
        self.active_transactions.remove(&txn_id);

        // Abort in storage
        self.storage.abort_transaction(txn_id);

        // Release all locks
        self.lock_manager.release_all(txn_id);

        Ok(())
    }

    fn begin_transaction(&mut self, txn_id: HlcTimestamp) {
        // Create new transaction context
        let tx_ctx = TransactionContext::new(txn_id);
        
        // Register with storage
        self.storage.register_transaction(txn_id, txn_id);
        
        // Store context
        self.active_transactions.insert(txn_id, tx_ctx);
    }

    fn is_transaction_active(&self, txn_id: &HlcTimestamp) -> bool {
        self.active_transactions.contains_key(txn_id)
    }

    fn engine_name(&self) -> &str {
        "kv"
    }
}

impl Default for KvTransactionEngine {
    fn default() -> Self {
        Self::new()
    }
}