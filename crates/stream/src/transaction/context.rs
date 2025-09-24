//! Transaction context that holds all state needed for transaction processing
//!
//! This centralizes all the transaction-related state that was previously
//! scattered throughout the processor.

use super::DeferredOperationsManager;
use super::recovery::RecoveryManager;
use crate::engine::TransactionEngine;
use proven_engine::MockClient;
use proven_hlc::HlcTimestamp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Centralized transaction context holding all transaction-related state
pub struct TransactionContext<E: TransactionEngine> {
    /// Map from transaction ID to coordinator ID (for responses)
    pub transaction_coordinators: HashMap<HlcTimestamp, String>,

    /// Track wounded transactions (txn_id -> wounded_by)
    pub wounded_transactions: HashMap<HlcTimestamp, HlcTimestamp>,

    /// Track transaction deadlines (txn_id -> deadline)
    pub transaction_deadlines: HashMap<HlcTimestamp, HlcTimestamp>,

    /// Track transaction participants (txn_id -> (participant -> offset))
    pub transaction_participants: HashMap<HlcTimestamp, HashMap<String, u64>>,

    /// Track which transactions have been begun in the engine
    pub begun_transactions: HashSet<HlcTimestamp>,

    /// Manager for deferred operations (blocked on locks)
    pub deferred_manager: DeferredOperationsManager<E::Operation>,

    /// Recovery manager for handling coordinator failures
    pub recovery_manager: RecoveryManager<E>,

    /// Track commits for snapshot management
    pub commits_since_snapshot: u64,
}

impl<E: TransactionEngine> TransactionContext<E> {
    /// Create a new transaction context
    pub fn new(client: Arc<MockClient>, stream_name: String) -> Self {
        Self {
            transaction_coordinators: HashMap::new(),
            wounded_transactions: HashMap::new(),
            transaction_deadlines: HashMap::new(),
            transaction_participants: HashMap::new(),
            begun_transactions: HashSet::new(),
            deferred_manager: DeferredOperationsManager::new(),
            recovery_manager: RecoveryManager::new(client, stream_name),
            commits_since_snapshot: 0,
        }
    }

    /// Check if a transaction has been begun
    pub fn is_begun(&self, txn_id: &HlcTimestamp) -> bool {
        self.begun_transactions.contains(txn_id)
    }

    /// Mark a transaction as begun
    pub fn mark_begun(&mut self, txn_id: HlcTimestamp) {
        self.begun_transactions.insert(txn_id);
    }

    /// Check if a transaction is wounded
    pub fn is_wounded(&self, txn_id: &HlcTimestamp) -> Option<HlcTimestamp> {
        self.wounded_transactions.get(txn_id).copied()
    }

    /// Mark a transaction as wounded
    pub fn wound_transaction(&mut self, victim: HlcTimestamp, wounded_by: HlcTimestamp) {
        self.wounded_transactions.insert(victim, wounded_by);
    }

    /// Get the deadline for a transaction
    pub fn get_deadline(&self, txn_id: &HlcTimestamp) -> Option<HlcTimestamp> {
        self.transaction_deadlines.get(txn_id).copied()
    }

    /// Set the deadline for a transaction
    pub fn set_deadline(&mut self, txn_id: HlcTimestamp, deadline: HlcTimestamp) {
        self.transaction_deadlines.insert(txn_id, deadline);
    }

    /// Get the coordinator for a transaction
    pub fn get_coordinator(&self, txn_id: &HlcTimestamp) -> Option<String> {
        self.transaction_coordinators.get(txn_id).cloned()
    }

    /// Set the coordinator for a transaction
    pub fn set_coordinator(&mut self, txn_id: HlcTimestamp, coordinator: String) {
        self.transaction_coordinators.insert(txn_id, coordinator);
    }

    /// Clean up state for a committed transaction
    pub fn cleanup_committed(&mut self, txn_id: &HlcTimestamp) {
        self.transaction_coordinators.remove(txn_id);
        self.transaction_deadlines.remove(txn_id);
        self.transaction_participants.remove(txn_id);
        self.begun_transactions.remove(txn_id);
        self.commits_since_snapshot += 1;
    }

    /// Clean up state for an aborted transaction
    pub fn cleanup_aborted(&mut self, txn_id: &HlcTimestamp) {
        self.transaction_coordinators.remove(txn_id);
        self.transaction_deadlines.remove(txn_id);
        self.transaction_participants.remove(txn_id);
        self.wounded_transactions.remove(txn_id);
        self.begun_transactions.remove(txn_id);
        self.deferred_manager
            .remove_operations_for_transaction(txn_id);
    }

    /// Check if there are any active transactions
    pub fn has_active_transactions(&self) -> bool {
        !self.transaction_coordinators.is_empty()
    }

    /// Get all transactions past their deadline
    pub fn get_expired_transactions(&self, current_time: HlcTimestamp) -> Vec<HlcTimestamp> {
        self.transaction_deadlines
            .iter()
            .filter_map(|(txn_id, deadline)| {
                if current_time > *deadline
                    && self.transaction_coordinators.contains_key(txn_id)
                    && !self.wounded_transactions.contains_key(txn_id)
                {
                    Some(*txn_id)
                } else {
                    None
                }
            })
            .collect()
    }
}
