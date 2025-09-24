//! Transaction management utilities

pub mod context;
pub mod deferred;
pub mod recovery;

pub use context::TransactionContext;
pub use deferred::DeferredOperationsManager;
pub use recovery::{RecoveryManager, RecoveryState, TransactionDecision};

use proven_hlc::HlcTimestamp;
use std::collections::{HashMap, HashSet};

/// Transaction state tracking
#[derive(Default)]
pub struct TransactionState {
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
}

impl TransactionState {
    pub fn new() -> Self {
        Self {
            transaction_coordinators: HashMap::new(),
            wounded_transactions: HashMap::new(),
            transaction_deadlines: HashMap::new(),
            transaction_participants: HashMap::new(),
            begun_transactions: HashSet::new(),
        }
    }

    /// Clean up state for a completed transaction
    pub fn cleanup_transaction(&mut self, txn_id: HlcTimestamp) {
        self.transaction_coordinators.remove(&txn_id);
        self.wounded_transactions.remove(&txn_id);
        self.transaction_deadlines.remove(&txn_id);
        self.transaction_participants.remove(&txn_id);
        self.begun_transactions.remove(&txn_id);
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
    pub fn is_wounded(&self, txn_id: &HlcTimestamp) -> bool {
        self.wounded_transactions.contains_key(txn_id)
    }

    /// Mark a transaction as wounded
    pub fn mark_wounded(&mut self, txn_id: HlcTimestamp, wounded_by: HlcTimestamp) {
        self.wounded_transactions.insert(txn_id, wounded_by);
    }
}
