//! Transaction management for KV stream processing
//!
//! This module contains transaction state management and context
//! for executing KV operations within the stream processor.

use crate::storage::lock::LockMode;
use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// Transaction is active and can execute operations
    Active,
    /// Transaction has committed
    Committed,
    /// Transaction has been aborted
    Aborted,
}

/// Transaction execution state
#[derive(Debug, Clone)]
pub struct TransactionContext {
    /// Transaction ID (HLC timestamp provides total ordering)
    pub id: HlcTimestamp,
    /// Timestamp for this transaction (same as ID)
    pub timestamp: HlcTimestamp,
    /// Current state of the transaction
    pub state: TransactionState,
    /// Keys currently locked by this transaction
    pub locks_held: Vec<(String, LockMode)>,
    /// Access log for debugging
    pub access_log: Vec<AccessLogEntry>,
    /// If this transaction has been wounded, tracks who wounded it
    pub wounded_by: Option<HlcTimestamp>,
}

/// Access log entry for debugging and coordination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessLogEntry {
    pub operation: String,
    pub key: String,
    pub lock_mode: LockMode,
}

impl TransactionContext {
    /// Create a new transaction context with an HLC timestamp
    pub fn new(hlc_timestamp: HlcTimestamp) -> Self {
        Self {
            id: hlc_timestamp,
            timestamp: hlc_timestamp,
            state: TransactionState::Active,
            locks_held: Vec::new(),
            access_log: Vec::new(),
            wounded_by: None,
        }
    }

    /// Check if transaction is active
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    /// Mark transaction as committed
    pub fn commit(&mut self) {
        self.state = TransactionState::Committed;
    }

    /// Mark transaction as aborted
    pub fn abort(&mut self) {
        self.state = TransactionState::Aborted;
    }

    /// Add a lock to the transaction's held locks
    pub fn add_lock(&mut self, key: String, mode: LockMode) {
        self.locks_held.push((key.clone(), mode));
        self.access_log.push(AccessLogEntry {
            operation: match mode {
                LockMode::Shared => "READ".to_string(),
                LockMode::Exclusive => "WRITE".to_string(),
            },
            key,
            lock_mode: mode,
        });
    }
}
