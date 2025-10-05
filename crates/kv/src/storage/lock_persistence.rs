//! Lock persistence for crash recovery
//!
//! Similar to SQL's predicate persistence, we persist locks so they can be
//! restored after a crash and properly released on commit/abort.
//!
//! IMPORTANT: Persisted locks are ONLY read during recovery (startup scan).
//! During normal operation, all lock operations use the in-memory LockManager.

use super::lock::LockMode;
use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Persisted lock information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedLock {
    pub key: String,
    pub mode: LockMode,
}

/// Lock state for a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionLocks {
    pub txn_id: HlcTimestamp,
    pub locks: Vec<PersistedLock>,
}

impl TransactionLocks {
    pub fn new(txn_id: HlcTimestamp) -> Self {
        Self {
            txn_id,
            locks: Vec::new(),
        }
    }

    pub fn add_lock(&mut self, key: String, mode: LockMode) {
        self.locks.push(PersistedLock { key, mode });
    }

    pub fn to_hashmap(&self) -> HashMap<String, LockMode> {
        self.locks
            .iter()
            .map(|lock| (lock.key.clone(), lock.mode))
            .collect()
    }
}

/// Encode transaction locks for persistence (using bincode for efficiency)
pub fn encode_transaction_locks(locks: &TransactionLocks) -> Result<Vec<u8>, String> {
    bincode::serialize(locks).map_err(|e| e.to_string())
}

/// Decode transaction locks from persistence
pub fn decode_transaction_locks(bytes: &[u8]) -> Result<TransactionLocks, String> {
    bincode::deserialize(bytes).map_err(|e| e.to_string())
}
