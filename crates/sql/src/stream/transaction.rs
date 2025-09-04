//! Transaction management for stream processing
//!
//! This module contains transaction state management and context
//! for executing SQL operations within the stream processor.

use crate::storage::lock::LockKey;
use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// Transaction is active and can execute operations
    Active,
    /// Transaction is preparing to commit (2PC)
    Preparing,
    /// Transaction has committed
    Committed,
    /// Transaction has been aborted
    Aborted,
}

/// Transaction execution state
#[derive(Clone)]
pub struct TransactionContext {
    /// Transaction ID (HLC timestamp provides total ordering across the distributed system)
    pub id: HlcTimestamp,
    /// Timestamp for this transaction (same as ID)
    pub timestamp: HlcTimestamp,
    /// Current state of the transaction
    pub state: TransactionState,
    /// Locks currently held by this transaction
    pub locks_held: Vec<LockKey>,
    /// Access log for distributed coordination
    pub access_log: Vec<AccessLogEntry>,
    /// If this transaction has been wounded, tracks who wounded it
    pub wounded_by: Option<HlcTimestamp>,
}

/// Access log entry for distributed coordination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessLogEntry {
    pub operation: String,
    pub table: String,
    pub keys: Vec<u64>,
    pub lock_mode: crate::storage::lock::LockMode,
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

    /// Get the timestamp for deterministic SQL functions
    pub fn timestamp(&self) -> &HlcTimestamp {
        &self.timestamp
    }

    /// Generate a deterministic UUID based on transaction ID and a sequence
    pub fn deterministic_uuid(&self, sequence: u64) -> uuid::Uuid {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.id.hash(&mut hasher);
        sequence.hash(&mut hasher);

        let hash = hasher.finish();
        let bytes = hash.to_be_bytes();

        // Create a v4-like UUID but deterministically
        let mut uuid_bytes = [0u8; 16];
        uuid_bytes[..8].copy_from_slice(&bytes);
        uuid_bytes[8..].copy_from_slice(&bytes); // Repeat for full 16 bytes

        // Set version (4) and variant bits
        uuid_bytes[6] = (uuid_bytes[6] & 0x0f) | 0x40;
        uuid_bytes[8] = (uuid_bytes[8] & 0x3f) | 0x80;

        uuid::Uuid::from_bytes(uuid_bytes)
    }
}
