//! Transaction management for stream processing
//!
//! This module contains transaction state management and context
//! for executing SQL operations within the stream processor.

use crate::planning::predicate::QueryPredicates;
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
pub struct TransactionContext {
    /// Transaction ID (HLC timestamp provides total ordering across the distributed system)
    pub id: HlcTimestamp,
    /// Timestamp for this transaction (same as ID)
    pub timestamp: HlcTimestamp,
    /// Current state of the transaction
    pub state: TransactionState,
    /// Predicates for conflict detection
    pub predicates: QueryPredicates,
    /// Sequence counter for generating unique UUIDs within the transaction
    uuid_sequence: std::sync::atomic::AtomicU64,
}

impl TransactionContext {
    /// Create a new transaction context with an HLC timestamp
    pub fn new(hlc_timestamp: HlcTimestamp) -> Self {
        Self {
            id: hlc_timestamp,
            timestamp: hlc_timestamp,
            state: TransactionState::Active,
            predicates: QueryPredicates::new(),
            uuid_sequence: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Add predicates from a query to this transaction
    pub fn add_predicates(&mut self, predicates: QueryPredicates) {
        self.predicates.merge(predicates);
    }

    /// Prepare the transaction (releases read predicates)
    pub fn prepare(&mut self) -> bool {
        if self.state == TransactionState::Active {
            self.state = TransactionState::Preparing;
            // At PREPARE, we can release read predicates
            self.predicates.release_reads();
            true
        } else {
            false
        }
    }

    /// Get the timestamp for deterministic SQL functions
    pub fn timestamp(&self) -> &HlcTimestamp {
        &self.timestamp
    }

    /// Generate a deterministic UUID based on transaction ID and an auto-incrementing sequence
    pub fn deterministic_uuid(&self) -> uuid::Uuid {
        // Ignore the passed sequence and use our internal counter
        let sequence = self
            .uuid_sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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

impl Clone for TransactionContext {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            timestamp: self.timestamp,
            state: self.state,
            predicates: self.predicates.clone(),
            // Create a new AtomicU64 with the current value
            uuid_sequence: std::sync::atomic::AtomicU64::new(
                self.uuid_sequence.load(std::sync::atomic::Ordering::SeqCst),
            ),
        }
    }
}
