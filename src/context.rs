//! Transaction context for SQL execution
//!
//! Provides TransactionContext for carrying transaction state through SQL operations.
//! Uses HLC timestamps as transaction IDs for global ordering.

use crate::hlc::HlcTimestamp;

/// Transaction context carried through SQL execution
#[derive(Debug, Clone)]
pub struct TransactionContext {
    /// Transaction ID (HLC timestamp provides total ordering across the distributed system)
    pub tx_id: HlcTimestamp,

    /// Read-only transaction flag
    pub read_only: bool,
}

impl TransactionContext {
    /// Create a new transaction context
    pub fn new(tx_id: HlcTimestamp) -> Self {
        Self {
            tx_id,
            read_only: false,
        }
    }

    /// Create a read-only transaction context
    pub fn read_only(tx_id: HlcTimestamp) -> Self {
        Self {
            tx_id,
            read_only: true,
        }
    }

    /// Get the timestamp for deterministic SQL functions
    pub fn timestamp(&self) -> &HlcTimestamp {
        &self.tx_id
    }

    /// Generate a deterministic UUID based on transaction ID and a sequence
    pub fn deterministic_uuid(&self, sequence: u64) -> uuid::Uuid {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.tx_id.hash(&mut hasher);
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
