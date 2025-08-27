//! Transaction ID structure for distributed and sub-transactions
//!
//! Every SQL operation in the system is associated with a transaction ID that consists of:
//! 1. A global distributed transaction ID (HLC timestamp from the coordinator)
//! 2. A sub-transaction sequence number for local sub-transactions (BEGIN, SAVEPOINT)

use crate::hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Complete transaction identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId {
    /// Global distributed transaction ID from coordinator
    /// This HLC timestamp provides total ordering across the distributed system
    pub global_id: HlcTimestamp,
    
    /// Sub-transaction sequence number
    /// Incremented for each BEGIN or SAVEPOINT within the global transaction
    /// 0 = main transaction, 1+ = sub-transactions
    pub sub_seq: u32,
}

impl TransactionId {
    /// Create a new main transaction ID
    pub fn new(global_id: HlcTimestamp) -> Self {
        Self {
            global_id,
            sub_seq: 0,
        }
    }
    
    /// Create a sub-transaction ID
    pub fn sub_transaction(&self, seq: u32) -> Self {
        Self {
            global_id: self.global_id,
            sub_seq: seq,
        }
    }
    
    /// Check if this is a sub-transaction
    pub fn is_sub_transaction(&self) -> bool {
        self.sub_seq > 0
    }
    
    /// Get the parent transaction ID (strips sub_seq)
    pub fn parent(&self) -> Self {
        Self {
            global_id: self.global_id,
            sub_seq: 0,
        }
    }
    
    /// Compare priority for wound-wait (older = higher priority)
    /// Uses only the global_id since sub-transactions inherit parent priority
    pub fn has_higher_priority_than(&self, other: &Self) -> bool {
        self.global_id.is_older_than(&other.global_id)
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.sub_seq == 0 {
            write!(f, "{}", self.global_id)
        } else {
            write!(f, "{}.{}", self.global_id, self.sub_seq)
        }
    }
}

/// Transaction context carried through SQL execution
#[derive(Debug, Clone)]
pub struct TransactionContext {
    /// Full transaction ID
    pub tx_id: TransactionId,
    
    /// Read-only transaction flag
    pub read_only: bool,
    
    /// Isolation level (future enhancement)
    pub isolation_level: IsolationLevel,
}

impl TransactionContext {
    /// Create a new transaction context
    pub fn new(global_id: HlcTimestamp) -> Self {
        Self {
            tx_id: TransactionId::new(global_id),
            read_only: false,
            isolation_level: IsolationLevel::Serializable,
        }
    }
    
    /// Create a read-only transaction context
    pub fn read_only(global_id: HlcTimestamp) -> Self {
        Self {
            tx_id: TransactionId::new(global_id),
            read_only: true,
            isolation_level: IsolationLevel::Serializable,
        }
    }
    
    /// Create a sub-transaction context
    pub fn sub_transaction(&self, seq: u32) -> Self {
        Self {
            tx_id: self.tx_id.sub_transaction(seq),
            read_only: self.read_only,
            isolation_level: self.isolation_level,
        }
    }
    
    /// Get the timestamp for deterministic SQL functions
    pub fn timestamp(&self) -> &HlcTimestamp {
        &self.tx_id.global_id
    }
    
    /// Generate a deterministic UUID based on transaction ID and a sequence
    pub fn deterministic_uuid(&self, sequence: u64) -> uuid::Uuid {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        
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

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// Full serializability (default)
    Serializable,
    /// Repeatable read (future)
    RepeatableRead,
    /// Read committed (future)
    ReadCommitted,
}