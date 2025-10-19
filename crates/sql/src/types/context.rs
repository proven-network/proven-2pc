//! Transaction management for stream processing
//!
//! This module contains transaction state management and context
//! for executing SQL operations within the stream processor.

use crate::semantic::predicate::QueryPredicates;
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

/// DDL operation that can be rolled back
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum PendingDdl {
    Create {
        name: String,
    },
    Drop {
        name: String,
        old_schema: crate::types::schema::Table,
    },
    Alter {
        name: String,
        old_schema: crate::types::schema::Table,
    },
    Rename {
        old_name: String,
        new_name: String,
        old_schema: crate::types::schema::Table,
    },
    CreateIndex {
        name: String,
    },
    DropIndex {
        name: String,
        metadata: crate::types::index::IndexMetadata,
    },
}

/// Transaction-level context (long-lived, spans multiple operations)
pub struct TransactionContext {
    /// Transaction ID (HLC timestamp provides total ordering across the distributed system)
    pub id: HlcTimestamp,
    /// Predicates accumulated across all operations in this transaction
    pub predicates: QueryPredicates,
    /// Pending DDL operations (for rollback)
    pub pending_ddls: Vec<PendingDdl>,
}

impl TransactionContext {
    /// Create a new transaction context with an HLC timestamp
    pub fn new(hlc_timestamp: HlcTimestamp) -> Self {
        Self {
            id: hlc_timestamp,
            predicates: QueryPredicates::new(),
            pending_ddls: Vec::new(),
        }
    }

    /// Record a pending DDL operation
    pub fn add_pending_ddl(&mut self, ddl: PendingDdl) {
        self.pending_ddls.push(ddl);
    }

    /// Add predicates from a query to this transaction
    pub fn add_predicates(&mut self, predicates: QueryPredicates) {
        self.predicates.merge(predicates);
    }

    /// Prepare the transaction (releases read predicates)
    pub fn prepare(&mut self) {
        // At PREPARE, we can release read predicates
        self.predicates.release_reads();
    }

    /// Create an execution context for a single operation
    pub fn create_execution_context(
        &self,
        log_index: u64,
        new_predicates: QueryPredicates,
    ) -> ExecutionContext {
        ExecutionContext {
            txn_id: self.id,
            log_index,
            predicates: new_predicates,
            uuid_sequence: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl Clone for TransactionContext {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            predicates: self.predicates.clone(),
            pending_ddls: self.pending_ddls.clone(),
        }
    }
}

/// Execution context for a single operation (short-lived)
#[derive(Debug)]
pub struct ExecutionContext {
    /// Transaction ID
    pub txn_id: HlcTimestamp,
    /// Log index for this operation (for deterministic replay)
    pub log_index: u64,
    /// Predicates for this specific operation only
    pub predicates: QueryPredicates,
    /// Sequence counter for generating unique UUIDs within this operation
    uuid_sequence: std::sync::atomic::AtomicU64,
}

impl ExecutionContext {
    pub fn new(txn_id: HlcTimestamp, log_index: u64) -> Self {
        Self {
            txn_id,
            log_index,
            predicates: QueryPredicates::new(),
            uuid_sequence: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Get the transaction timestamp for deterministic SQL functions
    pub fn timestamp(&self) -> &HlcTimestamp {
        &self.txn_id
    }

    /// Generate a deterministic UUID based on transaction ID, log_index, and sequence
    /// This ensures that replaying operations from the log produces identical UUIDs
    pub fn deterministic_uuid(&self) -> uuid::Uuid {
        let sequence = self
            .uuid_sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        // Hash transaction ID
        self.txn_id.hash(&mut hasher);
        // Hash log_index for deterministic replay
        self.log_index.hash(&mut hasher);
        // Hash sequence for uniqueness within operation
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

impl Clone for ExecutionContext {
    fn clone(&self) -> Self {
        Self {
            txn_id: self.txn_id,
            log_index: self.log_index,
            predicates: self.predicates.clone(),
            // Create a new AtomicU64 with the current value
            uuid_sequence: std::sync::atomic::AtomicU64::new(
                self.uuid_sequence.load(std::sync::atomic::Ordering::SeqCst),
            ),
        }
    }
}
