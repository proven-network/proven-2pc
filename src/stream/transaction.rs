//! Transaction management for stream processing
//!
//! This module contains transaction state management and context
//! for executing SQL operations within the stream processor.

use crate::error::{Error, Result};
use crate::hlc::{HlcTimestamp, NodeId};
use crate::storage::lock::LockKey;
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
    pub id: HlcTimestamp,
    pub timestamp: HlcTimestamp,
    pub state: TransactionState,
    pub locks_held: Vec<LockKey>,
    pub access_log: Vec<AccessLogEntry>,
    pub context: crate::context::TransactionContext,
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
    /// Create a new transaction context from a transaction ID string
    pub fn from_txn_id(txn_id: &str) -> Result<Self> {
        // Parse the transaction ID to extract timestamp
        let parts: Vec<&str> = txn_id.split('_').collect();
        if parts.len() < 3 {
            return Err(Error::InvalidValue(format!(
                "Invalid txn_id format: {}",
                txn_id
            )));
        }

        let timestamp_str = parts[2];
        let timestamp_nanos: u64 = timestamp_str
            .parse()
            .map_err(|_| Error::InvalidValue(format!("Invalid timestamp in txn_id: {}", txn_id)))?;

        // Create HLC timestamp
        let hlc_timestamp = HlcTimestamp::new(
            timestamp_nanos / 1_000_000_000,
            (timestamp_nanos % 1_000_000_000) as u32,
            NodeId::new(1),
        );

        // Create new transaction context
        Ok(Self {
            id: hlc_timestamp,
            timestamp: hlc_timestamp,
            state: TransactionState::Active,
            locks_held: Vec::new(),
            access_log: Vec::new(),
            context: crate::context::TransactionContext::new(hlc_timestamp),
        })
    }
}
