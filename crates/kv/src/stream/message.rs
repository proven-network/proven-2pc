//! Stream messages for KV operations
//!
//! This module defines the message types that flow through the KV stream processor,
//! similar to the SQL stream messages but simplified for key-value operations.

use crate::types::Value;
use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};

/// KV operation types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvOperation {
    /// Get a value by key
    Get { key: String },

    /// Put a key-value pair
    Put { key: String, value: Value },

    /// Delete a key
    Delete { key: String },
}

/// Stream message containing a KV operation and transaction metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMessage {
    /// Transaction ID (HLC timestamp)
    pub tx_id: HlcTimestamp,

    /// Coordinator ID that sent this message
    pub coordinator_id: String,

    /// The operation to perform
    pub operation: KvOperation,

    /// Whether this starts a new transaction
    pub begin: bool,

    /// Whether to commit the transaction after this operation
    pub commit: bool,

    /// Whether to abort the transaction
    pub abort: bool,
}

impl StreamMessage {
    /// Create a new stream message
    pub fn new(tx_id: HlcTimestamp, coordinator_id: String, operation: KvOperation) -> Self {
        Self {
            tx_id,
            coordinator_id,
            operation,
            begin: false,
            commit: false,
            abort: false,
        }
    }

    /// Create a message that begins a transaction
    pub fn with_begin(mut self) -> Self {
        self.begin = true;
        self
    }

    /// Create a message that commits a transaction
    pub fn with_commit(mut self) -> Self {
        self.commit = true;
        self
    }

    /// Create a message that aborts a transaction
    pub fn with_abort(mut self) -> Self {
        self.abort = true;
        self
    }
}
