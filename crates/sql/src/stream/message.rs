//! Message types for stream processing
//!
//! This module defines the message format and operation types
//! that flow through the stream processor.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// SQL operation types that can be sent in messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlOperation {
    /// Execute SQL statement (DDL or DML)
    Execute { sql: String },

    /// Query that returns results
    Query { sql: String },

    /// Schema migration with version tracking
    Migrate { version: u32, sql: String },
}

/// Message from the stream
pub struct StreamMessage {
    /// Message body (serialized SqlOperation or empty for commit/abort)
    pub body: Vec<u8>,

    /// Headers for transaction control
    pub headers: HashMap<String, String>,
}

impl StreamMessage {
    /// Create a new stream message
    pub fn new(body: Vec<u8>, headers: HashMap<String, String>) -> Self {
        Self { body, headers }
    }

    /// Get transaction ID from headers
    pub fn txn_id(&self) -> Option<&str> {
        self.headers.get("txn_id").map(|s| s.as_str())
    }

    /// Get coordinator ID from headers
    pub fn coordinator_id(&self) -> Option<&str> {
        self.headers.get("coordinator_id").map(|s| s.as_str())
    }

    /// Get transaction phase from headers
    pub fn txn_phase(&self) -> Option<&str> {
        self.headers.get("txn_phase").map(|s| s.as_str())
    }

    /// Check if this is an auto-commit message
    pub fn is_auto_commit(&self) -> bool {
        self.headers.get("auto_commit") == Some(&"true".to_string())
    }

    /// Check if this is a commit or abort message
    pub fn is_transaction_control(&self) -> bool {
        self.body.is_empty() && self.txn_phase().is_some()
    }
}
