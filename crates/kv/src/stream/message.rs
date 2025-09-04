//! Stream messages for KV operations
//!
//! This module defines the message types that flow through the KV stream processor,
//! matching the SQL stream message pattern for production use.

use crate::types::Value;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// KV operation types that can be sent in messages
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum KvOperation {
    /// Get a value by key
    Get { key: String },

    /// Put a key-value pair
    Put { key: String, value: Value },

    /// Delete a key
    Delete { key: String },
}

/// Message from the stream (matches SQL message structure)
pub struct StreamMessage {
    /// Message body (serialized KvOperation or empty for commit/abort)
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

// Helper functions for tests to create messages easily
#[cfg(test)]
impl StreamMessage {
    /// Create a message with an operation
    pub fn with_operation(txn_id: &str, coordinator_id: &str, operation: KvOperation) -> Self {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("coordinator_id".to_string(), coordinator_id.to_string());

        let body = bincode::encode_to_vec(&operation, bincode::config::standard()).unwrap();
        Self { body, headers }
    }

    /// Set auto-commit flag
    pub fn with_auto_commit(mut self) -> Self {
        self.headers
            .insert("auto_commit".to_string(), "true".to_string());
        self
    }

    /// Create a commit message
    pub fn commit(txn_id: &str) -> Self {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("txn_phase".to_string(), "commit".to_string());
        Self {
            body: Vec::new(),
            headers,
        }
    }

    /// Create an abort message
    pub fn abort(txn_id: &str) -> Self {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("txn_phase".to_string(), "abort".to_string());
        Self {
            body: Vec::new(),
            headers,
        }
    }
}
