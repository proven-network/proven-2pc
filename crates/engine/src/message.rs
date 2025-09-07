//! Message types for the mock engine
//!
//! This module defines the message format used by the mock engine,
//! matching the production engine's message structure.

use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Message that flows through the mock engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Message body (serialized data)
    pub body: Vec<u8>,

    /// Headers for metadata
    pub headers: HashMap<String, String>,

    /// Sequence number (for streams)
    pub sequence: Option<u64>,

    /// HLC timestamp (for ordering and deadline comparison)
    pub timestamp: Option<HlcTimestamp>,
}

impl Message {
    /// Create a new message with body and headers
    pub fn new(body: Vec<u8>, headers: HashMap<String, String>) -> Self {
        Self {
            body,
            headers,
            sequence: None,
            timestamp: None,
        }
    }

    /// Create a message with just body
    pub fn with_body(body: Vec<u8>) -> Self {
        Self {
            body,
            headers: HashMap::new(),
            sequence: None,
            timestamp: None,
        }
    }

    /// Add a header to the message
    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }

    /// Set transaction deadline header
    pub fn with_txn_deadline(mut self, deadline: String) -> Self {
        self.headers.insert("txn_deadline".to_string(), deadline);
        self
    }

    /// Set sequence number
    pub fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = Some(sequence);
        self
    }

    /// Set timestamp
    pub fn with_timestamp(mut self, timestamp: HlcTimestamp) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Get HLC timestamp
    pub fn hlc_timestamp(&self) -> Option<HlcTimestamp> {
        self.timestamp
    }

    /// Get header value
    pub fn get_header(&self, key: &str) -> Option<&str> {
        self.headers.get(key).map(|s| s.as_str())
    }

    /// Get transaction ID from headers
    pub fn txn_id(&self) -> Option<&str> {
        self.get_header("txn_id")
    }

    /// Get transaction deadline from headers
    pub fn txn_deadline(&self) -> Option<&str> {
        self.get_header("txn_deadline")
    }

    /// Get coordinator ID from headers
    pub fn coordinator_id(&self) -> Option<&str> {
        self.get_header("coordinator_id")
    }

    /// Get transaction phase from headers
    pub fn txn_phase(&self) -> Option<&str> {
        self.get_header("txn_phase")
    }

    /// Get request ID from headers
    pub fn request_id(&self) -> Option<&str> {
        self.get_header("request_id")
    }

    /// Check if this is an auto-commit message
    pub fn is_auto_commit(&self) -> bool {
        self.get_header("auto_commit") == Some("true")
    }

    /// Check if this is a transaction control message (commit/abort/prepare)
    pub fn is_transaction_control(&self) -> bool {
        self.body.is_empty() && self.txn_phase().is_some()
    }
}

// Allow conversion from byte vectors
impl From<Vec<u8>> for Message {
    fn from(body: Vec<u8>) -> Self {
        Message::with_body(body)
    }
}

// Allow conversion from tuple of body and headers
impl From<(Vec<u8>, HashMap<String, String>)> for Message {
    fn from((body, headers): (Vec<u8>, HashMap<String, String>)) -> Self {
        Message::new(body, headers)
    }
}
