//! Message types for the mock engine
//!
//! This module defines the message format used by the mock engine,
//! matching the production engine's message structure.

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

    /// Timestamp (for ordering)
    pub timestamp: Option<u64>,
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

    /// Set sequence number
    pub fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = Some(sequence);
        self
    }

    /// Set timestamp
    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Get header value
    pub fn get_header(&self, key: &str) -> Option<&str> {
        self.headers.get(key).map(|s| s.as_str())
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
