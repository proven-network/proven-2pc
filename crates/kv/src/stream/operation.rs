//! KV operation types
//!
//! This module defines the KV-specific operations that can be sent in messages.
//! The actual message structure is provided by the engine crate.

use crate::types::Value;
use serde::{Deserialize, Serialize};

/// KV operation types that can be sent in messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvOperation {
    /// Get a value by key
    Get { key: String },

    /// Put a key-value pair
    Put { key: String, value: Value },

    /// Delete a key
    Delete { key: String },
}
