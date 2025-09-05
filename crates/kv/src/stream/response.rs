//! Response handling for KV stream processing
//!
//! This module defines response types for KV operations.
//! Protocol-level responses (Prepared, Wounded, etc.) are handled
//! by the generic stream processor via message headers.

use crate::types::Value;
use serde::{Deserialize, Serialize};

/// Response sent back to coordinator for KV operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvResponse {
    /// Successful get operation
    GetResult { key: String, value: Option<Value> },

    /// Successful put operation
    PutResult {
        key: String,
        previous: Option<Value>,
    },

    /// Successful delete operation
    DeleteResult { key: String, deleted: bool },

    /// Error occurred during operation
    Error(String),
}
