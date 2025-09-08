//! Queue value types

use serde::{Deserialize, Serialize};

/// Values that can be stored in a queue
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueueValue {
    /// String value
    String(String),
    /// Integer value
    Integer(i64),
    /// Float value
    Float(f64),
    /// Boolean value
    Boolean(bool),
    /// Binary data
    Bytes(Vec<u8>),
    /// JSON value
    Json(serde_json::Value),
}

impl QueueValue {
    /// Get the type name of this value
    pub fn type_name(&self) -> &'static str {
        match self {
            QueueValue::String(_) => "string",
            QueueValue::Integer(_) => "integer",
            QueueValue::Float(_) => "float",
            QueueValue::Boolean(_) => "boolean",
            QueueValue::Bytes(_) => "bytes",
            QueueValue::Json(_) => "json",
        }
    }

    /// Get the size of this value in bytes (approximate)
    pub fn size_bytes(&self) -> usize {
        match self {
            QueueValue::String(s) => s.len(),
            QueueValue::Integer(_) => 8,
            QueueValue::Float(_) => 8,
            QueueValue::Boolean(_) => 1,
            QueueValue::Bytes(b) => b.len(),
            QueueValue::Json(j) => j.to_string().len(),
        }
    }
}
