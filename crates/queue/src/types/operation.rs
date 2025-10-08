//! Queue operation types
//!
//! This module defines the queue-specific operations that can be sent in messages.

use crate::types::QueueValue;
use proven_common::{Operation, OperationType};
use serde::{Deserialize, Serialize};

/// Queue operation types that can be sent in messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueueOperation {
    /// Enqueue a value to the back of the queue
    Enqueue { value: QueueValue },

    /// Dequeue a value from the front of the queue
    Dequeue,

    /// Peek at the front value without removing it
    Peek,

    /// Get the size of the queue
    Size,

    /// Check if the queue is empty
    IsEmpty,

    /// Clear all values from the queue
    Clear,
}

impl PartialEq for QueueOperation {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (QueueOperation::Enqueue { value: v1 }, QueueOperation::Enqueue { value: v2 }) => {
                values_equal(v1, v2)
            }
            (QueueOperation::Dequeue, QueueOperation::Dequeue) => true,
            (QueueOperation::Peek, QueueOperation::Peek) => true,
            (QueueOperation::Size, QueueOperation::Size) => true,
            (QueueOperation::IsEmpty, QueueOperation::IsEmpty) => true,
            (QueueOperation::Clear, QueueOperation::Clear) => true,
            _ => false,
        }
    }
}

impl Eq for QueueOperation {}

// Helper to compare QueueValues treating f64 specially
fn values_equal(v1: &QueueValue, v2: &QueueValue) -> bool {
    match (v1, v2) {
        (QueueValue::String(s1), QueueValue::String(s2)) => s1 == s2,
        (QueueValue::Integer(i1), QueueValue::Integer(i2)) => i1 == i2,
        (QueueValue::Float(f1), QueueValue::Float(f2)) => f1.to_bits() == f2.to_bits(),
        (QueueValue::Boolean(b1), QueueValue::Boolean(b2)) => b1 == b2,
        (QueueValue::Bytes(b1), QueueValue::Bytes(b2)) => b1 == b2,
        (QueueValue::Json(j1), QueueValue::Json(j2)) => j1 == j2,
        _ => false,
    }
}

impl Operation for QueueOperation {
    fn operation_type(&self) -> OperationType {
        match self {
            QueueOperation::Enqueue { .. } => OperationType::Write,
            QueueOperation::Dequeue => OperationType::Write,
            QueueOperation::Peek => OperationType::Read,
            QueueOperation::Size => OperationType::Read,
            QueueOperation::IsEmpty => OperationType::Read,
            QueueOperation::Clear => OperationType::Write,
        }
    }
}
