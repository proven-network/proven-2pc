//! Queue operation types
//!
//! This module defines the queue-specific operations that can be sent in messages.

use crate::types::QueueValue;
use proven_common::{Operation, OperationType};
use serde::{Deserialize, Serialize};

/// Queue operation types that can be sent in messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueueOperation {
    /// Enqueue a value to the back of a queue
    Enqueue {
        queue_name: String,
        value: QueueValue,
    },

    /// Dequeue a value from the front of a queue
    Dequeue { queue_name: String },

    /// Peek at the front value without removing it
    Peek { queue_name: String },

    /// Get the size of a queue
    Size { queue_name: String },

    /// Check if a queue is empty
    IsEmpty { queue_name: String },

    /// Clear all values from a queue
    Clear { queue_name: String },
}

impl PartialEq for QueueOperation {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                QueueOperation::Enqueue {
                    queue_name: q1,
                    value: v1,
                },
                QueueOperation::Enqueue {
                    queue_name: q2,
                    value: v2,
                },
            ) => q1 == q2 && values_equal(v1, v2),
            (
                QueueOperation::Dequeue { queue_name: q1 },
                QueueOperation::Dequeue { queue_name: q2 },
            ) => q1 == q2,
            (QueueOperation::Peek { queue_name: q1 }, QueueOperation::Peek { queue_name: q2 }) => {
                q1 == q2
            }
            (QueueOperation::Size { queue_name: q1 }, QueueOperation::Size { queue_name: q2 }) => {
                q1 == q2
            }
            (
                QueueOperation::IsEmpty { queue_name: q1 },
                QueueOperation::IsEmpty { queue_name: q2 },
            ) => q1 == q2,
            (
                QueueOperation::Clear { queue_name: q1 },
                QueueOperation::Clear { queue_name: q2 },
            ) => q1 == q2,
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
            QueueOperation::Dequeue { .. } => OperationType::Write,
            QueueOperation::Peek { .. } => OperationType::Read,
            QueueOperation::Size { .. } => OperationType::Read,
            QueueOperation::IsEmpty { .. } => OperationType::Read,
            QueueOperation::Clear { .. } => OperationType::Write,
        }
    }
}
