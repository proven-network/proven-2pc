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

// Helper to compare QueueValues treating floats specially
fn values_equal(v1: &QueueValue, v2: &QueueValue) -> bool {
    match (v1, v2) {
        // Strings
        (QueueValue::Str(s1), QueueValue::Str(s2)) => s1 == s2,
        // Integers (check all integer types)
        (QueueValue::I64(i1), QueueValue::I64(i2)) => i1 == i2,
        (QueueValue::I32(i1), QueueValue::I32(i2)) => i1 == i2,
        (QueueValue::I16(i1), QueueValue::I16(i2)) => i1 == i2,
        (QueueValue::I8(i1), QueueValue::I8(i2)) => i1 == i2,
        (QueueValue::U64(i1), QueueValue::U64(i2)) => i1 == i2,
        (QueueValue::U32(i1), QueueValue::U32(i2)) => i1 == i2,
        (QueueValue::U16(i1), QueueValue::U16(i2)) => i1 == i2,
        (QueueValue::U8(i1), QueueValue::U8(i2)) => i1 == i2,
        // Floats (bitwise comparison)
        (QueueValue::F64(f1), QueueValue::F64(f2)) => f1.to_bits() == f2.to_bits(),
        (QueueValue::F32(f1), QueueValue::F32(f2)) => f1.to_bits() == f2.to_bits(),
        // Booleans
        (QueueValue::Bool(b1), QueueValue::Bool(b2)) => b1 == b2,
        // Bytes
        (QueueValue::Bytea(b1), QueueValue::Bytea(b2)) => b1 == b2,
        // JSON
        (QueueValue::Json(j1), QueueValue::Json(j2)) => j1 == j2,
        // Default: use structural equality
        _ => v1 == v2,
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
