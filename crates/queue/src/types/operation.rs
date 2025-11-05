//! Queue operation types
//!
//! This module defines the queue-specific operations that can be sent in messages.

use proven_common::{Operation, OperationType, ProcessorType};
use proven_value::Value;
use serde::{Deserialize, Serialize};

/// Queue operation types that can be sent in messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueueOperation {
    /// Enqueue a value to the back of the queue
    Enqueue { value: Value },

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

// Helper to compare Values treating floats specially
fn values_equal(v1: &Value, v2: &Value) -> bool {
    match (v1, v2) {
        // Strings
        (Value::Str(s1), Value::Str(s2)) => s1 == s2,
        // Integers (check all integer types)
        (Value::I64(i1), Value::I64(i2)) => i1 == i2,
        (Value::I32(i1), Value::I32(i2)) => i1 == i2,
        (Value::I16(i1), Value::I16(i2)) => i1 == i2,
        (Value::I8(i1), Value::I8(i2)) => i1 == i2,
        (Value::U64(i1), Value::U64(i2)) => i1 == i2,
        (Value::U32(i1), Value::U32(i2)) => i1 == i2,
        (Value::U16(i1), Value::U16(i2)) => i1 == i2,
        (Value::U8(i1), Value::U8(i2)) => i1 == i2,
        // Floats (bitwise comparison)
        (Value::F64(f1), Value::F64(f2)) => f1.to_bits() == f2.to_bits(),
        (Value::F32(f1), Value::F32(f2)) => f1.to_bits() == f2.to_bits(),
        // Booleans
        (Value::Bool(b1), Value::Bool(b2)) => b1 == b2,
        // Bytes
        (Value::Bytea(b1), Value::Bytea(b2)) => b1 == b2,
        // JSON
        (Value::Json(j1), Value::Json(j2)) => j1 == j2,
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

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Queue
    }
}
