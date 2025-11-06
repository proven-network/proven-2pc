//! Queue operation types

use proven_common::{Operation, OperationType, ProcessorType};
use proven_value::Value;
use serde::{Deserialize, Serialize};

/// Queue operations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueOperation {
    /// Add a value to the tail of the queue
    Enqueue { value: Value },
    /// Remove and return the value at the head of the queue
    Dequeue,
    /// View the value at the head without removing it
    Peek,
    /// Get the current size of the queue
    Size,
    /// Check if the queue is empty
    IsEmpty,
    /// Remove all items from the queue
    Clear,
}

impl Operation for QueueOperation {
    fn operation_type(&self) -> OperationType {
        match self {
            QueueOperation::Peek | QueueOperation::Size | QueueOperation::IsEmpty => {
                OperationType::Read
            }
            _ => OperationType::Write,
        }
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Queue
    }
}
