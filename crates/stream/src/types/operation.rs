//! Stream operation types

use proven_common::{Operation, OperationType, ProcessorType};
use proven_value::Value;
use serde::{Deserialize, Serialize};

/// Stream operations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamOperation {
    /// Append one or more values to the stream
    /// Positions are assigned at commit time, not during append
    Append { values: Vec<Value> },

    /// Read from a specific position with a limit
    /// Returns (position, value) tuples
    /// Stops at first missing position (gap-free guarantee)
    ReadFrom { position: u64, limit: usize },

    /// Get the latest committed position in the stream
    /// Returns 0 if stream is empty
    GetLatestPosition,
}

impl Operation for StreamOperation {
    fn operation_type(&self) -> OperationType {
        match self {
            StreamOperation::Append { .. } => OperationType::Write,
            StreamOperation::ReadFrom { .. } | StreamOperation::GetLatestPosition => {
                OperationType::Read
            }
        }
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Stream
    }
}
