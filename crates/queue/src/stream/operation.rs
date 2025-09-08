//! Queue operation types
//!
//! This module defines the queue-specific operations that can be sent in messages.

use crate::types::QueueValue;
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
