//! Queue response types

use proven_common::Response;
use proven_value::Value;
use serde::{Deserialize, Serialize};

/// Queue operation responses
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueueResponse {
    /// Enqueue completed
    Enqueued,
    /// Dequeue result (None if queue was empty)
    Dequeued(Option<Value>),
    /// Peek result (None if queue is empty)
    Peeked(Option<Value>),
    /// Current queue size
    Size(usize),
    /// Whether the queue is empty
    IsEmpty(bool),
    /// Clear completed
    Cleared,
    /// Error
    Error(String),
}

impl Response for QueueResponse {}
