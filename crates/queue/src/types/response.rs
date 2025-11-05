//! Queue response types

use proven_common::Response;
use proven_value::Value;
use serde::{Deserialize, Serialize};

/// Response types for queue operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueueResponse {
    /// Successfully enqueued
    Enqueued,

    /// Successfully dequeued a value
    Dequeued(Option<Value>),

    /// Peeked at a value
    Peeked(Option<Value>),

    /// Queue size
    Size(usize),

    /// Queue empty status
    IsEmpty(bool),

    /// Queue cleared
    Cleared,

    /// Operation failed
    Error(String),
}

impl Response for QueueResponse {}
