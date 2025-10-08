//! Queue response types

use crate::types::QueueValue;
use proven_common::Response;
use serde::{Deserialize, Serialize};

/// Response types for queue operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueueResponse {
    /// Successfully enqueued
    Enqueued,

    /// Successfully dequeued a value
    Dequeued(Option<QueueValue>),

    /// Peeked at a value
    Peeked(Option<QueueValue>),

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
