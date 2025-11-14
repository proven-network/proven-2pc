//! Stream response types

use proven_common::Response;
use proven_value::Value;
use serde::{Deserialize, Serialize};

/// Stream operation responses
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StreamResponse {
    /// Append completed
    /// Note: Positions are not known until commit
    Appended,

    /// Read result with (position, value) tuples
    Read { entries: Vec<(u64, Value)> },

    /// Latest committed position (0 if empty)
    Position(u64),

    /// Error
    Error(String),
}

impl Response for StreamResponse {}
