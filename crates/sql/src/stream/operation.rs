//! SQL operation types
//!
//! This module defines the SQL-specific operations that can be sent in messages.
//! The actual message structure is provided by the engine crate.

use serde::{Deserialize, Serialize};

/// SQL operation types that can be sent in messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlOperation {
    /// Execute SQL statement (DDL or DML)
    Execute { sql: String },

    /// Query that returns results
    Query { sql: String },

    /// Schema migration with version tracking
    Migrate { version: u32, sql: String },
}
