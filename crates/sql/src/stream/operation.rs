//! SQL operation types
//!
//! This module defines the SQL-specific operations that can be sent in messages.
//! The actual message structure is provided by the engine crate.

use crate::types::value::Value;
use serde::{Deserialize, Serialize};

/// SQL operation types that can be sent in messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlOperation {
    /// Execute SQL statement (DDL or DML)
    Execute {
        sql: String,
        /// Optional parameter values for prepared statements (? placeholders)
        params: Option<Vec<Value>>,
    },

    /// Query that returns results
    Query {
        sql: String,
        /// Optional parameter values for prepared statements (? placeholders)
        params: Option<Vec<Value>>,
    },

    /// Schema migration with version tracking
    Migrate { version: u32, sql: String },
}
