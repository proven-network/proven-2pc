//! SQL operation types
//!
//! This module defines the SQL-specific operations that can be sent in messages.
//! The actual message structure is provided by the engine crate.

use crate::types::Value;
use proven_common::{Operation, OperationType, ProcessorType};
use serde::{Deserialize, Serialize};

/// SQL operation types that can be sent in messages
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl Operation for SqlOperation {
    fn operation_type(&self) -> OperationType {
        match self {
            SqlOperation::Query { .. } => OperationType::Read,
            SqlOperation::Execute { sql, .. } | SqlOperation::Migrate { sql, .. } => {
                // Simple heuristic: SELECT queries are reads, everything else is writes
                let trimmed = sql.trim().to_uppercase();
                if trimmed.starts_with("SELECT") || trimmed.starts_with("WITH") {
                    OperationType::Read
                } else {
                    OperationType::Write
                }
            }
        }
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Sql
    }
}
