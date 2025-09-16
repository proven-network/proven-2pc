//! Response handling for stream processing
//!
//! This module defines response types and channels for sending
//! results back to coordinators.

use crate::execution::ExecutionResult;
use crate::types::value::Value;
use serde::{Deserialize, Serialize};

/// Response sent back to coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlResponse {
    /// Query results
    QueryResult {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },

    /// Execution result
    ExecuteResult {
        result_type: String,
        rows_affected: Option<usize>,
        message: Option<String>,
    },

    /// Error occurred
    Error(String),
}

/// Convert ExecutionResult to SqlResponse
pub fn convert_execution_result(result: ExecutionResult) -> SqlResponse {
    match result {
        ExecutionResult::Select { columns, rows } => {
            let rows = rows.into_iter().map(|row| row.as_ref().clone()).collect();
            SqlResponse::QueryResult { columns, rows }
        }
        ExecutionResult::Modified(count) => SqlResponse::ExecuteResult {
            result_type: "modified".to_string(),
            rows_affected: Some(count),
            message: None,
        },
        ExecutionResult::Ddl(msg) => SqlResponse::ExecuteResult {
            result_type: "ddl".to_string(),
            rows_affected: None,
            message: Some(msg),
        },
    }
}
