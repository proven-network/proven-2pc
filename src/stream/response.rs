//! Response handling for stream processing
//!
//! This module defines response types and channels for sending
//! results back to coordinators.

use crate::execution::ExecutionResult;
use crate::hlc::HlcTimestamp;
use crate::types::value::Value;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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

    /// Transaction was wounded by an older transaction
    Wounded {
        /// Transaction that wounded us
        wounded_by: HlcTimestamp,
        /// Reason for wounding
        reason: String,
    },

    /// Operation is deferred waiting for locks
    Deferred {
        /// Transaction we're waiting for
        waiting_for: HlcTimestamp,
        /// Lock that we need
        lock_key: String,
    },
}

/// Channel for sending responses back to coordinators
pub trait ResponseChannel: Send + Sync {
    fn send(&self, coordinator_id: &str, txn_id: &str, response: SqlResponse);

    fn as_any(&self) -> &dyn std::any::Any {
        panic!("as_any not implemented for this ResponseChannel")
    }
}

/// Mock implementation for testing
pub struct MockResponseChannel {
    responses: Arc<parking_lot::Mutex<Vec<(String, String, SqlResponse)>>>,
}

impl MockResponseChannel {
    pub fn new() -> Self {
        Self {
            responses: Arc::new(parking_lot::Mutex::new(Vec::new())),
        }
    }

    pub fn get_responses(&self) -> Vec<(String, String, SqlResponse)> {
        self.responses.lock().clone()
    }
}

impl ResponseChannel for Arc<MockResponseChannel> {
    fn send(&self, coordinator_id: &str, txn_id: &str, response: SqlResponse) {
        self.responses
            .lock()
            .push((coordinator_id.to_string(), txn_id.to_string(), response));
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
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
        ExecutionResult::DDL(msg) => SqlResponse::ExecuteResult {
            result_type: "ddl".to_string(),
            rows_affected: None,
            message: Some(msg),
        },
    }
}
