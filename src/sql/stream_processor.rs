//! Stream-based SQL engine processor
//!
//! Consumes ordered messages from a Raft-ordered stream and processes SQL operations
//! with proper transaction isolation using MVCC for rollback support and PCC for
//! conflict prevention.

use crate::error::{Error, Result};
use crate::hlc::HlcTimestamp;
use crate::lock::LockManager;
use crate::sql::execution::{ExecutionResult, Executor};
use crate::sql::planner::planner::Planner;
use crate::sql::types::schema::Table;
use crate::storage::mvcc::MvccStorage;
use crate::transaction::{
    MvccTransaction as Transaction, MvccTransactionManager as TransactionManager,
};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// SQL operation types that can be sent in messages
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum SqlOperation {
    /// Execute SQL statement (DDL or DML)
    Execute { sql: String },

    /// Query that returns results
    Query { sql: String },

    /// Schema migration with version tracking
    Migrate { version: u32, sql: String },
}

/// Response sent back to coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlResponse {
    /// Query results
    QueryResult {
        columns: Vec<String>,
        rows: Vec<Vec<crate::sql::types::value::Value>>,
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

/// Message from the stream
pub struct StreamMessage {
    /// Message body (serialized SqlOperation or empty for commit/abort)
    pub body: Vec<u8>,

    /// Headers for transaction control
    pub headers: HashMap<String, String>,
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

/// SQL stream processor with transaction isolation
pub struct SqlStreamProcessor {
    /// MVCC storage (rebuilt from stream)
    storage: Arc<MvccStorage>,

    /// Lock manager for PCC
    lock_manager: Arc<LockManager>,

    /// Transaction manager
    tx_manager: TransactionManager,

    /// Active transactions (not yet committed/aborted)
    active_transactions: HashMap<String, Arc<Transaction>>,

    /// SQL planner
    planner: Planner,
    /// SQL executor
    executor: Executor,

    /// Response channel for sending results back
    response_channel: Box<dyn ResponseChannel>,

    /// Current migration version
    migration_version: u32,

    /// Table schemas (rebuilt from stream)
    schemas: HashMap<String, Table>,
}

impl SqlStreamProcessor {
    /// Create a new processor with empty storage
    pub fn new(response_channel: Box<dyn ResponseChannel>) -> Self {
        let storage = Arc::new(MvccStorage::new());
        let lock_manager = Arc::new(LockManager::new());
        let tx_manager = TransactionManager::new(lock_manager.clone(), storage.clone());

        Self {
            storage,
            lock_manager,
            tx_manager,
            active_transactions: HashMap::new(),
            planner: Planner::new(HashMap::new()),
            executor: Executor::new(HashMap::new()),
            response_channel,
            migration_version: 0,
            schemas: HashMap::new(),
        }
    }

    /// Process a message from the stream
    pub async fn process_message(&mut self, message: StreamMessage) -> Result<()> {
        let txn_id = message
            .headers
            .get("txn_id")
            .ok_or_else(|| Error::InvalidValue("Missing txn_id header".into()))?
            .clone();

        // Check if this is a commit/abort message (empty body)
        if message.body.is_empty() {
            if let Some(phase) = message.headers.get("txn_phase") {
                match phase.as_str() {
                    "commit" => return self.commit_transaction(&txn_id).await,
                    "abort" => return self.abort_transaction(&txn_id).await,
                    _ => return Err(Error::InvalidValue(format!("Unknown txn_phase: {}", phase))),
                }
            }
            return Err(Error::InvalidValue(
                "Empty message without txn_phase".into(),
            ));
        }

        // Deserialize the operation
        let operation: SqlOperation =
            bincode::decode_from_slice(&message.body, bincode::config::standard())
                .map_err(|e| {
                    Error::InvalidValue(format!("Failed to deserialize operation: {}", e))
                })?
                .0;

        // Get or create transaction
        let tx = self.get_or_create_transaction(&txn_id)?;

        // Execute the operation
        let result = self.execute_operation(operation, &tx).await;

        // Send response if coordinator_id is present
        if let Some(coordinator_id) = message.headers.get("coordinator_id") {
            let response = match result {
                Ok(res) => res,
                Err(e) => SqlResponse::Error(format!("{:?}", e)),
            };
            self.response_channel
                .send(coordinator_id, &txn_id, response);
        }

        // Auto-commit if specified
        if message.headers.get("auto_commit") == Some(&"true".to_string()) {
            self.commit_transaction(&txn_id).await?;
        }

        Ok(())
    }

    /// Get existing transaction or create new one
    fn get_or_create_transaction(&mut self, txn_id: &str) -> Result<Arc<Transaction>> {
        if let Some(tx) = self.active_transactions.get(txn_id) {
            return Ok(tx.clone());
        }

        // Parse the transaction ID to extract timestamp
        let parts: Vec<&str> = txn_id.split('_').collect();
        if parts.len() < 3 {
            return Err(Error::InvalidValue(format!(
                "Invalid txn_id format: {}",
                txn_id
            )));
        }

        let timestamp_str = parts[2];
        let timestamp_nanos: u64 = timestamp_str
            .parse()
            .map_err(|_| Error::InvalidValue(format!("Invalid timestamp in txn_id: {}", txn_id)))?;

        // Create HLC timestamp
        let hlc_timestamp = HlcTimestamp::new(
            timestamp_nanos / 1_000_000_000,
            (timestamp_nanos % 1_000_000_000) as u32,
            crate::hlc::NodeId::new(1),
        );

        // Create MVCC transaction
        let tx = self.tx_manager.begin(hlc_timestamp);
        self.active_transactions
            .insert(txn_id.to_string(), tx.clone());

        Ok(tx)
    }

    /// Execute a SQL operation
    async fn execute_operation(
        &mut self,
        operation: SqlOperation,
        tx: &Arc<Transaction>,
    ) -> Result<SqlResponse> {
        match operation {
            SqlOperation::Execute { sql } => {
                let result = self.execute_sql(&sql, tx)?;
                Ok(convert_execution_result(result))
            }

            SqlOperation::Query { sql } => {
                let result = self.execute_sql(&sql, tx)?;
                match result {
                    ExecutionResult::Select { columns, rows } => {
                        let rows = rows.into_iter().map(|row| row.as_ref().clone()).collect();
                        Ok(SqlResponse::QueryResult { columns, rows })
                    }
                    other => Ok(convert_execution_result(other)),
                }
            }

            SqlOperation::Migrate { version, sql } => {
                if version <= self.migration_version {
                    return Ok(SqlResponse::ExecuteResult {
                        result_type: "migration_skip".to_string(),
                        rows_affected: None,
                        message: Some(format!("Migration {} already applied", version)),
                    });
                }

                let result = self.execute_sql(&sql, tx)?;
                self.migration_version = version;
                Ok(convert_execution_result(result))
            }
        }
    }

    /// Execute SQL statement
    fn execute_sql(&mut self, sql: &str, tx: &Arc<Transaction>) -> Result<ExecutionResult> {
        // Parse SQL
        let statement = crate::sql::parse_sql(sql)?;

        // Plan the query
        let plan = self.planner.plan(statement.clone())?;

        // Check if this is DDL that affects schemas
        match &plan {
            crate::sql::planner::plan::Plan::CreateTable { name, schema } => {
                // Update our schema tracking
                self.schemas.insert(name.clone(), schema.clone());

                // Create table in storage
                self.storage.create_table(name.clone(), schema.clone())?;

                // Update planner and executor with new schemas
                self.planner = Planner::new(self.schemas.clone());
                self.executor.update_schemas(self.schemas.clone());

                return Ok(ExecutionResult::DDL(format!("Table '{}' created", name)));
            }

            crate::sql::planner::plan::Plan::DropTable { name, .. } => {
                self.schemas.remove(name);
                self.storage.drop_table(name)?;

                // Update planner and executor
                self.planner = Planner::new(self.schemas.clone());
                self.executor.update_schemas(self.schemas.clone());

                return Ok(ExecutionResult::DDL(format!("Table '{}' dropped", name)));
            }

            _ => {}
        }

        // Execute with MVCC-aware executor
        self.executor.execute(plan, tx, &tx.context)
    }

    /// Commit a transaction
    async fn commit_transaction(&mut self, txn_id: &str) -> Result<()> {
        if let Some(tx) = self.active_transactions.remove(txn_id) {
            tx.commit()?;
        }
        // If transaction doesn't exist, it might have been auto-committed
        Ok(())
    }

    /// Abort a transaction
    async fn abort_transaction(&mut self, txn_id: &str) -> Result<()> {
        if let Some(tx) = self.active_transactions.remove(txn_id) {
            tx.abort()?;
        }
        // If transaction doesn't exist, ignore
        Ok(())
    }

    /// Run garbage collection
    pub fn garbage_collect(&self) -> usize {
        self.tx_manager.garbage_collect()
    }
}

/// Convert ExecutionResult to SqlResponse
fn convert_execution_result(result: ExecutionResult) -> SqlResponse {
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
        ExecutionResult::Transaction(msg) => SqlResponse::ExecuteResult {
            result_type: "transaction".to_string(),
            rows_affected: None,
            message: Some(msg),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_message(
        operation: Option<SqlOperation>,
        txn_id: &str,
        coordinator_id: Option<&str>,
        auto_commit: bool,
        txn_phase: Option<&str>,
    ) -> StreamMessage {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());

        if let Some(coord) = coordinator_id {
            headers.insert("coordinator_id".to_string(), coord.to_string());
        }

        if auto_commit {
            headers.insert("auto_commit".to_string(), "true".to_string());
        }

        if let Some(phase) = txn_phase {
            headers.insert("txn_phase".to_string(), phase.to_string());
        }

        let body = if let Some(op) = operation {
            bincode::encode_to_vec(&op, bincode::config::standard()).unwrap()
        } else {
            Vec::new()
        };

        StreamMessage { body, headers }
    }

    #[tokio::test]
    async fn test_transaction_isolation_with_stream() {
        let mock_channel = Arc::new(MockResponseChannel::new());
        let response_channel: Box<dyn ResponseChannel> = Box::new(mock_channel.clone());
        let mut processor = SqlStreamProcessor::new(response_channel);

        // Create table
        let create_table = SqlOperation::Execute {
            sql: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)".to_string(),
        };
        let msg = create_message(
            Some(create_table),
            "txn_runtime1_1000000000",
            Some("coordinator_1"),
            true,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Transaction 1: Insert but don't commit
        let insert = SqlOperation::Execute {
            sql: "INSERT INTO users (id, name) VALUES (1, 'Alice')".to_string(),
        };
        let msg = create_message(
            Some(insert),
            "txn_runtime1_2000000000",
            Some("coordinator_1"),
            false, // No auto-commit
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Transaction 2: Query (should not see uncommitted data)
        let query = SqlOperation::Query {
            sql: "SELECT * FROM users".to_string(),
        };
        let msg = create_message(
            Some(query),
            "txn_runtime1_3000000000",
            Some("coordinator_2"),
            true,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Check responses - Transaction 2 should see empty results
        let responses = mock_channel.get_responses();
        if let Some((_, _, SqlResponse::QueryResult { rows, .. })) = responses
            .iter()
            .find(|(coord, _, _)| coord == "coordinator_2")
        {
            assert_eq!(rows.len(), 0, "Should not see uncommitted data");
        }

        // Now commit Transaction 1
        let msg = create_message(None, "txn_runtime1_2000000000", None, false, Some("commit"));
        processor.process_message(msg).await.unwrap();

        // Transaction 3: Query (should see committed data)
        let query = SqlOperation::Query {
            sql: "SELECT * FROM users".to_string(),
        };
        let msg = create_message(
            Some(query),
            "txn_runtime1_4000000000",
            Some("coordinator_3"),
            true,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Check that Transaction 3 sees the committed data
        let responses = mock_channel.get_responses();
        if let Some((_, _, SqlResponse::QueryResult { rows, .. })) = responses
            .iter()
            .rev()
            .find(|(coord, _, _)| coord == "coordinator_3")
        {
            assert_eq!(rows.len(), 1, "Should see committed data");
        }
    }

    #[tokio::test]
    async fn test_abort_rollback_with_stream() {
        let mock_channel = Arc::new(MockResponseChannel::new());
        let response_channel: Box<dyn ResponseChannel> = Box::new(mock_channel.clone());
        let mut processor = SqlStreamProcessor::new(response_channel);

        // Create table and insert initial data
        let create_table = SqlOperation::Execute {
            sql: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)".to_string(),
        };
        let msg = create_message(
            Some(create_table),
            "txn_runtime1_1000000000",
            Some("coordinator_1"),
            true,
            None,
        );
        processor.process_message(msg).await.unwrap();

        let insert = SqlOperation::Execute {
            sql: "INSERT INTO users (id, name) VALUES (1, 'Alice')".to_string(),
        };
        let msg = create_message(
            Some(insert),
            "txn_runtime1_2000000000",
            Some("coordinator_1"),
            true,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Transaction that will be aborted: Update the name
        let update = SqlOperation::Execute {
            sql: "UPDATE users SET name = 'Bob' WHERE id = 1".to_string(),
        };
        let msg = create_message(
            Some(update),
            "txn_runtime1_3000000000",
            Some("coordinator_2"),
            false,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Abort the transaction
        let msg = create_message(None, "txn_runtime1_3000000000", None, false, Some("abort"));
        processor.process_message(msg).await.unwrap();

        // Query should still see original value
        let query = SqlOperation::Query {
            sql: "SELECT * FROM users WHERE id = 1".to_string(),
        };
        let msg = create_message(
            Some(query),
            "txn_runtime1_4000000000",
            Some("coordinator_3"),
            true,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Verify the name is still 'Alice'
        let responses = mock_channel.get_responses();
        if let Some((_, _, SqlResponse::QueryResult { rows, columns })) = responses
            .iter()
            .rev()
            .find(|(coord, _, _)| coord == "coordinator_3")
        {
            assert_eq!(rows.len(), 1, "Expected 1 row");
            assert_eq!(
                columns.len(),
                2,
                "Expected 2 columns (id, name), got: {:?}",
                columns
            );
            println!("Row data: {:?}", rows[0]);
            assert_eq!(
                rows[0].len(),
                2,
                "Expected 2 values in row, got: {:?}",
                rows[0]
            );
            // Check id is 1 and name is 'Alice'
            assert_eq!(
                rows[0][0],
                crate::sql::types::value::Value::Integer(1),
                "ID should be 1"
            );
            assert_eq!(
                rows[0][1],
                crate::sql::types::value::Value::String("Alice".to_string()),
                "Name should be 'Alice'"
            );
        } else {
            panic!("Could not find query result from coordinator_3");
        }
    }
}
