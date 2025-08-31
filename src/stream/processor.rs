//! Core stream processor implementation
//!
//! Consumes ordered messages from a Raft-ordered stream and processes SQL operations
//! with proper transaction isolation using MVCC for rollback support and PCC for
//! conflict prevention.

use super::message::{SqlOperation, StreamMessage};
use super::response::{ResponseChannel, SqlResponse, convert_execution_result};
use super::transaction::{TransactionContext, TransactionState};
use crate::error::{Error, Result};
use crate::execution::{ExecutionResult, Executor};
use crate::planner::planner::Planner;
use crate::storage::lock::LockManager;
use crate::storage::mvcc::MvccStorage;
use crate::types::schema::Table;
use std::collections::HashMap;

/// SQL stream processor with transaction isolation
pub struct SqlStreamProcessor {
    /// MVCC storage (owned directly)
    pub storage: MvccStorage,

    /// Lock manager for PCC (owned directly)
    pub lock_manager: LockManager,

    /// Active transaction contexts
    active_transactions: HashMap<String, TransactionContext>,

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
        Self {
            storage: MvccStorage::new(),
            lock_manager: LockManager::new(),
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
            .txn_id()
            .ok_or_else(|| Error::InvalidValue("Missing txn_id header".into()))?;

        // Check if this is a commit/abort message (empty body)
        if message.is_transaction_control() {
            match message.txn_phase() {
                Some("commit") => return self.commit_transaction(txn_id).await,
                Some("abort") => return self.abort_transaction(txn_id).await,
                Some(phase) => {
                    return Err(Error::InvalidValue(format!("Unknown txn_phase: {}", phase)));
                }
                None => {
                    return Err(Error::InvalidValue(
                        "Empty message without txn_phase".into(),
                    ));
                }
            }
        }

        // Deserialize the operation
        let operation: SqlOperation =
            bincode::decode_from_slice(&message.body, bincode::config::standard())
                .map_err(|e| {
                    Error::InvalidValue(format!("Failed to deserialize operation: {}", e))
                })?
                .0;

        // Ensure transaction exists
        if !self.active_transactions.contains_key(txn_id) {
            self.create_transaction(txn_id)?;
        }

        // Execute the operation
        let result = self.execute_operation(operation, txn_id).await;

        // Send response if coordinator_id is present
        if let Some(coordinator_id) = message.coordinator_id() {
            let response = match result {
                Ok(res) => res,
                Err(e) => SqlResponse::Error(format!("{:?}", e)),
            };
            self.response_channel.send(coordinator_id, txn_id, response);
        }

        // Auto-commit if specified
        if message.is_auto_commit() {
            self.commit_transaction(txn_id).await?;
        }

        Ok(())
    }

    /// Create new transaction context
    fn create_transaction(&mut self, txn_id: &str) -> Result<()> {
        let tx_ctx = TransactionContext::from_txn_id(txn_id)?;
        self.active_transactions.insert(txn_id.to_string(), tx_ctx);
        Ok(())
    }

    /// Execute a SQL operation
    async fn execute_operation(
        &mut self,
        operation: SqlOperation,
        txn_id: &str,
    ) -> Result<SqlResponse> {
        match operation {
            SqlOperation::Execute { sql } => {
                let result = self.execute_sql(&sql, txn_id)?;
                Ok(convert_execution_result(result))
            }

            SqlOperation::Query { sql } => {
                let result = self.execute_sql(&sql, txn_id)?;
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

                let result = self.execute_sql(&sql, txn_id)?;
                self.migration_version = version;
                Ok(convert_execution_result(result))
            }
        }
    }

    /// Execute SQL statement
    fn execute_sql(&mut self, sql: &str, txn_id: &str) -> Result<ExecutionResult> {
        // Parse SQL
        let statement = crate::parser::parse_sql(sql)?;

        // Plan the query
        let plan = self.planner.plan(statement.clone())?;

        // Check if this is DDL that affects schemas
        match &plan {
            crate::planner::plan::Plan::CreateTable { name, schema } => {
                // Update our schema tracking
                self.schemas.insert(name.clone(), schema.clone());

                // Create table in storage
                self.storage.create_table(name.clone(), schema.clone())?;

                // Update planner and executor with new schemas
                self.planner = Planner::new(self.schemas.clone());
                self.executor.update_schemas(self.schemas.clone());

                return Ok(ExecutionResult::DDL(format!("Table '{}' created", name)));
            }

            crate::planner::plan::Plan::DropTable { name, .. } => {
                self.schemas.remove(name);
                self.storage.drop_table(name)?;

                // Update planner and executor
                self.planner = Planner::new(self.schemas.clone());
                self.executor.update_schemas(self.schemas.clone());

                return Ok(ExecutionResult::DDL(format!("Table '{}' dropped", name)));
            }

            _ => {}
        }

        // Execute with direct references to storage and lock manager
        let tx_ctx = self
            .active_transactions
            .get_mut(txn_id)
            .ok_or_else(|| Error::InvalidValue(format!("Transaction {} not found", txn_id)))?;
        let ctx = tx_ctx.context.clone();
        self.executor.execute(
            plan,
            &mut self.storage,
            &mut self.lock_manager,
            tx_ctx,
            &ctx,
        )
    }

    /// Commit a transaction
    async fn commit_transaction(&mut self, txn_id: &str) -> Result<()> {
        if let Some(mut tx_ctx) = self.active_transactions.remove(txn_id) {
            // Commit in storage
            self.storage.commit_transaction(tx_ctx.id)?;

            // Release all locks
            for lock in tx_ctx.locks_held {
                self.lock_manager.release(tx_ctx.id, lock)?;
            }

            tx_ctx.state = TransactionState::Committed;
        }
        // If transaction doesn't exist, it might have been auto-committed
        Ok(())
    }

    /// Abort a transaction
    async fn abort_transaction(&mut self, txn_id: &str) -> Result<()> {
        if let Some(mut tx_ctx) = self.active_transactions.remove(txn_id) {
            // Abort in storage
            self.storage.abort_transaction(tx_ctx.id)?;

            // Release all locks
            for lock in tx_ctx.locks_held {
                self.lock_manager.release(tx_ctx.id, lock)?;
            }

            tx_ctx.state = TransactionState::Aborted;
        }
        // If transaction doesn't exist, ignore
        Ok(())
    }

    /// Run garbage collection
    pub fn garbage_collect(&mut self) -> usize {
        self.storage.garbage_collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::response::MockResponseChannel;
    use std::sync::Arc;

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

        StreamMessage::new(body, headers)
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
                crate::types::value::Value::Integer(1),
                "ID should be 1"
            );
            assert_eq!(
                rows[0][1],
                crate::types::value::Value::String("Alice".to_string()),
                "Name should be 'Alice'"
            );
        } else {
            panic!("Could not find query result from coordinator_3");
        }
    }
}
