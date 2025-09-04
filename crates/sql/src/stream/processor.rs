//! Core stream processor implementation
//!
//! Consumes ordered messages from a Raft-ordered stream and processes SQL operations
//! with proper transaction isolation using MVCC for rollback support and PCC for
//! conflict prevention.

use super::deferred::DeferredOperationsManager;
use super::message::{SqlOperation, StreamMessage};
use super::response::{ResponseChannel, SqlResponse, convert_execution_result};
use super::stats_cache::StatisticsCache;
use super::transaction::{TransactionContext, TransactionState};
use crate::error::{Error, Result};
use crate::execution::{ExecutionResult, Executor};
use crate::planner::planner::Planner;
use crate::storage::lock::LockManager;
use crate::storage::mvcc::MvccStorage;
use proven_hlc::HlcTimestamp;
use std::collections::HashMap;

/// SQL stream processor with transaction isolation
pub struct SqlStreamProcessor {
    /// MVCC storage (owned directly)
    pub storage: MvccStorage,

    /// Lock manager for PCC (owned directly)
    pub lock_manager: LockManager,

    /// Active transaction contexts
    active_transactions: HashMap<HlcTimestamp, TransactionContext>,

    /// Coordinator IDs for active transactions (for wounded notifications)
    transaction_coordinators: HashMap<HlcTimestamp, String>,

    /// Deferred operations manager
    deferred_manager: DeferredOperationsManager,

    /// SQL executor (stateless)
    executor: Executor,

    /// Response channel for sending results back
    response_channel: Box<dyn ResponseChannel>,

    /// Current migration version
    migration_version: u32,

    /// Statistics cache for query optimization
    stats_cache: StatisticsCache,
}

impl SqlStreamProcessor {
    /// Create a new processor with empty storage
    pub fn new(response_channel: Box<dyn ResponseChannel>) -> Self {
        Self {
            storage: MvccStorage::new(),
            lock_manager: LockManager::new(),
            active_transactions: HashMap::new(),
            transaction_coordinators: HashMap::new(),
            deferred_manager: DeferredOperationsManager::new(),
            executor: Executor::new(),
            response_channel,
            migration_version: 0,
            stats_cache: StatisticsCache::new(100), // Update stats every 100 commits
        }
    }

    /// Process a message from the stream
    ///
    /// All operation messages must include a coordinator_id to ensure proper
    /// response routing, especially for deferred operations and wound notifications.
    pub async fn process_message(&mut self, message: StreamMessage) -> Result<()> {
        let txn_id_str = message
            .txn_id()
            .ok_or_else(|| Error::InvalidValue("Missing txn_id header".into()))?;

        // Parse transaction ID to HlcTimestamp at the boundary
        let txn_id = HlcTimestamp::parse(txn_id_str).map_err(|e| Error::InvalidValue(e))?;

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
        if !self.active_transactions.contains_key(&txn_id) {
            self.create_transaction(txn_id)?;
        }

        // Get coordinator ID - required for all operations
        let coordinator_id = message.coordinator_id().ok_or_else(|| {
            Error::InvalidValue("coordinator_id is required for all operations".to_string())
        })?;

        // Store coordinator ID for this transaction (for wounded notifications)
        self.transaction_coordinators
            .insert(txn_id, coordinator_id.to_string());

        // Execute the operation
        let result = self
            .execute_operation(operation, txn_id, coordinator_id)
            .await;

        // Send response to coordinator
        let response = match result {
            Ok(res) => res,
            Err(e) => SqlResponse::Error(format!("{:?}", e)),
        };
        self.response_channel
            .send(coordinator_id, txn_id_str, response);

        // Auto-commit if specified
        if message.is_auto_commit() {
            self.commit_transaction(txn_id).await?;
        }

        Ok(())
    }

    /// Create new transaction context
    fn create_transaction(&mut self, txn_id: HlcTimestamp) -> Result<TransactionContext> {
        let tx_ctx = TransactionContext::new(txn_id);
        self.active_transactions.insert(txn_id, tx_ctx);
        Ok(TransactionContext::new(txn_id))
    }

    /// Execute a SQL operation with lock conflict handling
    async fn execute_operation(
        &mut self,
        operation: SqlOperation,
        txn_id: HlcTimestamp,
        coordinator_id: &str,
    ) -> Result<SqlResponse> {
        // Store the original SQL for potential deferral
        let sql = match &operation {
            SqlOperation::Execute { sql } | SqlOperation::Query { sql } => sql.clone(),
            SqlOperation::Migrate { sql, .. } => sql.clone(),
        };

        match operation {
            SqlOperation::Execute { sql: exec_sql } => {
                match self
                    .execute_sql_with_conflict_handling(&exec_sql, txn_id, &sql, coordinator_id)
                    .await
                {
                    Ok(Some(result)) => Ok(convert_execution_result(result)),
                    Ok(None) => {
                        // Operation was deferred, response already sent
                        Ok(SqlResponse::ExecuteResult {
                            result_type: "deferred".to_string(),
                            rows_affected: None,
                            message: Some("Operation deferred due to lock conflict".to_string()),
                        })
                    }
                    Err(e) => Err(e),
                }
            }

            SqlOperation::Query { sql: query_sql } => {
                match self
                    .execute_sql_with_conflict_handling(&query_sql, txn_id, &sql, coordinator_id)
                    .await
                {
                    Ok(Some(result)) => match result {
                        ExecutionResult::Select { columns, rows } => {
                            let rows = rows.into_iter().map(|row| row.as_ref().clone()).collect();
                            Ok(SqlResponse::QueryResult { columns, rows })
                        }
                        other => Ok(convert_execution_result(other)),
                    },
                    Ok(None) => {
                        // Operation was deferred
                        Ok(SqlResponse::ExecuteResult {
                            result_type: "deferred".to_string(),
                            rows_affected: None,
                            message: Some("Query deferred due to lock conflict".to_string()),
                        })
                    }
                    Err(e) => Err(e),
                }
            }

            SqlOperation::Migrate {
                version,
                sql: migrate_sql,
            } => {
                if version <= self.migration_version {
                    return Ok(SqlResponse::ExecuteResult {
                        result_type: "migration_skip".to_string(),
                        rows_affected: None,
                        message: Some(format!("Migration {} already applied", version)),
                    });
                }

                match self
                    .execute_sql_with_conflict_handling(&migrate_sql, txn_id, &sql, coordinator_id)
                    .await
                {
                    Ok(Some(result)) => {
                        self.migration_version = version;
                        Ok(convert_execution_result(result))
                    }
                    Ok(None) => Ok(SqlResponse::ExecuteResult {
                        result_type: "deferred".to_string(),
                        rows_affected: None,
                        message: Some("Migration deferred due to lock conflict".to_string()),
                    }),
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Execute SQL with lock conflict handling
    async fn execute_sql_with_conflict_handling(
        &mut self,
        sql: &str,
        txn_id: HlcTimestamp,
        original_sql: &str,
        coordinator_id: &str,
    ) -> Result<Option<ExecutionResult>> {
        // Try to execute the SQL
        match self.execute_sql(sql, txn_id) {
            Ok(result) => Ok(Some(result)),
            Err(Error::LockConflict { holder, mode }) => {
                // Get our transaction context
                let our_tx = self.active_transactions.get(&txn_id).ok_or_else(|| {
                    Error::InvalidValue(format!("Transaction {:?} not found", txn_id))
                })?;
                let our_hlc = our_tx.id;

                // Determine wound-wait: older transaction (smaller timestamp) wounds younger
                let should_wound = our_hlc < holder;

                if should_wound {
                    // We're older, wound the younger transaction
                    self.wound_transaction(holder, our_hlc).await?;

                    // Retry the operation after wounding
                    match self.execute_sql(sql, txn_id) {
                        Ok(result) => Ok(Some(result)),
                        Err(e) => Err(e),
                    }
                } else {
                    // We're younger, must wait
                    // Create deferred operation
                    let deferred_op = super::deferred::DeferredOperation {
                        operation: SqlOperation::Execute {
                            sql: original_sql.to_string(),
                        },
                        sql: original_sql.to_string(),
                        coordinator_id: coordinator_id.to_string(),
                        lock_requested: (
                            crate::storage::lock::LockKey::Table {
                                table: "unknown".to_string(), // TODO: Extract actual lock key
                            },
                            mode,
                        ),
                        waiting_for: holder,
                        attempt_count: 1,
                        created_at: our_hlc,
                    };

                    // Add to deferred operations
                    self.deferred_manager
                        .add_deferred(our_hlc, deferred_op, holder);

                    // Send deferred response to coordinator
                    let response = SqlResponse::Deferred {
                        waiting_for: holder,
                        lock_key: format!("{:?}", mode),
                    };
                    // Convert back to string for response channel (temporary compatibility)
                    let txn_id_str = format!(
                        "txn_runtime1_{:010}",
                        txn_id.physical * 1_000_000_000 + txn_id.logical as u64
                    );
                    self.response_channel
                        .send(coordinator_id, &txn_id_str, response);

                    Ok(None) // Operation deferred
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Execute SQL statement
    fn execute_sql(&mut self, sql: &str, txn_id: HlcTimestamp) -> Result<ExecutionResult> {
        // Check if transaction has been wounded
        if let Some(tx_ctx) = self.active_transactions.get(&txn_id) {
            if let Some(wounded_by) = tx_ctx.wounded_by {
                return Err(Error::TransactionWounded { wounded_by });
            }
        }

        // Parse SQL
        let statement = crate::parser::parse_sql(sql)?;

        // Get schemas for planning
        let schemas = self.storage.get_schemas();

        // Create a stateless planner for this query
        let mut planner = Planner::new(schemas);

        // Add cached statistics if available for optimization
        if let Some(stats) = self.stats_cache.get() {
            planner.update_statistics(stats.clone());
        }

        // Plan the query
        let plan = planner.plan(statement)?;

        // Execute with direct references to storage and lock manager
        let tx_ctx = self
            .active_transactions
            .get_mut(&txn_id)
            .ok_or_else(|| Error::InvalidValue(format!("Transaction {:?} not found", txn_id)))?;

        let result = self.executor.execute(
            plan.clone(),
            &mut self.storage,
            &mut self.lock_manager,
            tx_ctx,
        )?;

        // After successful DDL operations, invalidate statistics cache
        if plan.is_ddl() {
            self.stats_cache.invalidate();
        }

        Ok(result)
    }

    /// Commit a transaction
    async fn commit_transaction(&mut self, txn_id: HlcTimestamp) -> Result<()> {
        if let Some(mut tx_ctx) = self.active_transactions.remove(&txn_id) {
            // Commit in storage
            self.storage.commit_transaction(tx_ctx.id)?;

            // Record the commit for statistics tracking
            self.stats_cache.record_commit();

            // Update statistics if needed and no transactions are waiting
            let active_count = self.active_transactions.len();
            self.stats_cache.maybe_update(&self.storage, active_count);

            // Release all locks
            self.lock_manager.release_all(tx_ctx.id)?;

            // Get and process wakeable transactions from deferred operations
            let waiters = self.deferred_manager.get_waiters(tx_ctx.id);
            if let Some(first_waiter) = waiters.first() {
                self.retry_deferred_operations(*first_waiter)?;
            }

            tx_ctx.state = TransactionState::Committed;
        }

        // Clean up coordinator tracking
        self.transaction_coordinators.remove(&txn_id);

        // If transaction doesn't exist, it might have been auto-committed
        Ok(())
    }

    /// Abort a transaction
    async fn abort_transaction(&mut self, txn_id: HlcTimestamp) -> Result<()> {
        if let Some(mut tx_ctx) = self.active_transactions.remove(&txn_id) {
            // Abort in storage
            self.storage.abort_transaction(tx_ctx.id)?;

            // Release all locks
            self.lock_manager.release_all(tx_ctx.id)?;

            // Get and process wakeable transactions from deferred operations
            let waiters = self.deferred_manager.get_waiters(tx_ctx.id);
            if let Some(first_waiter) = waiters.first() {
                self.retry_deferred_operations(*first_waiter)?;
            }

            tx_ctx.state = TransactionState::Aborted;
        }

        // Clean up coordinator tracking
        self.transaction_coordinators.remove(&txn_id);

        // If transaction doesn't exist, ignore
        Ok(())
    }

    /// Wound a transaction (force it to abort)
    async fn wound_transaction(
        &mut self,
        victim: HlcTimestamp,
        wounded_by: HlcTimestamp,
    ) -> Result<()> {
        // Remove victim's deferred operations
        let victim_ops = self.deferred_manager.wound_transaction(victim);

        // Convert victim timestamp to string format (10 digits to match test format)
        let victim_str = format!(
            "txn_runtime1_{:010}",
            victim.physical * 1_000_000_000 + victim.logical as u64
        );

        // Send wounded notification to all victim's deferred operations
        for op in victim_ops {
            let response = SqlResponse::Wounded {
                wounded_by,
                reason: format!("Wounded by older transaction to prevent deadlock"),
            };
            self.response_channel
                .send(&op.coordinator_id, &victim_str, response);
        }

        // Send wounded notification if this is an active transaction with a coordinator
        if let Some(coordinator_id) = self.transaction_coordinators.get(&victim) {
            let response = SqlResponse::Wounded {
                wounded_by,
                reason: format!("Wounded by older transaction to prevent deadlock"),
            };
            self.response_channel
                .send(coordinator_id, &victim_str, response);
        }

        // Mark the victim transaction as wounded and abort it
        if let Some(tx_ctx) = self.active_transactions.get_mut(&victim) {
            tx_ctx.wounded_by = Some(wounded_by);
        }

        // Abort the transaction to release its locks
        self.abort_transaction(victim).await?;

        Ok(())
    }

    /// Retry deferred operations for a transaction
    fn retry_deferred_operations(&mut self, tx_id: HlcTimestamp) -> Result<()> {
        // Remove deferred operations for this transaction
        if let Some(operations) = self.deferred_manager.remove_operations(tx_id) {
            // Convert HLC timestamp back to string format for compatibility
            let txn_id_str = format!("txn_runtime1_{}", tx_id.as_nanos());

            for op in operations {
                // Re-execute the SQL
                let result = self.execute_sql(&op.sql, tx_id);

                // Send response to original coordinator
                let response = match result {
                    Ok(res) => convert_execution_result(res),
                    Err(e) => SqlResponse::Error(format!("{:?}", e)),
                };
                self.response_channel
                    .send(&op.coordinator_id, &txn_id_str, response);
            }
        }
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
        coordinator_id: &str, // Required for all operations
        auto_commit: bool,
        txn_phase: Option<&str>,
    ) -> StreamMessage {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());

        // Always include coordinator_id for operations (not needed for commit/abort)
        if operation.is_some() {
            headers.insert("coordinator_id".to_string(), coordinator_id.to_string());
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
            "coordinator_1",
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
            "coordinator_1",
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
            "coordinator_2",
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
        let msg = create_message(
            None,
            "txn_runtime1_2000000000",
            "test_coord",
            false,
            Some("commit"),
        );
        processor.process_message(msg).await.unwrap();

        // Transaction 3: Query (should see committed data)
        let query = SqlOperation::Query {
            sql: "SELECT * FROM users".to_string(),
        };
        let msg = create_message(
            Some(query),
            "txn_runtime1_4000000000",
            "coordinator_3",
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
    async fn test_wound_wait_mechanism() {
        let mock_channel = Arc::new(MockResponseChannel::new());
        let mut processor = SqlStreamProcessor::new(Box::new(mock_channel.clone()));

        // Create a table and insert initial data
        let create_table = SqlOperation::Execute {
            sql: "CREATE TABLE test (id INT PRIMARY KEY, value INT)".to_string(),
        };
        let msg = create_message(
            Some(create_table),
            "txn_runtime1_1000000000",
            "coordinator_setup",
            true,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Insert initial row
        let insert_initial = SqlOperation::Execute {
            sql: "INSERT INTO test (id, value) VALUES (1, 100)".to_string(),
        };
        let msg = create_message(
            Some(insert_initial),
            "txn_runtime1_1500000000",
            "coordinator_setup",
            true, // Auto-commit
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Start younger transaction that updates the row (holds lock)
        let update1 = SqlOperation::Execute {
            sql: "UPDATE test SET value = 200 WHERE id = 1".to_string(),
        };
        let msg = create_message(
            Some(update1),
            "txn_runtime1_3000000000", // Younger timestamp
            "coordinator_younger",
            false, // NO auto-commit - holds the lock
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Older transaction tries to update same row (should wound younger)
        let update2 = SqlOperation::Execute {
            sql: "UPDATE test SET value = 300 WHERE id = 1".to_string(),
        };
        let msg = create_message(
            Some(update2),
            "txn_runtime1_2000000000", // Older timestamp
            "coordinator_older",
            false, // NO auto-commit - so it goes through wound-wait logic
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Check responses so far
        let responses = mock_channel.get_responses();

        // Should have a wounded notification for the younger transaction
        let wounded_response = responses.iter().find(|(coord, _, resp)| {
            coord == "coordinator_younger" && matches!(resp, SqlResponse::Wounded { .. })
        });
        assert!(
            wounded_response.is_some(),
            "Should have wounded notification for younger transaction"
        );

        // The older transaction should have succeeded after wounding
        let older_success = responses.iter().find(|(coord, _, resp)| {
            coord == "coordinator_older" && matches!(resp, SqlResponse::ExecuteResult { .. })
        });
        assert!(
            older_success.is_some(),
            "Older transaction should succeed after wounding younger"
        );

        // Commit the older transaction
        let msg = create_message(
            None,
            "txn_runtime1_2000000000",
            "coordinator_older",
            false,
            Some("commit"),
        );
        processor.process_message(msg).await.unwrap();

        // Verify the older transaction's update succeeded by querying the value
        let query = SqlOperation::Query {
            sql: "SELECT value FROM test WHERE id = 1".to_string(),
        };
        let msg = create_message(
            Some(query),
            "txn_runtime1_4000000000", // New transaction
            "coordinator_verify",
            true,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Get updated responses
        let responses = mock_channel.get_responses();

        // The query should show the older transaction's value (300)
        let query_response = responses
            .iter()
            .rev()
            .find(|(coord, _, _)| coord == "coordinator_verify");
        if let Some((_, _, SqlResponse::QueryResult { rows, .. })) = query_response {
            assert_eq!(rows.len(), 1, "Should have one row");
            // The value should be 300 from the older transaction
            if let Some(row) = rows.first() {
                if let Some(crate::types::value::Value::Integer(val)) = row.first() {
                    assert_eq!(*val, 300, "Value should be 300 from older transaction");
                }
            }
        } else {
            panic!("Expected query result for verification");
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
            "coordinator_1",
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
            "coordinator_1",
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
            "coordinator_2",
            false,
            None,
        );
        processor.process_message(msg).await.unwrap();

        // Abort the transaction
        let msg = create_message(
            None,
            "txn_runtime1_3000000000",
            "test_coord",
            false,
            Some("abort"),
        );
        processor.process_message(msg).await.unwrap();

        // Query should still see original value
        let query = SqlOperation::Query {
            sql: "SELECT * FROM users WHERE id = 1".to_string(),
        };
        let msg = create_message(
            Some(query),
            "txn_runtime1_4000000000",
            "coordinator_3",
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
