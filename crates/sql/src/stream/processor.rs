//! Core stream processor implementation
//!
//! Consumes ordered messages from a Raft-ordered stream and processes SQL operations
//! with proper transaction isolation using MVCC for rollback support and PCC for
//! conflict prevention.

use super::deferred::DeferredOperationsManager;
use super::message::{SqlOperation, StreamMessage};
use super::response::{SqlResponse, convert_execution_result};
use super::stats_cache::StatisticsCache;
use super::transaction::{TransactionContext, TransactionState};
use crate::error::{Error, Result};
use crate::execution::{ExecutionResult, Executor};
use crate::planner::planner::Planner;
use crate::storage::lock::{LockManager, LockMode};
use crate::storage::mvcc::MvccStorage;
use proven_engine::MockClient;
use proven_hlc::HlcTimestamp;
use std::collections::HashMap;
use std::sync::Arc;

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

    /// Engine client for sending responses
    client: Arc<MockClient>,

    /// Stream name for identification in responses
    stream_name: String,

    /// Current migration version
    migration_version: u32,

    /// Statistics cache for query optimization
    stats_cache: StatisticsCache,
}

impl SqlStreamProcessor {
    /// Create a processor for testing (creates a mock engine internally)
    #[cfg(test)]
    pub fn new_for_testing() -> (Self, Arc<proven_engine::MockEngine>) {
        use proven_engine::MockEngine;
        let engine = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-sql-processor".to_string(),
            engine.clone(),
        ));
        (Self::new(client, "sql-stream".to_string()), engine)
    }

    /// Send a response back to the coordinator via engine pub/sub with optional request_id
    fn send_response_with_request(
        &self,
        coordinator_id: &str,
        txn_id: &str,
        response: SqlResponse,
        request_id: Option<String>,
    ) {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("participant".to_string(), self.stream_name.clone());
        if let Some(req_id) = request_id {
            headers.insert("request_id".to_string(), req_id);
        }

        // Serialize the response to the body using serde_json
        let body = serde_json::to_vec(&response).unwrap_or_else(|e| {
            eprintln!("Failed to serialize SQL response: {}", e);
            Vec::new()
        });

        let subject = format!("coordinator.{}.response", coordinator_id);
        let message = proven_engine::Message::new(body, headers);

        // Use tokio::spawn to avoid blocking
        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.publish(&subject, vec![message]).await;
        });
    }

    /// Send a response back to the coordinator via engine pub/sub
    fn send_response(&self, coordinator_id: &str, txn_id: &str, response: SqlResponse) {
        self.send_response_with_request(coordinator_id, txn_id, response, None)
    }

    /// Create a new processor with empty storage
    pub fn new(client: Arc<MockClient>, stream_name: String) -> Self {
        Self {
            storage: MvccStorage::new(),
            lock_manager: LockManager::new(),
            active_transactions: HashMap::new(),
            transaction_coordinators: HashMap::new(),
            deferred_manager: DeferredOperationsManager::new(),
            executor: Executor::new(),
            client,
            stream_name,
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

        // Check if this is a commit/abort/prepare message (empty body)
        if message.is_transaction_control() {
            match message.txn_phase() {
                Some("prepare") => {
                    return self
                        .prepare_transaction(
                            txn_id,
                            txn_id_str,
                            message.headers.get("request_id").cloned(),
                        )
                        .await;
                }
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
        let operation: SqlOperation = serde_json::from_slice(&message.body)
            .map_err(|e| Error::InvalidValue(format!("Failed to deserialize operation: {}", e)))?;

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

        // Get request ID if present
        let request_id = message.headers.get("request_id").cloned();

        // Execute the operation
        let result = self
            .execute_operation(operation, txn_id, coordinator_id)
            .await;

        // Send response to coordinator with request ID
        let response = match result {
            Ok(res) => res,
            Err(e) => SqlResponse::Error(format!("{:?}", e)),
        };
        self.send_response_with_request(coordinator_id, txn_id_str, response, request_id);

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
                    self.send_response(coordinator_id, &txn_id_str, response);

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

    /// Prepare a transaction for commit (2PC)
    async fn prepare_transaction(
        &mut self,
        txn_id: HlcTimestamp,
        txn_id_str: &str,
        request_id: Option<String>,
    ) -> Result<()> {
        // Get coordinator ID for response
        let coordinator_id = self
            .transaction_coordinators
            .get(&txn_id)
            .cloned()
            .unwrap_or_default();

        if let Some(tx_ctx) = self.active_transactions.get_mut(&txn_id) {
            if let Some(wounded_by) = tx_ctx.wounded_by {
                // Transaction was wounded - send wounded response
                let response = SqlResponse::Wounded {
                    wounded_by,
                    reason: "Transaction wounded before prepare".to_string(),
                };
                self.send_response_with_request(
                    &coordinator_id,
                    txn_id_str,
                    response,
                    request_id.clone(),
                );
                Err(Error::TransactionWounded { wounded_by })
            } else if tx_ctx.prepare() {
                // Transaction successfully prepared - send prepared response
                self.send_response_with_request(
                    &coordinator_id,
                    txn_id_str,
                    SqlResponse::Prepared,
                    request_id.clone(),
                );
                Ok(())
            } else {
                // Transaction cannot be prepared for other reasons
                let response =
                    SqlResponse::Error("Cannot prepare: transaction not active".to_string());
                self.send_response_with_request(
                    &coordinator_id,
                    txn_id_str,
                    response,
                    request_id.clone(),
                );
                Err(Error::TransactionNotActive(txn_id))
            }
        } else {
            // Transaction doesn't exist
            let response = SqlResponse::Error("Cannot prepare: transaction not found".to_string());
            self.send_response_with_request(&coordinator_id, txn_id_str, response, request_id);
            Err(Error::TransactionNotFound(txn_id))
        }
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
        // Check if victim is prepared - prepared transactions cannot be wounded
        if let Some(victim_ctx) = self.active_transactions.get(&victim) {
            if victim_ctx.state == TransactionState::Preparing {
                // Cannot wound a prepared transaction
                // The older transaction must wait
                return Err(Error::LockConflict {
                    holder: victim,
                    mode: LockMode::Exclusive, // Prepared transactions hold all their locks
                });
            }
        }

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
            self.send_response(&op.coordinator_id, &victim_str, response);
        }

        // Send wounded notification if this is an active transaction with a coordinator
        if let Some(coordinator_id) = self.transaction_coordinators.get(&victim) {
            let response = SqlResponse::Wounded {
                wounded_by,
                reason: format!("Wounded by older transaction to prevent deadlock"),
            };
            self.send_response(coordinator_id, &victim_str, response);
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
                self.send_response(&op.coordinator_id, &txn_id_str, response);
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
            serde_json::to_vec(&op).unwrap()
        } else {
            Vec::new()
        };

        StreamMessage::new(body, headers)
    }

    #[tokio::test]
    async fn test_transaction_isolation_with_stream() {
        let (mut processor, _engine) = SqlStreamProcessor::new_for_testing();

        // Create a client to subscribe to responses
        let test_client = MockClient::new("test-observer".to_string(), _engine.clone());
        let mut response_stream_2 = test_client
            .subscribe("coordinator.coordinator_2.response", None)
            .await
            .unwrap();

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
        if let Some(msg) = response_stream_2.recv().await {
            if let Ok(response) = serde_json::from_slice::<SqlResponse>(&msg.body) {
                match response {
                    SqlResponse::QueryResult { rows, .. } => {
                        assert_eq!(rows.len(), 0, "Should not see uncommitted data");
                    }
                    _ => panic!("Expected QueryResult"),
                }
            } else {
                panic!("Failed to deserialize response");
            }
        } else {
            panic!("Expected response for transaction 2");
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
        // Subscribe to coordinator_3's responses
        let mut response_stream_3 = test_client
            .subscribe("coordinator.coordinator_3.response", None)
            .await
            .unwrap();

        processor.process_message(msg).await.unwrap();

        // Check that Transaction 3 sees the committed data
        if let Some(msg) = response_stream_3.recv().await {
            if let Ok(response) = serde_json::from_slice::<SqlResponse>(&msg.body) {
                match response {
                    SqlResponse::QueryResult { rows, .. } => {
                        assert_eq!(rows.len(), 1, "Should see committed data");
                    }
                    _ => panic!("Expected QueryResult"),
                }
            } else {
                panic!("Failed to deserialize response");
            }
        } else {
            panic!("Expected response for transaction 3");
        }
    }

    #[tokio::test]
    async fn test_wound_wait_mechanism() {
        let (mut processor, engine) = SqlStreamProcessor::new_for_testing();

        // Create a test client to subscribe to coordinator responses
        let test_client = MockClient::new("test-observer".to_string(), engine.clone());
        let mut younger_responses = test_client
            .subscribe("coordinator.coordinator_younger.response", None)
            .await
            .unwrap();
        let mut older_responses = test_client
            .subscribe("coordinator.coordinator_older.response", None)
            .await
            .unwrap();

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

        // Check younger transaction gets execute result
        let younger_response_msg = younger_responses
            .recv()
            .await
            .expect("Should receive response for younger transaction");
        let younger_response: SqlResponse = serde_json::from_slice(&younger_response_msg.body)
            .expect("Should deserialize response");
        assert!(
            matches!(younger_response, SqlResponse::ExecuteResult { .. }),
            "Younger transaction should succeed initially"
        );

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

        // Check that younger transaction gets wounded notification
        let wounded_msg = younger_responses
            .recv()
            .await
            .expect("Should receive wounded notification for younger transaction");
        let wounded_response: SqlResponse =
            serde_json::from_slice(&wounded_msg.body).expect("Should deserialize wounded response");
        assert!(
            matches!(wounded_response, SqlResponse::Wounded { .. }),
            "Younger transaction should be wounded"
        );

        // Check that older transaction succeeds
        let older_response_msg = older_responses
            .recv()
            .await
            .expect("Should receive response for older transaction");
        let older_response: SqlResponse =
            serde_json::from_slice(&older_response_msg.body).expect("Should deserialize response");
        assert!(
            matches!(older_response, SqlResponse::ExecuteResult { .. }),
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
    }

    #[tokio::test]
    async fn test_abort_rollback_with_stream() {
        let (mut processor, engine) = SqlStreamProcessor::new_for_testing();

        // Create a test client to subscribe to coordinator responses
        let test_client = MockClient::new("test-observer".to_string(), engine.clone());
        let mut coord3_responses = test_client
            .subscribe("coordinator.coordinator_3.response", None)
            .await
            .unwrap();

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

        // Verify the name is still 'Alice' after abort
        let query_response_msg = coord3_responses
            .recv()
            .await
            .expect("Should receive query response");
        let query_response: SqlResponse = serde_json::from_slice(&query_response_msg.body)
            .expect("Should deserialize query response");

        if let SqlResponse::QueryResult { rows, columns } = query_response {
            assert_eq!(rows.len(), 1, "Expected 1 row");
            assert_eq!(columns.len(), 2, "Expected 2 columns (id, name)");
            assert_eq!(rows[0].len(), 2, "Expected 2 values in row");

            // Check id is 1 and name is 'Alice'
            assert_eq!(
                rows[0][0],
                crate::types::value::Value::Integer(1),
                "ID should be 1"
            );
            assert_eq!(
                rows[0][1],
                crate::types::value::Value::String("Alice".to_string()),
                "Name should still be 'Alice' after abort"
            );
        } else {
            panic!("Expected QueryResult response");
        }
    }
}
