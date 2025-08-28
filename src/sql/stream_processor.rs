//! Stream-based SQL engine processor
//! 
//! Consumes ordered messages from a Raft-ordered stream and processes SQL operations.
//! Implements command sourcing with PCC-based transaction management.

use crate::error::{Error, Result};
use crate::hlc::HlcTimestamp;
use crate::lock::LockManager;
use crate::sql::execution::executor::{Executor, ExecutionResult};
use crate::storage::Schema as StorageSchema;
use crate::sql::planner::planner::Planner;
use crate::sql::types::schema::Table;
use crate::storage::Storage;
use crate::transaction::{Transaction, TransactionManager};
use serde::{Deserialize, Serialize};
use bincode::{Decode, Encode};
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
    
    /// Execution result - simplified version
    ExecuteResult {
        /// Type of result
        result_type: String,
        /// Rows affected or other numeric result
        rows_affected: Option<usize>,
        /// Message for DDL operations
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
    
    /// For testing - allows downcasting
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
        self.responses.lock().push((
            coordinator_id.to_string(),
            txn_id.to_string(),
            response,
        ));
    }
    
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// SQL stream processor that consumes ordered messages
pub struct SqlStreamProcessor {
    /// In-memory storage (rebuilt from stream)
    storage: Arc<Storage>,
    
    /// Lock manager for PCC
    lock_manager: Arc<LockManager>,
    
    /// Transaction manager
    tx_manager: Arc<TransactionManager>,
    
    /// Active transactions (not yet committed/aborted)
    active_transactions: HashMap<String, Arc<Transaction>>,
    
    /// SQL planner (with schema tracking)
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
        let storage = Arc::new(Storage::new());
        let lock_manager = Arc::new(LockManager::new());
        let tx_manager = Arc::new(TransactionManager::new(
            lock_manager.clone(),
            storage.clone(),
        ));
        
        Self {
            storage,
            lock_manager,
            tx_manager,
            active_transactions: HashMap::new(),
            planner: Planner::new(HashMap::new()),
            executor: Executor::new(),
            response_channel,
            migration_version: 0,
            schemas: HashMap::new(),
        }
    }
    
    /// Process a message from the stream
    pub async fn process_message(&mut self, message: StreamMessage) -> Result<()> {
        let txn_id = message.headers.get("txn_id")
            .ok_or_else(|| Error::InvalidValue("Missing txn_id header".into()))?
            .clone();
        
        // Check if this is a commit/abort message (empty body)
        if message.body.is_empty() {
            if let Some(phase) = message.headers.get("txn_phase") {
                match phase.as_str() {
                    "commit" => return self.commit_transaction(&txn_id),
                    "abort" => return self.abort_transaction(&txn_id),
                    _ => return Err(Error::InvalidValue(format!("Unknown txn_phase: {}", phase))),
                }
            }
            return Err(Error::InvalidValue("Empty message without txn_phase".into()));
        }
        
        // Deserialize the operation
        let operation: SqlOperation = bincode::decode_from_slice(&message.body, bincode::config::standard())
            .map_err(|e| Error::InvalidValue(format!("Failed to deserialize operation: {}", e)))?
            .0;
        
        // Get or create transaction
        let tx = self.get_or_create_transaction(&txn_id, &message.headers)?;
        
        // Execute the operation
        let result = self.execute_operation(operation, &tx, &message.headers).await;
        
        // Send response if coordinator_id is present
        if let Some(coordinator_id) = message.headers.get("coordinator_id") {
            let response = match result {
                Ok(res) => res,
                Err(e) => SqlResponse::Error(format!("{:?}", e)),
            };
            self.response_channel.send(coordinator_id, &txn_id, response);
        }
        
        // Auto-commit if specified
        if message.headers.get("auto_commit") == Some(&"true".to_string()) {
            self.commit_transaction(&txn_id)?;
        }
        
        Ok(())
    }
    
    /// Get existing transaction or create new one
    fn get_or_create_transaction(
        &mut self,
        txn_id: &str,
        headers: &HashMap<String, String>,
    ) -> Result<Arc<Transaction>> {
        if let Some(tx) = self.active_transactions.get(txn_id) {
            return Ok(tx.clone());
        }
        
        // Parse the transaction ID to extract timestamp
        // Format: "txn_runtime123_1234567890"
        let parts: Vec<&str> = txn_id.split('_').collect();
        if parts.len() < 3 {
            return Err(Error::InvalidValue(format!("Invalid txn_id format: {}", txn_id)));
        }
        
        let timestamp_str = parts[2];
        let timestamp_nanos: u64 = timestamp_str.parse()
            .map_err(|_| Error::InvalidValue(format!("Invalid timestamp in txn_id: {}", txn_id)))?;
        
        // Create HLC timestamp (assuming node_id 1 for now)
        let hlc_timestamp = HlcTimestamp::new(
            timestamp_nanos / 1_000_000_000, // Convert to seconds
            (timestamp_nanos % 1_000_000_000) as u32, // Nanosecond remainder as logical
            crate::hlc::NodeId::new(1),
        );
        
        // Create transaction with the given timestamp
        let tx = self.tx_manager.begin_with_timestamp(hlc_timestamp)?;
        self.active_transactions.insert(txn_id.to_string(), tx.clone());
        
        Ok(tx)
    }
    
    /// Execute a SQL operation
    async fn execute_operation(
        &mut self,
        operation: SqlOperation,
        tx: &Arc<Transaction>,
        _headers: &HashMap<String, String>,
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
                        // Convert Arc<Vec<Value>> to Vec<Value>
                        let rows = rows.into_iter()
                            .map(|row| row.as_ref().clone())
                            .collect();
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
        let plan = self.planner.plan(statement)?;
        
        // Check if this is DDL that affects schemas
        match &plan {
            crate::sql::planner::plan::Plan::CreateTable { name, schema } => {
                // Update our schema tracking
                self.schemas.insert(name.clone(), schema.clone());
                
                // Convert to storage schema
                let storage_schema = convert_to_storage_schema(&schema)?;
                self.storage.create_table(name.clone(), storage_schema)?;
                
                // Update planner and executor with new schemas
                self.planner = Planner::new(self.schemas.clone());
                self.executor.update_schemas(self.schemas.clone());
            }
            
            crate::sql::planner::plan::Plan::DropTable { name, .. } => {
                self.schemas.remove(name);
                // TODO: Drop from storage
                
                // Update planner and executor
                self.planner = Planner::new(self.schemas.clone());
                self.executor.update_schemas(self.schemas.clone());
            }
            
            _ => {}
        }
        
        // Execute the plan
        self.executor.execute(plan, tx, &tx.context)
    }
    
    /// Commit a transaction
    fn commit_transaction(&mut self, txn_id: &str) -> Result<()> {
        if let Some(tx) = self.active_transactions.remove(txn_id) {
            tx.commit()?;
        }
        // If transaction doesn't exist, it might have been auto-committed
        Ok(())
    }
    
    /// Abort a transaction
    fn abort_transaction(&mut self, txn_id: &str) -> Result<()> {
        if let Some(tx) = self.active_transactions.remove(txn_id) {
            tx.abort()?;
        }
        // If transaction doesn't exist, ignore (might have been auto-committed)
        Ok(())
    }
    
    /// Process a stream of messages (for testing)
    pub async fn process_stream(&mut self, messages: Vec<StreamMessage>) -> Result<()> {
        for message in messages {
            self.process_message(message).await?;
        }
        Ok(())
    }
}

/// Convert ExecutionResult to SqlResponse
fn convert_execution_result(result: ExecutionResult) -> SqlResponse {
    match result {
        ExecutionResult::Select { columns, rows } => {
            let rows = rows.into_iter()
                .map(|row| row.as_ref().clone())
                .collect();
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

/// Convert SQL schema to storage schema
fn convert_to_storage_schema(table: &Table) -> Result<StorageSchema> {
    use crate::storage::Column as StorageColumn;
    
    let columns = table.columns.iter()
        .map(|col| {
            let mut storage_col = StorageColumn::new(col.name.clone(), col.datatype.clone());
            if col.primary_key {
                storage_col = storage_col.primary_key();
            }
            if !col.nullable {
                storage_col = storage_col.nullable(col.nullable);
            }
            if col.unique {
                storage_col = storage_col.unique();
            }
            storage_col
        })
        .collect();
    
    StorageSchema::new(columns)
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
    async fn test_create_table_and_insert() {
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
            true, // auto-commit
            None,
        );
        
        processor.process_message(msg).await.unwrap();
        
        // Insert data
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
        
        // Check responses
        let responses = mock_channel.get_responses();
        assert_eq!(responses.len(), 2);
    }
}