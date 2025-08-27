//! Raft state machine integration for distributed consensus

use crate::error::{Error, Result};
use crate::lock::{LockManager, Priority, TxId};
use crate::storage::Storage;
use crate::transaction::{Transaction, TransactionManager};
use crate::types::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Operations that can be applied through Raft consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    /// Begin a new transaction
    BeginTransaction { tx_id: TxId, priority: Priority },

    /// Execute a SQL statement within a transaction
    Execute { tx_id: TxId, sql: String },

    /// Direct data manipulation (for simplified execution)
    DataOperation { tx_id: TxId, op: DataOp },

    /// Commit a transaction
    Commit { tx_id: TxId },

    /// Abort a transaction
    Abort { tx_id: TxId, reason: String },

    /// Create a table
    CreateTable {
        name: String,
        schema: crate::storage::Schema,
    },

    /// Drop a table
    DropTable { name: String },
}

/// Data operations for simplified execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataOp {
    Insert {
        table: String,
        values: Vec<Value>,
    },
    Update {
        table: String,
        row_id: u64,
        values: Vec<Value>,
    },
    Delete {
        table: String,
        row_id: u64,
    },
    Read {
        table: String,
        row_id: u64,
    },
    Scan {
        table: String,
        start: Option<u64>,
        end: Option<u64>,
    },
}

/// Result of an operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationResult {
    /// Operation succeeded
    Success(SuccessResult),

    /// Operation failed with error
    Error(String),

    /// Operation would block (needs distributed coordination)
    WouldBlock { tx_id: TxId, reason: String },
}

/// Success results for different operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SuccessResult {
    /// Transaction begun
    TransactionBegun { tx_id: TxId },

    /// Data returned from read
    Data(Vec<Value>),

    /// Multiple rows returned from scan
    Rows(Vec<(u64, Vec<Value>)>),

    /// Row ID from insert
    InsertedId(u64),

    /// Number of affected rows
    AffectedRows(usize),

    /// Transaction committed
    Committed,

    /// Transaction aborted
    Aborted,

    /// Table created
    TableCreated,

    /// Table dropped
    TableDropped,

    /// Generic OK
    Ok,
}

/// The SQL engine as a Raft state machine
pub struct SqlStateMachine {
    /// Storage engine
    storage: Arc<Storage>,

    /// Lock manager
    lock_manager: Arc<LockManager>,

    /// Transaction manager
    tx_manager: Arc<TransactionManager>,

    /// Active transactions
    transactions: Arc<RwLock<HashMap<TxId, Arc<Transaction>>>>,

    /// Operation counter for determinism
    operation_counter: Arc<RwLock<u64>>,
}

impl SqlStateMachine {
    pub fn new() -> Self {
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
            transactions: Arc::new(RwLock::new(HashMap::new())),
            operation_counter: Arc::new(RwLock::new(0)),
        }
    }

    /// Apply an operation to the state machine
    pub fn apply(&self, operation: Operation) -> OperationResult {
        // Increment operation counter for determinism
        {
            let mut counter = self.operation_counter.write().unwrap();
            *counter += 1;
        }

        match operation {
            Operation::BeginTransaction { tx_id, priority } => {
                self.begin_transaction(tx_id, priority)
            }

            Operation::Execute { tx_id, sql } => self.execute_sql(tx_id, sql),

            Operation::DataOperation { tx_id, op } => self.execute_data_op(tx_id, op),

            Operation::Commit { tx_id } => self.commit_transaction(tx_id),

            Operation::Abort { tx_id, reason } => self.abort_transaction(tx_id, reason),

            Operation::CreateTable { name, schema } => self.create_table(name, schema),

            Operation::DropTable { name } => self.drop_table(name),
        }
    }

    /// Create a snapshot of the current state
    pub fn snapshot(&self) -> Result<Vec<u8>> {
        // In production, we'd serialize the entire state
        // For now, we'll create a simple snapshot
        let snapshot = StateSnapshot {
            tables: self.storage.list_tables(),
            operation_count: *self.operation_counter.read().unwrap(),
        };

        bincode::encode_to_vec(&snapshot, bincode::config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Restore from a snapshot
    pub fn restore(&mut self, snapshot: Vec<u8>) -> Result<()> {
        let (_snapshot, _): (StateSnapshot, _) =
            bincode::decode_from_slice(&snapshot, bincode::config::standard())
                .map_err(|e| Error::Serialization(e.to_string()))?;

        // In production, we'd restore the full state
        // For now, this is a placeholder

        Ok(())
    }

    // Internal operation handlers

    fn begin_transaction(&self, tx_id: TxId, priority: Priority) -> OperationResult {
        // Create a new transaction directly with the given ID
        let tx = Arc::new(Transaction::new(
            tx_id,
            priority,
            self.lock_manager.clone(),
            self.storage.clone(),
        ));

        self.transactions.write().unwrap().insert(tx_id, tx);

        OperationResult::Success(SuccessResult::TransactionBegun { tx_id })
    }

    fn execute_sql(&self, _tx_id: TxId, _sql: String) -> OperationResult {
        // In production, we'd parse and execute SQL here
        // For now, return a placeholder
        OperationResult::Success(SuccessResult::Ok)
    }

    fn execute_data_op(&self, tx_id: TxId, op: DataOp) -> OperationResult {
        let transactions = self.transactions.read().unwrap();

        let tx = match transactions.get(&tx_id) {
            Some(t) => t.clone(),
            None => {
                return OperationResult::Error(format!("Transaction {} not found", tx_id));
            }
        };

        drop(transactions); // Release read lock

        match op {
            DataOp::Insert { table, values } => match tx.insert(&table, values) {
                Ok(id) => OperationResult::Success(SuccessResult::InsertedId(id)),
                Err(Error::WouldBlock) => OperationResult::WouldBlock {
                    tx_id,
                    reason: "Waiting for lock".to_string(),
                },
                Err(e) => OperationResult::Error(e.to_string()),
            },

            DataOp::Update {
                table,
                row_id,
                values,
            } => match tx.write(&table, row_id, values) {
                Ok(()) => OperationResult::Success(SuccessResult::AffectedRows(1)),
                Err(Error::WouldBlock) => OperationResult::WouldBlock {
                    tx_id,
                    reason: "Waiting for lock".to_string(),
                },
                Err(e) => OperationResult::Error(e.to_string()),
            },

            DataOp::Delete { table, row_id } => match tx.delete(&table, row_id) {
                Ok(()) => OperationResult::Success(SuccessResult::AffectedRows(1)),
                Err(Error::WouldBlock) => OperationResult::WouldBlock {
                    tx_id,
                    reason: "Waiting for lock".to_string(),
                },
                Err(e) => OperationResult::Error(e.to_string()),
            },

            DataOp::Read { table, row_id } => match tx.read(&table, row_id) {
                Ok(values) => OperationResult::Success(SuccessResult::Data(values)),
                Err(Error::WouldBlock) => OperationResult::WouldBlock {
                    tx_id,
                    reason: "Waiting for lock".to_string(),
                },
                Err(e) => OperationResult::Error(e.to_string()),
            },

            DataOp::Scan { table, start, end } => match tx.scan(&table, start, end) {
                Ok(rows) => OperationResult::Success(SuccessResult::Rows(rows)),
                Err(Error::WouldBlock) => OperationResult::WouldBlock {
                    tx_id,
                    reason: "Waiting for lock".to_string(),
                },
                Err(e) => OperationResult::Error(e.to_string()),
            },
        }
    }

    fn commit_transaction(&self, tx_id: TxId) -> OperationResult {
        let transactions = self.transactions.read().unwrap();

        if let Some(tx) = transactions.get(&tx_id) {
            match tx.commit() {
                Ok(()) => {
                    drop(transactions);
                    self.transactions.write().unwrap().remove(&tx_id);
                    OperationResult::Success(SuccessResult::Committed)
                }
                Err(e) => OperationResult::Error(e.to_string()),
            }
        } else {
            OperationResult::Error(format!("Transaction {} not found", tx_id))
        }
    }

    fn abort_transaction(&self, tx_id: TxId, reason: String) -> OperationResult {
        let transactions = self.transactions.read().unwrap();

        if let Some(tx) = transactions.get(&tx_id) {
            match tx.abort() {
                Ok(()) => {
                    drop(transactions);
                    self.transactions.write().unwrap().remove(&tx_id);
                    OperationResult::Success(SuccessResult::Aborted)
                }
                Err(e) => OperationResult::Error(format!("{}: {}", reason, e)),
            }
        } else {
            OperationResult::Error(format!("Transaction {} not found", tx_id))
        }
    }

    fn create_table(&self, name: String, schema: crate::storage::Schema) -> OperationResult {
        match self.storage.create_table(name, schema) {
            Ok(()) => OperationResult::Success(SuccessResult::TableCreated),
            Err(e) => OperationResult::Error(e.to_string()),
        }
    }

    fn drop_table(&self, name: String) -> OperationResult {
        match self.storage.drop_table(&name) {
            Ok(()) => OperationResult::Success(SuccessResult::TableDropped),
            Err(e) => OperationResult::Error(e.to_string()),
        }
    }
}

/// Snapshot of the state machine
#[derive(Debug, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
struct StateSnapshot {
    tables: Vec<String>,
    operation_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Column, Schema};
    use crate::types::DataType;

    #[test]
    fn test_state_machine_operations() {
        let sm = SqlStateMachine::new();

        // Create a table
        let schema = Schema::new(vec![
            Column::new("id".into(), DataType::Integer).primary_key(),
            Column::new("name".into(), DataType::String),
        ])
        .unwrap();

        let result = sm.apply(Operation::CreateTable {
            name: "users".to_string(),
            schema,
        });

        assert!(matches!(
            result,
            OperationResult::Success(SuccessResult::TableCreated)
        ));

        // Begin a transaction
        let result = sm.apply(Operation::BeginTransaction {
            tx_id: 1,
            priority: 100,
        });

        assert!(matches!(
            result,
            OperationResult::Success(SuccessResult::TransactionBegun { .. })
        ));

        // Insert data
        let result = sm.apply(Operation::DataOperation {
            tx_id: 1,
            op: DataOp::Insert {
                table: "users".to_string(),
                values: vec![Value::Integer(1), Value::String("Alice".into())],
            },
        });

        assert!(matches!(
            result,
            OperationResult::Success(SuccessResult::InsertedId(1))
        ));

        // Commit
        let result = sm.apply(Operation::Commit { tx_id: 1 });
        assert!(matches!(
            result,
            OperationResult::Success(SuccessResult::Committed)
        ));
    }

    #[test]
    fn test_deterministic_execution() {
        let sm1 = SqlStateMachine::new();
        let sm2 = SqlStateMachine::new();

        // Create the same table on both
        let schema = Schema::new(vec![
            Column::new("id".into(), DataType::Integer).primary_key(),
            Column::new("value".into(), DataType::Integer),
        ])
        .unwrap();

        let ops = vec![
            Operation::CreateTable {
                name: "test".to_string(),
                schema: schema.clone(),
            },
            Operation::BeginTransaction {
                tx_id: 1,
                priority: 100,
            },
            Operation::DataOperation {
                tx_id: 1,
                op: DataOp::Insert {
                    table: "test".to_string(),
                    values: vec![Value::Integer(1), Value::Integer(100)],
                },
            },
            Operation::Commit { tx_id: 1 },
        ];

        // Apply same operations to both state machines
        for op in ops {
            let r1 = sm1.apply(op.clone());
            let r2 = sm2.apply(op);

            // Results should be identical
            match (r1, r2) {
                (OperationResult::Success(s1), OperationResult::Success(s2)) => {
                    assert_eq!(format!("{:?}", s1), format!("{:?}", s2));
                }
                _ => {}
            }
        }
    }
}
