//! Transaction management with MVCC and PCC integration
//!
//! This module bridges MVCC (for isolation) with PCC (for conflict prevention).
//! - PCC handles lock acquisition and deadlock prevention
//! - MVCC handles versioning and visibility

use crate::context::TransactionContext;
use crate::error::{Error, Result};
use crate::hlc::HlcTimestamp;
use crate::sql::types::value::Value;
use crate::storage::lock::{LockKey, LockManager, LockMode, LockResult};
use crate::storage::mvcc::MvccStorage;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// Transaction is active and can execute operations
    Active,
    /// Transaction is preparing to commit (2PC)
    Preparing,
    /// Transaction has committed
    Committed,
    /// Transaction has been aborted
    Aborted,
}

/// Access log entry for distributed coordination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessLogEntry {
    pub operation: String,
    pub table: String,
    pub keys: Vec<u64>,
    pub lock_mode: LockMode,
}

/// A database transaction with MVCC support
pub struct MvccTransaction {
    /// Transaction ID (HLC timestamp)
    pub id: HlcTimestamp,

    /// Transaction context for deterministic SQL execution
    pub context: TransactionContext,

    /// Timestamp for this transaction (from global_id)
    pub timestamp: HlcTimestamp,

    /// Current state
    pub state: RwLock<TransactionState>,

    /// Locks held by this transaction (PCC)
    locks_held: RwLock<Vec<LockKey>>,

    /// Reference to lock manager (PCC)
    lock_manager: Arc<LockManager>,

    /// Reference to MVCC storage
    storage: Arc<MvccStorage>,

    /// Track access for distributed coordination
    pub access_log: RwLock<Vec<AccessLogEntry>>,
}

impl MvccTransaction {
    /// Create a new transaction
    pub fn new(
        global_id: HlcTimestamp,
        lock_manager: Arc<LockManager>,
        storage: Arc<MvccStorage>,
    ) -> Self {
        let context = TransactionContext::new(global_id);

        // Register with MVCC storage
        storage.register_transaction(global_id, global_id);

        Self {
            id: global_id,
            context,
            timestamp: global_id,
            state: RwLock::new(TransactionState::Active),
            locks_held: RwLock::new(Vec::new()),
            lock_manager,
            storage,
            access_log: RwLock::new(Vec::new()),
        }
    }

    /// Get the current state
    pub fn state(&self) -> TransactionState {
        *self.state.read().unwrap()
    }

    /// Check if transaction is active
    pub fn is_active(&self) -> bool {
        self.state() == TransactionState::Active
    }

    /// Acquire a lock (PCC layer)
    pub fn acquire_lock(&self, key: LockKey, mode: LockMode) -> Result<()> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        match self.lock_manager.try_acquire(self.id, key.clone(), mode)? {
            LockResult::Granted => {
                self.locks_held.write().unwrap().push(key);
                Ok(())
            }
            LockResult::Conflict {
                holder,
                mode: held_mode,
            } => Err(Error::LockConflict {
                holder,
                mode: held_mode,
            }),
        }
    }

    /// Release all locks
    fn release_all_locks(&self) -> Result<()> {
        let locks = self.locks_held.read().unwrap().clone();
        for lock in locks {
            self.lock_manager.release(self.id, lock)?;
        }
        self.locks_held.write().unwrap().clear();
        Ok(())
    }

    /// Insert a row
    pub fn insert(&self, table: &str, values: Vec<Value>) -> Result<u64> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        // Get the table
        let tables = self.storage.tables.read().unwrap();
        let _table_ref = tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        // We need write access to insert, so drop read lock and get write lock
        drop(tables);
        let mut tables = self.storage.tables.write().unwrap();
        let table_mut = tables
            .get_mut(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        // Insert creates a new row_id
        let row_id = table_mut.insert(self.id, self.timestamp, values)?;

        // Acquire exclusive lock for the new row (PCC)
        let lock_key = LockKey::Row {
            table: table.to_string(),
            row_id,
        };
        self.acquire_lock(lock_key, LockMode::Exclusive)?;

        // Log access
        self.access_log.write().unwrap().push(AccessLogEntry {
            operation: "INSERT".to_string(),
            table: table.to_string(),
            keys: vec![row_id],
            lock_mode: LockMode::Exclusive,
        });

        Ok(row_id)
    }

    /// Update a row
    pub fn update(&self, table: &str, row_id: u64, values: Vec<Value>) -> Result<()> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        // Acquire exclusive lock first (PCC)
        let lock_key = LockKey::Row {
            table: table.to_string(),
            row_id,
        };
        self.acquire_lock(lock_key, LockMode::Exclusive)?;

        // Update in MVCC storage
        let mut tables = self.storage.tables.write().unwrap();
        let table_mut = tables
            .get_mut(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        table_mut.update(self.id, self.timestamp, row_id, values)?;

        // Log access
        self.access_log.write().unwrap().push(AccessLogEntry {
            operation: "UPDATE".to_string(),
            table: table.to_string(),
            keys: vec![row_id],
            lock_mode: LockMode::Exclusive,
        });

        Ok(())
    }

    /// Delete a row
    pub fn delete(&self, table: &str, row_id: u64) -> Result<()> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        // Acquire exclusive lock first (PCC)
        let lock_key = LockKey::Row {
            table: table.to_string(),
            row_id,
        };
        self.acquire_lock(lock_key, LockMode::Exclusive)?;

        // Delete in MVCC storage
        let mut tables = self.storage.tables.write().unwrap();
        let table_mut = tables
            .get_mut(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        table_mut.delete(self.id, self.timestamp, row_id)?;

        // Log access
        self.access_log.write().unwrap().push(AccessLogEntry {
            operation: "DELETE".to_string(),
            table: table.to_string(),
            keys: vec![row_id],
            lock_mode: LockMode::Exclusive,
        });

        Ok(())
    }

    /// Read a row
    pub fn read(&self, table: &str, row_id: u64) -> Result<Vec<Value>> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        // Acquire shared lock first (PCC)
        let lock_key = LockKey::Row {
            table: table.to_string(),
            row_id,
        };
        self.acquire_lock(lock_key, LockMode::Shared)?;

        // Read from MVCC storage
        let tables = self.storage.tables.read().unwrap();
        let table_ref = tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        let row = table_ref
            .read(self.id, self.timestamp, row_id)
            .ok_or_else(|| Error::InvalidValue(format!("Row {} not found", row_id)))?;

        // Log access
        self.access_log.write().unwrap().push(AccessLogEntry {
            operation: "READ".to_string(),
            table: table.to_string(),
            keys: vec![row_id],
            lock_mode: LockMode::Shared,
        });

        Ok(row.values.clone())
    }

    /// Scan all visible rows in a table (returns only values, no row IDs)
    pub fn scan(&self, table: &str) -> Result<Vec<Vec<Value>>> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        // Acquire table-level shared lock (PCC)
        let lock_key = LockKey::Table {
            table: table.to_string(),
        };
        self.acquire_lock(lock_key, LockMode::Shared)?;

        // Scan from MVCC storage
        let tables = self.storage.tables.read().unwrap();
        let table_ref = tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        let rows = table_ref.scan(self.id, self.timestamp);

        // Collect row IDs for logging but only return values
        let mut row_ids = Vec::new();
        let mut values_only = Vec::new();
        for (id, row) in rows {
            row_ids.push(id);
            values_only.push(row.values.clone());
        }

        // Log access
        self.access_log.write().unwrap().push(AccessLogEntry {
            operation: "SCAN".to_string(),
            table: table.to_string(),
            keys: row_ids,
            lock_mode: LockMode::Shared,
        });

        Ok(values_only)
    }

    /// Internal method to scan with row IDs (for UPDATE/DELETE operations)
    pub(crate) fn scan_with_ids(&self, table: &str) -> Result<Vec<(u64, Vec<Value>)>> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        // Acquire table-level shared lock (PCC)
        let lock_key = LockKey::Table {
            table: table.to_string(),
        };
        self.acquire_lock(lock_key, LockMode::Shared)?;

        // Scan from MVCC storage
        let tables = self.storage.tables.read().unwrap();
        let table_ref = tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        let rows = table_ref.scan(self.id, self.timestamp);

        // Convert to values with IDs
        let result: Vec<(u64, Vec<Value>)> = rows
            .into_iter()
            .map(|(id, row)| (id, row.values.clone()))
            .collect();

        // Log access
        self.access_log.write().unwrap().push(AccessLogEntry {
            operation: "SCAN".to_string(),
            table: table.to_string(),
            keys: result.iter().map(|(id, _)| *id).collect(),
            lock_mode: LockMode::Shared,
        });

        Ok(result)
    }

    /// Prepare for commit (2PC phase 1)
    pub fn prepare(&self) -> Result<()> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        *self.state.write().unwrap() = TransactionState::Preparing;
        Ok(())
    }

    /// Commit the transaction
    pub fn commit(&self) -> Result<()> {
        let state = self.state();
        if state != TransactionState::Active && state != TransactionState::Preparing {
            return Err(Error::InvalidValue(format!(
                "Cannot commit transaction in state {:?}",
                state
            )));
        }

        // Commit in MVCC storage (makes all versions visible)
        self.storage.commit_transaction(self.id)?;

        // Release all PCC locks
        self.release_all_locks()?;

        *self.state.write().unwrap() = TransactionState::Committed;
        Ok(())
    }

    /// Abort the transaction
    pub fn abort(&self) -> Result<()> {
        if self.state() == TransactionState::Committed {
            return Err(Error::InvalidValue(
                "Cannot abort committed transaction".to_string(),
            ));
        }

        // Abort in MVCC storage (removes all uncommitted versions)
        self.storage.abort_transaction(self.id)?;

        // Release all PCC locks
        self.release_all_locks()?;

        *self.state.write().unwrap() = TransactionState::Aborted;
        Ok(())
    }
}

/// Transaction manager that creates MVCC-enabled transactions
pub struct MvccTransactionManager {
    lock_manager: Arc<LockManager>,
    storage: Arc<MvccStorage>,
}

impl MvccTransactionManager {
    pub fn new(lock_manager: Arc<LockManager>, storage: Arc<MvccStorage>) -> Self {
        Self {
            lock_manager,
            storage,
        }
    }

    /// Begin a new transaction
    pub fn begin(&self, timestamp: HlcTimestamp) -> Arc<MvccTransaction> {
        Arc::new(MvccTransaction::new(
            timestamp,
            self.lock_manager.clone(),
            self.storage.clone(),
        ))
    }

    /// Run periodic garbage collection
    pub fn garbage_collect(&self) -> usize {
        self.storage.garbage_collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::{HlcTimestamp, NodeId};
    use crate::sql::types::schema::{Column, Table};
    use crate::sql::types::value::DataType;
    use crate::storage::mvcc::MvccStorage;

    fn setup_test_env() -> (Arc<LockManager>, Arc<MvccStorage>) {
        let lock_manager = Arc::new(LockManager::new());
        let storage = Arc::new(MvccStorage::new());

        // Create test table
        let schema = Table::new(
            "test_table".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer).primary_key(),
                Column::new("name".to_string(), DataType::String),
            ],
        )
        .unwrap();

        storage.create_table("users".to_string(), schema).unwrap();

        (lock_manager, storage)
    }

    #[test]
    fn test_pcc_blocking_and_mvcc_abort() {
        let (lock_manager, storage) = setup_test_env();

        // Create and commit initial data
        let tx0 = MvccTransaction::new(
            HlcTimestamp::new(50, 0, NodeId::new(1)),
            lock_manager.clone(),
            storage.clone(),
        );
        let row_id = tx0
            .insert(
                "users",
                vec![Value::Integer(1), Value::String("Alice".to_string())],
            )
            .unwrap();
        tx0.commit().unwrap();

        // TX1 starts and updates the row (holds exclusive lock)
        let tx1 = MvccTransaction::new(
            HlcTimestamp::new(100, 0, NodeId::new(1)),
            lock_manager.clone(),
            storage.clone(),
        );
        tx1.update(
            "users",
            row_id,
            vec![Value::Integer(1), Value::String("Alice_TX1".to_string())],
        )
        .unwrap();

        // TX2 tries to read the same row - should get blocked by PCC
        let tx2 = MvccTransaction::new(
            HlcTimestamp::new(200, 0, NodeId::new(1)),
            lock_manager.clone(),
            storage.clone(),
        );

        // This should fail with a lock conflict (PCC blocking)
        let result = tx2.read("users", row_id);
        assert!(
            matches!(result, Err(Error::LockConflict { .. })),
            "TX2 should be blocked by TX1's exclusive lock"
        );

        // Now TX1 aborts (MVCC enables this rollback)
        tx1.abort().unwrap();

        // TX2 can now read and should see the original value (TX1's changes rolled back)
        let row_data = tx2.read("users", row_id).unwrap();
        assert_eq!(
            row_data[1],
            Value::String("Alice".to_string()),
            "After TX1 abort, TX2 should see original value"
        );

        // Start TX3 to verify abort was complete
        let tx3 = MvccTransaction::new(
            HlcTimestamp::new(300, 0, NodeId::new(1)),
            lock_manager.clone(),
            storage.clone(),
        );

        let row_data = tx3.read("users", row_id).unwrap();
        assert_eq!(
            row_data[1],
            Value::String("Alice".to_string()),
            "TX3 should also see original value after TX1 abort"
        );
    }

    #[test]
    fn test_abort_rollback() {
        let (lock_manager, storage) = setup_test_env();

        // Insert and commit a row
        let tx1 = MvccTransaction::new(
            HlcTimestamp::new(100, 0, NodeId::new(1)),
            lock_manager.clone(),
            storage.clone(),
        );

        let row_id = tx1
            .insert(
                "users",
                vec![Value::Integer(1), Value::String("Alice".to_string())],
            )
            .unwrap();
        tx1.commit().unwrap();

        // Start new transaction, update, then abort
        let tx2 = MvccTransaction::new(
            HlcTimestamp::new(200, 0, NodeId::new(1)),
            lock_manager.clone(),
            storage.clone(),
        );

        tx2.update(
            "users",
            row_id,
            vec![Value::Integer(1), Value::String("Bob".to_string())],
        )
        .unwrap();

        // Abort the transaction
        tx2.abort().unwrap();

        // New transaction should still see original value
        let tx3 = MvccTransaction::new(
            HlcTimestamp::new(300, 0, NodeId::new(1)),
            lock_manager.clone(),
            storage.clone(),
        );

        let values = tx3.read("users", row_id).unwrap();
        assert_eq!(values[1], Value::String("Alice".to_string()));
    }
}
