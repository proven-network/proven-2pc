//! Transaction management with pessimistic concurrency control

use crate::error::{Error, Result};
use crate::hlc::{HlcClock, HlcTimestamp, SharedHlcClock};
use crate::lock::{LockKey, LockManager, LockMode, LockResult, TxId};
use crate::storage::Storage;
use crate::transaction_id::{TransactionContext, TransactionId};
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

/// A database transaction
pub struct Transaction {
    /// Full transaction ID (global + sub-transaction sequence)
    pub id: TransactionId,

    /// Transaction context for deterministic SQL execution
    pub context: TransactionContext,

    /// Current state
    pub state: RwLock<TransactionState>,

    /// Locks held by this transaction
    locks_held: RwLock<Vec<LockKey>>,

    /// Reference to lock manager
    lock_manager: Arc<LockManager>,

    /// Reference to storage
    storage: Arc<Storage>,

    /// Track access for distributed coordination
    pub access_log: RwLock<Vec<AccessLogEntry>>,
}

impl Transaction {
    pub fn new(
        global_id: HlcTimestamp,
        lock_manager: Arc<LockManager>,
        storage: Arc<Storage>,
    ) -> Self {
        let id = TransactionId::new(global_id);
        let context = TransactionContext::new(global_id);
        Self {
            id,
            context,
            state: RwLock::new(TransactionState::Active),
            locks_held: RwLock::new(Vec::new()),
            lock_manager,
            storage,
            access_log: RwLock::new(Vec::new()),
        }
    }

    /// Create a sub-transaction
    pub fn new_sub(
        parent_id: TransactionId,
        sub_seq: u32,
        lock_manager: Arc<LockManager>,
        storage: Arc<Storage>,
    ) -> Self {
        let id = parent_id.sub_transaction(sub_seq);
        let context = TransactionContext::new(parent_id.global_id).sub_transaction(sub_seq);
        Self {
            id,
            context,
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

    /// Acquire a lock - returns error if blocked
    /// The transaction manager will handle wound-wait logic
    pub fn acquire_lock(&self, key: LockKey, mode: LockMode) -> Result<()> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        match self.lock_manager.try_acquire(self.id, key.clone(), mode)? {
            LockResult::Granted => {
                self.locks_held.write().unwrap().push(key.clone());
                Ok(())
            }
            LockResult::Conflict {
                holder,
                mode: held_mode,
            } => {
                // Return conflict information for transaction manager to handle
                Err(Error::LockConflict {
                    holder,
                    mode: held_mode,
                })
            }
        }
    }

    /// Read a row from a table
    pub fn read(&self, table: &str, row_id: u64) -> Result<Vec<crate::sql::types::value::Value>> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        // Acquire shared lock
        let lock_key = LockKey::Row {
            table: table.to_string(),
            row_id,
        };
        self.acquire_lock(lock_key, LockMode::Shared)?;

        // Log access
        self.access_log.write().unwrap().push(AccessLogEntry {
            operation: "READ".to_string(),
            table: table.to_string(),
            keys: vec![row_id],
            lock_mode: LockMode::Shared,
        });

        // Read from storage
        let table_obj = self.storage.get_table(table)?;
        let row = table_obj
            .get(row_id)
            .ok_or_else(|| Error::InvalidValue(format!("Row {} not found", row_id)))?;

        Ok(row.values.clone())
    }

    /// Write a row to a table
    pub fn write(
        &self,
        table: &str,
        row_id: u64,
        values: Vec<crate::sql::types::value::Value>,
    ) -> Result<()> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        // Acquire exclusive lock
        let lock_key = LockKey::Row {
            table: table.to_string(),
            row_id,
        };
        self.acquire_lock(lock_key, LockMode::Exclusive)?;

        // Log access
        self.access_log.write().unwrap().push(AccessLogEntry {
            operation: "WRITE".to_string(),
            table: table.to_string(),
            keys: vec![row_id],
            lock_mode: LockMode::Exclusive,
        });

        // Write to storage
        self.storage
            .with_table_mut(table, |t| t.update(row_id, values))
    }

    /// Insert a new row
    pub fn insert(&self, table: &str, values: Vec<crate::sql::types::value::Value>) -> Result<u64> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        // For inserts, we need a table-level intent exclusive lock
        let lock_key = LockKey::Table {
            table: table.to_string(),
        };
        self.acquire_lock(lock_key, LockMode::IntentExclusive)?;

        // Insert and get the new row ID
        let row_id = self.storage.with_table_mut(table, |t| t.insert(values))?;

        // Now acquire exclusive lock on the new row
        let row_lock = LockKey::Row {
            table: table.to_string(),
            row_id,
        };
        self.acquire_lock(row_lock, LockMode::Exclusive)?;

        // Log access
        self.access_log.write().unwrap().push(AccessLogEntry {
            operation: "INSERT".to_string(),
            table: table.to_string(),
            keys: vec![row_id],
            lock_mode: LockMode::Exclusive,
        });

        Ok(row_id)
    }

    /// Delete a row
    pub fn delete(&self, table: &str, row_id: u64) -> Result<()> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        // Acquire exclusive lock
        let lock_key = LockKey::Row {
            table: table.to_string(),
            row_id,
        };
        self.acquire_lock(lock_key, LockMode::Exclusive)?;

        // Log access
        self.access_log.write().unwrap().push(AccessLogEntry {
            operation: "DELETE".to_string(),
            table: table.to_string(),
            keys: vec![row_id],
            lock_mode: LockMode::Exclusive,
        });

        // Delete from storage
        self.storage.with_table_mut(table, |t| t.delete(row_id))
    }

    /// Scan a table with optional range
    pub fn scan(
        &self,
        table: &str,
        start: Option<u64>,
        end: Option<u64>,
    ) -> Result<Vec<(u64, Vec<crate::sql::types::value::Value>)>> {
        if !self.is_active() {
            return Err(Error::TransactionNotActive(self.id));
        }

        // Determine lock type based on range
        let lock_key = match (start, end) {
            (Some(s), Some(e)) => LockKey::Range {
                table: table.to_string(),
                start: s,
                end: e,
            },
            _ => LockKey::Table {
                table: table.to_string(),
            },
        };

        self.acquire_lock(lock_key, LockMode::Shared)?;

        // Log access
        self.access_log.write().unwrap().push(AccessLogEntry {
            operation: "SCAN".to_string(),
            table: table.to_string(),
            keys: match (start, end) {
                (Some(s), Some(e)) => (s..=e).collect(),
                _ => vec![],
            },
            lock_mode: LockMode::Shared,
        });

        // Scan from storage
        let table_obj = self.storage.get_table(table)?;
        let results = match (start, end) {
            (Some(s), Some(e)) => table_obj
                .scan_range(s, e)
                .map(|(id, row)| (id, row.values.clone()))
                .collect(),
            _ => table_obj
                .scan()
                .map(|(id, row)| (id, row.values.clone()))
                .collect(),
        };

        Ok(results)
    }

    /// Prepare to commit (2PC phase 1)
    pub fn prepare(&self) -> Result<()> {
        let mut state = self.state.write().unwrap();

        if *state != TransactionState::Active {
            return Err(Error::TransactionNotActive(self.id));
        }

        *state = TransactionState::Preparing;
        Ok(())
    }

    /// Commit the transaction
    pub fn commit(&self) -> Result<()> {
        let mut state = self.state.write().unwrap();

        if *state != TransactionState::Active && *state != TransactionState::Preparing {
            return Err(Error::TransactionNotActive(self.id));
        }

        // Release all locks
        self.lock_manager.release_all(self.id)?;

        *state = TransactionState::Committed;
        Ok(())
    }

    /// Abort the transaction
    pub fn abort(&self) -> Result<()> {
        let mut state = self.state.write().unwrap();

        // Release all locks
        self.lock_manager.release_all(self.id)?;

        *state = TransactionState::Aborted;
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // Ensure locks are released
        if self.is_active() {
            let _ = self.abort();
        }
    }
}

/// Transaction manager that implements wound-wait policy
pub struct TransactionManager {
    /// All active transactions (keyed by full transaction ID)
    transactions: Arc<RwLock<HashMap<TransactionId, Arc<Transaction>>>>,

    /// Sub-transaction sequence counters per global transaction
    sub_sequences: Arc<RwLock<HashMap<HlcTimestamp, u32>>>,

    /// HLC clock for generating transaction IDs
    clock: SharedHlcClock,

    /// Lock manager
    lock_manager: Arc<LockManager>,

    /// Storage
    storage: Arc<Storage>,
}

impl TransactionManager {
    pub fn new(lock_manager: Arc<LockManager>, storage: Arc<Storage>) -> Self {
        use crate::hlc::NodeId;
        // In a real system, this would use the actual node ID
        let clock = Arc::new(HlcClock::new(NodeId::new(1)));

        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            sub_sequences: Arc::new(RwLock::new(HashMap::new())),
            clock,
            lock_manager,
            storage,
        }
    }

    /// Create a new transaction manager with a specific clock
    pub fn with_clock(
        lock_manager: Arc<LockManager>,
        storage: Arc<Storage>,
        clock: SharedHlcClock,
    ) -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            sub_sequences: Arc::new(RwLock::new(HashMap::new())),
            clock,
            lock_manager,
            storage,
        }
    }

    /// Begin a new transaction (main transaction, not a sub-transaction)
    pub fn begin(&self) -> Result<Arc<Transaction>> {
        let global_id = self.clock.now();

        let tx = Arc::new(Transaction::new(
            global_id,
            self.lock_manager.clone(),
            self.storage.clone(),
        ));

        self.transactions.write().unwrap().insert(tx.id, tx.clone());
        // Initialize sub-sequence counter for this global transaction
        self.sub_sequences.write().unwrap().insert(global_id, 0);

        Ok(tx)
    }

    /// Begin a new transaction with a specific timestamp (for testing)
    pub fn begin_with_timestamp(&self, global_id: HlcTimestamp) -> Result<Arc<Transaction>> {
        let tx = Arc::new(Transaction::new(
            global_id,
            self.lock_manager.clone(),
            self.storage.clone(),
        ));

        self.transactions.write().unwrap().insert(tx.id, tx.clone());
        self.sub_sequences.write().unwrap().insert(global_id, 0);

        Ok(tx)
    }

    /// Begin a sub-transaction under an existing global transaction
    pub fn begin_sub(&self, global_id: HlcTimestamp) -> Result<Arc<Transaction>> {
        // Increment and get the sub-sequence number
        let mut sequences = self.sub_sequences.write().unwrap();
        let sub_seq = sequences
            .entry(global_id)
            .and_modify(|s| *s += 1)
            .or_insert(1);
        let sub_seq = *sub_seq;
        drop(sequences);

        let parent_id = TransactionId::new(global_id);
        let tx = Arc::new(Transaction::new_sub(
            parent_id,
            sub_seq,
            self.lock_manager.clone(),
            self.storage.clone(),
        ));

        self.transactions.write().unwrap().insert(tx.id, tx.clone());

        Ok(tx)
    }

    /// Get a transaction by ID
    pub fn get(&self, tx_id: TxId) -> Option<Arc<Transaction>> {
        self.transactions.read().unwrap().get(&tx_id).cloned()
    }

    /// Remove completed transaction
    pub fn remove(&self, tx_id: TxId) -> Result<()> {
        let mut txs = self.transactions.write().unwrap();

        if let Some(tx) = txs.get(&tx_id) {
            let state = tx.state();
            if state != TransactionState::Committed && state != TransactionState::Aborted {
                return Err(Error::TransactionNotActive(tx_id));
            }
            txs.remove(&tx_id);
        }

        Ok(())
    }

    /// Get all active transactions
    pub fn active_transactions(&self) -> Vec<TxId> {
        self.transactions
            .read()
            .unwrap()
            .iter()
            .filter(|(_, tx)| tx.is_active())
            .map(|(&id, _)| id)
            .collect()
    }

    /// Abort a transaction (for wound-wait)
    pub fn abort_transaction(&self, tx_id: TxId) -> Result<()> {
        if let Some(tx) = self.get(tx_id) {
            tx.abort()?;
        }
        Ok(())
    }

    /// Try to acquire a lock with wound-wait handling
    /// Returns Ok(()) if lock acquired, Err(WouldBlock) if must wait
    pub fn acquire_lock_with_wound_wait(
        &self,
        tx: &Transaction,
        key: LockKey,
        mode: LockMode,
    ) -> Result<()> {
        loop {
            match tx.acquire_lock(key.clone(), mode) {
                Ok(()) => return Ok(()),
                Err(Error::LockConflict { holder, .. }) => {
                    // Get the holder transaction to check its timestamp
                    if let Some(holder_tx) = self.get(holder) {
                        // Wound-wait: if we're older (earlier timestamp), wound the holder
                        // Note: sub-transactions inherit parent priority
                        if tx.id.has_higher_priority_than(&holder_tx.id) {
                            // Wound the younger transaction
                            self.abort_transaction(holder)?;
                            // Retry acquiring the lock
                            continue;
                        } else {
                            // We're younger, must wait
                            return Err(Error::WouldBlock);
                        }
                    } else {
                        // Holder transaction not found, it may have completed
                        // Retry acquiring the lock
                        continue;
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}

use std::collections::HashMap;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::types::value::{DataType, Value};
    use crate::storage::{Column, Schema};

    #[test]
    fn test_transaction_lifecycle() {
        let lock_manager = Arc::new(LockManager::new());
        let storage = Arc::new(Storage::new());

        // Create a test table
        let schema = Schema::new(vec![
            Column::new("id".into(), DataType::Integer).primary_key(),
            Column::new("name".into(), DataType::String),
        ])
        .unwrap();
        storage.create_table("users".into(), schema).unwrap();

        let tx_manager = TransactionManager::new(lock_manager, storage);

        // Begin transaction
        let tx = tx_manager.begin().unwrap();
        assert_eq!(tx.state(), TransactionState::Active);
        assert!(tx.id.global_id.physical > 0); // Should have a valid timestamp

        // Insert data
        let id = tx
            .insert(
                "users",
                vec![Value::Integer(1), Value::String("Alice".into())],
            )
            .unwrap();
        assert_eq!(id, 1);

        // Commit
        tx.commit().unwrap();
        assert_eq!(tx.state(), TransactionState::Committed);
    }

    #[test]
    fn test_concurrent_transactions() {
        let lock_manager = Arc::new(LockManager::new());
        let storage = Arc::new(Storage::new());

        // Create a test table
        let schema = Schema::new(vec![
            Column::new("id".into(), DataType::Integer).primary_key(),
            Column::new("balance".into(), DataType::Integer),
        ])
        .unwrap();
        storage.create_table("accounts".into(), schema).unwrap();

        // Insert initial data
        storage
            .with_table_mut("accounts", |table| {
                table.insert(vec![Value::Integer(1), Value::Integer(100)])
            })
            .unwrap();

        let tx_manager = TransactionManager::new(lock_manager, storage);

        // Start two concurrent transactions
        let tx1 = tx_manager.begin().unwrap();
        // Small delay to ensure tx2 gets a later timestamp
        std::thread::sleep(std::time::Duration::from_millis(1));
        let tx2 = tx_manager.begin().unwrap();

        // tx2 should be younger (later timestamp)
        assert!(tx1.id.has_higher_priority_than(&tx2.id));

        // tx1 reads the balance
        let balance = tx1.read("accounts", 1).unwrap();
        assert_eq!(balance[1], Value::Integer(100));

        // tx2 tries to write - should get lock conflict
        let result = tx2.write("accounts", 1, vec![Value::Integer(1), Value::Integer(200)]);
        assert!(matches!(result, Err(Error::LockConflict { .. })));
    }
}
