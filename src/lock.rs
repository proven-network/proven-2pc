//! Lock manager that tracks lock ownership and detects conflicts

use crate::error::Result;
use crate::transaction_id::TransactionId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, RwLock};

/// Transaction ID type alias
pub type TxId = TransactionId;

/// Lock modes with compatibility matrix
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LockMode {
    /// Shared lock for reading
    Shared,
    /// Exclusive lock for writing
    Exclusive,
    /// Intent shared - intent to acquire shared locks on children
    IntentShared,
    /// Intent exclusive - intent to acquire exclusive locks on children
    IntentExclusive,
}

impl LockMode {
    /// Check if two lock modes are compatible
    pub fn is_compatible_with(&self, other: LockMode) -> bool {
        use LockMode::*;
        match (*self, other) {
            // Shared is compatible with Shared and IntentShared
            (Shared, Shared) | (Shared, IntentShared) => true,
            // IntentShared is compatible with everything except Exclusive
            (IntentShared, Shared)
            | (IntentShared, IntentShared)
            | (IntentShared, IntentExclusive) => true,
            // IntentExclusive is compatible only with Intent locks
            (IntentExclusive, IntentShared) | (IntentExclusive, IntentExclusive) => true,
            // Everything else is incompatible
            _ => false,
        }
    }
}

/// Lock key identifying what is being locked
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LockKey {
    /// Row-level lock
    Row { table: String, row_id: u64 },
    /// Range lock for scans
    Range { table: String, start: u64, end: u64 },
    /// Table-level lock
    Table { table: String },
    /// Schema lock for DDL operations
    Schema,
}

impl fmt::Display for LockKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockKey::Row { table, row_id } => write!(f, "Row({}:{})", table, row_id),
            LockKey::Range { table, start, end } => write!(f, "Range({}:{}-{})", table, start, end),
            LockKey::Table { table } => write!(f, "Table({})", table),
            LockKey::Schema => write!(f, "Schema"),
        }
    }
}

/// Information about a held lock
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockInfo {
    pub holder: TxId,
    pub mode: LockMode,
}

/// Result of attempting to acquire a lock
#[derive(Debug, Clone, PartialEq)]
pub enum LockResult {
    /// Lock was granted
    Granted,
    /// Lock conflicts with an existing lock
    Conflict {
        /// Transaction holding the conflicting lock
        holder: TxId,
        /// Mode of the conflicting lock
        mode: LockMode,
    },
}

/// A transaction waiting for a lock
#[derive(Debug, Clone)]
struct Waiter {
    tx_id: TxId,
    mode: LockMode,
}

/// Lock manager that tracks locks and detects conflicts
///
/// This manager does NOT implement any deadlock prevention policy.
/// It simply tracks who holds what locks and reports conflicts.
/// The transaction layer is responsible for implementing policies
/// like wound-wait, wait-die, or timeout-based deadlock prevention.
pub struct LockManager {
    /// All currently held locks
    locks: Arc<RwLock<HashMap<LockKey, Vec<LockInfo>>>>,

    /// Wait queue for blocked transactions
    wait_queue: Arc<RwLock<HashMap<LockKey, VecDeque<Waiter>>>>,

    /// Lock escalation threshold
    escalation_threshold: usize,
}

impl LockManager {
    pub fn new() -> Self {
        Self::with_escalation_threshold(100)
    }

    pub fn with_escalation_threshold(threshold: usize) -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
            wait_queue: Arc::new(RwLock::new(HashMap::new())),
            escalation_threshold: threshold,
        }
    }

    /// Try to acquire a lock, returning conflict information if it fails
    pub fn try_acquire(&self, tx_id: TxId, key: LockKey, mode: LockMode) -> Result<LockResult> {
        let mut locks = self.locks.write().unwrap();

        // Check for existing locks on this key
        if let Some(holders) = locks.get(&key) {
            // Check compatibility with all current holders
            for holder in holders {
                if holder.holder != tx_id && !holder.mode.is_compatible_with(mode) {
                    // Conflict detected - just report it
                    return Ok(LockResult::Conflict {
                        holder: holder.holder,
                        mode: holder.mode,
                    });
                }
            }
        }

        // Grant the lock
        let lock_info = LockInfo {
            holder: tx_id,
            mode,
        };

        locks.entry(key).or_insert_with(Vec::new).push(lock_info);

        Ok(LockResult::Granted)
    }

    /// Release a specific lock
    pub fn release(&self, tx_id: TxId, key: LockKey) -> Result<()> {
        let mut locks = self.locks.write().unwrap();

        if let Some(holders) = locks.get_mut(&key) {
            holders.retain(|lock| lock.holder != tx_id);
            if holders.is_empty() {
                locks.remove(&key);
            }

            // Wake up waiters if possible
            self.process_wait_queue(&key);
        }

        Ok(())
    }

    /// Release all locks held by a transaction
    pub fn release_all(&self, tx_id: TxId) -> Result<()> {
        let mut locks = self.locks.write().unwrap();
        let mut keys_to_check = Vec::new();

        // Remove all locks held by this transaction
        locks.retain(|key, holders| {
            holders.retain(|lock| lock.holder != tx_id);
            if holders.is_empty() {
                keys_to_check.push(key.clone());
                false
            } else {
                true
            }
        });

        // Process wait queues for released locks
        drop(locks); // Release write lock before processing queues
        for key in keys_to_check {
            self.process_wait_queue(&key);
        }

        Ok(())
    }

    /// Get all locks held by a transaction
    pub fn get_locks_held(&self, tx_id: TxId) -> Vec<(LockKey, LockMode)> {
        let locks = self.locks.read().unwrap();
        let mut held = Vec::new();

        for (key, holders) in locks.iter() {
            for lock_info in holders {
                if lock_info.holder == tx_id {
                    held.push((key.clone(), lock_info.mode));
                }
            }
        }

        held
    }

    /// Get all current locks (for visibility/debugging)
    pub fn get_all_locks(&self) -> Vec<(LockKey, Vec<LockInfo>)> {
        let locks = self.locks.read().unwrap();
        locks.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Check if a transaction holds a specific lock
    pub fn holds_lock(&self, tx_id: TxId, key: &LockKey, mode: LockMode) -> bool {
        let locks = self.locks.read().unwrap();

        if let Some(holders) = locks.get(key) {
            holders.iter().any(|lock| {
                lock.holder == tx_id
                    && (lock.mode == mode ||
                 // Exclusive lock satisfies shared requirement
                 (mode == LockMode::Shared && lock.mode == LockMode::Exclusive))
            })
        } else {
            false
        }
    }

    /// Add a transaction to the wait queue for a lock
    pub fn add_to_wait_queue(&self, tx_id: TxId, key: LockKey, mode: LockMode) {
        let mut wait_queue = self.wait_queue.write().unwrap();
        let waiter = Waiter { tx_id, mode };

        wait_queue
            .entry(key)
            .or_insert_with(VecDeque::new)
            .push_back(waiter);
    }

    /// Remove a transaction from all wait queues
    pub fn remove_from_wait_queues(&self, tx_id: TxId) {
        let mut wait_queue = self.wait_queue.write().unwrap();

        wait_queue.retain(|_, waiters| {
            waiters.retain(|w| w.tx_id != tx_id);
            !waiters.is_empty()
        });
    }

    /// Get waiting transactions for a lock
    pub fn get_waiters(&self, key: &LockKey) -> Vec<TxId> {
        let wait_queue = self.wait_queue.read().unwrap();

        wait_queue
            .get(key)
            .map(|waiters| waiters.iter().map(|w| w.tx_id).collect())
            .unwrap_or_default()
    }

    /// Check lock escalation for a table
    pub fn check_escalation(&self, table: &str, row_locks: Vec<u64>) -> Option<LockKey> {
        if row_locks.len() > self.escalation_threshold {
            Some(LockKey::Table {
                table: table.to_string(),
            })
        } else {
            None
        }
    }

    // Internal helper methods

    fn process_wait_queue(&self, key: &LockKey) {
        // In a real implementation, this would notify waiting transactions
        // For now, we just track them
        let wait_queue = self.wait_queue.read().unwrap();
        if let Some(waiters) = wait_queue.get(key) {
            // TODO: Notify waiters that the lock might be available
            // This would be done through a callback or channel
            let _ = waiters;
        }
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_tx_id(seed: u8) -> TxId {
        use crate::hlc::{HlcClock, NodeId};
        let clock = HlcClock::new(NodeId::from_seed(seed));
        TransactionId::new(clock.now())
    }

    #[test]
    fn test_lock_compatibility() {
        assert!(LockMode::Shared.is_compatible_with(LockMode::Shared));
        assert!(!LockMode::Shared.is_compatible_with(LockMode::Exclusive));
        assert!(LockMode::IntentShared.is_compatible_with(LockMode::IntentExclusive));
        assert!(!LockMode::Exclusive.is_compatible_with(LockMode::Exclusive));
    }

    #[test]
    fn test_basic_lock_acquisition() {
        let manager = LockManager::new();
        let key = LockKey::Row {
            table: "users".into(),
            row_id: 1,
        };

        let tx1 = create_tx_id(1);
        let tx2 = create_tx_id(2);

        // First lock should succeed
        assert_eq!(
            manager
                .try_acquire(tx1, key.clone(), LockMode::Exclusive)
                .unwrap(),
            LockResult::Granted
        );

        // Conflicting lock should report conflict
        let result = manager
            .try_acquire(tx2, key.clone(), LockMode::Exclusive)
            .unwrap();
        assert!(matches!(result, LockResult::Conflict { holder, .. } if holder == tx1));
    }

    #[test]
    fn test_shared_locks() {
        let manager = LockManager::new();
        let key = LockKey::Row {
            table: "users".into(),
            row_id: 1,
        };

        let tx1 = create_tx_id(1);
        let tx2 = create_tx_id(2);
        let tx3 = create_tx_id(3);

        // Multiple shared locks should succeed
        assert_eq!(
            manager
                .try_acquire(tx1, key.clone(), LockMode::Shared)
                .unwrap(),
            LockResult::Granted
        );
        assert_eq!(
            manager
                .try_acquire(tx2, key.clone(), LockMode::Shared)
                .unwrap(),
            LockResult::Granted
        );

        // Exclusive lock should conflict
        let result = manager
            .try_acquire(tx3, key.clone(), LockMode::Exclusive)
            .unwrap();
        assert!(matches!(result, LockResult::Conflict { .. }));
    }

    #[test]
    fn test_lock_release() {
        let manager = LockManager::new();
        let key = LockKey::Row {
            table: "users".into(),
            row_id: 1,
        };

        let tx1 = create_tx_id(1);
        let tx2 = create_tx_id(2);

        // Acquire and release a lock
        manager
            .try_acquire(tx1, key.clone(), LockMode::Exclusive)
            .unwrap();
        manager.release(tx1, key.clone()).unwrap();

        // New lock should succeed
        assert_eq!(
            manager
                .try_acquire(tx2, key.clone(), LockMode::Exclusive)
                .unwrap(),
            LockResult::Granted
        );
    }
}
