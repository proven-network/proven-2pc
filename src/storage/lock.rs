//! Lock manager that tracks lock ownership and detects conflicts

use crate::error::Result;
use crate::hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Transaction ID type alias
pub type TxId = HlcTimestamp;

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

/// Result of checking if a lock can be acquired (pure function)
#[derive(Debug, Clone, PartialEq)]
pub enum LockAttemptResult {
    /// Lock would be granted if requested
    WouldGrant,
    /// Lock conflicts with an existing lock
    Conflict {
        /// Transaction holding the conflicting lock
        holder: TxId,
        /// Mode of the conflicting lock
        mode: LockMode,
    },
}

/// Lock manager that tracks locks and detects conflicts
///
/// This manager does NOT implement any deadlock prevention policy.
/// It simply tracks who holds what locks and reports conflicts.
/// The transaction layer (stream processor) is responsible for implementing
/// policies like wound-wait and managing deferred operations.
pub struct LockManager {
    /// All currently held locks
    locks: HashMap<LockKey, Vec<LockInfo>>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
        }
    }

    /// Check if a lock can be acquired without modifying state (pure function)
    pub fn check(&self, tx_id: TxId, key: &LockKey, mode: LockMode) -> LockAttemptResult {
        // Check for existing locks on this key
        if let Some(holders) = self.locks.get(key) {
            // Check compatibility with all current holders
            for holder in holders {
                if holder.holder != tx_id && !holder.mode.is_compatible_with(mode) {
                    return LockAttemptResult::Conflict {
                        holder: holder.holder,
                        mode: holder.mode,
                    };
                }
            }
        }

        LockAttemptResult::WouldGrant
    }

    /// Grant a lock that was previously checked (modifies state)
    pub fn grant(&mut self, tx_id: TxId, key: LockKey, mode: LockMode) -> Result<()> {
        let lock_info = LockInfo {
            holder: tx_id,
            mode,
        };

        self.locks
            .entry(key)
            .or_insert_with(Vec::new)
            .push(lock_info);

        Ok(())
    }

    /// Release all locks held by a transaction
    pub fn release_all(&mut self, tx_id: TxId) -> Result<()> {
        // Remove all locks held by this transaction
        self.locks.retain(|_key, holders| {
            holders.retain(|lock| lock.holder != tx_id);
            !holders.is_empty()
        });

        Ok(())
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
        clock.now()
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
        let mut manager = LockManager::new();
        let key = LockKey::Row {
            table: "users".into(),
            row_id: 1,
        };

        let tx1 = create_tx_id(1);
        let tx2 = create_tx_id(2);

        // First lock should succeed
        assert_eq!(
            manager.check(tx1, &key, LockMode::Exclusive),
            LockAttemptResult::WouldGrant
        );
        manager
            .grant(tx1, key.clone(), LockMode::Exclusive)
            .unwrap();

        // Conflicting lock should report conflict
        match manager.check(tx2, &key, LockMode::Exclusive) {
            LockAttemptResult::Conflict { holder, .. } => assert_eq!(holder, tx1),
            _ => panic!("Expected conflict"),
        }
    }

    #[test]
    fn test_shared_locks() {
        let mut manager = LockManager::new();
        let key = LockKey::Row {
            table: "users".into(),
            row_id: 1,
        };

        let tx1 = create_tx_id(1);
        let tx2 = create_tx_id(2);
        let tx3 = create_tx_id(3);

        // Multiple shared locks should succeed
        assert_eq!(
            manager.check(tx1, &key, LockMode::Shared),
            LockAttemptResult::WouldGrant
        );
        manager.grant(tx1, key.clone(), LockMode::Shared).unwrap();

        assert_eq!(
            manager.check(tx2, &key, LockMode::Shared),
            LockAttemptResult::WouldGrant
        );
        manager.grant(tx2, key.clone(), LockMode::Shared).unwrap();

        // Exclusive lock should conflict
        assert!(matches!(
            manager.check(tx3, &key, LockMode::Exclusive),
            LockAttemptResult::Conflict { .. }
        ));
    }

    #[test]
    fn test_lock_release() {
        let mut manager = LockManager::new();
        let key = LockKey::Row {
            table: "users".into(),
            row_id: 1,
        };

        let tx1 = create_tx_id(1);
        let tx2 = create_tx_id(2);

        // Acquire and release a lock
        manager
            .grant(tx1, key.clone(), LockMode::Exclusive)
            .unwrap();
        manager.release_all(tx1).unwrap();

        // New lock should succeed
        assert_eq!(
            manager.check(tx2, &key, LockMode::Exclusive),
            LockAttemptResult::WouldGrant
        );
        manager
            .grant(tx2, key.clone(), LockMode::Exclusive)
            .unwrap();
    }

    #[test]
    fn test_pure_lock_check() {
        let mut manager = LockManager::new();
        let key = LockKey::Row {
            table: "users".into(),
            row_id: 1,
        };

        let tx1 = create_tx_id(1);
        let tx2 = create_tx_id(2);

        // Check before any locks
        assert_eq!(
            manager.check(tx1, &key, LockMode::Exclusive),
            LockAttemptResult::WouldGrant
        );

        // Grant lock to tx1
        manager
            .grant(tx1, key.clone(), LockMode::Exclusive)
            .unwrap();

        // Check should show conflict for tx2
        match manager.check(tx2, &key, LockMode::Exclusive) {
            LockAttemptResult::Conflict { holder, mode } => {
                assert_eq!(holder, tx1);
                assert_eq!(mode, LockMode::Exclusive);
            }
            _ => panic!("Expected conflict"),
        }

        // Check with older transaction
        let tx0 = HlcTimestamp::new(0, 0, crate::hlc::NodeId::new(0));
        assert!(matches!(
            manager.check(tx0, &key, LockMode::Exclusive),
            LockAttemptResult::Conflict { .. }
        ))
    }

    #[test]
    fn test_release_returns_empty() {
        let mut manager = LockManager::new();
        let key1 = LockKey::Row {
            table: "users".into(),
            row_id: 1,
        };

        let tx1 = create_tx_id(1);

        // tx1 holds lock on key1
        manager
            .grant(tx1, key1.clone(), LockMode::Exclusive)
            .unwrap();

        // Release all locks - returns nothing since wake-up is handled by stream processor
        manager.release_all(tx1).unwrap();
    }
}
