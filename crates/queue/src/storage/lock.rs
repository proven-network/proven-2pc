//! Lock manager for queue storage
//!
//! Provides queue-level locking with shared/exclusive modes for
//! coordinating concurrent access to queues.

use proven_common::TransactionId;
use serde::{Deserialize, Serialize};

/// Transaction ID type alias
pub type TxId = TransactionId;

/// Lock modes for queue operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockMode {
    /// Shared lock for reading (peek, size, isEmpty)
    Shared,
    /// Append lock for enqueue operations (compatible with other appends)
    Append,
    /// Exclusive lock for destructive operations (dequeue, clear)
    Exclusive,
}

impl LockMode {
    /// Check if two lock modes are compatible
    pub fn is_compatible_with(&self, other: LockMode) -> bool {
        match (*self, other) {
            // Multiple shared locks are compatible
            (LockMode::Shared, LockMode::Shared) => true,
            // Multiple append locks are compatible (concurrent enqueues)
            (LockMode::Append, LockMode::Append) => true,
            // Append and shared are compatible (can read while appending)
            (LockMode::Append, LockMode::Shared) | (LockMode::Shared, LockMode::Append) => true,
            // Everything else is incompatible
            _ => false,
        }
    }
}

/// Information about a held lock
#[derive(Debug, Clone)]
pub struct LockInfo {
    pub holder: TxId,
    pub mode: LockMode,
}

/// Result of checking if a lock can be acquired
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

/// Lock manager for single queue locking
pub struct LockManager {
    /// All currently held locks for this queue
    locks: Vec<LockInfo>,
}

impl LockManager {
    /// Create a new lock manager
    pub fn new() -> Self {
        Self { locks: Vec::new() }
    }

    /// Check if a lock can be acquired without modifying state
    pub fn check(&self, tx_id: TxId, mode: LockMode) -> LockAttemptResult {
        // Check compatibility with all current holders
        for holder in &self.locks {
            // Skip if it's the same transaction (re-entrant locks)
            if holder.holder == tx_id {
                continue;
            }

            // Check compatibility
            if !holder.mode.is_compatible_with(mode) {
                return LockAttemptResult::Conflict {
                    holder: holder.holder,
                    mode: holder.mode,
                };
            }
        }

        LockAttemptResult::WouldGrant
    }

    /// Grant a lock that was previously checked
    pub fn grant(&mut self, tx_id: TxId, mode: LockMode) {
        let lock_info = LockInfo {
            holder: tx_id,
            mode,
        };

        self.locks.push(lock_info);
    }

    /// Release a specific lock held by a transaction
    pub fn release(&mut self, tx_id: TxId) {
        self.locks.retain(|lock| lock.holder != tx_id);
    }

    /// Release all locks held by a transaction
    pub fn release_all(&mut self, tx_id: TxId) {
        // Remove all locks held by this transaction
        self.locks.retain(|lock| lock.holder != tx_id);
    }

    /// Get all locks held by a transaction
    pub fn locks_held_by(&self, tx_id: TxId) -> Vec<LockMode> {
        self.locks
            .iter()
            .filter(|lock| lock.holder == tx_id)
            .map(|lock| lock.mode)
            .collect()
    }

    /// Check if a transaction holds any locks
    pub fn has_locks(&self, tx_id: TxId) -> bool {
        self.locks.iter().any(|lock| lock.holder == tx_id)
    }

    /// Get all lock holders (for checking snapshot read conflicts)
    pub fn get_all_holders(&self) -> Vec<(TxId, LockMode)> {
        self.locks
            .iter()
            .map(|lock| (lock.holder, lock.mode))
            .collect()
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

    fn create_tx_id() -> TxId {
        TransactionId::new()
    }

    #[test]
    fn test_lock_compatibility() {
        assert!(LockMode::Shared.is_compatible_with(LockMode::Shared));
        assert!(!LockMode::Shared.is_compatible_with(LockMode::Exclusive));
        assert!(!LockMode::Exclusive.is_compatible_with(LockMode::Shared));
        assert!(!LockMode::Exclusive.is_compatible_with(LockMode::Exclusive));
    }

    #[test]
    fn test_basic_lock_acquisition() {
        let mut manager = LockManager::new();
        let tx1 = create_tx_id();
        let tx2 = create_tx_id();

        // First exclusive lock should succeed
        assert_eq!(
            manager.check(tx1, LockMode::Exclusive),
            LockAttemptResult::WouldGrant
        );
        manager.grant(tx1, LockMode::Exclusive);

        // Conflicting lock should report conflict
        match manager.check(tx2, LockMode::Exclusive) {
            LockAttemptResult::Conflict { holder, mode } => {
                assert_eq!(holder, tx1);
                assert_eq!(mode, LockMode::Exclusive);
            }
            _ => panic!("Expected conflict"),
        }
    }

    #[test]
    fn test_shared_locks() {
        let mut manager = LockManager::new();
        let tx1 = create_tx_id();
        let tx2 = create_tx_id();
        let tx3 = create_tx_id();

        // Multiple shared locks should succeed
        assert_eq!(
            manager.check(tx1, LockMode::Shared),
            LockAttemptResult::WouldGrant
        );
        manager.grant(tx1, LockMode::Shared);

        assert_eq!(
            manager.check(tx2, LockMode::Shared),
            LockAttemptResult::WouldGrant
        );
        manager.grant(tx2, LockMode::Shared);

        // Exclusive lock should conflict
        assert!(matches!(
            manager.check(tx3, LockMode::Exclusive),
            LockAttemptResult::Conflict { .. }
        ));
    }

    #[test]
    fn test_lock_release() {
        let mut manager = LockManager::new();
        let tx1 = create_tx_id();
        let tx2 = create_tx_id();

        // Acquire and release a lock
        manager.grant(tx1, LockMode::Exclusive);
        assert!(manager.has_locks(tx1));

        manager.release_all(tx1);
        assert!(!manager.has_locks(tx1));

        // New lock should succeed
        assert_eq!(
            manager.check(tx2, LockMode::Exclusive),
            LockAttemptResult::WouldGrant
        );
    }
}
