//! Lock manager for KV storage
//!
//! Provides key-level locking with shared/exclusive modes for
//! coordinating concurrent access to keys.

use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Transaction ID type alias
pub type TxId = HlcTimestamp;

/// Lock modes for KV operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockMode {
    /// Shared lock for reading
    Shared,
    /// Exclusive lock for writing
    Exclusive,
}

impl LockMode {
    /// Check if two lock modes are compatible
    pub fn is_compatible_with(&self, other: LockMode) -> bool {
        match (*self, other) {
            // Multiple shared locks are compatible
            (LockMode::Shared, LockMode::Shared) => true,
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

/// Lock manager for key-level locking
pub struct LockManager {
    /// All currently held locks (key -> lock holders)
    locks: HashMap<String, Vec<LockInfo>>,
}

impl LockManager {
    /// Create a new lock manager
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
        }
    }

    /// Check if a lock can be acquired without modifying state
    pub fn check(&self, tx_id: TxId, key: &str, mode: LockMode) -> LockAttemptResult {
        // Check for existing locks on this key
        if let Some(holders) = self.locks.get(key) {
            // Check compatibility with all current holders
            for holder in holders {
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
        }

        LockAttemptResult::WouldGrant
    }

    /// Grant a lock that was previously checked
    pub fn grant(&mut self, tx_id: TxId, key: String, mode: LockMode) {
        let lock_info = LockInfo {
            holder: tx_id,
            mode,
        };

        self.locks
            .entry(key)
            .or_insert_with(Vec::new)
            .push(lock_info);
    }

    /// Release all locks held by a transaction
    pub fn release_all(&mut self, tx_id: TxId) {
        // Remove all locks held by this transaction
        self.locks.retain(|_key, holders| {
            holders.retain(|lock| lock.holder != tx_id);
            !holders.is_empty()
        });
    }

    /// Get all locks held by a transaction
    pub fn locks_held_by(&self, tx_id: TxId) -> Vec<(String, LockMode)> {
        let mut result = Vec::new();

        for (key, holders) in &self.locks {
            for holder in holders {
                if holder.holder == tx_id {
                    result.push((key.clone(), holder.mode));
                }
            }
        }

        result.sort_by(|a, b| a.0.cmp(&b.0)); // Sort by key for determinism
        result
    }

    /// Check if a transaction holds any locks
    pub fn has_locks(&self, tx_id: TxId) -> bool {
        for holders in self.locks.values() {
            if holders.iter().any(|h| h.holder == tx_id) {
                return true;
            }
        }
        false
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
    use proven_hlc::NodeId;

    fn create_tx_id(seed: u64) -> TxId {
        HlcTimestamp::new(seed, 0, NodeId::new(1))
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
        let tx1 = create_tx_id(100);
        let tx2 = create_tx_id(200);

        // First exclusive lock should succeed
        assert_eq!(
            manager.check(tx1, "key1", LockMode::Exclusive),
            LockAttemptResult::WouldGrant
        );
        manager.grant(tx1, "key1".to_string(), LockMode::Exclusive);

        // Conflicting lock should report conflict
        match manager.check(tx2, "key1", LockMode::Exclusive) {
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
        let tx1 = create_tx_id(100);
        let tx2 = create_tx_id(200);
        let tx3 = create_tx_id(300);

        // Multiple shared locks should succeed
        assert_eq!(
            manager.check(tx1, "key1", LockMode::Shared),
            LockAttemptResult::WouldGrant
        );
        manager.grant(tx1, "key1".to_string(), LockMode::Shared);

        assert_eq!(
            manager.check(tx2, "key1", LockMode::Shared),
            LockAttemptResult::WouldGrant
        );
        manager.grant(tx2, "key1".to_string(), LockMode::Shared);

        // Exclusive lock should conflict
        assert!(matches!(
            manager.check(tx3, "key1", LockMode::Exclusive),
            LockAttemptResult::Conflict { .. }
        ));
    }

    #[test]
    fn test_lock_release() {
        let mut manager = LockManager::new();
        let tx1 = create_tx_id(100);
        let tx2 = create_tx_id(200);

        // Acquire and release a lock
        manager.grant(tx1, "key1".to_string(), LockMode::Exclusive);
        assert!(manager.has_locks(tx1));

        manager.release_all(tx1);
        assert!(!manager.has_locks(tx1));

        // New lock should succeed
        assert_eq!(
            manager.check(tx2, "key1", LockMode::Exclusive),
            LockAttemptResult::WouldGrant
        );
    }

    #[test]
    fn test_reentrant_locks() {
        let mut manager = LockManager::new();
        let tx1 = create_tx_id(100);

        // Same transaction can acquire multiple locks on same key
        manager.grant(tx1, "key1".to_string(), LockMode::Exclusive);

        // Should be able to check for another lock on same key
        assert_eq!(
            manager.check(tx1, "key1", LockMode::Exclusive),
            LockAttemptResult::WouldGrant
        );
    }
}
