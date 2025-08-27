//! Lock manager with wound-wait deadlock prevention

use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, RwLock};

/// Transaction ID
pub type TxId = u64;

/// Transaction priority (lower value = higher priority)
pub type Priority = u64;

/// Lock modes with compatibility matrix
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
            (IntentShared, Shared) | (IntentShared, IntentShared) | (IntentShared, IntentExclusive) => true,
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
    pub priority: Priority,
    pub mode: LockMode,
    pub acquired_at: u64,
}

/// A transaction waiting for a lock
#[derive(Debug, Clone)]
struct Waiter {
    tx_id: TxId,
    priority: Priority,
    mode: LockMode,
}

/// Result of attempting to acquire a lock
#[derive(Debug, Clone, PartialEq)]
pub enum LockResult {
    /// Lock was granted
    Granted,
    /// Should wound (abort) the victim transaction
    ShouldWound(TxId),
    /// Must wait for lock to be released
    MustWait,
}

/// Lock manager statistics
#[derive(Debug, Default, Clone)]
pub struct LockStatistics {
    pub locks_granted: u64,
    pub locks_waited: u64,
    pub wounds_inflicted: u64,
    pub escalations: u64,
}

/// Lock manager with wound-wait deadlock prevention
pub struct LockManager {
    /// All currently held locks
    locks: Arc<RwLock<HashMap<LockKey, Vec<LockInfo>>>>,
    
    /// Wait queue for blocked transactions
    wait_queue: Arc<RwLock<HashMap<LockKey, VecDeque<Waiter>>>>,
    
    /// Statistics for monitoring
    stats: Arc<RwLock<LockStatistics>>,
    
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
            stats: Arc::new(RwLock::new(LockStatistics::default())),
            escalation_threshold: threshold,
        }
    }
    
    /// Try to acquire a lock with wound-wait deadlock prevention
    pub fn try_acquire(
        &self,
        tx_id: TxId,
        priority: Priority,
        key: LockKey,
        mode: LockMode,
    ) -> Result<LockResult> {
        let mut locks = self.locks.write().unwrap();
        let mut stats = self.stats.write().unwrap();
        
        // Check for existing locks on this key
        if let Some(holders) = locks.get(&key) {
            // Check compatibility with all current holders
            for holder in holders {
                if !self.is_compatible_internal(&holder, mode) {
                    // Conflict detected - apply wound-wait
                    if priority < holder.priority {
                        // Higher priority transaction wounds lower priority holder
                        stats.wounds_inflicted += 1;
                        return Ok(LockResult::ShouldWound(holder.holder));
                    } else {
                        // Lower priority transaction must wait
                        self.add_to_wait_queue(tx_id, priority, key.clone(), mode);
                        stats.locks_waited += 1;
                        return Ok(LockResult::MustWait);
                    }
                }
            }
        }
        
        // Grant the lock
        let lock_info = LockInfo {
            holder: tx_id,
            priority,
            mode,
            acquired_at: self.logical_timestamp(),
        };
        
        locks.entry(key).or_insert_with(Vec::new).push(lock_info);
        stats.locks_granted += 1;
        
        Ok(LockResult::Granted)
    }
    
    /// Release a lock
    pub fn release(&self, tx_id: TxId, key: LockKey) -> Result<()> {
        let mut locks = self.locks.write().unwrap();
        
        if let Some(holders) = locks.get_mut(&key) {
            holders.retain(|lock| lock.holder != tx_id);
            if holders.is_empty() {
                locks.remove(&key);
            }
            
            // Wake up waiters if possible
            self.wake_waiters(key);
        }
        
        Ok(())
    }
    
    /// Release all locks held by a transaction
    pub fn release_all(&self, tx_id: TxId) -> Result<()> {
        let mut locks = self.locks.write().unwrap();
        let mut keys_to_wake = Vec::new();
        
        // Remove all locks held by this transaction
        locks.retain(|key, holders| {
            holders.retain(|lock| lock.holder != tx_id);
            if holders.is_empty() {
                keys_to_wake.push(key.clone());
                false
            } else {
                true
            }
        });
        
        // Wake up waiters for released locks
        drop(locks); // Release write lock before waking waiters
        for key in keys_to_wake {
            self.wake_waiters(key);
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
    
    /// Get all current locks (for visibility)
    pub fn get_all_locks(&self) -> Vec<(LockKey, Vec<LockInfo>)> {
        let locks = self.locks.read().unwrap();
        locks.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    
    /// Check if a transaction holds a specific lock
    pub fn holds_lock(&self, tx_id: TxId, key: &LockKey, mode: LockMode) -> bool {
        let locks = self.locks.read().unwrap();
        
        if let Some(holders) = locks.get(key) {
            holders.iter().any(|lock| {
                lock.holder == tx_id && 
                (lock.mode == mode || 
                 // Exclusive lock satisfies shared requirement
                 (mode == LockMode::Shared && lock.mode == LockMode::Exclusive))
            })
        } else {
            false
        }
    }
    
    /// Get statistics
    pub fn stats(&self) -> LockStatistics {
        self.stats.read().unwrap().clone()
    }
    
    /// Check lock escalation for a table
    pub fn maybe_escalate(&self, _tx_id: TxId, table: &str, row_locks: Vec<u64>) -> Option<LockKey> {
        if row_locks.len() > self.escalation_threshold {
            let mut stats = self.stats.write().unwrap();
            stats.escalations += 1;
            Some(LockKey::Table { table: table.to_string() })
        } else {
            None
        }
    }
    
    // Internal helper methods
    
    fn is_compatible_internal(&self, existing: &LockInfo, requested: LockMode) -> bool {
        existing.mode.is_compatible_with(requested)
    }
    
    fn add_to_wait_queue(&self, tx_id: TxId, priority: Priority, key: LockKey, mode: LockMode) {
        let mut wait_queue = self.wait_queue.write().unwrap();
        let waiter = Waiter { tx_id, priority, mode };
        
        wait_queue.entry(key)
            .or_insert_with(VecDeque::new)
            .push_back(waiter);
    }
    
    fn wake_waiters(&self, key: LockKey) {
        let mut wait_queue = self.wait_queue.write().unwrap();
        
        if let Some(waiters) = wait_queue.get_mut(&key) {
            // Try to grant locks to waiting transactions
            // This is simplified - in production, we'd need to actually
            // notify the waiting transactions
            while !waiters.is_empty() {
                let waiter = waiters.front().unwrap();
                
                // Check if we can grant the lock
                if self.can_grant_to_waiter(&key, waiter.mode) {
                    waiters.pop_front();
                    // In a real system, we'd notify the transaction here
                } else {
                    break;
                }
            }
            
            if waiters.is_empty() {
                wait_queue.remove(&key);
            }
        }
    }
    
    fn can_grant_to_waiter(&self, key: &LockKey, mode: LockMode) -> bool {
        let locks = self.locks.read().unwrap();
        
        if let Some(holders) = locks.get(key) {
            holders.iter().all(|lock| self.is_compatible_internal(lock, mode))
        } else {
            true
        }
    }
    
    fn logical_timestamp(&self) -> u64 {
        // In production, this would use a proper logical clock
        // For now, we'll use a simple counter
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
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
        let key = LockKey::Row { table: "users".into(), row_id: 1 };
        
        // First lock should succeed
        assert_eq!(
            manager.try_acquire(1, 100, key.clone(), LockMode::Exclusive).unwrap(),
            LockResult::Granted
        );
        
        // Conflicting lock should trigger wound-wait
        let result = manager.try_acquire(2, 50, key.clone(), LockMode::Exclusive).unwrap();
        assert_eq!(result, LockResult::ShouldWound(1));
        
        // Lower priority should wait
        let result = manager.try_acquire(3, 150, key.clone(), LockMode::Exclusive).unwrap();
        assert_eq!(result, LockResult::MustWait);
    }
    
    #[test]
    fn test_shared_locks() {
        let manager = LockManager::new();
        let key = LockKey::Row { table: "users".into(), row_id: 1 };
        
        // Multiple shared locks should succeed
        assert_eq!(
            manager.try_acquire(1, 100, key.clone(), LockMode::Shared).unwrap(),
            LockResult::Granted
        );
        assert_eq!(
            manager.try_acquire(2, 200, key.clone(), LockMode::Shared).unwrap(),
            LockResult::Granted
        );
        
        // Exclusive lock should conflict
        let result = manager.try_acquire(3, 50, key.clone(), LockMode::Exclusive).unwrap();
        assert_eq!(result, LockResult::ShouldWound(1));
    }
    
    #[test]
    fn test_lock_release() {
        let manager = LockManager::new();
        let key = LockKey::Row { table: "users".into(), row_id: 1 };
        
        // Acquire and release a lock
        manager.try_acquire(1, 100, key.clone(), LockMode::Exclusive).unwrap();
        manager.release(1, key.clone()).unwrap();
        
        // New lock should succeed
        assert_eq!(
            manager.try_acquire(2, 200, key.clone(), LockMode::Exclusive).unwrap(),
            LockResult::Granted
        );
    }
}