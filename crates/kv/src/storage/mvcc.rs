//! Multi-Version Concurrency Control (MVCC) for KV storage
//!
//! Provides transaction isolation through versioning of key-value pairs,
//! enabling quick aborts and consistent reads.

use crate::types::Value;
use proven_hlc::HlcTimestamp;
use std::collections::{HashMap, HashSet};

/// A versioned value in storage
#[derive(Debug, Clone)]
pub struct VersionedValue {
    /// The actual value
    pub value: Value,
    /// Transaction that created this version
    pub created_by: HlcTimestamp,
    /// When this version was created
    pub created_at: HlcTimestamp,
    /// Transaction that deleted this version (if any)
    pub deleted_by: Option<HlcTimestamp>,
}

/// MVCC storage for key-value pairs
#[derive(Debug)]
pub struct MvccStorage {
    /// All versions of all keys (key -> versions)
    /// Versions are ordered by creation time (newest last)
    versions: HashMap<String, Vec<VersionedValue>>,

    /// Set of committed transactions
    committed_transactions: HashSet<HlcTimestamp>,

    /// Transaction start times for visibility checks
    transaction_start_times: HashMap<HlcTimestamp, HlcTimestamp>,
}

impl MvccStorage {
    /// Create a new MVCC storage
    pub fn new() -> Self {
        Self {
            versions: HashMap::new(),
            committed_transactions: HashSet::new(),
            transaction_start_times: HashMap::new(),
        }
    }

    /// Register a new transaction
    pub fn register_transaction(&mut self, tx_id: HlcTimestamp, start_time: HlcTimestamp) {
        self.transaction_start_times.insert(tx_id, start_time);
    }

    /// Mark a transaction as committed
    pub fn commit_transaction(&mut self, tx_id: HlcTimestamp) {
        self.committed_transactions.insert(tx_id);
        self.transaction_start_times.remove(&tx_id);
    }

    /// Abort a transaction by removing all its versions
    pub fn abort_transaction(&mut self, tx_id: HlcTimestamp) {
        // Remove all versions created by this transaction
        for versions in self.versions.values_mut() {
            // Remove versions created by this transaction
            versions.retain(|v| v.created_by != tx_id);

            // Clear deletion marks by this transaction
            for version in versions.iter_mut() {
                if version.deleted_by == Some(tx_id) {
                    version.deleted_by = None;
                }
            }
        }

        // Clean up empty version lists
        self.versions.retain(|_, versions| !versions.is_empty());

        // Remove transaction metadata
        self.transaction_start_times.remove(&tx_id);
    }

    /// Get the visible version of a key for a transaction
    pub fn get(&self, key: &str, tx_id: HlcTimestamp) -> Option<&Value> {
        let versions = self.versions.get(key)?;
        let tx_start = self
            .transaction_start_times
            .get(&tx_id)
            .copied()
            .unwrap_or(tx_id);

        // Find the latest visible version
        self.find_visible_version(versions, tx_id, tx_start)
            .map(|v| &v.value)
    }

    /// Put a new value for a key
    pub fn put(&mut self, key: String, value: Value, tx_id: HlcTimestamp, timestamp: HlcTimestamp) {
        let tx_start = self
            .transaction_start_times
            .get(&tx_id)
            .copied()
            .unwrap_or(tx_id);

        // Check if we need to mark any existing version as deleted
        // We need to capture the committed_transactions state for visibility check
        let committed_transactions = self.committed_transactions.clone();

        if let Some(versions) = self.versions.get_mut(&key) {
            // Find and mark the current visible version as deleted
            for version in versions.iter_mut().rev() {
                // Inline visibility check to avoid borrow issues
                let created_visible = version.created_by == tx_id
                    || (committed_transactions.contains(&version.created_by)
                        && version.created_at <= tx_start);

                let not_deleted = version.deleted_by.is_none()
                    || (version.deleted_by.is_some()
                        && version.deleted_by != Some(tx_id)  // We don't see our own deletes
                        && !committed_transactions.contains(&version.deleted_by.unwrap()));

                if created_visible && not_deleted {
                    version.deleted_by = Some(tx_id);
                    break;
                }
            }
        }

        // Add the new version
        let new_version = VersionedValue {
            value,
            created_by: tx_id,
            created_at: timestamp,
            deleted_by: None,
        };

        self.versions
            .entry(key)
            .or_insert_with(Vec::new)
            .push(new_version);
    }

    /// Delete a key
    pub fn delete(&mut self, key: &str, tx_id: HlcTimestamp) {
        let tx_start = self
            .transaction_start_times
            .get(&tx_id)
            .copied()
            .unwrap_or(tx_id);

        // Capture committed_transactions for visibility check
        let committed_transactions = self.committed_transactions.clone();

        if let Some(versions) = self.versions.get_mut(key) {
            // Find and mark the current visible version as deleted
            for version in versions.iter_mut().rev() {
                // Inline visibility check to avoid borrow issues
                let created_visible = version.created_by == tx_id
                    || (committed_transactions.contains(&version.created_by)
                        && version.created_at <= tx_start);

                let not_deleted = version.deleted_by.is_none()
                    || (version.deleted_by.is_some()
                        && version.deleted_by != Some(tx_id)  // We don't see our own deletes
                        && !committed_transactions.contains(&version.deleted_by.unwrap()));

                if created_visible && not_deleted {
                    version.deleted_by = Some(tx_id);
                    break;
                }
            }
        }
    }

    /// Check if a key exists for a transaction
    pub fn exists(&self, key: &str, tx_id: HlcTimestamp) -> bool {
        if let Some(versions) = self.versions.get(key) {
            let tx_start = self
                .transaction_start_times
                .get(&tx_id)
                .copied()
                .unwrap_or(tx_id);
            self.find_visible_version(versions, tx_id, tx_start)
                .is_some()
        } else {
            false
        }
    }

    /// Get all keys visible to a transaction
    pub fn scan_keys(&self, tx_id: HlcTimestamp) -> Vec<String> {
        let tx_start = self
            .transaction_start_times
            .get(&tx_id)
            .copied()
            .unwrap_or(tx_id);

        let mut keys = Vec::new();
        for (key, versions) in &self.versions {
            if self
                .find_visible_version(versions, tx_id, tx_start)
                .is_some()
            {
                keys.push(key.clone());
            }
        }
        keys.sort(); // Return in deterministic order
        keys
    }

    /// Find the visible version for a transaction
    fn find_visible_version<'a>(
        &self,
        versions: &'a [VersionedValue],
        tx_id: HlcTimestamp,
        tx_start: HlcTimestamp,
    ) -> Option<&'a VersionedValue> {
        // Iterate backwards to find the latest visible version
        for version in versions.iter().rev() {
            if self.is_version_visible(version, tx_id, tx_start) {
                return Some(version);
            }
        }
        None
    }

    /// Check if a version is visible to a transaction
    fn is_version_visible(
        &self,
        version: &VersionedValue,
        tx_id: HlcTimestamp,
        tx_start: HlcTimestamp,
    ) -> bool {
        // Version is visible if:
        // 1. Created by current transaction OR created by committed transaction before our start
        let created_visible = version.created_by == tx_id
            || (self.committed_transactions.contains(&version.created_by)
                && version.created_at <= tx_start);

        // 2. Not deleted OR deleted by uncommitted transaction (but NOT by us)
        let not_deleted = version.deleted_by.is_none()
            || (version.deleted_by.is_some()
                && version.deleted_by != Some(tx_id)  // We don't see our own deletes
                && !self
                    .committed_transactions
                    .contains(&version.deleted_by.unwrap()));

        created_visible && not_deleted
    }

    /// Get statistics about the storage
    pub fn stats(&self) -> StorageStats {
        let total_keys = self.versions.len();
        let total_versions: usize = self.versions.values().map(|v| v.len()).sum();
        let committed_txns = self.committed_transactions.len();
        let active_txns = self.transaction_start_times.len();

        StorageStats {
            total_keys,
            total_versions,
            committed_txns,
            active_txns,
        }
    }
}

impl Default for MvccStorage {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the MVCC storage
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub total_keys: usize,
    pub total_versions: usize,
    pub committed_txns: usize,
    pub active_txns: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_timestamp(seconds: u64) -> HlcTimestamp {
        use proven_hlc::NodeId;
        HlcTimestamp::new(seconds, 0, NodeId::new(1))
    }

    #[test]
    fn test_basic_put_get() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp(100);

        storage.register_transaction(tx1, tx1);
        storage.put(
            "key1".to_string(),
            Value::String("value1".to_string()),
            tx1,
            tx1,
        );

        // Should see our own write
        assert_eq!(
            storage.get("key1", tx1),
            Some(&Value::String("value1".to_string()))
        );

        // Other transaction shouldn't see uncommitted write
        let tx2 = create_timestamp(200);
        storage.register_transaction(tx2, tx2);
        assert_eq!(storage.get("key1", tx2), None);

        // After commit, other transaction should see it
        storage.commit_transaction(tx1);
        assert_eq!(
            storage.get("key1", tx2),
            Some(&Value::String("value1".to_string()))
        );
    }

    #[test]
    fn test_abort_rollback() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp(100);
        let tx2 = create_timestamp(200);

        // tx1 writes and commits
        storage.register_transaction(tx1, tx1);
        storage.put(
            "key1".to_string(),
            Value::String("value1".to_string()),
            tx1,
            tx1,
        );
        storage.commit_transaction(tx1);

        // tx2 overwrites but aborts
        storage.register_transaction(tx2, tx2);
        storage.put(
            "key1".to_string(),
            Value::String("value2".to_string()),
            tx2,
            tx2,
        );

        // tx2 sees its own write
        assert_eq!(
            storage.get("key1", tx2),
            Some(&Value::String("value2".to_string()))
        );

        // Abort tx2
        storage.abort_transaction(tx2);

        // New transaction should see original value
        let tx3 = create_timestamp(300);
        storage.register_transaction(tx3, tx3);
        assert_eq!(
            storage.get("key1", tx3),
            Some(&Value::String("value1".to_string()))
        );
    }

    #[test]
    fn test_delete() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp(100);
        let tx2 = create_timestamp(200);

        // tx1 writes and commits
        storage.register_transaction(tx1, tx1);
        storage.put(
            "key1".to_string(),
            Value::String("value1".to_string()),
            tx1,
            tx1,
        );
        storage.commit_transaction(tx1);

        // tx2 deletes
        storage.register_transaction(tx2, tx2);
        storage.delete("key1", tx2);

        // tx2 shouldn't see the key
        assert!(!storage.exists("key1", tx2));

        // Other transactions should still see it until commit
        let tx3 = create_timestamp(300);
        storage.register_transaction(tx3, tx3);
        assert!(storage.exists("key1", tx3));

        // After commit, key should be gone
        storage.commit_transaction(tx2);
        let tx4 = create_timestamp(400);
        storage.register_transaction(tx4, tx4);
        assert!(!storage.exists("key1", tx4));
    }
}
