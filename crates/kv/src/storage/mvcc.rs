//! Multi-Version Concurrency Control (MVCC) for KV storage
//!
//! Provides transaction isolation through versioning of key-value pairs,
//! enabling quick aborts and consistent reads.

use crate::types::Value;
use proven_hlc::HlcTimestamp;
use std::collections::HashMap;
use std::sync::Arc;

/// A versioned value in storage
#[derive(Debug, Clone)]
pub struct VersionedValue {
    /// The actual value (wrapped in Arc for cheap clones)
    pub value: Arc<Value>,
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
    /// Committed versions only (for reads)
    /// Versions are ordered by creation time (newest last)
    committed_versions: HashMap<String, Vec<VersionedValue>>,

    /// Pending writes by transaction (for conflict detection and own-reads)
    /// Structure: tx_id -> (key -> value)
    pending_writes: HashMap<HlcTimestamp, HashMap<String, Arc<Value>>>,

    /// Pending deletes by transaction
    /// Structure: tx_id -> set of keys to delete
    pending_deletes: HashMap<HlcTimestamp, Vec<String>>,
}

impl MvccStorage {
    /// Create a new MVCC storage
    pub fn new() -> Self {
        Self {
            committed_versions: HashMap::new(),
            pending_writes: HashMap::new(),
            pending_deletes: HashMap::new(),
        }
    }

    /// Mark a transaction as committed
    pub fn commit_transaction(&mut self, tx_id: HlcTimestamp) {
        // Move pending writes to committed versions
        if let Some(writes) = self.pending_writes.remove(&tx_id) {
            for (key, value) in writes {
                let version = VersionedValue {
                    value,
                    created_by: tx_id,
                    created_at: tx_id, // Using tx_id as timestamp
                    deleted_by: None,
                };

                self.committed_versions
                    .entry(key)
                    .or_default()
                    .push(version);
            }
        }

        // Process pending deletes
        if let Some(deletes) = self.pending_deletes.remove(&tx_id) {
            for key in deletes {
                if let Some(versions) = self.committed_versions.get_mut(&key) {
                    // Mark the latest version as deleted
                    if let Some(last) = versions.last_mut()
                        && last.deleted_by.is_none()
                    {
                        last.deleted_by = Some(tx_id);
                    }
                }
            }
        }
    }

    /// Abort a transaction by removing all its pending changes
    pub fn abort_transaction(&mut self, tx_id: HlcTimestamp) {
        // Just remove from pending - super cheap!
        self.pending_writes.remove(&tx_id);
        self.pending_deletes.remove(&tx_id);
    }

    /// Get the visible version of a key for a transaction
    pub fn get(&self, key: &str, tx_id: HlcTimestamp) -> Option<Arc<Value>> {
        // Check if we're planning to delete this key
        if let Some(deletes) = self.pending_deletes.get(&tx_id)
            && deletes.contains(&key.to_string())
        {
            return None;
        }

        // Check own writes first
        if let Some(pending) = self.pending_writes.get(&tx_id)
            && let Some(value) = pending.get(key)
        {
            return Some(value.clone());
        }

        // Then check committed versions visible to this transaction
        self.get_committed_for_transaction(key, tx_id)
    }

    /// Put a new value for a key
    pub fn put(
        &mut self,
        key: String,
        value: Value,
        tx_id: HlcTimestamp,
        _timestamp: HlcTimestamp,
    ) {
        // Just add to pending writes
        self.pending_writes
            .entry(tx_id)
            .or_default()
            .insert(key, Arc::new(value));
    }

    /// Delete a key
    pub fn delete(&mut self, key: &str, tx_id: HlcTimestamp) {
        // Remove from pending writes if it exists
        if let Some(pending) = self.pending_writes.get_mut(&tx_id) {
            pending.remove(key);
        }

        // Add to pending deletes
        self.pending_deletes
            .entry(tx_id)
            .or_default()
            .push(key.to_string());
    }

    /// Get visible committed version for a transaction
    fn get_committed_for_transaction(&self, key: &str, tx_id: HlcTimestamp) -> Option<Arc<Value>> {
        self.committed_versions
            .get(key)?
            .iter()
            .rev()
            .find(|v| {
                // Version is visible if created before our transaction started (tx_id IS the start time)
                // and not deleted (or deleted after we started)
                v.created_at <= tx_id
                    && (v.deleted_by.is_none()
                        || v.deleted_by.unwrap() > tx_id
                        || v.deleted_by.unwrap() == tx_id)
            })
            .map(|v| v.value.clone())
    }

    /// Get compacted data for snapshot
    pub fn get_compacted_data(&self) -> HashMap<String, Value> {
        let mut result = HashMap::new();

        for (key, versions) in &self.committed_versions {
            // Get the latest non-deleted version
            if let Some(version) = versions.iter().rev().find(|v| v.deleted_by.is_none()) {
                result.insert(key.clone(), (*version.value).clone());
            }
        }

        result
    }

    /// Restore from compacted snapshot data
    pub fn restore_from_compacted(&mut self, data: HashMap<String, Value>) {
        // Clear current state
        self.committed_versions.clear();
        self.pending_writes.clear();
        self.pending_deletes.clear();

        // Restore as committed versions with timestamp 0
        for (key, value) in data {
            let version = VersionedValue {
                value: Arc::new(value),
                created_by: HlcTimestamp::new(0, 0, proven_hlc::NodeId::new(0)),
                created_at: HlcTimestamp::new(0, 0, proven_hlc::NodeId::new(0)),
                deleted_by: None,
            };

            self.committed_versions.insert(key, vec![version]);
        }
    }

    /// Check if there are pending writes from a transaction for a key
    pub fn has_pending_write(&self, tx_id: &HlcTimestamp, key: &str) -> bool {
        self.pending_writes
            .get(tx_id)
            .is_some_and(|writes| writes.contains_key(key))
    }

    /// Get all transactions with pending writes for a key
    pub fn get_pending_writers(&self, key: &str) -> Vec<HlcTimestamp> {
        self.pending_writes
            .iter()
            .filter_map(|(tx_id, writes)| {
                if writes.contains_key(key) {
                    Some(*tx_id)
                } else {
                    None
                }
            })
            .collect()
    }
}

impl Default for MvccStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::NodeId;

    fn create_timestamp(val: u64) -> HlcTimestamp {
        HlcTimestamp::new(val, 0, NodeId::new(1))
    }

    #[test]
    fn test_basic_put_get() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp(100);

        storage.put(
            "key1".to_string(),
            Value::String("value1".to_string()),
            tx1,
            tx1,
        );

        // Should see own write before commit
        let result = storage.get("key1", tx1);
        assert_eq!(result, Some(Arc::new(Value::String("value1".to_string()))));

        // Commit and verify
        storage.commit_transaction(tx1);
        let result = storage.get("key1", tx1);
        assert_eq!(result, Some(Arc::new(Value::String("value1".to_string()))));
    }

    #[test]
    fn test_abort_removes_changes() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp(100);

        storage.put(
            "key1".to_string(),
            Value::String("value1".to_string()),
            tx1,
            tx1,
        );

        // Abort the transaction
        storage.abort_transaction(tx1);

        // Should not see aborted write
        let tx2 = create_timestamp(200);
        let result = storage.get("key1", tx2);
        assert_eq!(result, None);
    }

    #[test]
    fn test_snapshot_read_isolation() {
        let mut storage = MvccStorage::new();

        // Commit a value at timestamp 100
        let tx1 = create_timestamp(100);
        storage.put(
            "key1".to_string(),
            Value::String("value1".to_string()),
            tx1,
            tx1,
        );
        storage.commit_transaction(tx1);

        // Commit another value at timestamp 300
        let tx2 = create_timestamp(300);
        storage.put(
            "key1".to_string(),
            Value::String("value2".to_string()),
            tx2,
            tx2,
        );
        storage.commit_transaction(tx2);

        // Read at timestamp 200 should see value1
        let read_ts1 = create_timestamp(200);
        let result = storage.get("key1", read_ts1);
        assert_eq!(result, Some(Arc::new(Value::String("value1".to_string()))));

        // Read at timestamp 400 should see value2
        let read_ts2 = create_timestamp(400);
        let result = storage.get("key1", read_ts2);
        assert_eq!(result, Some(Arc::new(Value::String("value2".to_string()))));
    }

    #[test]
    fn test_delete_operation() {
        let mut storage = MvccStorage::new();

        // First transaction: create a value
        let tx1 = create_timestamp(100);
        storage.put(
            "key1".to_string(),
            Value::String("value1".to_string()),
            tx1,
            tx1,
        );
        storage.commit_transaction(tx1);

        // Second transaction: delete the value
        let tx2 = create_timestamp(200);
        storage.delete("key1", tx2);

        // Should not see deleted value in same transaction
        let result = storage.get("key1", tx2);
        assert_eq!(result, None);

        // Commit the delete
        storage.commit_transaction(tx2);

        // Future reads should not see the deleted value
        let tx3 = create_timestamp(300);
        let result = storage.get("key1", tx3);
        assert_eq!(result, None);

        // But reads at earlier timestamps should still see it
        let read_ts = create_timestamp(150);
        let result = storage.get("key1", read_ts);
        assert_eq!(result, Some(Arc::new(Value::String("value1".to_string()))));
    }

    #[test]
    fn test_pending_writes_check() {
        let mut storage = MvccStorage::new();
        let tx1 = create_timestamp(100);

        storage.put(
            "key1".to_string(),
            Value::String("value1".to_string()),
            tx1,
            tx1,
        );

        // Check pending write exists
        assert!(storage.has_pending_write(&tx1, "key1"));
        assert!(!storage.has_pending_write(&tx1, "key2"));

        // Check pending writers
        let writers = storage.get_pending_writers("key1");
        assert_eq!(writers.len(), 1);
        assert_eq!(writers[0], tx1);

        // After commit, no pending writes
        storage.commit_transaction(tx1);
        assert!(!storage.has_pending_write(&tx1, "key1"));
        let writers = storage.get_pending_writers("key1");
        assert_eq!(writers.len(), 0);
    }
}
