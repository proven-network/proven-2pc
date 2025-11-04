//! Uncommitted delta store for active transactions
//!
//! Stores uncommitted deltas per transaction in a single partition, providing:
//! - Read-your-own-writes semantics
//! - Fast abort (prefix delete by transaction ID)
//! - Crash recovery (persisted to disk)
//!
//! Cleanup strategy: Transaction lifecycle (commit/abort) handles cleanup.
//! Orphaned data only persists after crashes and is cleaned during recovery.

use crate::encoding::{Decode, Encode};
use crate::entity::{MvccDelta, MvccEntity};
use crate::error::Result;
use fjall::{Batch, PartitionHandle};
use proven_common::TransactionId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Uncommitted delta store using a single partition
pub struct UncommittedStore<E: MvccEntity> {
    partition: PartitionHandle,
    /// Sequence number for ordering deltas within a transaction
    /// In-memory only, resets on restart (fine for uncommitted data)
    next_seq: AtomicU64,
    _phantom: std::marker::PhantomData<E>,
}

impl<E: MvccEntity> UncommittedStore<E> {
    /// Create a new uncommitted store with a single partition
    pub fn new(partition: PartitionHandle) -> Self {
        Self {
            partition,
            next_seq: AtomicU64::new(0),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Encode key for uncommitted delta: {txn_id(16)}{key_bytes}{seq(8)}
    fn encode_key(txn_id: TransactionId, key_bytes: &[u8], seq: u64) -> Vec<u8> {
        let mut encoded = Vec::new();
        // Txn ID first for efficient prefix scans (16 bytes for UUIDv7)
        encoded.extend_from_slice(&txn_id.to_bytes());
        // Key bytes (varies by entity)
        encoded.extend_from_slice(key_bytes);
        // Sequence number to maintain delta order
        encoded.extend_from_slice(&seq.to_be_bytes());
        encoded
    }

    /// Encode prefix for scanning transaction deltas
    fn encode_tx_prefix(txn_id: TransactionId) -> Vec<u8> {
        txn_id.to_bytes().to_vec()
    }

    /// Add a delta to a batch (for atomic writes)
    pub fn add_delta_to_batch(
        &mut self,
        batch: &mut Batch,
        txn_id: TransactionId,
        delta: E::Delta,
    ) -> Result<()> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);

        // Encode key and delta
        let key_bytes = delta.key().encode()?;
        let encoded_key = Self::encode_key(txn_id, &key_bytes, seq);
        let encoded_delta = delta.encode()?;

        // Add to batch (single partition)
        batch.insert(&self.partition, encoded_key, encoded_delta);

        Ok(())
    }

    /// Get the current value for a key in a transaction
    ///
    /// Returns:
    /// - `Some(value)` if the key exists with uncommitted changes
    /// - `None` if the key was deleted or has no uncommitted changes
    pub fn get(&self, txn_id: TransactionId, key: &E::Key) -> Result<Option<E::Value>> {
        // Scan for deltas affecting this key
        let prefix = Self::encode_tx_prefix(txn_id);
        let key_bytes = key.encode()?;

        let mut current_value: Option<E::Value> = None;
        let mut has_deltas = false;

        for result in self.partition.prefix(&prefix) {
            let (_key_bytes, value_bytes) = result?;
            let delta = E::Delta::decode(&value_bytes)?;

            // Check if this delta affects our key
            if delta.key().encode()? == key_bytes {
                has_deltas = true;
                // Apply delta forward (deltas are ordered by seq)
                current_value = delta.apply(current_value);
            }
        }

        if has_deltas {
            Ok(current_value)
        } else {
            // No deltas for this key in uncommitted store
            // Caller should check committed data
            Ok(None)
        }
    }

    /// Get all deltas for a transaction
    pub fn get_transaction_deltas(&self, txn_id: TransactionId) -> Result<Vec<E::Delta>> {
        let prefix = Self::encode_tx_prefix(txn_id);
        let mut deltas = Vec::new();

        for result in self.partition.prefix(&prefix) {
            let (_key, value_bytes) = result?;
            let delta = E::Delta::decode(&value_bytes)?;
            deltas.push(delta);
        }

        Ok(deltas)
    }

    /// Get all keys with uncommitted changes in a transaction
    ///
    /// Returns a map of key -> current value after applying all deltas
    pub fn get_transaction_state(
        &self,
        txn_id: TransactionId,
    ) -> Result<HashMap<Vec<u8>, Option<E::Value>>> {
        let prefix = Self::encode_tx_prefix(txn_id);

        // Map of encoded key -> deltas
        let mut key_deltas: HashMap<Vec<u8>, Vec<E::Delta>> = HashMap::new();

        for result in self.partition.prefix(&prefix) {
            let (_key_bytes, value_bytes) = result?;
            let delta = E::Delta::decode(&value_bytes)?;
            let key_bytes = delta.key().encode()?;

            key_deltas.entry(key_bytes).or_default().push(delta);
        }

        // Apply deltas to get final state
        let mut state = HashMap::new();
        for (key_bytes, deltas) in key_deltas {
            let mut current = None;
            for delta in deltas {
                current = delta.apply(current);
            }
            state.insert(key_bytes, current);
        }

        Ok(state)
    }

    /// Remove transaction's uncommitted deltas from a batch
    pub fn remove_transaction(&mut self, batch: &mut Batch, txn_id: TransactionId) -> Result<()> {
        let prefix = Self::encode_tx_prefix(txn_id);

        // Collect all keys to delete
        let mut keys_to_delete = Vec::new();
        for result in self.partition.prefix(&prefix) {
            let (key, _) = result?;
            keys_to_delete.push(key.to_vec());
        }

        // Add deletions to batch
        for key in keys_to_delete {
            batch.remove(self.partition.clone(), key);
        }

        Ok(())
    }

    /// Cleanup orphaned transactions (after crash recovery)
    ///
    /// Removes uncommitted data for transactions older than the given transaction ID.
    /// This should be called during recovery after identifying active transactions.
    pub fn cleanup_before(
        &mut self,
        batch: &mut Batch,
        oldest_txn_id: TransactionId,
    ) -> Result<usize> {
        let cutoff_bytes = oldest_txn_id.to_bytes();

        // Scan all keys and remove those with txn_id < oldest_txn_id
        let mut keys_to_delete = Vec::new();
        for result in self.partition.iter() {
            let (key, _) = result?;
            // Extract txn_id from key (first 16 bytes)
            if key.len() >= 16 {
                let txn_bytes = &key[0..16];
                if txn_bytes < cutoff_bytes.as_slice() {
                    keys_to_delete.push(key.to_vec());
                }
            }
        }

        let removed_count = keys_to_delete.len();
        for key in keys_to_delete {
            batch.remove(self.partition.clone(), key);
        }

        Ok(removed_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{Decode, Encode};
    use proven_common::TransactionId;

    // Implement Encode/Decode for String (for tests)
    impl Encode for String {
        fn encode(&self) -> Result<Vec<u8>> {
            Ok(self.as_bytes().to_vec())
        }
    }

    impl Decode for String {
        fn decode(bytes: &[u8]) -> Result<Self> {
            String::from_utf8(bytes.to_vec()).map_err(|e| crate::Error::Encoding(e.to_string()))
        }
    }

    // Test entity
    struct TestEntity;

    impl MvccEntity for TestEntity {
        type Key = String;
        type Value = String;
        type Delta = TestDelta;

        fn entity_name() -> &'static str {
            "test"
        }
    }

    #[derive(Clone)]
    enum TestDelta {
        Put { key: String, value: String },
        Delete { key: String },
    }

    impl Encode for TestDelta {
        fn encode(&self) -> Result<Vec<u8>> {
            match self {
                TestDelta::Put { key, value } => {
                    let mut buf = vec![0u8]; // Put tag
                    buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
                    buf.extend_from_slice(key.as_bytes());
                    buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
                    buf.extend_from_slice(value.as_bytes());
                    Ok(buf)
                }
                TestDelta::Delete { key } => {
                    let mut buf = vec![1u8]; // Delete tag
                    buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
                    buf.extend_from_slice(key.as_bytes());
                    Ok(buf)
                }
            }
        }
    }

    impl Decode for TestDelta {
        fn decode(bytes: &[u8]) -> Result<Self> {
            let tag = bytes[0];
            let mut pos = 1;

            let key_len =
                u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                    as usize;
            pos += 4;
            let key = String::from_utf8(bytes[pos..pos + key_len].to_vec())
                .map_err(|e| crate::Error::Encoding(e.to_string()))?;
            pos += key_len;

            match tag {
                0 => {
                    let value_len = u32::from_be_bytes([
                        bytes[pos],
                        bytes[pos + 1],
                        bytes[pos + 2],
                        bytes[pos + 3],
                    ]) as usize;
                    pos += 4;
                    let value = String::from_utf8(bytes[pos..pos + value_len].to_vec())
                        .map_err(|e| crate::Error::Encoding(e.to_string()))?;
                    Ok(TestDelta::Put { key, value })
                }
                1 => Ok(TestDelta::Delete { key }),
                _ => Err(crate::Error::Encoding("Invalid tag".to_string())),
            }
        }
    }

    impl MvccDelta<TestEntity> for TestDelta {
        fn key(&self) -> String {
            match self {
                TestDelta::Put { key, .. } => key.clone(),
                TestDelta::Delete { key } => key.clone(),
            }
        }

        fn apply(&self, _current: Option<String>) -> Option<String> {
            match self {
                TestDelta::Put { value, .. } => Some(value.clone()),
                TestDelta::Delete { .. } => None,
            }
        }

        fn unapply(&self, _current: Option<String>) -> Option<String> {
            match self {
                TestDelta::Put { .. } => None,
                TestDelta::Delete { .. } => None,
            }
        }

        fn merge(self, next: Self) -> Self {
            // For test delta, last write wins
            next
        }
    }

    #[test]
    fn test_uncommitted_put_get() -> Result<()> {
        use fjall::PartitionCreateOptions;

        let temp_dir = tempfile::tempdir().unwrap();
        let keyspace = fjall::Config::new(temp_dir.path()).open()?;

        let partition =
            keyspace.open_partition("test_uncommitted", PartitionCreateOptions::default())?;

        let mut store = UncommittedStore::<TestEntity>::new(partition);

        // Create a TransactionId for testing
        let txn_id = TransactionId::new();
        let mut batch = keyspace.batch();

        // Add a put delta
        let delta = TestDelta::Put {
            key: "key1".to_string(),
            value: "value1".to_string(),
        };
        store.add_delta_to_batch(&mut batch, txn_id, delta)?;
        batch.commit()?;

        // Read back
        let result = store.get(txn_id, &"key1".to_string())?;
        assert_eq!(result, Some("value1".to_string()));

        Ok(())
    }
}
