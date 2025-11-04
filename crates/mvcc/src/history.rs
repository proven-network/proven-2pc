//! History store for committed deltas (time-travel support)
//!
//! Stores committed deltas in a single partition, enabling:
//! - Snapshot isolation (read at any past timestamp)
//! - Time-travel queries
//! - MVCC reconstruction via delta apply/unapply
//!
//! Cleanup strategy: Deltas are removed when they're older than the oldest
//! active transaction, ensuring correct snapshot isolation semantics.

use crate::encoding::{Decode, Encode};
use crate::entity::{MvccDelta, MvccEntity};
use crate::error::Result;
use fjall::{Batch, PartitionHandle};
use proven_common::TransactionId;
use std::collections::BTreeMap;

/// History store for committed deltas using a single partition
pub struct HistoryStore<E: MvccEntity> {
    partition: PartitionHandle,
    _phantom: std::marker::PhantomData<E>,
}

impl<E: MvccEntity> HistoryStore<E> {
    /// Create a new history store with a single partition
    pub fn new(partition: PartitionHandle) -> Self {
        Self {
            partition,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Check if the store is empty (optimization for reads)
    pub fn is_empty(&self) -> bool {
        // Check if partition has any keys
        self.partition.iter().next().is_none()
    }

    /// Encode key: {commit_txn_id(16)}{key_bytes}{seq(4)}
    fn encode_key(commit_txn_id: TransactionId, key_bytes: &[u8], seq: u32) -> Vec<u8> {
        let mut encoded = Vec::new();
        // Transaction ID first for efficient range scans (16 bytes for UUIDv7)
        // This ensures consistency with wound-wait ordering
        encoded.extend_from_slice(&commit_txn_id.to_bytes());
        // Key bytes
        encoded.extend_from_slice(key_bytes);
        // Sequence number
        encoded.extend_from_slice(&seq.to_be_bytes());
        encoded
    }

    /// Add committed deltas to a batch
    ///
    /// Deltas are stored with commit_txn_id-first keys for efficient
    /// transaction-ordered queries during snapshot reads.
    pub fn add_committed_deltas_to_batch(
        &mut self,
        batch: &mut Batch,
        commit_txn_id: TransactionId,
        deltas: Vec<E::Delta>,
    ) -> Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }

        // Add each delta with sequential numbering (single partition)
        for (seq, delta) in deltas.into_iter().enumerate() {
            let key_bytes = delta.key().encode()?;
            let encoded_key = Self::encode_key(commit_txn_id, &key_bytes, seq as u32);
            let encoded_delta = delta.encode()?;

            batch.insert(&self.partition, encoded_key, encoded_delta);
        }

        Ok(())
    }

    /// Get deltas committed after a snapshot transaction
    ///
    /// Returns a map of encoded_key -> deltas that were committed after the snapshot.
    /// These deltas need to be unapplied to reconstruct the snapshot state.
    pub fn get_deltas_after(
        &self,
        snapshot_txn_id: TransactionId,
    ) -> Result<BTreeMap<Vec<u8>, Vec<E::Delta>>> {
        let mut result: BTreeMap<Vec<u8>, Vec<E::Delta>> = BTreeMap::new();
        let snapshot_bytes = snapshot_txn_id.to_bytes();

        // Scan partition for deltas after snapshot (single partition range scan)
        for entry in self.partition.range(snapshot_bytes.to_vec()..) {
            let (_key, value_bytes) = entry?;
            let delta = E::Delta::decode(&value_bytes)?;
            let key_bytes = delta.key().encode()?;

            result.entry(key_bytes).or_default().push(delta);
        }

        Ok(result)
    }

    /// Get deltas for a specific key committed after snapshot transaction
    ///
    /// Note: Currently reuses get_deltas_after for simplicity. This scans all
    /// history deltas and extracts one key. For better performance with many
    /// point reads, consider caching the full map at the storage level, or
    /// restructure history to support key-based lookups (requires trade-offs
    /// in time-bucketing efficiency).
    pub fn get_key_deltas_after(
        &self,
        key: &E::Key,
        snapshot_txn_id: TransactionId,
    ) -> Result<Vec<E::Delta>> {
        let key_bytes = key.encode()?;
        let all_deltas = self.get_deltas_after(snapshot_txn_id)?;

        Ok(all_deltas.get(&key_bytes).cloned().unwrap_or_default())
    }

    /// Cleanup history deltas older than oldest active transaction
    ///
    /// This ensures that no active transaction can reference deleted deltas,
    /// maintaining snapshot isolation guarantees.
    pub fn cleanup_before(
        &mut self,
        batch: &mut Batch,
        oldest_txn_id: TransactionId,
    ) -> Result<usize> {
        let cutoff_bytes = oldest_txn_id.to_bytes();

        // Scan for keys older than cutoff
        let mut keys_to_delete = Vec::new();
        for result in self.partition.range(..cutoff_bytes.to_vec()) {
            let (key, _) = result?;
            keys_to_delete.push(key.to_vec());
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
    use fjall::PartitionCreateOptions;
    use proven_common::TransactionId;

    // Reuse test types from uncommitted tests
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
        Put {
            key: String,
            value: String,
            old_value: Option<String>,
        },
    }

    impl Encode for TestDelta {
        fn encode(&self) -> Result<Vec<u8>> {
            match self {
                TestDelta::Put {
                    key,
                    value,
                    old_value,
                } => {
                    let mut buf = vec![0u8]; // Put tag
                    buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
                    buf.extend_from_slice(key.as_bytes());
                    buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
                    buf.extend_from_slice(value.as_bytes());
                    if let Some(old) = old_value {
                        buf.push(1);
                        buf.extend_from_slice(&(old.len() as u32).to_be_bytes());
                        buf.extend_from_slice(old.as_bytes());
                    } else {
                        buf.push(0);
                    }
                    Ok(buf)
                }
            }
        }
    }

    impl Decode for TestDelta {
        fn decode(_bytes: &[u8]) -> Result<Self> {
            // Simplified for tests
            Ok(TestDelta::Put {
                key: "test".to_string(),
                value: "test".to_string(),
                old_value: None,
            })
        }
    }

    impl MvccDelta<TestEntity> for TestDelta {
        fn key(&self) -> String {
            match self {
                TestDelta::Put { key, .. } => key.clone(),
            }
        }

        fn apply(&self, _current: Option<String>) -> Option<String> {
            match self {
                TestDelta::Put { value, .. } => Some(value.clone()),
            }
        }

        fn unapply(&self, _current: Option<String>) -> Option<String> {
            match self {
                TestDelta::Put { old_value, .. } => old_value.clone(),
            }
        }

        fn merge(self, next: Self) -> Self {
            // For test delta, last write wins
            next
        }
    }

    #[test]
    fn test_history_add_and_get() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyspace = fjall::Config::new(temp_dir.path()).open()?;

        let partition =
            keyspace.open_partition("test_history", PartitionCreateOptions::default())?;

        let mut store = HistoryStore::<TestEntity>::new(partition);

        // Add some deltas with a transaction ID
        let commit_txn_id = TransactionId::new();
        let deltas = vec![TestDelta::Put {
            key: "key1".to_string(),
            value: "value1".to_string(),
            old_value: None,
        }];

        let mut batch = keyspace.batch();
        store.add_committed_deltas_to_batch(&mut batch, commit_txn_id, deltas)?;
        batch.commit()?;

        // Use a very old transaction ID for snapshot query (all zeros - earlier than any real UUIDv7)
        // This ensures we find the delta committed by commit_txn_id
        let snapshot_txn_id = TransactionId::from_bytes([0u8; 16]);
        let deltas_after = store.get_deltas_after(snapshot_txn_id)?;

        assert!(!deltas_after.is_empty());

        Ok(())
    }
}
