//! History store for committed deltas (time-travel support)
//!
//! Stores committed deltas within a retention window, enabling:
//! - Snapshot isolation (read at any past timestamp)
//! - Time-travel queries
//! - MVCC reconstruction via delta apply/unapply
//!
//! Uses time-bucketed partitions for O(1) cleanup.

use crate::bucket_manager::BucketManager;
use crate::encoding::{Decode, Encode};
use crate::entity::{MvccDelta, MvccEntity};
use crate::error::Result;
use fjall::Batch;
use proven_hlc::HlcTimestamp;
use std::collections::BTreeMap;
use std::time::Duration;

/// History store for committed deltas
pub struct HistoryStore<E: MvccEntity> {
    bucket_manager: BucketManager,
    retention_window: Duration,
    _phantom: std::marker::PhantomData<E>,
}

impl<E: MvccEntity> HistoryStore<E> {
    /// Create a new history store
    pub fn new(bucket_manager: BucketManager, retention_window: Duration) -> Self {
        Self {
            bucket_manager,
            retention_window,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Check if the store is empty (optimization for reads)
    pub fn is_empty(&self) -> bool {
        !self.bucket_manager.has_partitions()
    }

    /// Encode key: {commit_time(20)}{key_bytes}{seq(4)}
    fn encode_key(commit_time: HlcTimestamp, key_bytes: &[u8], seq: u32) -> Vec<u8> {
        let mut encoded = Vec::new();
        // Commit time first for efficient range scans
        encoded.extend_from_slice(&commit_time.to_lexicographic_bytes());
        // Key bytes
        encoded.extend_from_slice(key_bytes);
        // Sequence number
        encoded.extend_from_slice(&seq.to_be_bytes());
        encoded
    }

    /// Add committed deltas to a batch
    ///
    /// Deltas are stored with commit_time-first keys for efficient
    /// time-range queries during snapshot reads.
    pub fn add_committed_deltas_to_batch(
        &mut self,
        batch: &mut Batch,
        commit_time: HlcTimestamp,
        deltas: Vec<E::Delta>,
    ) -> Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }

        // Get or create partition for this time bucket
        let partition = self
            .bucket_manager
            .get_or_create_partition(E::entity_name(), commit_time)?;

        // Add each delta with sequential numbering
        for (seq, delta) in deltas.into_iter().enumerate() {
            let key_bytes = delta.key().encode()?;
            let encoded_key = Self::encode_key(commit_time, &key_bytes, seq as u32);
            let encoded_delta = delta.encode()?;

            batch.insert(&partition, encoded_key, encoded_delta);
        }

        Ok(())
    }

    /// Get deltas committed after a snapshot time
    ///
    /// Returns a map of encoded_key -> deltas that were committed after the snapshot.
    /// These deltas need to be unapplied to reconstruct the snapshot state.
    pub fn get_deltas_after(
        &self,
        snapshot_time: HlcTimestamp,
    ) -> Result<BTreeMap<Vec<u8>, Vec<E::Delta>>> {
        // Get partitions in the time range (snapshot_time .. now)
        let far_future = HlcTimestamp::new(u64::MAX, 0, snapshot_time.node_id);
        let partitions = self.bucket_manager.get_existing_partitions_for_range(
            E::entity_name(),
            snapshot_time,
            far_future,
        );

        let mut result: BTreeMap<Vec<u8>, Vec<E::Delta>> = BTreeMap::new();
        let snapshot_bytes = snapshot_time.to_lexicographic_bytes();

        // Scan all partitions for deltas after snapshot_time
        for partition in partitions {
            for entry in partition.range(snapshot_bytes.to_vec()..) {
                let (_key, value_bytes) = entry?;
                let delta = E::Delta::decode(&value_bytes)?;
                let key_bytes = delta.key().encode()?;

                result.entry(key_bytes).or_default().push(delta);
            }
        }

        Ok(result)
    }

    /// Get deltas for a specific key committed after snapshot time
    ///
    /// Note: Currently reuses get_deltas_after for simplicity. This scans all
    /// history deltas and extracts one key. For better performance with many
    /// point reads, consider caching the full map at the storage level, or
    /// restructure history to support key-based lookups (requires trade-offs
    /// in time-bucketing efficiency).
    pub fn get_key_deltas_after(
        &self,
        key: &E::Key,
        snapshot_time: HlcTimestamp,
    ) -> Result<Vec<E::Delta>> {
        let key_bytes = key.encode()?;
        let all_deltas = self.get_deltas_after(snapshot_time)?;

        Ok(all_deltas.get(&key_bytes).cloned().unwrap_or_default())
    }

    /// Cleanup old buckets - O(1) per bucket!
    pub fn cleanup_old_buckets(&mut self, current_time: HlcTimestamp) -> Result<usize> {
        self.bucket_manager
            .cleanup_old_buckets(current_time, self.retention_window)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use fjall::PartitionCreateOptions;
    use proven_hlc::NodeId;

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
        let config = StorageConfig::default();
        let temp_dir = tempfile::tempdir().unwrap();
        let keyspace = fjall::Config::new(temp_dir.path()).open()?;
        let keyspace_clone = keyspace.clone();

        let bucket_mgr = BucketManager::new(
            keyspace,
            "_history".to_string(),
            config.history_bucket_duration,
            PartitionCreateOptions::default(),
        );

        let mut store =
            HistoryStore::<TestEntity>::new(bucket_mgr, config.history_retention_window);

        // Add some deltas at time 1000
        let commit_time = HlcTimestamp::new(1000, 0, NodeId::new(1));
        let deltas = vec![TestDelta::Put {
            key: "key1".to_string(),
            value: "value1".to_string(),
            old_value: None,
        }];

        let mut batch = keyspace_clone.batch();
        store.add_committed_deltas_to_batch(&mut batch, commit_time, deltas)?;
        batch.commit()?;

        // Query deltas after time 500 (should include our delta)
        let snapshot_time = HlcTimestamp::new(500, 0, NodeId::new(1));
        let deltas_after = store.get_deltas_after(snapshot_time)?;

        assert!(!deltas_after.is_empty());

        Ok(())
    }
}
