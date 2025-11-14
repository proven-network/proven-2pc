//! Batch coordinator for stream operations
//!
//! Coordinates writes across staging and stream storages, ensuring atomic commits.

use fjall::Keyspace;
use proven_common::TransactionId;
use proven_processor::BatchOperations;

/// Batch wrapper that coordinates writes across staging and stream storages
pub struct StreamBatch {
    inner: proven_mvcc::Batch,
    metadata_partition: fjall::PartitionHandle,
    /// Track next_position value to persist at commit time
    next_position_to_persist: Option<u64>,
}

impl StreamBatch {
    pub fn new(keyspace: &Keyspace, metadata_partition: fjall::PartitionHandle) -> Self {
        Self {
            inner: keyspace.batch(),
            metadata_partition,
            next_position_to_persist: None,
        }
    }

    pub fn inner(&mut self) -> &mut proven_mvcc::Batch {
        &mut self.inner
    }

    pub fn set_next_position(&mut self, next_position: u64) {
        self.next_position_to_persist = Some(next_position);
    }

    pub fn commit(mut self, log_index: u64) -> Result<(), String> {
        // Persist log_index
        self.inner.insert(
            &self.metadata_partition,
            b"_log_index",
            log_index.to_le_bytes(),
        );

        // Persist next_position if it was updated
        if let Some(next_position) = self.next_position_to_persist {
            self.inner.insert(
                &self.metadata_partition,
                b"_next_position",
                next_position.to_le_bytes(),
            );
        }

        self.inner.commit().map_err(|e| e.to_string())
    }
}

impl BatchOperations for StreamBatch {
    fn insert_transaction_metadata(&mut self, txn_id: TransactionId, value: Vec<u8>) {
        let mut key = b"_txn_".to_vec();
        key.extend_from_slice(&txn_id.to_bytes());
        self.inner.insert(&self.metadata_partition, key, value);
    }

    fn remove_transaction_metadata(&mut self, txn_id: TransactionId) {
        let mut key = b"_txn_".to_vec();
        key.extend_from_slice(&txn_id.to_bytes());
        self.inner.remove(self.metadata_partition.clone(), key);
    }
}
