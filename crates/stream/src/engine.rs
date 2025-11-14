//! Stream engine with commit-time position allocation using simple fjall partitions
//!
//! Architecture:
//! - Two fjall partitions:
//!   1. staging: Uncommitted appends (keyed by txn_id + seq_within_txn) for crash recovery
//!   2. stream: Committed stream entries (keyed by position, 1-indexed)
//! - Gap-free positions (assigned at commit time, not append time)
//! - No MVCC complexity - simple key-value storage
//! - Values kept in memory during transaction, written to stream at commit

use crate::batch::StreamBatch;
use crate::types::{StreamOperation, StreamResponse};
use fjall::{Keyspace, PartitionHandle};
use proven_common::{ChangeData, TransactionId};
use proven_mvcc::StorageConfig;
use proven_processor::{OperationResult, TransactionEngine};
use proven_value::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Transaction state for tracking appends
#[derive(Debug)]
struct TxnState {
    /// Values appended in this transaction (kept in memory until commit)
    appended_values: Vec<Value>,
}

/// Stream transaction engine with commit-time position allocation
pub struct StreamTransactionEngine {
    /// Shared keyspace for atomic commits
    keyspace: Keyspace,

    /// Metadata partition for log index, next_position, and transaction metadata
    metadata_partition: PartitionHandle,

    /// Staging partition for uncommitted appends (for crash recovery)
    staging_partition: PartitionHandle,

    /// Stream partition for committed stream entries with consecutive positions
    stream_partition: PartitionHandle,

    /// Next position to allocate (1-indexed, 1 = first position)
    /// Persisted to metadata_partition (never rolls back!)
    /// Starts at 1 (position 0 means "before first item")
    next_position: u64,

    /// Track transaction state (in-memory appended values)
    transactions: HashMap<TransactionId, TxnState>,
}

impl StreamTransactionEngine {
    pub fn new() -> Self {
        let config = StorageConfig::default();
        Self::with_config(config)
    }

    pub fn with_config(config: StorageConfig) -> Self {
        // Create shared keyspace
        let keyspace = fjall::Config::new(&config.data_dir)
            .cache_size(config.block_cache_size)
            .open()
            .expect("Failed to open keyspace");

        // Create partitions
        let metadata_partition = keyspace
            .open_partition("stream_metadata", fjall::PartitionCreateOptions::default())
            .expect("Failed to open metadata partition");

        let staging_partition = keyspace
            .open_partition("stream_staging", fjall::PartitionCreateOptions::default())
            .expect("Failed to open staging partition");

        let stream_partition = keyspace
            .open_partition("stream", fjall::PartitionCreateOptions::default())
            .expect("Failed to open stream partition");

        // Recover next_position from metadata
        let next_position = metadata_partition
            .get(b"_next_position")
            .ok()
            .flatten()
            .map(|bytes| {
                let array: [u8; 8] = bytes.as_ref().try_into().unwrap_or([0; 8]);
                u64::from_le_bytes(array)
            })
            .unwrap_or(1); // Default to 1 (first position)

        Self {
            keyspace,
            metadata_partition,
            staging_partition,
            stream_partition,
            next_position,
            transactions: HashMap::new(),
        }
    }

    /// Get or create transaction state
    fn get_or_create_txn_state(&mut self, txn_id: TransactionId) -> &mut TxnState {
        self.transactions.entry(txn_id).or_insert(TxnState {
            appended_values: Vec::new(),
        })
    }

    /// Encode staging key: txn_id (16 bytes) + seq (8 bytes)
    fn encode_staging_key(txn_id: TransactionId, seq: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(24);
        key.extend_from_slice(&txn_id.to_bytes());
        key.extend_from_slice(&seq.to_be_bytes());
        key
    }

    /// Encode stream position key: position (8 bytes)
    fn encode_stream_key(position: u64) -> Vec<u8> {
        position.to_be_bytes().to_vec()
    }

    /// Execute append operation
    fn execute_append(
        &mut self,
        batch: &mut StreamBatch,
        values: Vec<Value>,
        txn_id: TransactionId,
    ) -> OperationResult<StreamResponse> {
        if values.is_empty() {
            return OperationResult::Complete(StreamResponse::Appended);
        }

        let txn_state = self.get_or_create_txn_state(txn_id);
        let start_seq = txn_state.appended_values.len() as u64;

        // Store values in memory
        txn_state.appended_values.extend(values.iter().cloned());

        // Also write to staging partition for crash recovery
        let inner_batch = batch.inner();
        for (i, value) in values.iter().enumerate() {
            let key = Self::encode_staging_key(txn_id, start_seq + i as u64);
            let value_bytes = proven_value::codec::encode_value(value);
            inner_batch.insert(&self.staging_partition, key, value_bytes);
        }

        OperationResult::Complete(StreamResponse::Appended)
    }

    /// Execute read operation
    fn execute_read_from(
        &self,
        position: u64,
        limit: usize,
        _txn_id: TransactionId,
    ) -> OperationResult<StreamResponse> {
        let mut entries = Vec::new();

        for i in 0..limit {
            let pos = position + i as u64;
            let key = Self::encode_stream_key(pos);

            match self.stream_partition.get(&key) {
                Ok(Some(value_bytes)) => {
                    match proven_value::codec::decode_value(&value_bytes) {
                        Ok(value) => entries.push((pos, value)),
                        Err(_) => break, // Decode error, stop
                    }
                }
                Ok(None) => break, // Missing position, stop
                Err(_) => break,   // Read error, stop
            }
        }

        OperationResult::Complete(StreamResponse::Read { entries })
    }

    /// Get the latest committed position
    fn execute_get_latest_position(&self) -> OperationResult<StreamResponse> {
        // next_position points to the NEXT available position
        // So latest committed position is next_position - 1
        let latest = self.next_position.saturating_sub(1);
        OperationResult::Complete(StreamResponse::Position(latest))
    }

    /// Get log index from metadata
    fn read_log_index_from_metadata(&self) -> u64 {
        self.metadata_partition
            .get(b"_log_index")
            .ok()
            .flatten()
            .map(|bytes| {
                let array: [u8; 8] = bytes.as_ref().try_into().unwrap_or([0; 8]);
                u64::from_le_bytes(array)
            })
            .unwrap_or(0)
    }
}

impl Default for StreamTransactionEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamChangeData;

impl ChangeData for StreamChangeData {
    fn merge(self, _other: Self) -> Self {
        self
    }
}

impl TransactionEngine for StreamTransactionEngine {
    type Operation = StreamOperation;
    type Response = StreamResponse;
    type ChangeData = StreamChangeData;
    type Batch = StreamBatch;

    fn start_batch(&mut self) -> Self::Batch {
        StreamBatch::new(&self.keyspace, self.metadata_partition.clone())
    }

    fn commit_batch(&mut self, batch: Self::Batch, log_index: u64) {
        batch.commit(log_index).expect("Batch commit failed");
    }

    fn read_at_timestamp(
        &self,
        operation: Self::Operation,
        read_timestamp: TransactionId,
    ) -> Self::Response {
        match operation {
            StreamOperation::ReadFrom { position, limit } => {
                match self.execute_read_from(position, limit, read_timestamp) {
                    OperationResult::Complete(response) => response,
                    _ => StreamResponse::Error("Unexpected result".to_string()),
                }
            }
            StreamOperation::GetLatestPosition => match self.execute_get_latest_position() {
                OperationResult::Complete(response) => response,
                _ => StreamResponse::Error("Unexpected result".to_string()),
            },
            _ => StreamResponse::Error("Invalid operation for snapshot read".to_string()),
        }
    }

    fn apply_operation(
        &mut self,
        batch: &mut Self::Batch,
        operation: Self::Operation,
        txn_id: TransactionId,
    ) -> OperationResult<Self::Response> {
        match operation {
            StreamOperation::Append { values } => self.execute_append(batch, values, txn_id),
            StreamOperation::ReadFrom { position, limit } => {
                self.execute_read_from(position, limit, txn_id)
            }
            StreamOperation::GetLatestPosition => self.execute_get_latest_position(),
        }
    }

    fn begin(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {
        // Nothing to do - transaction state is created on-demand
    }

    fn prepare(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {
        // No locks to release
    }

    fn commit(&mut self, batch: &mut Self::Batch, txn_id: TransactionId) -> Self::ChangeData {
        // Get values from transaction state
        let values = if let Some(txn_state) = self.transactions.get(&txn_id) {
            if txn_state.appended_values.is_empty() {
                None
            } else {
                Some((txn_state.appended_values.clone(), self.next_position))
            }
        } else {
            None
        };

        if let Some((values_to_commit, start_position)) = values {
            let count = values_to_commit.len() as u64;
            let end_position = start_position + count;

            // Set next_position in batch
            batch.set_next_position(end_position);

            let inner_batch = batch.inner();

            // Write values to stream partition with assigned positions
            for (idx, value) in values_to_commit.iter().enumerate() {
                let position = start_position + idx as u64;
                let key = Self::encode_stream_key(position);
                let value_bytes = proven_value::codec::encode_value(value);
                inner_batch.insert(&self.stream_partition, key, value_bytes);
            }

            // Delete from staging partition
            for idx in 0..count {
                let key = Self::encode_staging_key(txn_id, idx);
                inner_batch.remove(self.staging_partition.clone(), key);
            }

            // Update next_position
            self.next_position = end_position;
        }

        // Clean up transaction state
        self.transactions.remove(&txn_id);

        StreamChangeData
    }

    fn abort(&mut self, batch: &mut Self::Batch, txn_id: TransactionId) {
        // Get count of appended values to delete from staging
        let count = if let Some(txn_state) = self.transactions.get(&txn_id) {
            txn_state.appended_values.len() as u64
        } else {
            0
        };

        if count > 0 {
            let inner_batch = batch.inner();

            // Delete from staging partition
            for idx in 0..count {
                let key = Self::encode_staging_key(txn_id, idx);
                inner_batch.remove(self.staging_partition.clone(), key);
            }
        }

        // Clean up transaction state (discard in-memory values)
        self.transactions.remove(&txn_id);
    }

    fn get_log_index(&self) -> Option<u64> {
        Some(self.read_log_index_from_metadata())
    }

    fn scan_transaction_metadata(&self) -> Vec<(TransactionId, Vec<u8>)> {
        let mut transactions = Vec::new();

        // Scan metadata partition for transaction metadata
        let prefix = b"_txn_";
        for item in self.metadata_partition.prefix(prefix).flatten() {
            let key = item.0;
            let value = item.1;

            // Extract transaction ID from key
            if key.len() >= prefix.len() + 16 {
                let txn_bytes = &key[prefix.len()..prefix.len() + 16];
                let txn_id = TransactionId::from_bytes(txn_bytes.try_into().unwrap());
                transactions.push((txn_id, value.to_vec()));
            }
        }

        transactions
    }

    fn engine_name(&self) -> &str {
        "stream"
    }
}
