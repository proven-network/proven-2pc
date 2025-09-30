//! Fjall-based recent versions store for time-travel snapshot reads

use crate::storage::encoding::{deserialize, serialize};
use crate::storage::types::{RowId, StorageResult, TransactionId, WriteOp};
use fjall::{Keyspace, Partition};
use proven_hlc::HlcTimestamp;
use std::collections::HashMap;
use std::time::Duration;

/// Fjall-based store for recently committed operations
/// Maintains a sliding window of operations for time-travel queries
pub struct DataHistoryStore {
    partition: Partition,
    keyspace: Keyspace,
    retention_window: Duration,
}

impl DataHistoryStore {
    /// Create a new recent versions store
    pub fn new(partition: Partition, keyspace: Keyspace, retention_window: Duration) -> Self {
        Self {
            partition,
            keyspace,
            retention_window,
        }
    }

    /// Check if the store is empty (fast check for optimization)
    pub fn is_empty(&self) -> bool {
        // This is a very fast check - just see if there's any entry at all
        match self.partition.first_key_value() {
            Ok(None) => true,     // No entries, it's empty
            Ok(Some(_)) => false, // Has entries, not empty
            Err(_) => true,       // Error reading, treat as empty for safety
        }
    }

    /// Check if a specific table has any recent operations (optimization)
    pub fn table_has_operations(&self, table: &str) -> bool {
        // Build prefix for table
        let mut prefix = Vec::new();
        prefix.extend_from_slice(&(table.len() as u32).to_be_bytes());
        prefix.extend_from_slice(table.as_bytes());

        // Check if there's any entry for this table
        self.partition.prefix(prefix).next().is_some()
    }

    /// Encode key for recent version: {table_len(4)}{table}{commit_time(24)}{row_id(8)}{seq(4)}
    ///
    /// KEY DESIGN: table_name comes FIRST for efficient prefix scans!
    /// This allows O(k) lookup of all ops for a specific table, then filter by time.
    fn encode_key(commit_time: TransactionId, op: &WriteOp, seq: u32) -> Vec<u8> {
        let mut key = Vec::new();

        // Put table name FIRST for efficient prefix scans
        if let Some(table) = op.table_name() {
            key.extend_from_slice(&(table.len() as u32).to_be_bytes());
            key.extend_from_slice(table.as_bytes());
        }

        // Then commit_time for chronological ordering within table
        // Use lexicographic encoding to ensure byte-wise ordering matches logical ordering
        let time_bytes = commit_time.to_lexicographic_bytes();
        key.extend_from_slice(&time_bytes);

        // Then row_id for filtering by row
        key.extend_from_slice(&op.row_id().to_be_bytes());

        // Finally sequence number for ordering within transaction
        key.extend_from_slice(&seq.to_be_bytes());

        key
    }

    /// Add committed operations from a transaction to an existing batch
    pub fn add_committed_ops_to_batch(
        &self,
        batch: &mut fjall::Batch,
        commit_time: TransactionId,
        ops: Vec<WriteOp>,
    ) -> StorageResult<()> {
        // Use sequence numbers to preserve order within transaction
        for (seq, op) in ops.into_iter().enumerate() {
            let key = Self::encode_key(commit_time, &op, seq as u32);
            let value = serialize(&op)?;
            batch.insert(&self.partition, key, value);
        }
        Ok(())
    }

    /// Get all operations for a table after a timestamp, grouped by row_id
    /// This is used to load all operations once for iteration
    ///
    /// OPTIMIZED: Uses bounded range scan - stops at end of table namespace automatically
    pub fn get_table_ops_after(
        &self,
        snapshot_time: TransactionId,
        table: &str,
    ) -> StorageResult<HashMap<RowId, Vec<WriteOp>>> {
        let mut result: HashMap<RowId, Vec<WriteOp>> = HashMap::new();

        // Build start key: {table_len}{table}{snapshot_time}
        let mut start_key = Vec::new();
        start_key.extend_from_slice(&(table.len() as u32).to_be_bytes());
        start_key.extend_from_slice(table.as_bytes());
        let snapshot_time_bytes = snapshot_time.to_lexicographic_bytes();
        start_key.extend_from_slice(&snapshot_time_bytes);

        // Build end key: {table_len}{table}{max_time}
        let mut end_key = Vec::new();
        end_key.extend_from_slice(&(table.len() as u32).to_be_bytes());
        end_key.extend_from_slice(table.as_bytes());
        end_key.extend_from_slice(&[0xFF; 20]); // Max timestamp (20 bytes)

        // Bounded range scan - automatically stops at end of table
        for entry in self.partition.range(start_key..end_key) {
            let (_key, value) = entry?;

            if let Ok(op) = deserialize::<WriteOp>(&value) {
                let row_id = op.row_id();
                result.entry(row_id).or_default().push(op);
            }
        }

        Ok(result)
    }

    /// Clean up operations older than the retention window
    pub fn cleanup_old_operations(&self, current_time: TransactionId) -> StorageResult<()> {
        // Calculate the cutoff time (retention window ago)
        // Note: HlcTimestamp's physical component is in microseconds
        let retention_micros = self.retention_window.as_micros() as u64;
        let cutoff_physical = current_time.physical.saturating_sub(retention_micros);
        let cutoff = HlcTimestamp::new(cutoff_physical, 0, current_time.node_id);

        let mut batch = self.keyspace.batch();
        let mut removed_count = 0;

        // Key format: {table_len(4)}{table}{commit_time(20)}{row_id(8)}{seq(8)}
        for result in self.partition.iter() {
            let (key, _value) = result?;

            // Extract table name length to find timestamp offset
            if key.len() < 4 {
                continue;
            }

            let table_len = u32::from_be_bytes([key[0], key[1], key[2], key[3]]) as usize;
            let timestamp_offset = 4 + table_len;

            // Check if key has enough bytes for timestamp (20 bytes)
            if key.len() < timestamp_offset + 20 {
                continue;
            }

            // Extract and decode timestamp using lexicographic encoding
            let timestamp_bytes = &key[timestamp_offset..timestamp_offset + 20];
            if let Ok(commit_time) = HlcTimestamp::from_lexicographic_bytes(timestamp_bytes)
                && commit_time < cutoff
            {
                batch.remove(&self.partition, key);
                removed_count += 1;
            }
            // Note: We can't break early since keys are ordered by table name first, not time
        }

        if removed_count > 0 {
            batch.commit()?;
        }

        Ok(())
    }

    /// Remove all operations for a specific table (used when dropping a table)
    pub fn remove_table(&self, table_name: &str) -> StorageResult<()> {
        let mut batch = self.keyspace.batch();

        // Scan all entries and remove those matching the table
        for result in self.partition.iter() {
            let (key, value) = result?;

            // Deserialize the operation to check the table
            if let Ok(op) = deserialize::<WriteOp>(&value)
                && op.table_name() == Some(table_name)
            {
                batch.remove(&self.partition, key);
            }
        }

        batch.commit()?;
        Ok(())
    }
}
