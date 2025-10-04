//! Storage-backed predicate persistence for crash safety
//!
//! This module provides persistent storage for transaction predicates, enabling:
//! - Crash recovery (predicates survive restart)
//! - Coordinator failover (new coordinator can resume transactions)
//! - Memory efficiency (predicates can be queried from disk)
//!
//! Uses BucketManager for automatic time-based cleanup of orphaned predicates

use crate::error::{Error, Result};
use crate::semantic::predicate::Predicate;
use crate::storage::bucket_manager::BucketManager;
use fjall::Batch;
use proven_hlc::HlcTimestamp;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Stored predicate with metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredPredicate {
    predicate: Predicate,
    is_write: bool, // true = exclusive (x), false = shared (s)
}

/// Store for transaction predicates (storage-backed with time-based cleanup)
pub struct PredicateStore {
    /// BucketManager for time-based partitioning and automatic cleanup
    bucket_manager: BucketManager,
    /// Retention window for predicates (should match uncommitted data retention)
    retention_window: Duration,
    /// Sequence number for generating unique keys within a transaction
    next_seq: AtomicU64,
}

impl PredicateStore {
    /// Create a new predicate store with BucketManager
    pub fn new(bucket_manager: BucketManager, retention_window: Duration) -> Self {
        Self {
            bucket_manager,
            retention_window,
            next_seq: AtomicU64::new(0),
        }
    }

    /// Encode key: {txn_id}:{seq}
    fn encode_key(txn_id: HlcTimestamp, seq: u64) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(&txn_id.to_lexicographic_bytes());
        key.push(b':');
        key.extend_from_slice(&seq.to_be_bytes());
        key
    }

    /// Add a predicate to a batch (will be committed atomically with data)
    /// Returns the key that was added
    pub fn add_predicate_to_batch(
        &mut self,
        batch: &mut Batch,
        txn_id: HlcTimestamp,
        predicate: &Predicate,
        is_write: bool,
    ) -> Result<Vec<u8>> {
        // Generate unique key
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let key = Self::encode_key(txn_id, seq);

        // Serialize the full predicate with metadata
        let stored = StoredPredicate {
            predicate: predicate.clone(),
            is_write,
        };
        let value = bincode::serialize(&stored).map_err(|e| Error::Serialization(e.to_string()))?;

        // Get the partition for this transaction's timestamp
        let partition = self
            .bucket_manager
            .get_or_create_partition("predicates", txn_id)?;
        batch.insert(&partition, &key, value);

        Ok(key)
    }

    /// Remove all predicates for a transaction (on commit/abort)
    pub fn remove_transaction_predicates(
        &mut self,
        batch: &mut Batch,
        txn_id: HlcTimestamp,
        predicate_keys: &[Vec<u8>],
    ) -> Result<()> {
        // Get the partition for this transaction's timestamp (if it exists)
        if let Some(partition) = self
            .bucket_manager
            .get_existing_partition("predicates", txn_id)
        {
            // Remove all predicate keys atomically
            for key in predicate_keys {
                batch.remove(&partition, key);
            }
        }
        // If partition doesn't exist, nothing to clean up

        Ok(())
    }

    /// Clean up old predicate buckets (removes orphaned predicates)
    pub fn cleanup_old_buckets(&mut self, current_time: HlcTimestamp) -> Result<()> {
        let _num_cleaned = self
            .bucket_manager
            .cleanup_old_buckets(current_time, self.retention_window)?;
        Ok(())
    }

    /// Get all active transactions from storage (for recovery)
    /// Note: This scans all predicate partitions to rebuild transaction state
    pub fn get_all_active_transactions(
        &self,
        _current_time: HlcTimestamp,
    ) -> Result<HashMap<HlcTimestamp, Vec<Predicate>>> {
        let mut transactions: HashMap<HlcTimestamp, Vec<Predicate>> = HashMap::new();

        // For recovery, we need to scan ALL partitions, not just recent ones
        // This is because we don't know when the predicates were created relative to wall clock time
        // We use time 0 to max to cover all possible buckets
        let start_time = HlcTimestamp::from_physical_time(0, proven_hlc::NodeId::new(0));
        let end_time = HlcTimestamp::from_physical_time(u64::MAX, proven_hlc::NodeId::new(0));

        // Scan ALL predicate partitions
        let partitions = self.bucket_manager.get_existing_partitions_for_range(
            "predicates",
            start_time,
            end_time,
        );

        for partition in partitions {
            for result in partition.iter() {
                let (key, value) = result?;

                // Parse the key to extract txn_id and predicate
                if let Some((txn_id, predicate)) = self.parse_predicate_key(&key, &value)? {
                    transactions.entry(txn_id).or_default().push(predicate);
                }
            }
        }

        Ok(transactions)
    }

    /// Parse a predicate key back into a transaction ID and predicate
    fn parse_predicate_key(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(HlcTimestamp, Predicate)>> {
        // Key format: {txn_id_bytes}:{seq_bytes}
        // Extract txn_id from the key (everything before the colon)
        let colon_pos = key.iter().position(|&b| b == b':');
        if colon_pos.is_none() {
            return Ok(None);
        }

        let txn_id_bytes = &key[..colon_pos.unwrap()];
        let txn_id = HlcTimestamp::from_lexicographic_bytes(txn_id_bytes)
            .map_err(|e| Error::Other(format!("Invalid txn_id in key: {}", e)))?;

        // Deserialize the stored predicate from the value
        let stored: StoredPredicate = bincode::deserialize(value)
            .map_err(|e| Error::Serialization(format!("Failed to deserialize predicate: {}", e)))?;

        Ok(Some((txn_id, stored.predicate)))
    }
}
