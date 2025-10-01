//! Time-bucketed partition management for O(1) cleanup
//!
//! This module provides BucketManager which manages time-bucketed partitions.
//! Instead of using tombstones for cleanup (O(n)), we drop entire partitions (O(1)).

use crate::error::Result;
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle};
use proven_hlc::HlcTimestamp;
use std::collections::HashMap;
use std::time::Duration;

/// Calculate bucket ID from timestamp and bucket duration
pub fn get_bucket_id(time: HlcTimestamp, bucket_duration: Duration) -> u64 {
    let duration_micros = bucket_duration.as_micros() as u64;
    time.physical / duration_micros
}

/// Manages partitions for time-bucketed storage
pub struct BucketManager {
    keyspace: Keyspace,
    prefix: String, // e.g., "_data_history", "_uncommitted_data"
    bucket_duration: Duration,
    partition_options: PartitionCreateOptions,

    // Active partitions: (entity, bucket_id) -> PartitionHandle
    pub(crate) active_partitions: HashMap<(String, u64), PartitionHandle>,

    // Tracked buckets for cleanup: (entity, bucket_id, handle)
    tracked_buckets: Vec<(String, u64, PartitionHandle)>,
}

impl BucketManager {
    pub fn new(
        keyspace: Keyspace,
        prefix: String,
        bucket_duration: Duration,
        partition_options: PartitionCreateOptions,
    ) -> Self {
        Self {
            keyspace,
            prefix,
            bucket_duration,
            partition_options,
            active_partitions: HashMap::new(),
            tracked_buckets: Vec::new(),
        }
    }

    /// Get or create partition for writes (requires &mut self)
    pub fn get_or_create_partition(
        &mut self,
        entity: &str,
        time: HlcTimestamp,
    ) -> Result<&PartitionHandle> {
        let bucket_id = get_bucket_id(time, self.bucket_duration);
        let key = (entity.to_string(), bucket_id);

        if self.active_partitions.contains_key(&key) {
            return Ok(&self.active_partitions[&key]);
        }

        // Create partition: prefix_entity_bucket_ID
        let partition_name = format!("{}_{}_bucket_{:010}", self.prefix, entity, bucket_id);
        let partition = self
            .keyspace
            .open_partition(&partition_name, self.partition_options.clone())?;

        self.tracked_buckets
            .push((entity.to_string(), bucket_id, partition.clone()));
        self.active_partitions.insert(key.clone(), partition);
        Ok(&self.active_partitions[&key])
    }

    /// Get existing partition for reads - doesn't create (&self - read-only)
    pub fn get_existing_partition(
        &self,
        entity: &str,
        time: HlcTimestamp,
    ) -> Option<&PartitionHandle> {
        let bucket_id = get_bucket_id(time, self.bucket_duration);
        self.active_partitions.get(&(entity.to_string(), bucket_id))
    }

    /// Get existing partitions for a time range - read-only (&self)
    pub fn get_existing_partitions_for_range(
        &self,
        entity: &str,
        start_time: HlcTimestamp,
        end_time: HlcTimestamp,
    ) -> Vec<PartitionHandle> {
        let start_bucket = get_bucket_id(start_time, self.bucket_duration);
        let end_bucket = get_bucket_id(end_time, self.bucket_duration);

        // OPTIMIZATION: Instead of iterating through potentially billions of buckets,
        // only check the buckets that actually exist for this entity
        self.active_partitions
            .iter()
            .filter_map(|((e, bucket_id), handle)| {
                if e == entity && *bucket_id >= start_bucket && *bucket_id <= end_bucket {
                    Some(handle.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Cleanup old buckets - drops entire partitions (O(1) per bucket)
    pub fn cleanup_old_buckets(
        &mut self,
        current_time: HlcTimestamp,
        retention: Duration,
    ) -> Result<usize> {
        let retention_micros = retention.as_micros() as u64;
        let cutoff_time = current_time.physical.saturating_sub(retention_micros);
        let cutoff_bucket = cutoff_time / self.bucket_duration.as_micros() as u64;

        let mut removed = 0;

        // Collect partition handles to remove
        let mut handles_to_delete = Vec::new();

        self.tracked_buckets.retain(|(entity, bucket_id, handle)| {
            if *bucket_id < cutoff_bucket {
                // Remove from active partitions HashMap (drops one reference)
                self.active_partitions.remove(&(entity.clone(), *bucket_id));

                // Collect the handle for deletion (we'll drop it after removing from tracked_buckets)
                handles_to_delete.push(handle.clone());

                removed += 1;
                false // Don't retain in tracked_buckets
            } else {
                true // Keep this bucket
            }
        });

        // Now all references from active_partitions and tracked_buckets are dropped
        // Delete the partitions (this should now actually delete the data)
        for handle in handles_to_delete {
            // The delete_partition call should work now that we've dropped all our references
            let _ = self.keyspace.delete_partition(handle);
        }

        Ok(removed)
    }
}
