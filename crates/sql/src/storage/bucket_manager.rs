//! Time-bucketed partition management for O(1) cleanup
//!
//! This module provides BucketManager which manages time-bucketed partitions.
//! Instead of using tombstones for cleanup (O(n)), we drop entire partitions (O(1)).

use crate::error::Result;
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle};
use proven_hlc::HlcTimestamp;
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
        }
    }

    /// Get or create partition for writes
    pub fn get_or_create_partition(
        &mut self,
        entity: &str,
        time: HlcTimestamp,
    ) -> Result<PartitionHandle> {
        let bucket_id = get_bucket_id(time, self.bucket_duration);
        let partition_name = format!("{}_{}_bucket_{:010}", self.prefix, entity, bucket_id);

        // Fjall handles caching internally
        self.keyspace
            .open_partition(&partition_name, self.partition_options.clone())
            .map_err(Into::into)
    }

    /// Get existing partition for reads - doesn't create (&self - read-only)
    pub fn get_existing_partition(
        &self,
        entity: &str,
        time: HlcTimestamp,
    ) -> Option<PartitionHandle> {
        let bucket_id = get_bucket_id(time, self.bucket_duration);

        // Just try to open from keyspace - fjall handles caching internally
        let partition_name = format!("{}_{}_bucket_{:010}", self.prefix, entity, bucket_id);
        self.keyspace
            .open_partition(&partition_name, self.partition_options.clone())
            .ok()
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

        let mut partitions = Vec::new();

        // Scan keyspace for all matching partitions
        // Fjall handles caching internally, so we don't need to duplicate that logic
        let prefix = format!("{}_{}_bucket_", self.prefix, entity);
        for partition_name in self.keyspace.list_partitions() {
            let name = partition_name.as_ref();
            if let Some(bucket_str) = name.strip_prefix(&prefix)
                && let Ok(bucket_id) = bucket_str.parse::<u64>()
                && bucket_id >= start_bucket
                && bucket_id <= end_bucket
            {
                // Open the partition - fjall will use its internal cache if already open
                if let Ok(handle) = self
                    .keyspace
                    .open_partition(name, self.partition_options.clone())
                {
                    partitions.push(handle);
                }
            }
        }

        partitions
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

        // Scan all partitions matching our prefix
        let prefix = format!("{}_", self.prefix);
        for partition_name in self.keyspace.list_partitions() {
            let name = partition_name.as_ref();
            if let Some(rest) = name.strip_prefix(&prefix) {
                // Parse: entity_bucket_NNNNNNNNNN
                if let Some(bucket_pos) = rest.rfind("_bucket_") {
                    let bucket_str = &rest[bucket_pos + 8..];
                    if let Ok(bucket_id) = bucket_str.parse::<u64>()
                        && bucket_id < cutoff_bucket
                    {
                        // Open and delete the partition
                        if let Ok(handle) = self
                            .keyspace
                            .open_partition(name, self.partition_options.clone())
                        {
                            let _ = self.keyspace.delete_partition(handle);
                            removed += 1;
                        }
                    }
                }
            }
        }

        Ok(removed)
    }

    /// Check if there are any partitions with this prefix
    pub fn has_partitions(&self) -> bool {
        // Check partition count > 1 (since _metadata always exists)
        self.keyspace.partition_count() > 1
    }
}
