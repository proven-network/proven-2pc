//! Queue engine with separate storages for data and metadata
//!
//! Architecture:
//! - Three MvccStorage instances sharing a keyspace for atomic commits:
//!   1. data_storage: Queue items (linked list nodes)
//!   2. head_storage: Head pointer
//!   3. tail_storage: Tail pointer
//! - Simpler locking (just head/tail/both)
//! - All updates go through same MVCC flow
//! - Atomic batching across all three storages

use crate::entity::{
    HeadPointerDelta, HeadPointerEntity, HeadPointerKey, QueueEntity, QueueKey, QueueValue,
    TailPointerDelta, TailPointerEntity, TailPointerKey,
};
use crate::types::{QueueOperation, QueueResponse};
use fjall::Keyspace;
use proven_common::{ChangeData, TransactionId};
use proven_mvcc::{MvccStorage, StorageConfig};
use proven_stream::{BatchOperations, BlockingInfo, OperationResult, RetryOn, TransactionEngine};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Simplified lock modes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockMode {
    /// Shared read lock (Peek, Size, IsEmpty)
    Shared,
    /// Exclusive write lock on head (Dequeue)
    ExclusiveHead,
    /// Exclusive lock on entire queue (Clear)
    ExclusiveBoth,
}

/// Simple lock manager
struct LockManager {
    /// Current lock holders: (txn_id, mode)
    locks: Vec<(TransactionId, LockMode)>,
}

impl LockManager {
    fn new() -> Self {
        Self { locks: Vec::new() }
    }

    /// Try to acquire a lock
    fn try_lock(&mut self, txn_id: TransactionId, mode: LockMode) -> Result<(), Vec<BlockingInfo>> {
        // Check for conflicts
        let mut blockers = Vec::new();
        let mut already_holds_lock = false;

        for (holder_txn, holder_mode) in &self.locks {
            if *holder_txn == txn_id {
                // Same transaction already holds a lock
                already_holds_lock = true;
                // Same transaction can always re-acquire - just succeed without adding duplicate
                continue;
            }

            // Check compatibility with other transaction's locks
            if !self.is_compatible(mode, *holder_mode) {
                blockers.push(BlockingInfo {
                    txn: *holder_txn,
                    retry_on: if *holder_mode == LockMode::Shared {
                        RetryOn::Prepare
                    } else {
                        RetryOn::CommitOrAbort
                    },
                });
            }
        }

        if blockers.is_empty() {
            // Only add lock if we don't already hold one
            if !already_holds_lock {
                self.locks.push((txn_id, mode));
            }
            Ok(())
        } else {
            blockers.sort_by_key(|b| b.txn);
            blockers.dedup_by_key(|b| b.txn);
            Err(blockers)
        }
    }

    /// Check if two lock modes are compatible
    fn is_compatible(&self, mode1: LockMode, mode2: LockMode) -> bool {
        match (mode1, mode2) {
            // Shared locks are compatible with each other
            (LockMode::Shared, LockMode::Shared) => true,
            // Nothing is compatible with ExclusiveBoth
            (LockMode::ExclusiveBoth, _) | (_, LockMode::ExclusiveBoth) => false,
            // All other combinations are incompatible
            _ => false,
        }
    }

    /// Release locks for a transaction
    fn release(&mut self, txn_id: TransactionId) {
        self.locks.retain(|(t, _)| *t != txn_id);
    }

    /// Release shared locks only (for prepare phase)
    fn release_shared(&mut self, txn_id: TransactionId) {
        self.locks
            .retain(|(t, mode)| !(*t == txn_id && *mode == LockMode::Shared));
    }
}

/// Batch wrapper that coordinates writes across all storages
pub struct QueueBatch {
    inner: proven_mvcc::Batch,
    metadata_partition: fjall::PartitionHandle,
    /// Track next_seq value to persist at commit time
    next_seq_to_persist: Option<u64>,
}

impl QueueBatch {
    fn new(keyspace: &Keyspace, metadata_partition: fjall::PartitionHandle) -> Self {
        Self {
            inner: keyspace.batch(),
            metadata_partition,
            next_seq_to_persist: None,
        }
    }

    fn inner(&mut self) -> &mut proven_mvcc::Batch {
        &mut self.inner
    }

    fn set_next_seq(&mut self, next_seq: u64) {
        self.next_seq_to_persist = Some(next_seq);
    }

    fn commit(mut self, log_index: u64) -> Result<(), String> {
        // Persist log_index
        self.inner.insert(
            &self.metadata_partition,
            b"_log_index",
            log_index.to_le_bytes(),
        );

        // Persist next_seq if it was updated
        if let Some(next_seq) = self.next_seq_to_persist {
            self.inner.insert(
                &self.metadata_partition,
                b"_next_seq",
                next_seq.to_le_bytes(),
            );
        }

        self.inner.commit().map_err(|e| e.to_string())
    }
}

impl BatchOperations for QueueBatch {
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

/// Queue engine with three storages for lock-free concurrent appends
pub struct QueueTransactionEngine {
    /// Shared keyspace for atomic commits across all storages
    keyspace: Keyspace,

    /// Metadata partition for log index, next_seq, and transaction metadata
    metadata_partition: fjall::PartitionHandle,

    /// MVCC storage for queue data (linked list items)
    data_storage: MvccStorage<QueueEntity>,

    /// MVCC storage for head pointer
    head_storage: MvccStorage<HeadPointerEntity>,

    /// MVCC storage for tail pointer (highest consecutive committed sequence)
    tail_storage: MvccStorage<TailPointerEntity>,

    /// Lock manager (now only used for dequeue and read operations)
    lock_manager: LockManager,

    /// Cached next sequence number for fast atomic allocation
    /// Persisted to metadata_partition (NOT MVCC - never rolls back!)
    /// Rebuilt from metadata on startup
    next_seq_cached: AtomicU64,

    /// Track uncommitted appends (seq -> txn_id) for gap detection
    /// NOT persisted - rebuilt during recovery by scanning uncommitted MVCC data
    uncommitted_appends: BTreeMap<u64, TransactionId>,
}

impl QueueTransactionEngine {
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

        // Create metadata partition for log index
        let metadata_partition = keyspace
            .open_partition(
                "queue_new_metadata",
                fjall::PartitionCreateOptions::default(),
            )
            .expect("Failed to open metadata partition");

        // Create three MvccStorage instances sharing the keyspace
        let data_storage = MvccStorage::<QueueEntity>::with_shared_keyspace(
            keyspace.clone(),
            "queue_new_data".to_string(),
            config.clone(),
        )
        .expect("Failed to create data storage");

        let head_storage = MvccStorage::<HeadPointerEntity>::with_shared_keyspace(
            keyspace.clone(),
            "queue_new_head".to_string(),
            config.clone(),
        )
        .expect("Failed to create head storage");

        let tail_storage = MvccStorage::<TailPointerEntity>::with_shared_keyspace(
            keyspace.clone(),
            "queue_new_tail".to_string(),
            config,
        )
        .expect("Failed to create tail storage");

        // Recover next_seq from metadata (NOT MVCC - never rolls back!)
        let next_seq = metadata_partition
            .get(b"_next_seq")
            .ok()
            .flatten()
            .map(|bytes| {
                let array: [u8; 8] = bytes.as_ref().try_into().unwrap_or([0; 8]);
                u64::from_le_bytes(array)
            })
            .unwrap_or(0);

        Self {
            keyspace,
            metadata_partition,
            data_storage,
            head_storage,
            tail_storage,
            lock_manager: LockManager::new(),
            next_seq_cached: AtomicU64::new(next_seq),
            uncommitted_appends: BTreeMap::new(),
        }
    }

    /// Get the head pointer
    fn get_head(&self, txn_id: TransactionId) -> Result<Option<u64>, String> {
        match self.head_storage.read(&HeadPointerKey, txn_id) {
            Ok(Some(head_ptr)) => Ok(head_ptr.seq),
            Ok(None) => Ok(None), // Queue is empty
            Err(e) => Err(format!("Failed to read head pointer: {}", e)),
        }
    }

    /// Get the tail pointer
    fn get_tail(&self, txn_id: TransactionId) -> Result<Option<u64>, String> {
        match self.tail_storage.read(&TailPointerKey, txn_id) {
            Ok(Some(tail_ptr)) => Ok(tail_ptr.seq),
            Ok(None) => Ok(None), // Queue is empty
            Err(e) => Err(format!("Failed to read tail pointer: {}", e)),
        }
    }

    /// Get head item (pointer + data)
    ///
    /// FALLBACK: If head pointer seems incorrect, search for the actual lowest sequence.
    fn get_head_item(&self, txn_id: TransactionId) -> Result<Option<(u64, QueueValue)>, String> {
        let head_seq = match self.get_head(txn_id)? {
            Some(seq) => seq,
            None => return Ok(None),
        };

        // Try to read the item at head_seq
        match self.data_storage.read(&QueueKey(head_seq), txn_id) {
            Ok(Some(value)) => {
                // Item exists - use it (no backward search needed if we found the item)
                // Backward searching would find items we've already processed
                Ok(Some((head_seq, value)))
            }
            Ok(None) => {
                // Head pointer exists but item not found - search for actual head
                let uncommitted = &self.uncommitted_appends;
                let max_seq = self.next_seq_cached.load(Ordering::SeqCst);
                for seq in 1..=max_seq {
                    if uncommitted.contains_key(&seq) {
                        continue;
                    }
                    if let Ok(Some(value)) = self.data_storage.read(&QueueKey(seq), txn_id) {
                        return Ok(Some((seq, value)));
                    }
                }
                Ok(None) // Queue is actually empty
            }
            Err(e) => Err(format!("Failed to read head item: {}", e)),
        }
    }

    /// Advance tail pointer to highest consecutive committed sequence
    fn advance_tail_pointer(
        &mut self,
        inner_batch: &mut proven_mvcc::Batch,
        txn_id: TransactionId,
    ) {
        let current_tail = self.get_tail(txn_id).ok().flatten().unwrap_or(0);

        // Early exit: Don't advance beyond allocated sequences
        let max_seq = self.next_seq_cached.load(Ordering::Relaxed);
        if current_tail >= max_seq {
            return; // Tail is already at or beyond the latest allocated sequence
        }

        let uncommitted = &self.uncommitted_appends;

        // Find highest consecutive committed sequence
        let mut new_tail = current_tail;
        loop {
            let next_seq = new_tail + 1;

            // Don't search beyond allocated sequences
            if next_seq > max_seq {
                break;
            }

            // If next_seq is uncommitted, stop (gap)
            if uncommitted.contains_key(&next_seq) {
                break;
            }

            // If next_seq exists in committed data, advance
            if self
                .data_storage
                .read(&QueueKey(next_seq), txn_id)
                .ok()
                .flatten()
                .is_some()
            {
                new_tail = next_seq;
            } else {
                break; // No more items
            }
        }

        // Update tail if it advanced
        if new_tail > current_tail {
            let tail_delta = TailPointerDelta::Set {
                new_seq: Some(new_tail),
                old_seq: if current_tail > 0 {
                    Some(current_tail)
                } else {
                    None
                },
            };
            self.tail_storage
                .write_to_batch(inner_batch, tail_delta, txn_id)
                .expect("Write tail pointer failed");
        }
    }

    /// Find the smallest committed sequence number above the given sequence.
    /// This is used as a fallback in dequeue when next pointer is None.
    ///
    /// Returns None if there are no committed items above seq.
    fn find_smallest_committed_above(&self, seq: u64, txn_id: TransactionId) -> Option<u64> {
        let uncommitted = &self.uncommitted_appends;

        // Get the current max sequence from next_seq_cached
        let max_seq = self.next_seq_cached.load(Ordering::SeqCst);

        // Search forwards from seq+1
        for candidate in (seq + 1)..=max_seq {
            // Skip if uncommitted
            if uncommitted.contains_key(&candidate) {
                continue;
            }

            // Check if item exists in committed storage
            if self
                .data_storage
                .read(&QueueKey(candidate), txn_id)
                .ok()
                .flatten()
                .is_some()
            {
                return Some(candidate);
            }
        }

        None
    }

    /// Execute enqueue operation (LOCK-FREE!)
    ///
    /// Creates an item and links it to the tail if visible to this transaction.
    ///
    /// LINKING STRATEGY:
    /// ================
    /// 1. EAGER LINKING (here in enqueue):
    ///    - Link to tail if we can see it (same transaction or already committed)
    ///    - Handles the common case: sequential transactions and same-transaction items
    ///
    /// 2. FALLBACK (in dequeue):
    ///    - If next pointer is None, search for next committed sequence
    ///    - Handles gaps from out-of-order commits or aborted transactions
    ///    - Common case: O(1), worst case: O(gaps)
    ///
    /// This approach prioritizes commit performance for high-throughput workloads
    /// while maintaining correctness through the dequeue fallback mechanism.
    fn execute_enqueue(
        &mut self,
        batch: &mut QueueBatch,
        value: proven_value::Value,
        txn_id: TransactionId,
    ) -> OperationResult<QueueResponse> {
        use crate::entity::QueueDelta;

        // STEP 1: Atomically reserve sequence number (LOCK-FREE!)
        // Relaxed ordering is safe here - we only need atomicity, not ordering guarantees
        let old_seq = self.next_seq_cached.fetch_add(1, Ordering::Relaxed);
        let seq = old_seq + 1;

        // STEP 2: Track as uncommitted (for gap detection during dequeue)
        self.uncommitted_appends.insert(seq, txn_id);

        // STEP 3: Mark next_seq for persistence at commit time
        batch.set_next_seq(seq + 1);

        let inner_batch = batch.inner();

        // STEP 4: Get current tail for potential linking
        let old_tail_seq = self.get_tail(txn_id).ok().flatten();

        // STEP 5: Read tail value once and reuse for both delta and update
        let tail_value = if let Some(tail_seq) = old_tail_seq {
            self.data_storage
                .read(&QueueKey(tail_seq), txn_id)
                .ok()
                .flatten()
        } else {
            None
        };

        // STEP 6: Write queue item with linking to tail if it's visible to us
        let delta = if let Some(ref tv) = tail_value {
            QueueDelta::Enqueue {
                seq,
                value: value.clone(),
                old_tail_seq,
                old_tail_value: Some(tv.value.clone()),
                old_tail_prev: tv.prev,
            }
        } else {
            // Queue is empty or can't see tail - create unlinked
            QueueDelta::Enqueue {
                seq,
                value: value.clone(),
                old_tail_seq: None,
                old_tail_value: None,
                old_tail_prev: None,
            }
        };

        self.data_storage
            .write_to_batch(inner_batch, delta, txn_id)
            .expect("Write failed");

        // STEP 7: If there was a visible tail, update its next pointer
        if let (Some(tail_seq), Some(tail_val)) = (old_tail_seq, tail_value) {
            let update_delta = QueueDelta::UpdatePointers {
                seq: tail_seq,
                value: tail_val.value.clone(),
                old_next: tail_val.next,
                new_next: Some(seq),
                old_prev: tail_val.prev,
                new_prev: tail_val.prev,
            };

            self.data_storage
                .write_to_batch(inner_batch, update_delta, txn_id)
                .expect("Write failed");
        }

        // STEP 8: Update tail pointer
        let tail_delta = TailPointerDelta::Set {
            new_seq: Some(seq),
            old_seq: old_tail_seq,
        };
        self.tail_storage
            .write_to_batch(inner_batch, tail_delta, txn_id)
            .expect("Write tail pointer failed");

        // STEP 9: If queue was empty AND this is the lowest sequence, update head pointer
        // Only set head=seq if seq=1 (truly the first item ever)
        // This avoids concurrent transactions both setting head to their own sequence
        if seq == 1 && self.get_head(txn_id).ok().flatten().is_none() {
            let head_delta = HeadPointerDelta::Set {
                new_seq: Some(seq),
                old_seq: None,
            };
            self.head_storage
                .write_to_batch(inner_batch, head_delta, txn_id)
                .expect("Write head pointer failed");
        }

        // NO LOCK ACQUIRED - FULLY CONCURRENT!
        // Cross-transaction linking handled by lazy linking + fallback
        OperationResult::Complete(QueueResponse::Enqueued)
    }

    /// Execute dequeue operation
    fn execute_dequeue(
        &mut self,
        batch: &mut QueueBatch,
        txn_id: TransactionId,
    ) -> OperationResult<QueueResponse> {
        use crate::entity::QueueDelta;

        // CHECK FOR GAPS: Block if there are uncommitted appends from OTHER transactions
        // We need to wait for other transactions' uncommitted appends to commit or abort
        // to ensure we maintain FIFO ordering. Our own transaction's appends are fine.
        let uncommitted = &self.uncommitted_appends;
        let blockers: Vec<BlockingInfo> = uncommitted
            .values()
            .filter(|blocker_txn| **blocker_txn != txn_id) // Exclude our own transaction
            .map(|blocker_txn| BlockingInfo {
                txn: *blocker_txn,
                retry_on: RetryOn::CommitOrAbort,
            })
            .collect();

        if !blockers.is_empty() {
            return OperationResult::WouldBlock { blockers };
        }

        // Get the head item
        let head_info = match self.get_head_item(txn_id) {
            Ok(Some(info)) => info,
            Ok(None) => return OperationResult::Complete(QueueResponse::Dequeued(None)),
            Err(_) => return OperationResult::Complete(QueueResponse::Dequeued(None)),
        };

        let (head_seq, head_value) = head_info;
        let mut next_seq = head_value.next;

        // FALLBACK: If next is None, search for the next committed sequence
        // This handles the case where items were enqueued concurrently and
        // linking during commit might not have been visible to this transaction
        if next_seq.is_none() {
            next_seq = self.find_smallest_committed_above(head_seq, txn_id);
        }

        let inner_batch = batch.inner();

        // Create dequeue delta to delete the head item
        let delta = QueueDelta::Dequeue {
            seq: head_seq,
            old_value: head_value.value.clone(),
            old_next: next_seq,
            next_item_seq: next_seq,
            next_item_value: None,
            next_item_next: None,
        };

        self.data_storage
            .write_to_batch(inner_batch, delta, txn_id)
            .expect("Write failed");

        // If there's a next item, update its prev to None (it's the new head)
        if let Some(next_seq) = next_seq
            && let Ok(Some(next_value)) = self.data_storage.read(&QueueKey(next_seq), txn_id)
        {
            let update_delta = QueueDelta::UpdatePointers {
                seq: next_seq,
                value: next_value.value.clone(),
                old_next: next_value.next,
                new_next: next_value.next,
                old_prev: next_value.prev,
                new_prev: None, // New head has no prev
            };

            self.data_storage
                .write_to_batch(inner_batch, update_delta, txn_id)
                .expect("Write failed");
        }

        // Update head pointer
        let head_delta = HeadPointerDelta::Set {
            new_seq: next_seq, // None if queue is now empty
            old_seq: Some(head_seq),
        };
        self.head_storage
            .write_to_batch(inner_batch, head_delta, txn_id)
            .expect("Write head pointer failed");

        // If queue is now empty, also clear tail pointer
        if next_seq.is_none() {
            let tail_delta = TailPointerDelta::Set {
                new_seq: None,
                old_seq: Some(head_seq), // Old tail was the same as old head
            };
            self.tail_storage
                .write_to_batch(inner_batch, tail_delta, txn_id)
                .expect("Write tail pointer failed");
        }

        OperationResult::Complete(QueueResponse::Dequeued(Some(head_value.value)))
    }

    /// Execute peek operation
    fn execute_peek(
        &mut self,
        _batch: &mut QueueBatch,
        txn_id: TransactionId,
    ) -> OperationResult<QueueResponse> {
        match self.get_head_item(txn_id) {
            Ok(Some((_, head_value))) => {
                OperationResult::Complete(QueueResponse::Peeked(Some(head_value.value)))
            }
            Ok(None) => OperationResult::Complete(QueueResponse::Peeked(None)),
            Err(_) => OperationResult::Complete(QueueResponse::Peeked(None)),
        }
    }

    /// Execute size operation
    fn execute_size(
        &mut self,
        _batch: &mut QueueBatch,
        txn_id: TransactionId,
    ) -> OperationResult<QueueResponse> {
        // CHECK FOR GAPS: Block if there are uncommitted appends from OTHER transactions
        // We need to wait for other transactions' uncommitted appends to commit or abort
        // to ensure we get a consistent count.
        let uncommitted = &self.uncommitted_appends;
        let blockers: Vec<BlockingInfo> = uncommitted
            .values()
            .filter(|blocker_txn| **blocker_txn != txn_id) // Exclude our own transaction
            .map(|blocker_txn| BlockingInfo {
                txn: *blocker_txn,
                retry_on: RetryOn::CommitOrAbort,
            })
            .collect();

        if !blockers.is_empty() {
            return OperationResult::WouldBlock { blockers };
        }

        // Count all items by iterating
        match self.data_storage.iter(txn_id) {
            Ok(iter) => {
                let count = iter.count();
                OperationResult::Complete(QueueResponse::Size(count))
            }
            Err(_) => OperationResult::Complete(QueueResponse::Size(0)),
        }
    }

    /// Execute isEmpty operation
    fn execute_is_empty(
        &mut self,
        _batch: &mut QueueBatch,
        txn_id: TransactionId,
    ) -> OperationResult<QueueResponse> {
        match self.get_head(txn_id) {
            Ok(Some(_)) => OperationResult::Complete(QueueResponse::IsEmpty(false)),
            Ok(None) => OperationResult::Complete(QueueResponse::IsEmpty(true)),
            Err(_) => OperationResult::Complete(QueueResponse::IsEmpty(true)),
        }
    }

    /// Execute clear operation
    fn execute_clear(
        &mut self,
        batch: &mut QueueBatch,
        txn_id: TransactionId,
    ) -> OperationResult<QueueResponse> {
        use crate::entity::QueueDelta;

        // Get current head and tail before clearing
        let old_head_seq = self.get_head(txn_id).ok().flatten();
        let old_tail_seq = self.get_tail(txn_id).ok().flatten();

        // Use MVCC iter to see all items (including uncommitted)
        let items: Vec<_> = match self.data_storage.iter(txn_id) {
            Ok(iter) => iter.collect(),
            Err(_) => return OperationResult::Complete(QueueResponse::Cleared),
        };

        let inner_batch = batch.inner();
        for (key, value) in items.into_iter().flatten() {
            let delta = QueueDelta::Dequeue {
                seq: key.0,
                old_value: value.value,
                old_next: value.next,
                next_item_seq: None,
                next_item_value: None,
                next_item_next: None,
            };

            self.data_storage
                .write_to_batch(inner_batch, delta, txn_id)
                .expect("Write failed");
        }

        // Clear head pointer
        if old_head_seq.is_some() {
            let head_delta = HeadPointerDelta::Set {
                new_seq: None,
                old_seq: old_head_seq,
            };
            self.head_storage
                .write_to_batch(inner_batch, head_delta, txn_id)
                .expect("Write head pointer failed");
        }

        // Clear tail pointer
        if old_tail_seq.is_some() {
            let tail_delta = TailPointerDelta::Set {
                new_seq: None,
                old_seq: old_tail_seq,
            };
            self.tail_storage
                .write_to_batch(inner_batch, tail_delta, txn_id)
                .expect("Write tail pointer failed");
        }

        OperationResult::Complete(QueueResponse::Cleared)
    }
}

impl Default for QueueTransactionEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueChangeData;
impl ChangeData for QueueChangeData {
    fn merge(self, _other: Self) -> Self {
        self
    }
}

impl TransactionEngine for QueueTransactionEngine {
    type Operation = QueueOperation;
    type Response = QueueResponse;
    type ChangeData = QueueChangeData;
    type Batch = QueueBatch;

    fn start_batch(&mut self) -> Self::Batch {
        QueueBatch::new(&self.keyspace, self.metadata_partition.clone())
    }

    fn commit_batch(&mut self, batch: Self::Batch, log_index: u64) {
        batch.commit(log_index).expect("Batch commit failed");
    }

    fn read_at_timestamp(
        &self,
        operation: Self::Operation,
        read_timestamp: TransactionId,
    ) -> Self::Response {
        // No conflicts - proceed with snapshot read
        match operation {
            QueueOperation::Peek => match self.get_head_item(read_timestamp) {
                Ok(Some((_, value))) => QueueResponse::Peeked(Some(value.value)),
                Ok(None) => QueueResponse::Peeked(None),
                Err(_) => QueueResponse::Peeked(None),
            },
            QueueOperation::Size => match self.data_storage.iter(read_timestamp) {
                Ok(iter) => QueueResponse::Size(iter.count()),
                Err(_) => QueueResponse::Size(0),
            },
            QueueOperation::IsEmpty => match self.get_head(read_timestamp) {
                Ok(Some(_)) => QueueResponse::IsEmpty(false),
                Ok(None) => QueueResponse::IsEmpty(true),
                Err(_) => QueueResponse::IsEmpty(true),
            },
            // Other operations are not valid for snapshot reads
            _ => unreachable!(),
        }
    }

    fn apply_operation(
        &mut self,
        batch: &mut Self::Batch,
        operation: Self::Operation,
        txn_id: TransactionId,
    ) -> OperationResult<Self::Response> {
        // Enqueue is now LOCK-FREE - no lock needed!
        // Only dequeue and read operations need locks
        match &operation {
            QueueOperation::Enqueue { .. } => {
                // Execute enqueue without acquiring any lock - fully concurrent!
                match operation {
                    QueueOperation::Enqueue { value } => self.execute_enqueue(batch, value, txn_id),
                    _ => unreachable!(),
                }
            }
            _ => {
                // All other operations need locks
                let lock_mode = match &operation {
                    QueueOperation::Dequeue => LockMode::ExclusiveHead,
                    QueueOperation::Peek | QueueOperation::Size | QueueOperation::IsEmpty => {
                        LockMode::Shared
                    }
                    QueueOperation::Clear => LockMode::ExclusiveBoth,
                    _ => unreachable!(),
                };

                // Try to acquire lock
                if let Err(blockers) = self.lock_manager.try_lock(txn_id, lock_mode) {
                    return OperationResult::WouldBlock { blockers };
                }

                // Execute the operation
                match operation {
                    QueueOperation::Dequeue => self.execute_dequeue(batch, txn_id),
                    QueueOperation::Peek => self.execute_peek(batch, txn_id),
                    QueueOperation::Size => self.execute_size(batch, txn_id),
                    QueueOperation::IsEmpty => self.execute_is_empty(batch, txn_id),
                    QueueOperation::Clear => self.execute_clear(batch, txn_id),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn begin(&mut self, _batch: &mut Self::Batch, _txn_id: TransactionId) {
        // Nothing to do
    }

    fn prepare(&mut self, _batch: &mut Self::Batch, txn_id: TransactionId) {
        self.lock_manager.release_shared(txn_id);
    }

    fn commit(&mut self, batch: &mut Self::Batch, txn_id: TransactionId) -> Self::ChangeData {
        let inner_batch = batch.inner();

        // STEP 1: Remove this transaction's sequences from uncommitted_appends
        self.uncommitted_appends.retain(|_, tid| *tid != txn_id);

        // STEP 2: Advance tail to highest consecutive committed sequence
        self.advance_tail_pointer(inner_batch, txn_id);

        // STEP 3: Commit all three storages (next_seq is in metadata, not MVCC)
        self.data_storage
            .commit_transaction_to_batch(inner_batch, txn_id)
            .expect("Data commit failed");

        self.head_storage
            .commit_transaction_to_batch(inner_batch, txn_id)
            .expect("Head commit failed");

        self.tail_storage
            .commit_transaction_to_batch(inner_batch, txn_id)
            .expect("Tail commit failed");

        // NOTE: next_seq is committed via batch.commit() in commit_batch(), not here!

        // Release locks (only used by dequeue now)
        self.lock_manager.release(txn_id);

        QueueChangeData
    }

    fn abort(&mut self, batch: &mut Self::Batch, txn_id: TransactionId) {
        let inner_batch = batch.inner();

        // Remove this transaction's sequences from uncommitted_appends
        self.uncommitted_appends.retain(|_, tid| *tid != txn_id);

        // NOTE: When aborting, we DON'T roll back next_seq!
        // Once a sequence number is reserved, it stays reserved forever.
        // This prevents duplicate sequence numbers across crash/restart.
        // The batch.set_next_seq() will still commit the incremented value.

        // Abort all three storages
        self.data_storage
            .abort_transaction_to_batch(inner_batch, txn_id)
            .expect("Data abort failed");

        self.head_storage
            .abort_transaction_to_batch(inner_batch, txn_id)
            .expect("Head abort failed");

        self.tail_storage
            .abort_transaction_to_batch(inner_batch, txn_id)
            .expect("Tail abort failed");

        // Release locks
        self.lock_manager.release(txn_id);
    }

    fn get_log_index(&self) -> Option<u64> {
        // Read log_index from metadata partition where batch.commit() writes it
        let log_index = self
            .metadata_partition
            .get(b"_log_index")
            .ok()
            .flatten()
            .map(|bytes| {
                let array: [u8; 8] = bytes.as_ref().try_into().unwrap_or([0; 8]);
                u64::from_le_bytes(array)
            })
            .unwrap_or(0);

        if log_index > 0 { Some(log_index) } else { None }
    }

    fn scan_transaction_metadata(&self) -> Vec<(TransactionId, Vec<u8>)> {
        // Scan metadata partition shared across all storages
        let mut results = Vec::new();

        for (key_bytes, value_bytes) in self.metadata_partition.prefix("_txn_").flatten() {
            if key_bytes.len() == 5 + 16 {
                let txn_id_bytes: [u8; 16] = key_bytes[5..21]
                    .try_into()
                    .expect("Invalid txn_id in metadata key");
                let txn_id = TransactionId::from_bytes(txn_id_bytes);
                results.push((txn_id, value_bytes.to_vec()));
            }
        }

        results
    }

    fn engine_name(&self) -> &str {
        "queue"
    }
}
