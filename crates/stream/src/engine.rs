//! Transaction engine trait that storage systems must implement
//!
//! This trait defines the interface that SQL, KV, and other storage
//! systems must implement to work with the generic stream processor.

use proven_common::{ChangeData, Operation, Response, TransactionId};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Operations that can be performed on a storage batch
///
/// This trait allows the stream processor to add transaction metadata
/// to the same batch that the engine uses for data changes, ensuring
/// atomic persistence.
pub trait BatchOperations: Send + Sync {
    /// Insert transaction metadata into the batch
    ///
    /// The engine will commit this atomically with data changes.
    fn insert_transaction_metadata(&mut self, txn_id: TransactionId, value: Vec<u8>);

    /// Remove transaction metadata from the batch
    ///
    /// Used when cleaning up transaction state on commit/abort.
    fn remove_transaction_metadata(&mut self, txn_id: TransactionId);
}

/// When a blocked operation can be retried
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOn {
    /// Can retry after blocking transaction prepares (releases read locks)
    Prepare,
    /// Must wait until blocking transaction commits or aborts (releases all locks)
    CommitOrAbort,
}

/// Information about a blocking transaction
#[derive(Debug, Clone)]
pub struct BlockingInfo {
    /// The blocking transaction
    pub txn: TransactionId,
    /// When we can retry after this specific blocker
    pub retry_on: RetryOn,
}

/// Result of attempting to apply an operation
#[derive(Debug, Clone)]
pub enum OperationResult<R> {
    /// Operation completed successfully (including application-level errors)
    Complete(R),

    /// Operation would block - defer and retry when appropriate
    WouldBlock {
        /// Transactions holding locks that would cause blocking (sorted by age, oldest first)
        /// Each includes when we can retry after that specific blocker
        blockers: Vec<BlockingInfo>,
    },
}

/// Transaction engine that handles the actual storage operations
///
/// Note: All methods are synchronous since stream processing must be
/// ordered and sequential. Each message must be fully processed before
/// moving to the next one.
pub trait TransactionEngine: Send + Sync {
    /// The type of operations this engine processes
    /// MUST be serializable for crash recovery
    type Operation: Operation + Clone + Serialize + for<'de> Deserialize<'de>;

    /// The type of responses this engine produces
    type Response: Response;

    /// The type of change data this engine produces
    type ChangeData: ChangeData;

    /// The batch type for atomic writes
    type Batch: BatchOperations;

    // ═══════════════════════════════════════════════════════════
    // BATCH LIFECYCLE
    // ═══════════════════════════════════════════════════════════

    /// Create a new batch for accumulating operations
    ///
    /// All operations within a single message should use the same batch.
    /// The batch accumulates all changes and is committed once at the end.
    fn start_batch(&mut self) -> Self::Batch;

    /// Commit a batch atomically with log_index
    ///
    /// This is the ONLY place where log_index advances.
    /// All operations added to the batch are committed atomically.
    ///
    /// The engine MUST:
    /// 1. Add log_index to the batch as metadata
    /// 2. Commit the batch atomically (all-or-nothing)
    /// 3. Ensure durability (sync to disk if configured)
    fn commit_batch(&mut self, batch: Self::Batch, log_index: u64);

    // ═══════════════════════════════════════════════════════════
    // READ OPERATIONS (no batch needed - no state changes)
    // ═══════════════════════════════════════════════════════════

    /// Read an operation at a specific transaction ID (snapshot read)
    ///
    /// Read-only operations bypass the ordered stream and don't modify state.
    /// Therefore, no batch is needed.
    fn read_at_timestamp(
        &self,
        operation: Self::Operation,
        read_txn_id: TransactionId,
    ) -> Self::Response;

    // ═══════════════════════════════════════════════════════════
    // WRITE OPERATIONS (batch is passed in, operations add to it)
    // ═══════════════════════════════════════════════════════════

    /// Begin a new transaction
    ///
    /// Adds any engine-specific initialization to the batch.
    /// For many engines, this is a no-op.
    fn begin(&mut self, batch: &mut Self::Batch, txn_id: TransactionId);

    /// Apply an operation within a transaction
    ///
    /// Returns:
    /// - Complete(response): Operation succeeded, changes added to batch
    /// - WouldBlock { blockers }: Operation blocked, no changes to batch
    ///
    /// The engine should:
    /// 1. Attempt to acquire locks
    /// 2. If blocked: return WouldBlock (batch unchanged)
    /// 3. If successful:
    ///    a. Execute operation
    ///    b. Add data changes to batch
    ///    c. Add lock persistence to batch (if needed)
    ///    d. Return response
    ///
    /// The stream processor will add transaction metadata and commit the batch.
    fn apply_operation(
        &mut self,
        batch: &mut Self::Batch,
        operation: Self::Operation,
        txn_id: TransactionId,
    ) -> OperationResult<Self::Response>;

    /// Prepare a transaction for commit (2PC phase 1)
    ///
    /// Adds to the batch:
    /// - Updated lock state (read locks released, write locks persisted)
    /// - Any engine-specific prepare logic
    ///
    /// The stream processor will add updated transaction state (with participants)
    /// before committing the batch.
    fn prepare(&mut self, batch: &mut Self::Batch, txn_id: TransactionId);

    /// Commit a prepared transaction (2PC phase 2)
    ///
    /// Adds to the batch:
    /// - Data being committed (moved from uncommitted to committed storage)
    /// - Lock cleanup (all locks released)
    /// - Any engine-specific commit logic
    ///
    /// The stream processor will remove transaction metadata before committing the batch.
    fn commit(&mut self, batch: &mut Self::Batch, txn_id: TransactionId) -> Self::ChangeData;

    /// Abort a transaction
    ///
    /// Adds to the batch:
    /// - Uncommitted data cleanup
    /// - Lock cleanup (all locks released)
    /// - Any engine-specific abort logic
    ///
    /// The stream processor will remove transaction metadata before committing the batch.
    fn abort(&mut self, batch: &mut Self::Batch, txn_id: TransactionId);

    // ═══════════════════════════════════════════════════════════
    // RECOVERY
    // ═══════════════════════════════════════════════════════════

    /// Get the current log index that the engine has processed
    ///
    /// Used during startup to determine where to resume replay.
    fn get_log_index(&self) -> Option<u64>;

    /// Scan all persisted transaction metadata (for crash recovery)
    ///
    /// Returns: Vec<(TransactionId, metadata_bytes)>
    ///
    /// Called once during processor startup to rebuild TransactionManager state.
    fn scan_transaction_metadata(&self) -> Vec<(TransactionId, Vec<u8>)>;

    /// Get the name/type of this engine for logging and debugging
    fn engine_name(&self) -> &str;
}

/// Wrapper around a TransactionEngine that automatically handles batch lifecycle
/// and log index management.
///
/// This is primarily useful for tests and examples where you want to focus on
/// the operations rather than the batch management boilerplate.
pub struct AutoBatchEngine<E: TransactionEngine> {
    engine: E,
    log_index: AtomicU64,
}

impl<E: TransactionEngine> AutoBatchEngine<E> {
    /// Create a new auto-batch wrapper around an engine
    pub fn new(engine: E) -> Self {
        // Start from the engine's current log index, or 0 if none
        let log_index = AtomicU64::new(engine.get_log_index().unwrap_or(0));
        Self { engine, log_index }
    }

    /// Create a new auto-batch wrapper starting from a specific log index
    pub fn new_with_log_index(engine: E, starting_log_index: u64) -> Self {
        Self {
            engine,
            log_index: AtomicU64::new(starting_log_index),
        }
    }

    /// Get the next log index (increments automatically)
    fn next_log_index(&self) -> u64 {
        self.log_index.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the current log index without incrementing
    pub fn current_log_index(&self) -> u64 {
        self.log_index.load(Ordering::Relaxed)
    }

    /// Get a reference to the underlying engine
    pub fn engine(&self) -> &E {
        &self.engine
    }

    /// Get a mutable reference to the underlying engine
    pub fn engine_mut(&mut self) -> &mut E {
        &mut self.engine
    }

    /// Unwrap and return the underlying engine
    pub fn into_inner(self) -> E {
        self.engine
    }

    // ═══════════════════════════════════════════════════════════
    // AUTO-BATCHED OPERATIONS
    // ═══════════════════════════════════════════════════════════

    /// Begin a new transaction (automatically wrapped in batch)
    pub fn begin(&mut self, txn_id: TransactionId) {
        let mut batch = self.engine.start_batch();
        self.engine.begin(&mut batch, txn_id);
        self.engine.commit_batch(batch, self.next_log_index());
    }

    /// Apply an operation within a transaction (automatically wrapped in batch)
    pub fn apply_operation(
        &mut self,
        operation: E::Operation,
        txn_id: TransactionId,
    ) -> OperationResult<E::Response> {
        let mut batch = self.engine.start_batch();
        let result = self.engine.apply_operation(&mut batch, operation, txn_id);

        self.engine.commit_batch(batch, self.next_log_index());

        result
    }

    /// Prepare a transaction for commit (automatically wrapped in batch)
    pub fn prepare(&mut self, txn_id: TransactionId) {
        let mut batch = self.engine.start_batch();
        self.engine.prepare(&mut batch, txn_id);
        self.engine.commit_batch(batch, self.next_log_index());
    }

    /// Commit a prepared transaction (automatically wrapped in batch)
    pub fn commit(&mut self, txn_id: TransactionId) {
        let mut batch = self.engine.start_batch();
        self.engine.commit(&mut batch, txn_id);
        self.engine.commit_batch(batch, self.next_log_index());
    }

    /// Abort a transaction (automatically wrapped in batch)
    pub fn abort(&mut self, txn_id: TransactionId) {
        let mut batch = self.engine.start_batch();
        self.engine.abort(&mut batch, txn_id);
        self.engine.commit_batch(batch, self.next_log_index());
    }

    /// Read at timestamp (pass-through, doesn't need batching)
    pub fn read_at_timestamp(
        &self,
        operation: E::Operation,
        read_txn_id: TransactionId,
    ) -> E::Response {
        self.engine.read_at_timestamp(operation, read_txn_id)
    }

    /// Get the current log index (pass-through)
    pub fn get_log_index(&self) -> Option<u64> {
        self.engine.get_log_index()
    }
}
