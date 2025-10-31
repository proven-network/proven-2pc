//! Transaction engine trait that storage systems must implement
//!
//! This trait defines the interface that SQL, KV, and other storage
//! systems must implement to work with the generic stream processor.

use proven_common::{Operation, Response, TransactionId};

/// Operations that can be performed on a storage batch
///
/// This trait allows the stream processor to add transaction metadata
/// to the same batch that the engine uses for data changes, ensuring
/// atomic persistence.
pub trait BatchOperations: Send + Sync {
    /// Insert metadata into the batch
    ///
    /// Key format convention: "_txn_meta_{txn_id}" for transaction state
    /// The engine will commit this atomically with data changes.
    fn insert_metadata(&mut self, key: Vec<u8>, value: Vec<u8>);

    /// Remove metadata from the batch
    ///
    /// Used when cleaning up transaction state on commit/abort.
    fn remove_metadata(&mut self, key: Vec<u8>);
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

/// Transaction mode for execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// Read-only transaction using snapshot isolation
    ReadOnly,
    /// Ad-hoc operation with auto-commit
    AdHoc,
    /// Full read-write transaction with 2PC
    ReadWrite,
}

/// Transaction engine that handles the actual storage operations
///
/// Note: All methods are synchronous since stream processing must be
/// ordered and sequential. Each message must be fully processed before
/// moving to the next one.
pub trait TransactionEngine: Send + Sync {
    /// The type of operations this engine processes
    type Operation: Operation;

    /// The type of responses this engine produces
    type Response: Response;

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
        &mut self,
        operation: Self::Operation,
        read_txn_id: TransactionId,
    ) -> OperationResult<Self::Response>;

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
    fn commit(&mut self, batch: &mut Self::Batch, txn_id: TransactionId);

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
