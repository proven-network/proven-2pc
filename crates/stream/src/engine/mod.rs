//! Transaction engine trait that storage systems must implement
//!
//! This trait defines the interface that SQL, KV, and other storage
//! systems must implement to work with the generic stream processor.

use proven_common::{Operation, Response};
use proven_hlc::HlcTimestamp;

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
    pub txn: HlcTimestamp,
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

    /// Read an operation at a specific timestamp (snapshot read)
    ///
    /// This is for read-only operations that don't need locks or transaction state.
    ///
    /// Though state mutations are possible for updating caches, etc. - actual data
    /// changes must not be allowed.
    ///
    /// Returns the value as it existed at the given timestamp.
    fn read_at_timestamp(
        &mut self,
        operation: Self::Operation,
        read_timestamp: HlcTimestamp,
        log_index: u64,
    ) -> OperationResult<Self::Response>;

    /// Apply an operation within a transaction context
    ///
    /// Returns a result indicating success, blocking, or error.
    /// The stream processor will handle control flow based on the result.
    ///
    /// The engine should atomically persist the log_index as metadata when applying the operation.
    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: HlcTimestamp,
        log_index: u64,
    ) -> OperationResult<Self::Response>;

    /// Begin a new transaction
    ///
    /// Initialize any necessary transaction state.
    ///
    /// The engine should atomically persist the log_index as metadata.
    fn begin(&mut self, txn_id: HlcTimestamp, log_index: u64);

    /// Prepare a transaction for commit (2PC phase 1)
    ///
    /// Marks the transaction as prepared. The transaction must exist.
    /// This is an infallible operation - validation should happen in apply_operation.
    ///
    /// The engine should atomically persist the log_index as metadata.
    fn prepare(&mut self, txn_id: HlcTimestamp, log_index: u64);

    /// Commit a prepared transaction (2PC phase 2)
    ///
    /// Makes all changes from the transaction visible.
    /// This is an infallible operation - the transaction must exist.
    ///
    /// The engine should atomically persist the log_index as metadata.
    fn commit(&mut self, txn_id: HlcTimestamp, log_index: u64);

    /// Abort a transaction, rolling back any changes
    ///
    /// Cleans up all transaction state and releases locks.
    /// This is an infallible operation - safe to call even if transaction doesn't exist.
    ///
    /// The engine should atomically persist the log_index as metadata.
    fn abort(&mut self, txn_id: HlcTimestamp, log_index: u64);

    /// Get the name/type of this engine for logging and debugging
    fn engine_name(&self) -> &str;

    /// Get the current log index that the engine has processed
    ///
    /// This is used to verify the engine's position before starting replay.
    /// Engines that persist state should return the last log_index they processed.
    /// Engines that don't persist state can return 0.
    fn get_log_index(&self) -> u64 {
        0
    }
}
