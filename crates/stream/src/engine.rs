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

/// Result of attempting to apply an operation
#[derive(Debug, Clone)]
pub enum OperationResult<R> {
    /// Operation completed successfully (including application-level errors)
    Complete(R),

    /// Operation would block - defer and retry when appropriate
    WouldBlock {
        /// Transaction holding the lock that would cause blocking
        blocking_txn: HlcTimestamp,
        /// When this operation can be retried
        retry_on: RetryOn,
    },
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

    /// Apply an operation within a transaction context
    ///
    /// Returns a result indicating success, blocking, or error.
    /// The stream processor will handle control flow based on the result.
    fn apply_operation(
        &mut self,
        operation: Self::Operation,
        txn_id: HlcTimestamp,
    ) -> OperationResult<Self::Response>;

    /// Prepare a transaction for commit (2PC phase 1)
    ///
    /// Should validate that the transaction can be committed and
    /// make any necessary preparations, but not actually commit.
    fn prepare(&mut self, txn_id: HlcTimestamp) -> Result<(), String>;

    /// Commit a prepared transaction (2PC phase 2)
    ///
    /// Makes all changes from the transaction visible.
    fn commit(&mut self, txn_id: HlcTimestamp) -> Result<(), String>;

    /// Abort a transaction, rolling back any changes
    ///
    /// Should clean up all transaction state and release locks.
    fn abort(&mut self, txn_id: HlcTimestamp) -> Result<(), String>;

    /// Begin a new transaction
    ///
    /// Initialize any necessary transaction state.
    fn begin_transaction(&mut self, txn_id: HlcTimestamp);

    /// Check if a transaction is currently active
    fn is_transaction_active(&self, txn_id: &HlcTimestamp) -> bool;

    /// Get the name/type of this engine for logging and debugging
    fn engine_name(&self) -> &str;

    /// Generate a snapshot of current state
    ///
    /// This should only be called when no transactions are active.
    /// Returns serialized bytes representing the complete state.
    fn snapshot(&self) -> Result<Vec<u8>, String> {
        Err("Snapshots not supported by this engine".to_string())
    }

    /// Restore state from a snapshot
    ///
    /// This should completely replace the current state with the snapshot.
    /// Should only be called on a fresh engine instance.
    fn restore_from_snapshot(&mut self, _data: &[u8]) -> Result<(), String> {
        Err("Snapshot restoration not supported by this engine".to_string())
    }
}
