//! Transaction engine trait that storage systems must implement
//!
//! This trait defines the interface that SQL, KV, and other storage
//! systems must implement to work with the generic stream processor.

use proven_hlc::HlcTimestamp;
use serde::{Serialize, de::DeserializeOwned};

/// Result of attempting to apply an operation
#[derive(Debug, Clone)]
pub enum OperationResult<R> {
    /// Operation completed successfully
    Success(R),

    /// Operation would block - defer and retry when lock is released
    WouldBlock {
        /// Transaction holding the lock that would cause blocking
        blocking_txn: HlcTimestamp,
    },

    /// Transaction was wounded by a younger transaction
    Wounded {
        /// Transaction that caused the wound
        wounded_by: HlcTimestamp,
    },

    /// Operation failed with an error
    Error(String),
}

/// Transaction engine that handles the actual storage operations
///
/// Note: All methods are synchronous since stream processing must be
/// ordered and sequential. Each message must be fully processed before
/// moving to the next one.
pub trait TransactionEngine: Send + Sync {
    /// The type of operations this engine processes
    type Operation: DeserializeOwned + Send + Clone;

    /// The type of responses this engine produces
    type Response: Serialize + Send;

    /// Apply an operation within a transaction context
    ///
    /// Returns a result indicating success, blocking, wounding, or error.
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
}
