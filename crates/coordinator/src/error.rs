//! Error types for the coordinator module
//!
//! This module defines the error types that can occur during
//! distributed transaction coordination.

use thiserror::Error;

/// Coordinator errors that can occur during distributed transaction processing
#[derive(Debug, Error)]
pub enum CoordinatorError {
    /// Transaction not found in coordinator's state
    #[error("Transaction not found: {0}")]
    TransactionNotFound(String),

    /// Invalid transaction state for the requested operation
    #[error("Invalid transaction state: {0}")]
    InvalidState(String),

    /// Error from the underlying engine
    #[error("Engine error: {0}")]
    EngineError(#[from] proven_engine::MockEngineError),

    /// Transaction prepare phase failed
    #[error("Transaction prepare failed: {0}")]
    PrepareFailed(String),

    /// Timeout waiting for prepare votes from participants
    #[error("Timeout waiting for prepare votes")]
    PrepareTimeout,
}

/// Result type alias for coordinator operations
pub type Result<T> = std::result::Result<T, CoordinatorError>;
