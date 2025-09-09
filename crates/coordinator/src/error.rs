//! Error types for the coordinator

use thiserror::Error;

/// Coordinator error types
#[derive(Error, Debug)]
pub enum CoordinatorError {
    #[error("Transaction not found: {0}")]
    TransactionNotFound(String),

    #[error("Invalid transaction state: {0}")]
    InvalidState(String),

    #[error("Engine error: {0}")]
    EngineError(String),

    #[error("Prepare phase failed: {0}")]
    PrepareFailed(String),

    #[error("Prepare phase timed out")]
    PrepareTimeout,

    #[error("Response timeout")]
    ResponseTimeout,

    #[error("Response channel closed")]
    ResponseChannelClosed,

    #[error("Transaction deadline exceeded")]
    DeadlineExceeded,

    #[error("Transaction was wounded by {wounded_by}")]
    TransactionWounded { wounded_by: String },

    #[error("Operation failed: {0}")]
    OperationFailed(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("HLC timestamp parse error: {0}")]
    TimestampParseError(String),

    #[error("Other error: {0}")]
    Other(String),
}

/// Result type for coordinator operations
pub type Result<T> = std::result::Result<T, CoordinatorError>;
