//! Error types for the stream processor

use thiserror::Error;

/// Result type for stream operations
pub type Result<T> = std::result::Result<T, ProcessorError>;

/// Errors that can occur during stream processing
#[derive(Error, Debug, Clone)]
pub enum ProcessorError {
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Invalid transaction ID: {0}")]
    InvalidTransactionId(String),

    #[error("Transaction not found: {0}")]
    TransactionNotFound(String),

    #[error("Transaction in invalid state: {0}")]
    InvalidState(String),

    #[error("Engine error: {0}")]
    EngineError(String),

    #[error("Persistence failed: {0}")]
    PersistenceFailed(String),

    #[error("Recovery failed: {0}")]
    RecoveryFailed(String),
}

impl From<&str> for ProcessorError {
    fn from(s: &str) -> Self {
        ProcessorError::InvalidOperation(s.to_string())
    }
}

impl From<String> for ProcessorError {
    fn from(s: String) -> Self {
        ProcessorError::InvalidOperation(s)
    }
}
