//! Error types for the stream processor

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProcessorError {
    #[error("Missing required header: {0}")]
    MissingHeader(&'static str),

    #[error("Invalid transaction ID: {0}")]
    InvalidTransactionId(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Unknown transaction phase: {0}")]
    UnknownPhase(String),

    #[error("Engine error: {0}")]
    EngineError(String),
}

pub type Result<T> = std::result::Result<T, ProcessorError>;
