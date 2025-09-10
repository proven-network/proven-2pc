//! Error types for the runner

use thiserror::Error;

/// Result type for runner operations
pub type Result<T> = std::result::Result<T, RunnerError>;

/// Runner errors
#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("Engine error: {0}")]
    Engine(String),

    #[error("No processor available for stream: {0}")]
    NoProcessorAvailable(String),

    #[error("Processor start failed: {0}")]
    ProcessorStartFailed(String),

    #[error("Request timeout")]
    RequestTimeout,

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Subscription error: {0}")]
    Subscription(String),

    #[error("Other error: {0}")]
    Other(String),
}
