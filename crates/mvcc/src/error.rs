//! Error types for MVCC storage

use std::fmt;

/// Result type for MVCC operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error types that can occur in MVCC storage
#[derive(Debug)]
pub enum Error {
    /// Fjall storage error
    Fjall(fjall::Error),

    /// Encoding/decoding error
    Encoding(String),

    /// Other errors
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Fjall(e) => write!(f, "Fjall error: {}", e),
            Error::Encoding(e) => write!(f, "Encoding error: {}", e),
            Error::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}

impl From<fjall::Error> for Error {
    fn from(e: fjall::Error) -> Self {
        Error::Fjall(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Other(e.to_string())
    }
}
