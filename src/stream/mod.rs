//! Stream-based SQL engine processor
//!
//! This module provides stream processing capabilities for SQL operations
//! with proper transaction isolation using MVCC and pessimistic concurrency control.

pub mod message;
pub mod processor;
pub mod response;
pub mod transaction;

// Re-export commonly used types
pub use message::{SqlOperation, StreamMessage};
pub use processor::SqlStreamProcessor;
pub use response::{MockResponseChannel, ResponseChannel, SqlResponse};
pub use transaction::{AccessLogEntry, TransactionContext, TransactionState};
