//! Stream-based SQL engine processor
//!
//! This module provides stream processing capabilities for SQL operations
//! with proper transaction isolation using MVCC and pessimistic concurrency control.

pub mod engine;
pub mod operation;
pub mod response;
pub mod stats_cache;
pub mod transaction;

// Wrapper around generic processor
mod processor;

// Re-export commonly used types
pub use operation::SqlOperation;
pub use processor::SqlStreamProcessor;
pub use response::SqlResponse;
pub use transaction::{AccessLogEntry, TransactionContext, TransactionState};
