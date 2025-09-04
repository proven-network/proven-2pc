//! Stream-based SQL engine processor
//!
//! This module provides stream processing capabilities for SQL operations
//! with proper transaction isolation using MVCC and pessimistic concurrency control.

pub mod deferred;
pub mod message;
pub mod processor;
pub mod response;
pub mod stats_cache;
pub mod transaction;
pub mod wound_wait_tests;

// Re-export commonly used types
pub use message::{SqlOperation, StreamMessage};
pub use processor::SqlStreamProcessor;
pub use response::{MockResponseChannel, ResponseChannel, SqlResponse};
pub use transaction::{AccessLogEntry, TransactionContext, TransactionState};
