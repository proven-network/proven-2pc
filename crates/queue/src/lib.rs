//! Queue storage engine
//!
//! This crate provides a distributed queue using the same
//! HLC timestamps and stream processing patterns as the KV and SQL engines.

pub mod engine;
pub mod storage;
pub mod types;

// Re-export types for convenience
pub use engine::QueueTransactionEngine;
pub use proven_common::TransactionId;
pub use types::{QueueOperation, QueueResponse};
