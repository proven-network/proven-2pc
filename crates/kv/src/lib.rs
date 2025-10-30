//! Key-Value storage engine
//!
//! This crate will provide a distributed key-value store using the same
//! HLC timestamps and stream processing patterns as the SQL engine.

pub mod engine;
pub mod storage;
pub mod types;

// Re-export types for convenience
pub use engine::KvTransactionEngine;
pub use proven_common::TransactionId;
pub use types::{KvOperation, KvResponse, Value};
