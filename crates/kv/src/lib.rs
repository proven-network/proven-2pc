//! Key-Value storage engine
//!
//! This crate will provide a distributed key-value store using the same
//! HLC timestamps and stream processing patterns as the SQL engine.

pub mod engine;
pub mod storage;
pub mod types;

// Re-export HLC types for convenience
pub use proven_hlc::{HlcClock, HlcTimestamp, NodeId};

// Re-export types for convenience
pub use engine::KvTransactionEngine;
pub use types::{KvOperation, KvResponse, Value};
