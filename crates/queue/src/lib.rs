//! Queue storage engine
//!
//! This crate provides a distributed queue using the same
//! HLC timestamps and stream processing patterns as the KV and SQL engines.

pub mod engine;
pub mod storage;
pub mod types;

// Re-export HLC types for convenience
pub use proven_hlc::{HlcClock, HlcTimestamp, NodeId};

// Re-export transaction engine for stream processors
pub use engine::QueueTransactionEngine;
pub use types::{QueueOperation, QueueResponse, QueueValue};
