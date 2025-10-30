//! Resource storage engine
//!
//! This crate provides a resource/token ledger system using
//! TransactionIds and stream processing patterns as the KV, Queue, and SQL engines.
//!
//! Each resource instance tracks a single token/resource type with:
//! - Configurable decimals for divisibility
//! - Mint and burn operations
//! - Atomic transfers between accounts
//! - Balance tracking with MVCC
//! - Reservation-based concurrency control for high throughput

pub mod engine;
pub mod storage;
pub mod types;

// Re-export TransactionId for convenience
pub use proven_common::TransactionId;

// Re-export main types
pub use engine::ResourceTransactionEngine;
pub use types::{Amount, ResourceOperation, ResourceResponse};
