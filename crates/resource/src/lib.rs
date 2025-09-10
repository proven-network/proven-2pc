//! Resource storage engine
//!
//! This crate provides a resource/token ledger system using the same
//! HLC timestamps and stream processing patterns as the KV, Queue, and SQL engines.
//!
//! Each resource instance tracks a single token/resource type with:
//! - Configurable decimals for divisibility
//! - Mint and burn operations
//! - Atomic transfers between accounts
//! - Balance tracking with MVCC
//! - Reservation-based concurrency control for high throughput

pub mod storage;
pub mod stream;
pub mod types;

// Re-export HLC types for convenience
pub use proven_hlc::{HlcClock, HlcTimestamp, NodeId};

// Re-export main types
pub use stream::{ResourceOperation, ResourceResponse, ResourceTransactionEngine};
pub use types::Amount;
