//! Simplified queue implementation using MVCC with embedded pointers
//!
//! Key design differences from the original queue crate:
//! - Single MvccStorage instead of separate data + metadata storages
//! - Head/Tail pointers stored as special keys in the same storage
//! - No separate metadata entity needed
//! - Simpler batch handling - no cross-storage coordination issues

pub mod engine;
pub mod entity;
pub mod types;

pub use engine::{QueueBatch, QueueTransactionEngine};
pub use entity::{QueueDelta, QueueEntity, QueueKey, QueueValue};
pub use types::{QueueOperation, QueueResponse};
