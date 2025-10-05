//! Storage module for KV
//!
//! Provides MVCC storage with key-level locking for transaction isolation.

pub mod entity;
pub mod lock;
pub mod lock_persistence;

// Re-export main types
pub use entity::{KvDelta, KvEntity, KvKey};
pub use lock::{LockAttemptResult, LockManager, LockMode};
pub use lock_persistence::TransactionLocks;
