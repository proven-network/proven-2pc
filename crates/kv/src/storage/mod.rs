//! Storage module for KV
//!
//! Provides MVCC storage with key-level locking for transaction isolation.

pub mod lock;
pub mod mvcc;

// Re-export main types
pub use lock::{LockAttemptResult, LockManager, LockMode};
pub use mvcc::{MvccStorage, StorageStats};
