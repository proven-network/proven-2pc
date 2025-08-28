//! Storage module for proven-sql
//!
//! This module provides the MVCC (Multi-Version Concurrency Control) storage engine
//! that handles versioned data with transaction isolation, along with locking
//! and transaction management.

pub mod lock;
pub mod mvcc;
pub mod read_ops;
pub mod write_ops;

// Re-export the main types
pub use lock::{LockKey, LockManager, LockMode, LockResult};
pub use mvcc::{MvccRowIterator, MvccRowWithIdIterator, MvccStorage};
