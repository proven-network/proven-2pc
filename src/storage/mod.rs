//! Storage module for proven-sql
//!
//! This module provides the MVCC (Multi-Version Concurrency Control) storage engine
//! that handles versioned data with transaction isolation, along with locking
//! and transaction management.

pub mod lock;
pub mod mvcc;
pub mod transaction;

// Re-export the main types
pub use lock::{LockKey, LockManager, LockMode, LockResult};
pub use mvcc::MvccStorage;
pub use transaction::{MvccTransaction, MvccTransactionManager};
