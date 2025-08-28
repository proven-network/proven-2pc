//! Storage module for proven-sql
//!
//! This module provides the MVCC (Multi-Version Concurrency Control) storage engine
//! that handles versioned data with transaction isolation.

pub mod mvcc;

// Re-export the main storage type
pub use mvcc::MvccStorage;
