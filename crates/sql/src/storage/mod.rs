//! New MVCC-based storage implementation
//!
//! This module provides a reimplementation of SQL storage using the proven-mvcc
//! abstraction. It will eventually replace the existing storage/ module.
//!
//! Key differences from old storage:
//! - Uses proven-mvcc for table and index storage
//! - One MvccStorage per table (separate partitions)
//! - One MvccStorage per index (separate partitions)
//! - Shared keyspace for atomic cross-entity batching
//! - Preserves schema-aware row encoding (36.1 bytes/row)

pub mod encoding;
pub mod engine;
pub mod entity;
pub mod predicate_store;

// Re-export key types
pub use engine::{SqlStorage, SqlStorageConfig};
