//! Generic MVCC (Multi-Version Concurrency Control) storage layer
//!
//! This crate provides a generic, reusable MVCC storage implementation built on top of Fjall.
//! It supports:
//! - Snapshot isolation with time-travel queries
//! - Read-your-own-writes semantics
//! - Crash recovery with persistent log index
//! - Time-bucketed partitions for efficient cleanup
//!
//! # Architecture
//!
//! The storage layer is generic over an `MvccEntity` type, which defines:
//! - Key type (e.g., String for KV, (TableName, RowId) for SQL)
//! - Value type (e.g., Value for KV, Row for SQL)
//! - Delta type (how to apply/unapply operations for time-travel)
//!
//! Each crate (KV, SQL, Queue, Resource) implements the `MvccEntity` trait
//! to customize the storage for their specific needs while reusing the core MVCC logic.

pub mod bucket_manager;
pub mod config;
pub mod encoding;
pub mod entity;
pub mod error;
pub mod history;
pub mod iterator;
pub mod storage;
pub mod uncommitted;

// Re-export main types
pub use config::StorageConfig;
pub use encoding::{Decode, Encode};
pub use entity::{MvccDelta, MvccEntity};
pub use error::{Error, Result};
pub use iterator::MvccIterator;
pub use storage::MvccStorage;

// Re-export Fjall Batch for convenience
pub use fjall::Batch;

/// Type alias for the fjall KV iterator
type FjallIter<'a> =
    Box<dyn Iterator<Item = std::result::Result<(Box<[u8]>, Box<[u8]>), fjall::Error>> + 'a>;
