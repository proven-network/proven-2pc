//! Storage module with fjall-based persistence and MVCC support
//!
//! ## Architecture Overview
//!
//! The storage system provides MVCC (Multi-Version Concurrency Control) with snapshot isolation
//! and supports both read-your-own-writes and time-travel queries.
//!
//! ### Version Tracking System
//!
//! We maintain 4 separate stores for tracking different types of operations:
//!
//! **1. `uncommitted_data` (UncommittedDataStore)**
//! - Stores UNCOMMITTED data operations (Insert/Update/Delete) for active transactions
//! - Used for "read-your-own-writes" semantics
//! - Cleared when transaction commits or aborts
//! - Key format: `{txn_id}{table}{row_id}{seq}`
//!
//! **2. `data_history` (DataHistoryStore)**
//! - Stores COMMITTED data operations from the last ~5 minutes
//! - Used for snapshot reads/time-travel on table data
//! - Enables seeing the database state as it was at any point in the retention window
//! - Key format: `{commit_time}{table_len}{table}{row_id}{seq}` (time-first for efficient range queries!)
//!
//! **3. `uncommitted_index` (UncommittedIndexStore)**
//! - Stores UNCOMMITTED index operations (Insert/Delete) for active transactions
//! - Used for "read-your-own-writes" semantics on index lookups
//! - Cleared when transaction commits or aborts
//! - Key format: `{txn_id}{index_name}{seq}`
//!
//! **4. `index_history` (IndexHistoryStore)**
//! - Stores COMMITTED index operations from the last ~5 minutes
//! - Used for snapshot reads/time-travel on index scans
//! - Enables consistent point-in-time views when using indexes
//! - Key format: `{commit_time}{index_name_len}{index_name}{row_id}{seq}` (time-first for efficient range queries!)
//!
//! ### MVCC Semantics
//!
//! **Snapshot Reads (read_at_timestamp T):**
//! - Should show database state as it was at time T
//! - Any operation committed AFTER T is INVISIBLE
//! - Implementation:
//!   - For data: scan data_history for ops after T, "undo" them
//!     - INSERT after T → hide the row
//!     - UPDATE after T → show old_row values
//!     - DELETE after T → restore the row
//!   - For indexes: scan index_history for ops after T, reverse them
//!     - INSERT after T → remove from result set
//!     - DELETE after T → add back to result set
//!
//! **Read-Your-Own-Writes:**
//! - Active transaction sees its own uncommitted changes
//! - Implementation:
//!   - Check uncommitted_data first for data operations
//!   - Check uncommitted_index first for index operations
//!   - Fall back to committed data if not found
//!
//! ### Why Separate Data and Index Stores?
//!
//! We keep data operations and index operations in separate stores because:
//! 1. Different access patterns - table scans need data ops, index scans need index ops
//! 2. Avoid unnecessary work - table iterator doesn't need to scan index ops
//! 3. Better key structure - each can be optimized for its specific use case

pub mod bucket_manager;
pub mod config;
pub mod data_history;
pub mod encoding;
pub mod engine;
pub mod index;
pub mod index_history;
pub mod index_iterator;
pub mod iterator;
pub mod types;
pub mod uncommitted_data;
pub mod uncommitted_index;

// Re-export main types for convenience
pub use config::StorageConfig;
pub use engine::Storage;

#[cfg(test)]
mod tests;
