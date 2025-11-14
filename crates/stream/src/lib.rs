//! Append-only stream implementation with commit-time position allocation
//!
//! Key design principles:
//! - Gap-free positions: Positions are assigned at commit time, not append time
//! - Simple fjall partitions: staging (uncommitted) and stream (committed)
//! - 1-indexed positions: First stream entry is at position 1, position 0 means empty
//! - No locks needed: All operations are non-conflicting
//! - Values kept in memory during transaction, written to stream at commit
//!
//! # Semantics
//!
//! - **Uncommitted appends are not visible**: Positions are assigned at commit, so reads
//!   within a transaction cannot see uncommitted appends (even from the same transaction)
//! - **Gap-free positions**: All committed positions are consecutive with no gaps
//! - **Crash recovery**: Uncommitted appends stored in staging partition for recovery
//!
//! # Example
//!
//! ```rust,ignore
//! use proven_stream::{StreamTransactionEngine, StreamOperation, StreamResponse};
//! use proven_processor::TransactionEngine;
//!
//! let mut engine = StreamTransactionEngine::new();
//! let mut batch = engine.start_batch();
//! let txn_id = TransactionId::new();
//!
//! // Append values (positions assigned at commit)
//! engine.apply_operation(
//!     &mut batch,
//!     StreamOperation::Append {
//!         values: vec![Value::from(42), Value::from(100)],
//!     },
//!     txn_id,
//! );
//!
//! engine.commit(&mut batch, txn_id);
//! engine.commit_batch(batch, 1);
//!
//! // Read from position 1
//! let response = engine.read_at_timestamp(
//!     StreamOperation::ReadFrom { position: 1, limit: 10 },
//!     txn_id,
//! );
//! ```

pub mod batch;
pub mod engine;
pub mod types;

pub use batch::StreamBatch;
pub use engine::{StreamChangeData, StreamTransactionEngine};
pub use types::{StreamOperation, StreamResponse};
