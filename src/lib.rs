//! A distributed SQL engine with Pessimistic Concurrency Control (PCC)
//!
//! This crate provides a SQL engine designed for distributed systems that:
//! - Uses pessimistic locking instead of MVCC
//! - Implements wound-wait deadlock prevention
//! - Provides full lock visibility for distributed coordination
//! - Ensures deterministic execution for Raft consensus
//!
//! See LAYERS.md for detailed architecture documentation.

pub mod error;
pub mod hlc;
pub mod lock;
pub mod sql;
pub mod storage;
pub mod transaction;
pub mod types;

pub use error::{Error, Result};
pub use lock::LockManager;
pub use storage::Storage;
pub use transaction::{Transaction, TransactionManager};
pub use types::{DataType, Value};
