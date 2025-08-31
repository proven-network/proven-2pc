//! A distributed SQL engine with Pessimistic Concurrency Control (PCC)
//!
//! This crate provides a SQL engine designed for distributed systems that:
//! - Uses pessimistic locking instead of MVCC
//! - Implements wound-wait deadlock prevention
//! - Provides full lock visibility for distributed coordination
//! - Ensures deterministic execution for Raft consensus
//!
//! See LAYERS.md for detailed architecture documentation.

pub mod context;
pub mod error;
pub mod execution;
pub mod hlc;
pub mod parser;
pub mod planner;
pub mod storage;
pub mod stream;
pub mod types;

pub use error::{Error, Result};
pub use storage::lock::LockManager;
pub use storage::mvcc::MvccStorage;
pub use types::value::{DataType, Value};
