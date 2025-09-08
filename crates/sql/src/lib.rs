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
pub mod execution;
pub mod parsing;
pub mod planning;
pub mod storage;
pub mod stream;
pub mod types;

// Re-export HLC types from proven-hlc
pub use proven_hlc as hlc;

pub use error::{Error, Result};
pub use execution::{ExecutionResult, Executor};
pub use parsing::Parser;
pub use planning::planner::Planner;
pub use storage::lock::LockManager;
pub use storage::mvcc::MvccStorage;
pub use types::schema::{Column, Table};
pub use types::value::{DataType, Value};
