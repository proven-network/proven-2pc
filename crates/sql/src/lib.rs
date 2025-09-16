//! A distributed SQL engine with Pessimistic Concurrency Control (PCC)
//!
//! This crate provides a SQL engine designed for distributed systems that:
//! - Uses pessimistic locking instead of MVCC
//! - Implements wound-wait deadlock prevention
//! - Provides full lock visibility for distributed coordination
//! - Ensures deterministic execution for Raft consensus

pub mod error;
pub mod execution;
pub mod parsing;
pub mod planning;
pub mod semantic;
pub mod storage;
pub mod stream;
pub mod types;

pub use error::{Error, Result};
pub use stream::engine::SqlTransactionEngine;
pub use stream::operation::SqlOperation;
pub use stream::response::SqlResponse;
