//! A distributed SQL engine with Pessimistic Concurrency Control (PCC)
//!
//! This crate provides a SQL engine designed for distributed systems that:
//! - Uses pessimistic locking instead of MVCC
//! - Implements wound-wait deadlock prevention
//! - Provides full lock visibility for distributed coordination
//! - Ensures deterministic execution for Raft consensus

mod coercion;
mod engine;
mod error;
mod execution;
mod functions;
mod operators;
mod parsing;
mod planning;
mod semantic;
mod storage;
mod types;

pub use engine::SqlTransactionEngine;
pub use error::{Error, Result};
pub use fjall::{CompressionType, PersistMode};
pub use storage::SqlStorageConfig;
pub use types::Value;
pub use types::operation::SqlOperation;
pub use types::response::SqlResponse;
