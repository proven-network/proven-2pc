//! Query execution module for SQL queries
//!
//! This module contains the MVCC-aware query executor that runs execution plans.
//!
//! Note: Our join and aggregation implementations are MVCC-aware and require
//! transaction context for proper visibility checks and expression evaluation.

mod aggregator;
mod executor;
mod join;

// Re-export main types
pub use aggregator::Aggregator;
pub use executor::{ExecutionResult, Executor};
pub use join::{HashJoiner, NestedLoopJoiner};
