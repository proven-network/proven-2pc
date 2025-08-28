//! Query execution module for SQL queries
//!
//! This module contains:
//! - Query executor that runs execution plans
//! - Aggregator for GROUP BY operations  
//! - Join executors (hash join and nested loop)

pub mod aggregator;
pub mod executor;
pub mod join;

// Re-export main types
pub use aggregator::{Aggregate, Aggregator};
pub use executor::{ExecutionResult, Executor};
pub use join::{HashJoiner, NestedLoopJoiner};
