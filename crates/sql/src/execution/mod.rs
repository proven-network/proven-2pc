//! Query execution module for SQL queries
//!
//! This module contains the MVCC-aware query executor that runs execution plans.
//!
//! Note: Our join and aggregation implementations are MVCC-aware and require
//! transaction context for proper visibility checks and expression evaluation.

mod aggregator;
mod delete;
mod executor;
pub(crate) mod expression;
pub(crate) mod helpers;
mod insert;
mod join;
mod select;
mod update;

// Re-export main types and functions
pub use executor::{ExecutionResult, execute_with_params};
