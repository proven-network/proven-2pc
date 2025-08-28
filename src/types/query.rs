//! Query-related types used across parser, planner, and executor

use super::Value;
use std::sync::Arc;

/// A row of data with reference counting for efficient sharing
pub type RowRef = Arc<Vec<Value>>;

/// JOIN types for SQL queries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
    Full,
}

/// Sort direction for ORDER BY clauses
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Direction {
    #[default]
    Ascending,
    Descending,
}
