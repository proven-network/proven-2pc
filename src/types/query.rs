//! Query-related types used across parser, planner, and executor

use super::Value;
use crate::error::Result;
use std::sync::Arc;

/// A single row of data with reference counting for efficient sharing
pub type RowRef = Arc<Vec<Value>>;

/// Row iterator type for streaming execution
/// NOTE: We use lifetime parameter here which requires collecting in some cases.
/// Adding a 'static lifetime would enable true streaming but would require:
/// 1. Solving the aliasing problem (can't borrow storage mutably while iterating)
/// 2. Handling recursive calls (joins need two iterators from same storage)
/// 3. Extensive API changes throughout the codebase
pub type Rows<'a> = Box<dyn Iterator<Item = Result<RowRef>> + 'a>;

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
