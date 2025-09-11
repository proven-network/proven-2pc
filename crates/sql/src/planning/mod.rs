//! Query planner module for SQL execution
//!
//! This module contains:
//! - Query planner that converts AST to execution plans
//! - Plan node definitions for the execution tree
//! - Query optimizer with transformation rules

pub mod optimizer;
pub mod plan;
pub mod planner;
pub mod predicate;
pub mod prepared_cache;

// Re-export main types
pub use optimizer::Optimizer;
pub use plan::{AggregateFunc, Direction, JoinType, Node, Plan};
pub use planner::Planner;
pub use predicate::{ConflictInfo, Predicate, PredicateCondition, QueryPredicates};
