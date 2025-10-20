//! Query planner module for SQL execution
//!
//! This module contains:
//! - Query planner that converts AST to execution plans
//! - Plan node definitions for the execution tree
//! - Query optimizer with transformation rules
//!
//! The planner is organized into specialized modules:
//! - `planner` - Main coordinator and entry point
//! - `aggregate_utils` - Aggregate function utilities
//! - `aggregate_planner` - GROUP BY and aggregate planning
//! - `expression_resolver` - Expression resolution context
//! - `from_planner` - FROM clause and JOIN planning
//! - `index_selector` - Index selection for optimization
//! - `projection_builder` - SELECT projection building
//! - `select_planner` - SELECT statement planning
//! - `statement_planner` - DDL/DML statement planning
//! - `subquery_planner` - Subquery planning utilities

pub mod caching_planner;
pub mod planner;

// Internal modules
mod aggregate_planner;
mod aggregate_utils;
mod expression_resolver;
mod from_planner;
mod index_selector;
mod projection_builder;
mod select_planner;
mod statement_planner;
mod subquery_planner;
