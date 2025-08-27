//! SQL parsing and execution module for proven-sql
//!
//! This module provides:
//! - SQL parser (adapted from toydb)
//! - Query planner with lock analysis (TODO)
//! - Expression evaluator (TODO)
//! - Deterministic function validation

pub mod functions;
pub mod parser;

use crate::error::Result;
use parser::Parser;

/// Parse a SQL statement string into an AST
pub fn parse_sql(sql: &str) -> Result<Statement> {
    Parser::parse(sql)
}

// Re-export key types
pub use parser::{Expression, Literal, Statement};
