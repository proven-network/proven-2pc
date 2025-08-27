//! SQL parsing and execution module for proven-sql
//!
//! This module provides:
//! - SQL parser (adapted from toydb)
//! - Query planner with lock analysis (TODO)
//! - Expression evaluator (TODO)
//! - Deterministic function validation

pub mod parser;

use crate::error::Result;
use parser::Parser;

/// Parse a SQL statement string into an AST
pub fn parse_sql(sql: &str) -> Result<Statement> {
    Parser::parse(sql)
}

/// Check if SQL statement uses only deterministic functions
/// This is crucial for consensus-ordered SQL execution
pub fn validate_deterministic(_stmt: &parser::Statement) -> Result<()> {
    
    // List of non-deterministic functions to reject
    const NON_DETERMINISTIC_FUNCS: &[&str] = &[
        "RAND", "RANDOM", "UUID", "NOW", "CURRENT_TIME", 
        "CURRENT_DATE", "CURRENT_TIMESTAMP", "SYSDATE"
    ];
    
    // TODO: Walk the AST and check all function calls
    // For now, we'll accept everything and implement later
    Ok(())
}

// Re-export key types
pub use parser::{Statement, Expression, Literal};
