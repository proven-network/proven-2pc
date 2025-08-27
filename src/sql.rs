//! SQL parsing and execution (placeholder for now)
//!
//! This module will eventually contain:
//! - SQL parser (adapted from toydb)
//! - Query planner with lock analysis
//! - Expression evaluator
//! - Deterministic function validation

use crate::error::Result;

/// Placeholder for SQL statement parsing
pub fn parse_sql(_sql: &str) -> Result<Statement> {
    // TODO: Integrate toydb's parser
    Ok(Statement::Select)
}

/// Placeholder for SQL statements
#[derive(Debug, Clone)]
pub enum Statement {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
}

/// Check if SQL is deterministic
pub fn validate_deterministic(_stmt: &Statement) -> Result<()> {
    // TODO: Implement validation
    Ok(())
}
