//! SQL parser module - adapted from toydb for proven-sql's PCC architecture
//!
//! This module parses raw SQL strings into a structured Abstract Syntax Tree (AST).
//! Key differences from toydb:
//! - Removed MVCC-specific features (AS OF clause)
//! - Added support for additional data types (UUID, Timestamp, Blob, Decimal)
//! - Will integrate with lock analysis in the planner phase

pub mod ast;
pub mod caching_parser;
mod lexer;
mod parser;

use crate::error::Result;

pub use caching_parser::CachingParser;
pub use lexer::{Keyword, Lexer, Token};
pub use parser::Parser;

// Re-export commonly used AST types from the new structure
pub use ast::{Operator, Statement};

// Re-export submodules for more specific access

/// Parse a SQL statement string into an AST
pub fn parse_sql(sql: &str) -> Result<Statement> {
    Parser::parse(sql)
}
