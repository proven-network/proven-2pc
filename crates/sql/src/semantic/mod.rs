//! Semantic analysis module for SQL statements
//!
//! This module performs semantic validation and type checking on parsed SQL statements.
//! It operates between parsing and planning, ensuring that statements are not just
//! syntactically correct but also semantically valid.
//!
//! The semantic analyzer:
//! - Resolves table and column references
//! - Performs type checking on expressions
//! - Validates constraints and rules
//! - Infers parameter types from context
//! - Produces a lightweight analyzed statement with Arc-wrapped AST

// New efficient implementation
pub mod analyzer;
pub mod caching_analyzer;
pub mod parameters;
pub mod statement;

// Supporting modules
pub mod context;
pub mod resolver;
pub mod type_checker;
pub mod types;
pub mod validators; // New zero-copy validators

#[cfg(test)]
mod test_zero_copy;
// #[cfg(test)]
// mod type_checker_test;

// Export the new implementation types as primary interface
pub use caching_analyzer::CachingSemanticAnalyzer;
pub use parameters::{BoundParameters, bind_parameters};
pub use statement::AnalyzedStatement;
