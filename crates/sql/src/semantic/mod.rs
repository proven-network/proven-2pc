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

pub mod analyzer;
pub mod caching_analyzer;
pub mod predicate;
pub mod statement;

// New phase-based modules
pub mod optimization;
pub mod resolution;
pub mod typing;
pub mod validation;

#[cfg(test)]
mod test_aggregate_nesting;
#[cfg(test)]
mod test_default_validation;
#[cfg(test)]
mod test_foreign_key_validation;
#[cfg(test)]
mod test_group_by_validation;
#[cfg(test)]
mod test_insert_validation;
#[cfg(test)]
mod test_zero_copy;
// #[cfg(test)]
// mod type_checker_test;

// Export the new implementation types as primary interface
pub use caching_analyzer::CachingSemanticAnalyzer;
pub use statement::AnalyzedStatement;
