//! Abstract Syntax Tree (AST) for SQL statements
//!
//! Adapted from toydb for proven-sql's PCC architecture.
//! Changes:
//! - Removed MVCC-specific fields (as_of in BEGIN)
//! - Will add lock analysis annotations in query planner phase

pub mod common;
pub mod ddl;
pub mod dml;
pub mod expressions;

// Re-export commonly used types at the module level
pub use common::FromClause;
pub use ddl::{Column, DdlStatement, IndexColumn};
pub use dml::{DistinctClause, DmlStatement, InsertSource, SelectStatement};
pub use expressions::{Expression, Literal, Operator};

/// SQL statements represented as an Abstract Syntax Tree (AST).
/// The statement is the root node of this tree, describing the syntactic
/// structure of a SQL statement. Built from raw SQL by the parser,
/// passed to the planner which validates it and builds an execution plan.
#[derive(Debug, Clone)]
pub enum Statement {
    /// EXPLAIN: explains a SQL statement's execution plan.
    /// TODO: should handle this
    #[allow(dead_code)]
    Explain(Box<Statement>),

    /// DDL statements (CREATE, DROP, ALTER)
    Ddl(DdlStatement),

    /// DML statements (SELECT, INSERT, UPDATE, DELETE)
    Dml(DmlStatement),
}
