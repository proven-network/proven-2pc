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
pub use common::{Direction, FromClause, JoinType};
pub use ddl::{Column, DdlStatement, IndexColumn};
pub use dml::{DmlStatement, InsertSource, SelectStatement};
pub use expressions::{Expression, Literal, Operator};

/// SQL statements represented as an Abstract Syntax Tree (AST).
/// The statement is the root node of this tree, describing the syntactic
/// structure of a SQL statement. Built from raw SQL by the parser,
/// passed to the planner which validates it and builds an execution plan.
#[derive(Debug, Clone)]
pub enum Statement {
    /// EXPLAIN: explains a SQL statement's execution plan.
    Explain(Box<Statement>),

    /// DDL statements (CREATE, DROP, ALTER)
    Ddl(DdlStatement),

    /// DML statements (SELECT, INSERT, UPDATE, DELETE)
    Dml(DmlStatement),
}

// Convenience constructors and conversions for backwards compatibility
impl Statement {
    /// Creates a CreateTable statement
    pub fn create_table(name: String, columns: Vec<Column>, if_not_exists: bool) -> Self {
        Statement::Ddl(DdlStatement::CreateTable {
            name,
            columns,
            if_not_exists,
        })
    }

    /// Creates a DropTable statement
    pub fn drop_table(names: Vec<String>, if_exists: bool) -> Self {
        Statement::Ddl(DdlStatement::DropTable { names, if_exists })
    }

    /// Creates a CreateIndex statement
    pub fn create_index(
        name: String,
        table: String,
        columns: Vec<IndexColumn>,
        unique: bool,
        included_columns: Option<Vec<String>>,
    ) -> Self {
        Statement::Ddl(DdlStatement::CreateIndex {
            name,
            table,
            columns,
            unique,
            included_columns,
        })
    }

    /// Creates a DropIndex statement
    pub fn drop_index(name: String, if_exists: bool) -> Self {
        Statement::Ddl(DdlStatement::DropIndex { name, if_exists })
    }

    /// Creates a Delete statement
    pub fn delete(table: String, r#where: Option<Expression>) -> Self {
        Statement::Dml(DmlStatement::Delete { table, r#where })
    }

    /// Creates an Insert statement
    pub fn insert(table: String, columns: Option<Vec<String>>, source: InsertSource) -> Self {
        Statement::Dml(DmlStatement::Insert {
            table,
            columns,
            source,
        })
    }

    /// Creates an Update statement
    pub fn update(
        table: String,
        set: std::collections::BTreeMap<String, Option<Expression>>,
        r#where: Option<Expression>,
    ) -> Self {
        Statement::Dml(DmlStatement::Update {
            table,
            set,
            r#where,
        })
    }

    /// Creates a Select statement
    pub fn select(select_stmt: SelectStatement) -> Self {
        Statement::Dml(DmlStatement::Select(Box::new(select_stmt)))
    }
}
