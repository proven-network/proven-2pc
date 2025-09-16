//! AST with type annotations

use crate::parsing::ast::{Expression, Statement};
use crate::types::data_type::DataType;

/// Statement with type annotations
#[derive(Debug, Clone)]
pub enum AnnotatedStatement {
    /// SELECT statement with annotated expressions
    Select(AnnotatedSelect),
    /// INSERT statement with annotated values
    Insert(AnnotatedInsert),
    /// UPDATE statement with annotated assignments
    Update(AnnotatedUpdate),
    /// DELETE statement with annotated condition
    Delete(AnnotatedDelete),
    /// CREATE TABLE with type information
    CreateTable(AnnotatedCreateTable),
    /// Other DDL statements (no annotations needed)
    Ddl(Statement),
}

impl AnnotatedStatement {
    /// Convert back to original Statement for planning
    pub fn into_statement(self) -> Statement {
        match self {
            AnnotatedStatement::Select(select) => select.statement,
            AnnotatedStatement::Insert(insert) => insert.statement,
            AnnotatedStatement::Update(update) => update.statement,
            AnnotatedStatement::Delete(delete) => delete.statement,
            AnnotatedStatement::CreateTable(create) => create.statement,
            AnnotatedStatement::Ddl(statement) => statement,
        }
    }
}

/// Annotated SELECT statement
#[derive(Debug, Clone)]
pub struct AnnotatedSelect {
    /// Original statement
    pub statement: Statement,
    /// Type information for each projection
    pub projection_types: Vec<DataType>,
    /// Type of WHERE clause if present
    pub where_type: Option<DataType>,
    /// Type of GROUP BY expressions
    pub group_by_types: Vec<DataType>,
    /// Type of HAVING clause if present
    pub having_type: Option<DataType>,
    /// Type of ORDER BY expressions
    pub order_by_types: Vec<DataType>,
}

/// Annotated INSERT statement
#[derive(Debug, Clone)]
pub struct AnnotatedInsert {
    /// Original statement
    pub statement: Statement,
    /// Type information for inserted values
    pub value_types: Vec<Vec<DataType>>,
    /// Expected column types
    pub column_types: Vec<DataType>,
}

/// Annotated UPDATE statement
#[derive(Debug, Clone)]
pub struct AnnotatedUpdate {
    /// Original statement
    pub statement: Statement,
    /// Type information for SET assignments
    pub assignment_types: Vec<(String, DataType)>,
    /// Type of WHERE clause if present
    pub where_type: Option<DataType>,
}

/// Annotated DELETE statement
#[derive(Debug, Clone)]
pub struct AnnotatedDelete {
    /// Original statement
    pub statement: Statement,
    /// Type of WHERE clause if present
    pub where_type: Option<DataType>,
}

/// Annotated CREATE TABLE statement
#[derive(Debug, Clone)]
pub struct AnnotatedCreateTable {
    /// Original statement
    pub statement: Statement,
    /// Validated column types
    pub column_types: Vec<(String, DataType, bool)>, // (name, type, nullable)
}

/// Expression with type annotation
#[derive(Debug, Clone)]
pub struct AnnotatedExpression {
    /// Original expression
    pub expr: Expression,
    /// Inferred or validated type
    pub data_type: DataType,
    /// Whether this expression can be NULL
    pub nullable: bool,
}