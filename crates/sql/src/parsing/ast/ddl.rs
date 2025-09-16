//! Data Definition Language (DDL) statements: CREATE, DROP, ALTER

use super::common::Direction;
use super::expressions::Expression;
use crate::types::data_type::DataType;

/// CREATE TABLE column definition.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub primary_key: bool,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub unique: bool,
    pub index: bool,
    pub references: Option<String>,
}

/// An index column definition, including the expression and sort direction
#[derive(Debug, Clone)]
pub struct IndexColumn {
    /// The expression to index (can be a simple column or complex expression)
    pub expression: Expression,
    /// Sort direction for this column in the index
    pub direction: Option<Direction>,
}

/// DDL statements
#[derive(Debug, Clone)]
pub enum DdlStatement {
    /// CREATE TABLE: creates a new table.
    CreateTable {
        /// The table name.
        name: String,
        /// Column specifications.
        columns: Vec<Column>,
        /// IF NOT EXISTS: if true, don't error if the table already exists.
        if_not_exists: bool,
    },
    /// DROP TABLE: drops one or more tables.
    DropTable {
        /// The tables to drop.
        names: Vec<String>,
        /// IF EXISTS: if true, don't error if the table doesn't exist.
        if_exists: bool,
    },
    /// CREATE INDEX: creates an index on one or more table columns.
    CreateIndex {
        /// The index name.
        name: String,
        /// The table to index.
        table: String,
        /// The columns to index (supports composite indexes with expressions and ordering).
        columns: Vec<IndexColumn>,
        /// UNIQUE: if true, create a unique index.
        unique: bool,
        /// INCLUDE: additional columns to store in the index (covering index).
        included_columns: Option<Vec<String>>,
    },
    /// DROP INDEX: drops an index.
    DropIndex {
        /// The index name.
        name: String,
        /// IF EXISTS: if true, don't error if the index doesn't exist.
        if_exists: bool,
    },
}
