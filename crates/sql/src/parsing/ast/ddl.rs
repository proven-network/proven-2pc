//! Data Definition Language (DDL) statements: CREATE, DROP, ALTER

use super::common::Direction;
use super::dml::ValuesStatement;
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

/// Referential actions for foreign key constraints
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum ReferentialAction {
    /// NO ACTION (default) - reject the operation
    #[default]
    NoAction,
    /// RESTRICT - same as NO ACTION
    Restrict,
    /// CASCADE - propagate the operation to referencing rows
    Cascade,
    /// SET NULL - set referencing columns to NULL
    SetNull,
    /// SET DEFAULT - set referencing columns to their default values
    SetDefault,
}

/// Foreign key constraint definition
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ForeignKeyConstraint {
    /// Optional constraint name
    pub name: Option<String>,
    /// Columns in this table that reference the foreign table
    pub columns: Vec<String>,
    /// The referenced table
    pub referenced_table: String,
    /// Columns in the referenced table (if empty, uses primary key)
    pub referenced_columns: Vec<String>,
    /// Action to take on DELETE of referenced row
    pub on_delete: ReferentialAction,
    /// Action to take on UPDATE of referenced row
    pub on_update: ReferentialAction,
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
        /// Foreign key constraints.
        foreign_keys: Vec<ForeignKeyConstraint>,
        /// IF NOT EXISTS: if true, don't error if the table already exists.
        if_not_exists: bool,
    },
    /// CREATE TABLE AS VALUES: creates a new table from VALUES clause.
    CreateTableAsValues {
        /// The table name.
        name: String,
        /// The VALUES statement to populate from.
        values: ValuesStatement,
        /// IF NOT EXISTS: if true, don't error if the table already exists.
        if_not_exists: bool,
    },
    /// DROP TABLE: drops one or more tables.
    DropTable {
        /// The tables to drop.
        names: Vec<String>,
        /// IF EXISTS: if true, don't error if the table doesn't exist.
        if_exists: bool,
        /// CASCADE: if true, drop dependent objects (foreign key constraints).
        cascade: bool,
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
