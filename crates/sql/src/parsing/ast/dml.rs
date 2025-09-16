//! Data Manipulation Language (DML) statements: SELECT, INSERT, UPDATE, DELETE

use super::common::{Direction, FromClause};
use super::expressions::Expression;
use std::collections::BTreeMap;

/// Source of data for INSERT statements.
#[derive(Debug, Clone)]
pub enum InsertSource {
    /// VALUES: explicit values to insert.
    Values(Vec<Vec<Expression>>),
    /// SELECT: values from a SELECT query.
    Select(Box<SelectStatement>),
    /// DEFAULT VALUES: insert a row with all default values.
    DefaultValues,
}

/// SELECT statement structure
#[derive(Debug, Clone)]
pub struct SelectStatement {
    /// Expressions to select, with an optional column alias.
    pub select: Vec<(Expression, Option<String>)>,
    /// FROM: tables to select from.
    pub from: Vec<FromClause>,
    /// WHERE: optional condition to filter rows.
    pub r#where: Option<Expression>,
    /// GROUP BY: expressions to group and aggregate by.
    pub group_by: Vec<Expression>,
    /// HAVING: expression to filter groups by.
    pub having: Option<Expression>,
    /// ORDER BY: expressions to sort by, with direction.
    pub order_by: Vec<(Expression, Direction)>,
    /// OFFSET: row offset to start from.
    pub offset: Option<Expression>,
    /// LIMIT: maximum number of rows to return.
    pub limit: Option<Expression>,
}

/// DML statements
#[derive(Debug, Clone)]
pub enum DmlStatement {
    /// DELETE: deletes rows from a table.
    Delete {
        /// The table to delete from.
        table: String,
        /// WHERE: optional condition to match rows to delete.
        r#where: Option<Expression>,
    },
    /// INSERT INTO: inserts new rows into a table.
    Insert {
        /// Table to insert into.
        table: String,
        /// Columns to insert values into. If None, all columns are used.
        columns: Option<Vec<String>>,
        /// Source of data to insert.
        source: InsertSource,
    },
    /// UPDATE: updates rows in a table.
    Update {
        table: String,
        set: BTreeMap<String, Option<Expression>>, // column → value, None for default value
        r#where: Option<Expression>,
    },
    /// SELECT: selects rows, possibly from a table.
    Select(Box<SelectStatement>),
}
