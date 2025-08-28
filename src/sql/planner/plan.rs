//! Execution plan representation
//!
//! Plan nodes form a tree that is executed recursively. Each node pulls
//! from its children and processes the rows.

use crate::error::Result;
use crate::sql::types::expression::Expression;
use crate::sql::types::value::Row;
use std::sync::Arc;

/// Execution plan - the root of the plan tree
#[derive(Debug, Clone)]
pub enum Plan {
    /// SELECT query
    Select(Box<Node>),

    /// INSERT statement
    Insert {
        table: String,
        columns: Option<Vec<usize>>, // Column indices in table schema
        source: Box<Node>,
    },

    /// UPDATE statement  
    Update {
        table: String,
        assignments: Vec<(usize, Expression)>, // (column_index, value_expr)
        source: Box<Node>,
    },

    /// DELETE statement
    Delete {
        table: String,
        source: Box<Node>,
    },

    /// CREATE TABLE
    CreateTable {
        name: String,
        schema: crate::sql::types::schema::Table,
    },

    /// DROP TABLE
    DropTable {
        name: String,
        if_exists: bool,
    },

    /// Transaction control
    Begin {
        read_only: bool,
    },
    Commit,
    Rollback,

    /// CREATE INDEX
    CreateIndex {
        name: String,
        table: String,
        column: String,
        unique: bool,
    },

    /// DROP INDEX
    DropIndex {
        name: String,
        if_exists: bool,
    },
}

/// Execution node in the plan tree
#[derive(Debug, Clone)]
pub enum Node {
    /// Table scan
    Scan {
        table: String,
        alias: Option<String>,
    },

    /// Index scan - uses an index to lookup rows
    IndexScan {
        table: String,
        alias: Option<String>,
        index_column: String,
        value: Expression,
    },

    /// Filter rows (WHERE clause)
    Filter {
        source: Box<Node>,
        predicate: Expression,
    },

    /// Project columns (SELECT clause)
    Projection {
        source: Box<Node>,
        expressions: Vec<Expression>,
        aliases: Vec<Option<String>>,
    },

    /// Sort rows (ORDER BY)
    Order {
        source: Box<Node>,
        order_by: Vec<(Expression, Direction)>,
    },

    /// Limit rows
    Limit { source: Box<Node>, limit: usize },

    /// Skip rows (OFFSET)
    Offset { source: Box<Node>, offset: usize },

    /// Aggregate functions with GROUP BY
    Aggregate {
        source: Box<Node>,
        group_by: Vec<Expression>,
        aggregates: Vec<AggregateFunc>,
    },

    /// Hash join
    HashJoin {
        left: Box<Node>,
        right: Box<Node>,
        left_col: usize,
        right_col: usize,
        join_type: JoinType,
    },

    /// Nested loop join
    NestedLoopJoin {
        left: Box<Node>,
        right: Box<Node>,
        predicate: Expression,
        join_type: JoinType,
    },

    /// Values (for INSERT)
    Values { rows: Vec<Vec<Expression>> },

    /// Empty result
    Nothing,
}

impl Node {
    /// Get the column count this node produces
    pub fn column_count(
        &self,
        schemas: &std::collections::HashMap<String, crate::sql::types::schema::Table>,
    ) -> usize {
        match self {
            Node::Scan { table, .. } => schemas.get(table).map(|s| s.columns.len()).unwrap_or(0),
            Node::IndexScan { table, .. } => {
                schemas.get(table).map(|s| s.columns.len()).unwrap_or(0)
            }
            Node::Projection { expressions, .. } => expressions.len(),
            Node::Filter { source, .. } => source.column_count(schemas),
            Node::Order { source, .. } => source.column_count(schemas),
            Node::Limit { source, .. } => source.column_count(schemas),
            Node::Offset { source, .. } => source.column_count(schemas),
            Node::Aggregate {
                group_by,
                aggregates,
                ..
            } => group_by.len() + aggregates.len(),
            Node::HashJoin { left, right, .. } => {
                left.column_count(schemas) + right.column_count(schemas)
            }
            Node::NestedLoopJoin { left, right, .. } => {
                left.column_count(schemas) + right.column_count(schemas)
            }
            Node::Values { rows } => rows.first().map(|r| r.len()).unwrap_or(0),
            Node::Nothing => 0,
        }
    }
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Ascending,
    Descending,
}

/// Join type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// Aggregate function
#[derive(Debug, Clone)]
pub enum AggregateFunc {
    Count(Expression),
    Sum(Expression),
    Avg(Expression),
    Min(Expression),
    Max(Expression),
}

/// Row iterator type returned by node execution
pub type Rows = Box<dyn Iterator<Item = Result<Arc<Row>>> + Send>;

/// Execute a plan node, returning a row iterator
pub trait NodeExecutor {
    /// Execute the node and return rows
    fn execute(&self, node: &Node) -> Result<Rows>;
}
