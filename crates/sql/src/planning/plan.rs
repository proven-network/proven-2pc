//! Execution plan representation
//!
//! Plan nodes form a tree that is executed recursively. Each node pulls
//! from its children and processes the rows.

use crate::types::expression::Expression;
pub use crate::types::query::{Direction, JoinType};

/// An index column in the execution plan
/// Uses AST expression for now - will be converted to types::expression later
#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumn {
    /// The expression to index (using AST expression for now)
    pub expression: crate::parsing::ast::Expression,
    /// Sort direction for this column in the index
    pub direction: Option<Direction>,
}

/// Execution plan - the root of the plan tree
#[derive(Debug, Clone, PartialEq)]
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
    Delete { table: String, source: Box<Node> },

    /// CREATE TABLE
    CreateTable {
        name: String,
        schema: crate::types::schema::Table,
        if_not_exists: bool,
    },

    /// DROP TABLE
    DropTable { names: Vec<String>, if_exists: bool },

    /// CREATE INDEX
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<IndexColumn>,
        unique: bool,
        included_columns: Option<Vec<String>>,
    },

    /// DROP INDEX
    DropIndex { name: String, if_exists: bool },
}

impl Plan {
    /// Check if this plan is a DDL operation
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            Plan::CreateTable { .. }
                | Plan::DropTable { .. }
                | Plan::CreateIndex { .. }
                | Plan::DropIndex { .. }
        )
    }

    /// Check if this plan is a DML operation (modifies data)
    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            Plan::Insert { .. } | Plan::Update { .. } | Plan::Delete { .. }
        )
    }

    /// Check if this plan is a query (SELECT)
    pub fn is_query(&self) -> bool {
        matches!(self, Plan::Select(_))
    }
}

/// Execution node in the plan tree
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    /// Table scan
    Scan {
        table: String,
        alias: Option<String>,
    },

    /// Index scan - uses an index to lookup rows (equality)
    IndexScan {
        table: String,
        alias: Option<String>,
        index_name: String,      // Name of the index to use
        values: Vec<Expression>, // Values for each column in the index
    },

    /// Index range scan - uses an index for range queries
    IndexRangeScan {
        table: String,
        alias: Option<String>,
        index_name: String,             // Name of the index to use
        start: Option<Vec<Expression>>, // Start values for each column
        start_inclusive: bool,
        end: Option<Vec<Expression>>, // End values for each column
        end_inclusive: bool,
        reverse: bool, // Scan in reverse order (for DESC)
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
        schemas: &std::collections::HashMap<String, crate::types::schema::Table>,
    ) -> usize {
        match self {
            Node::Scan { table, .. } => schemas.get(table).map(|s| s.columns.len()).unwrap_or(0),
            Node::IndexScan { table, .. } => {
                schemas.get(table).map(|s| s.columns.len()).unwrap_or(0)
            }
            Node::IndexRangeScan { table, .. } => {
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

    /// Get the column names this node produces
    pub fn get_column_names(
        &self,
        schemas: &std::collections::HashMap<String, crate::types::schema::Table>,
    ) -> Vec<String> {
        match self {
            // Projection node has the aliases we want
            Node::Projection {
                aliases,
                expressions,
                ..
            } => {
                let mut names = Vec::new();
                for (i, alias) in aliases.iter().enumerate() {
                    if let Some(name) = alias {
                        names.push(name.clone());
                    } else {
                        // No alias provided, generate a default name
                        names.push(format!("column_{}", i));
                    }
                }
                // If we have fewer aliases than expressions, fill in defaults
                for i in names.len()..expressions.len() {
                    names.push(format!("column_{}", i));
                }
                names
            }

            // For nodes that pass through their source columns, recurse
            Node::Filter { source, .. }
            | Node::Order { source, .. }
            | Node::Limit { source, .. }
            | Node::Offset { source, .. } => source.get_column_names(schemas),

            // Scan nodes get column names from table schema
            Node::Scan { table, .. }
            | Node::IndexScan { table, .. }
            | Node::IndexRangeScan { table, .. } => {
                if let Some(schema) = schemas.get(table) {
                    schema.columns.iter().map(|c| c.name.clone()).collect()
                } else {
                    // Fallback if table not found
                    let count = self.column_count(schemas);
                    (0..count).map(|i| format!("column_{}", i)).collect()
                }
            }

            // Aggregate nodes need special handling
            Node::Aggregate {
                group_by,
                aggregates,
                ..
            } => {
                let mut names = Vec::new();
                // First the GROUP BY columns (would need more context to get their names)
                for i in 0..group_by.len() {
                    names.push(format!("group_{}", i));
                }
                // Then the aggregate columns
                for (i, agg) in aggregates.iter().enumerate() {
                    let name = match agg {
                        AggregateFunc::Count(_) => format!("COUNT_{}", i),
                        AggregateFunc::CountDistinct(_) => format!("COUNT_DISTINCT_{}", i),
                        AggregateFunc::Sum(_) => format!("SUM_{}", i),
                        AggregateFunc::SumDistinct(_) => format!("SUM_DISTINCT_{}", i),
                        AggregateFunc::Avg(_) => format!("AVG_{}", i),
                        AggregateFunc::AvgDistinct(_) => format!("AVG_DISTINCT_{}", i),
                        AggregateFunc::Min(_) => format!("MIN_{}", i),
                        AggregateFunc::MinDistinct(_) => format!("MIN_DISTINCT_{}", i),
                        AggregateFunc::Max(_) => format!("MAX_{}", i),
                        AggregateFunc::MaxDistinct(_) => format!("MAX_DISTINCT_{}", i),
                        AggregateFunc::StDev(_) => format!("STDEV_{}", i),
                        AggregateFunc::StDevDistinct(_) => format!("STDEV_DISTINCT_{}", i),
                        AggregateFunc::Variance(_) => format!("VARIANCE_{}", i),
                        AggregateFunc::VarianceDistinct(_) => format!("VARIANCE_DISTINCT_{}", i),
                    };
                    names.push(name);
                }
                names
            }

            // Join nodes concatenate columns from both sides
            Node::HashJoin { left, right, .. } | Node::NestedLoopJoin { left, right, .. } => {
                let mut names = left.get_column_names(schemas);
                names.extend(right.get_column_names(schemas));
                names
            }

            // Values and Nothing nodes
            Node::Values { rows } => {
                let count = rows.first().map(|r| r.len()).unwrap_or(0);
                (0..count).map(|i| format!("column_{}", i)).collect()
            }

            Node::Nothing => vec![],
        }
    }
}

/// Aggregate function
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunc {
    Count(Expression),
    CountDistinct(Expression),
    Sum(Expression),
    SumDistinct(Expression),
    Avg(Expression),
    AvgDistinct(Expression),
    Min(Expression),
    MinDistinct(Expression),
    Max(Expression),
    MaxDistinct(Expression),
    StDev(Expression),
    StDevDistinct(Expression),
    Variance(Expression),
    VarianceDistinct(Expression),
}
