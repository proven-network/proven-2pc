//! Execution plan representation
//!
//! Plan nodes form a tree that is executed recursively. Each node pulls
//! from its children and processes the rows.

use crate::types::expression::Expression;
pub use crate::types::query::{Direction, JoinType};
use std::fmt;

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
    /// Query plan (SELECT or VALUES)
    Query {
        root: Box<Node>,
        params: Vec<Expression>,
        column_names: Option<Vec<String>>,
    },

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
        foreign_keys: Vec<crate::parsing::ast::ddl::ForeignKeyConstraint>,
        if_not_exists: bool,
    },

    /// CREATE TABLE AS VALUES
    CreateTableAsValues {
        name: String,
        schema: crate::types::schema::Table,
        values_plan: Box<Plan>,
        if_not_exists: bool,
    },

    /// CREATE TABLE AS SELECT
    CreateTableAsSelect {
        name: String,
        schema: crate::types::schema::Table,
        select_plan: Box<Plan>,
        if_not_exists: bool,
    },

    /// DROP TABLE
    DropTable {
        names: Vec<String>,
        if_exists: bool,
        cascade: bool,
    },

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

    /// ALTER TABLE
    AlterTable {
        name: String,
        operation: crate::parsing::ast::ddl::AlterTableOperation,
    },
}

impl Plan {
    /// Check if this plan is a DDL operation
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            Plan::CreateTable { .. }
                | Plan::CreateTableAsValues { .. }
                | Plan::CreateTableAsSelect { .. }
                | Plan::DropTable { .. }
                | Plan::AlterTable { .. }
                | Plan::CreateIndex { .. }
                | Plan::DropIndex { .. }
        )
    }
}

impl fmt::Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Plan::Query {
                root, column_names, ..
            } => {
                writeln!(f, "Query")?;
                if let Some(names) = column_names {
                    writeln!(f, "  Output: {}", names.join(", "))?;
                }
                write_node(f, root, 1)
            }
            Plan::Insert {
                table,
                columns,
                source,
            } => {
                writeln!(f, "Insert into table '{}'", table)?;
                if let Some(cols) = columns {
                    writeln!(f, "  Columns: {:?}", cols)?;
                }
                write_node(f, source, 1)
            }
            Plan::Update {
                table,
                assignments,
                source,
            } => {
                writeln!(f, "Update table '{}'", table)?;
                writeln!(f, "  Assignments: {} columns", assignments.len())?;
                write_node(f, source, 1)
            }
            Plan::Delete { table, source } => {
                writeln!(f, "Delete from table '{}'", table)?;
                write_node(f, source, 1)
            }
            Plan::CreateTable {
                name,
                if_not_exists,
                ..
            } => {
                write!(f, "Create Table '{}'", name)?;
                if *if_not_exists {
                    write!(f, " (if not exists)")?;
                }
                Ok(())
            }
            Plan::CreateTableAsValues {
                name,
                if_not_exists,
                values_plan,
                ..
            } => {
                write!(f, "Create Table '{}' as", name)?;
                if *if_not_exists {
                    write!(f, " (if not exists)")?;
                }
                writeln!(f)?;
                write!(f, "{}", values_plan)
            }
            Plan::CreateTableAsSelect {
                name,
                if_not_exists,
                select_plan,
                ..
            } => {
                write!(f, "Create Table '{}' as", name)?;
                if *if_not_exists {
                    write!(f, " (if not exists)")?;
                }
                writeln!(f)?;
                write!(f, "{}", select_plan)
            }
            Plan::DropTable {
                names,
                if_exists,
                cascade,
            } => {
                write!(f, "Drop Table {}", names.join(", "))?;
                if *if_exists {
                    write!(f, " (if exists)")?;
                }
                if *cascade {
                    write!(f, " (cascade)")?;
                }
                Ok(())
            }
            Plan::CreateIndex {
                name,
                table,
                columns,
                unique,
                included_columns,
            } => {
                write!(f, "Create ")?;
                if *unique {
                    write!(f, "Unique ")?;
                }
                writeln!(f, "Index '{}' on table '{}'", name, table)?;
                writeln!(f, "  Columns: {} indexed", columns.len())?;
                if let Some(included) = included_columns {
                    writeln!(f, "  Included: {}", included.join(", "))?;
                }
                Ok(())
            }
            Plan::DropIndex { name, if_exists } => {
                write!(f, "Drop Index '{}'", name)?;
                if *if_exists {
                    write!(f, " (if exists)")?;
                }
                Ok(())
            }
            Plan::AlterTable { name, operation } => {
                writeln!(f, "Alter Table '{}'", name)?;
                write!(f, "  Operation: {:?}", operation)
            }
        }
    }
}

fn write_node(f: &mut fmt::Formatter<'_>, node: &Node, indent: usize) -> fmt::Result {
    let prefix = "  ".repeat(indent);

    match node {
        Node::Scan { table, alias } => {
            write!(f, "{}-> Scan table '{}'", prefix, table)?;
            if let Some(a) = alias {
                write!(f, " as '{}'", a)?;
            }
            writeln!(f)
        }
        Node::SeriesScan { size, alias } => {
            write!(f, "{}-> Series scan", prefix)?;
            if let Some(a) = alias {
                write!(f, " as '{}'", a)?;
            }
            writeln!(f)?;
            writeln!(f, "{}   Size: {:?}", prefix, size)
        }
        Node::IndexScan {
            table,
            alias,
            index_name,
            values,
        } => {
            write!(
                f,
                "{}-> Index scan on '{}' using index '{}'",
                prefix, table, index_name
            )?;
            if let Some(a) = alias {
                write!(f, " as '{}'", a)?;
            }
            writeln!(f)?;
            writeln!(f, "{}   Lookup values: {}", prefix, values.len())
        }
        Node::IndexRangeScan {
            table,
            alias,
            index_name,
            start,
            end,
            reverse,
            ..
        } => {
            write!(
                f,
                "{}-> Index range scan on '{}' using index '{}'",
                prefix, table, index_name
            )?;
            if let Some(a) = alias {
                write!(f, " as '{}'", a)?;
            }
            writeln!(f)?;
            if start.is_some() || end.is_some() {
                writeln!(
                    f,
                    "{}   Range: {:?} to {:?}",
                    prefix,
                    start.as_ref().map(|_| "start"),
                    end.as_ref().map(|_| "end")
                )?;
            }
            if *reverse {
                writeln!(f, "{}   Direction: reverse", prefix)?;
            }
            Ok(())
        }
        Node::Filter { source, predicate } => {
            writeln!(f, "{}-> Filter", prefix)?;
            writeln!(f, "{}   Condition: {:?}", prefix, predicate)?;
            write_node(f, source, indent + 1)
        }
        Node::Projection {
            source,
            expressions: _,
            aliases,
        } => {
            writeln!(f, "{}-> Projection", prefix)?;
            let cols: Vec<String> = aliases
                .iter()
                .enumerate()
                .map(|(i, alias)| alias.clone().unwrap_or_else(|| format!("col_{}", i)))
                .collect();
            writeln!(f, "{}   Columns: {}", prefix, cols.join(", "))?;
            write_node(f, source, indent + 1)
        }
        Node::Order { source, order_by } => {
            writeln!(f, "{}-> Order by", prefix)?;
            for (expr, dir) in order_by {
                writeln!(f, "{}   {:?} {:?}", prefix, expr, dir)?;
            }
            write_node(f, source, indent + 1)
        }
        Node::Limit { source, limit } => {
            writeln!(f, "{}-> Limit {}", prefix, limit)?;
            write_node(f, source, indent + 1)
        }
        Node::Offset { source, offset } => {
            writeln!(f, "{}-> Offset {}", prefix, offset)?;
            write_node(f, source, indent + 1)
        }
        Node::Distinct { source } => {
            writeln!(f, "{}-> Distinct", prefix)?;
            write_node(f, source, indent + 1)
        }
        Node::Aggregate {
            source,
            group_by,
            aggregates,
        } => {
            writeln!(f, "{}-> Aggregate", prefix)?;
            if !group_by.is_empty() {
                writeln!(f, "{}   Group by: {} expressions", prefix, group_by.len())?;
            }
            writeln!(f, "{}   Aggregates: {} functions", prefix, aggregates.len())?;
            write_node(f, source, indent + 1)
        }
        Node::HashJoin {
            left,
            right,
            left_col,
            right_col,
            join_type,
        } => {
            writeln!(f, "{}-> Hash join ({:?})", prefix, join_type)?;
            writeln!(
                f,
                "{}   Join on: left[{}] = right[{}]",
                prefix, left_col, right_col
            )?;
            writeln!(f, "{}   Left:", prefix)?;
            write_node(f, left, indent + 2)?;
            writeln!(f, "{}   Right:", prefix)?;
            write_node(f, right, indent + 2)
        }
        Node::NestedLoopJoin {
            left,
            right,
            predicate,
            join_type,
        } => {
            writeln!(f, "{}-> Nested loop join ({:?})", prefix, join_type)?;
            writeln!(f, "{}   Condition: {:?}", prefix, predicate)?;
            writeln!(f, "{}   Left:", prefix)?;
            write_node(f, left, indent + 2)?;
            writeln!(f, "{}   Right:", prefix)?;
            write_node(f, right, indent + 2)
        }
        Node::Values { rows } => {
            writeln!(f, "{}-> Values", prefix)?;
            writeln!(f, "{}   Rows: {}", prefix, rows.len())
        }
        Node::Nothing => {
            writeln!(f, "{}-> Nothing", prefix)
        }
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

    /// SERIES(N) scan - generates N rows with column "N" containing values 1..=N
    SeriesScan {
        size: Expression,
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

    /// Distinct - deduplicate result rows
    Distinct { source: Box<Node> },

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
            Node::SeriesScan { .. } => 1, // SERIES(N) produces one column: N
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
            Node::Distinct { source } => source.column_count(schemas),
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
            | Node::Offset { source, .. }
            | Node::Distinct { source } => source.get_column_names(schemas),

            // SeriesScan produces column "n" (lowercase for SQL case-insensitivity)
            Node::SeriesScan { .. } => vec!["n".to_string()],

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

            // Values node
            Node::Values { rows } => {
                let count = rows.first().map(|r| r.len()).unwrap_or(0);
                (1..=count).map(|i| format!("column{}", i)).collect()
            }

            // Nothing node
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
