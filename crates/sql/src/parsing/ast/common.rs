//! Common structures used across AST modules

use super::dml::{SelectStatement, ValuesStatement};
use super::expressions::Expression;

/// Sort direction for ORDER BY and indexes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Asc,
    Desc,
}

/// Join types for SQL joins
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
    Full,
}

/// Source for a subquery in FROM clause
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubquerySource {
    /// SELECT subquery
    Select(Box<SelectStatement>),
    /// VALUES subquery
    Values(ValuesStatement),
}

/// A FROM item.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FromClause {
    /// A table.
    Table {
        /// The table name.
        name: String,
        /// An optional alias for the table.
        alias: Option<String>,
    },
    /// A subquery (SELECT or VALUES)
    Subquery {
        /// The subquery source
        source: SubquerySource,
        /// Required alias for the subquery
        alias: String,
    },
    /// A join of two or more tables (may be nested).
    Join {
        /// The left table to join.
        left: Box<FromClause>,
        /// The right table to join.
        right: Box<FromClause>,
        /// The join type.
        r#type: JoinType,
        /// The join condition. None for a cross join.
        predicate: Option<Expression>,
    },
}
