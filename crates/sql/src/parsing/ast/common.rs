//! Common structures used across AST modules

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

/// A FROM item.
#[derive(Debug, Clone)]
pub enum FromClause {
    /// A table.
    Table {
        /// The table name.
        name: String,
        /// An optional alias for the table.
        alias: Option<String>,
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
