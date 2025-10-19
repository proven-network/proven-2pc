//! Index management and operations

use super::expression::Expression;
use serde::{Deserialize, Serialize};

/// Index type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    Unique,
}

/// Index column - either a simple column reference or an expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexColumn {
    /// Simple column reference: CREATE INDEX idx ON t (name)
    Column(String),

    /// Expression: CREATE INDEX idx ON t (age * 2)
    /// Note: Expression is stored as a display string for serialization purposes
    /// and rebuilt when needed for matching
    Expression {
        /// String representation of the normalized expression (for serialization)
        #[serde(rename = "expr_string")]
        expression_string: String,
        /// Referenced columns (for dependency tracking)
        dependencies: Vec<String>,
        /// The actual expression (skipped during serialization)
        #[serde(skip)]
        expression: Option<Expression>,
    },
}

impl IndexColumn {
    /// Get the column name if this is a simple column reference
    pub fn as_column(&self) -> Option<&str> {
        match self {
            IndexColumn::Column(name) => Some(name),
            _ => None,
        }
    }

    /// Get the expression if this is an expression index column
    pub fn as_expression(&self) -> Option<&Expression> {
        match self {
            IndexColumn::Expression { expression, .. } => expression.as_ref(),
            _ => None,
        }
    }

    /// Create an expression index column from a normalized expression
    pub fn new_expression(expression: Expression, dependencies: Vec<String>) -> Self {
        let expression_string = format!("{}", expression);
        IndexColumn::Expression {
            expression_string,
            dependencies,
            expression: Some(expression),
        }
    }
}

/// Index metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub name: String,
    pub table: String,
    pub columns: Vec<IndexColumn>,
    pub index_type: IndexType,
    pub unique: bool,
}
