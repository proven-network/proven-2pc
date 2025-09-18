//! Core traits for SQL operators

use crate::error::Result;
use crate::types::{DataType, Value};

/// Trait for binary operators (two operands)
pub trait BinaryOperator: Send + Sync {
    /// Get operator name for error messages
    #[allow(dead_code)]
    fn name(&self) -> &'static str;

    /// Get operator symbol for display
    #[allow(dead_code)]
    fn symbol(&self) -> &'static str;

    /// Validate operand types and return result type
    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType>;

    /// Execute the operation
    fn execute(&self, left: &Value, right: &Value) -> Result<Value>;

    /// Check if this operator is commutative (a op b = b op a) for given types
    #[allow(dead_code)]
    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        false
    }

    /// Check if this operator can use an index (for optimization)
    #[allow(dead_code)]
    fn supports_index(&self) -> bool {
        false
    }
}

/// Trait for unary operators (one operand)
pub trait UnaryOperator: Send + Sync {
    /// Get operator name for error messages
    #[allow(dead_code)]
    fn name(&self) -> &'static str;

    /// Get operator symbol for display
    #[allow(dead_code)]
    fn symbol(&self) -> &'static str;

    /// Validate operand type and return result type
    fn validate(&self, operand: &DataType) -> Result<DataType>;

    /// Execute the operation
    fn execute(&self, operand: &Value) -> Result<Value>;
}
