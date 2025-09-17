//! Core traits for SQL operators

use crate::error::Result;
use crate::types::{DataType, Value};

/// Trait for binary operators (two operands)
pub trait BinaryOperator: Send + Sync {
    /// Get operator name for error messages
    fn name(&self) -> &'static str;

    /// Get operator symbol for display
    fn symbol(&self) -> &'static str;

    /// Validate operand types and return result type
    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType>;

    /// Execute the operation
    fn execute(&self, left: &Value, right: &Value) -> Result<Value>;

    /// Check if this operator is deterministic
    fn is_deterministic(&self) -> bool {
        true
    }

    /// Check if this operator is commutative (a op b = b op a) for given types
    fn is_commutative(&self, left: &DataType, right: &DataType) -> bool {
        false
    }

    /// Check if this operator can use an index (for optimization)
    fn supports_index(&self) -> bool {
        false
    }
}

/// Trait for unary operators (one operand)
pub trait UnaryOperator: Send + Sync {
    /// Get operator name for error messages
    fn name(&self) -> &'static str;

    /// Get operator symbol for display
    fn symbol(&self) -> &'static str;

    /// Validate operand type and return result type
    fn validate(&self, operand: &DataType) -> Result<DataType>;

    /// Execute the operation
    fn execute(&self, operand: &Value) -> Result<Value>;

    /// Check if this operator is deterministic
    fn is_deterministic(&self) -> bool {
        true
    }
}

/// Special operators that don't fit the binary/unary pattern
pub trait SpecialOperator: Send + Sync {
    /// Get operator name for error messages
    fn name(&self) -> &'static str;
}
