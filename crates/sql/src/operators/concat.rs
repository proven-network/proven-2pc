//! String concatenation operator (||) implementation
//!
//! Implements SQL standard string concatenation operator with proper NULL handling.
//! Unlike the CONCAT function, this operator returns NULL if any operand is NULL.

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::Result;
use crate::types::{DataType, Value};

pub struct ConcatOperator;

impl BinaryOperator for ConcatOperator {
    fn name(&self) -> &'static str {
        "string concatenation"
    }

    fn symbol(&self) -> &'static str {
        "||"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        false // String concatenation is NOT commutative: 'a' || 'b' != 'b' || 'a'
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        // Handle nullable types
        let (_, _, nullable) = unwrap_nullable_pair(left, right);

        // Any type can be concatenated as string
        // Always return Text type (or nullable Text)
        Ok(wrap_nullable(Text, nullable))
    }

    fn execute(&self, left: &Value, right: &Value) -> Result<Value> {
        use Value::*;

        match (left, right) {
            // NULL handling - if either is NULL, return NULL (SQL standard)
            (Null, _) | (_, Null) => Ok(Null),

            // Convert both values to strings and concatenate
            (l, r) => {
                let left_str = value_to_string(l);
                let right_str = value_to_string(r);
                Ok(Str(format!("{}{}", left_str, right_str)))
            }
        }
    }
}

/// Helper to convert any value to string for concatenation
fn value_to_string(value: &Value) -> String {
    match value {
        Value::Str(s) => s.clone(),
        // For other types, use their Display implementation
        v => v.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concat_strings() {
        let op = ConcatOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::Text, &DataType::Text).unwrap(),
            DataType::Text
        );
        assert_eq!(
            op.validate(&DataType::Str, &DataType::Str).unwrap(),
            DataType::Text
        );

        // Execution
        assert_eq!(
            op.execute(&Value::Str("Hello".into()), &Value::Str(" World".into()))
                .unwrap(),
            Value::Str("Hello World".into())
        );
    }

    #[test]
    fn test_concat_with_null() {
        let op = ConcatOperator;

        // NULL with string should return NULL
        assert_eq!(
            op.execute(&Value::Null, &Value::Str("test".into()))
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            op.execute(&Value::Str("test".into()), &Value::Null)
                .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_concat_mixed_types() {
        let op = ConcatOperator;

        // Number with string
        assert_eq!(
            op.execute(&Value::I32(42), &Value::Str(" items".into()))
                .unwrap(),
            Value::Str("42 items".into())
        );

        // Boolean with string
        assert_eq!(
            op.execute(&Value::Bool(true), &Value::Str("!".into()))
                .unwrap(),
            Value::Str("true!".into())
        );
    }

    #[test]
    fn test_concat_not_commutative() {
        let op = ConcatOperator;

        let result1 = op
            .execute(&Value::Str("A".into()), &Value::Str("B".into()))
            .unwrap();
        let result2 = op
            .execute(&Value::Str("B".into()), &Value::Str("A".into()))
            .unwrap();

        assert_ne!(result1, result2);
        assert_eq!(result1, Value::Str("AB".into()));
        assert_eq!(result2, Value::Str("BA".into()));
    }
}
