//! Equal operator implementation

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct EqualOperator;

impl BinaryOperator for EqualOperator {
    fn name(&self) -> &'static str {
        "equality"
    }

    fn symbol(&self) -> &'static str {
        "="
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        true // Equality is always commutative
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, _) = unwrap_nullable_pair(left, right);

        // Check if types are comparable
        match (left_inner, right_inner) {
            // Unknown types (parameters) can be compared with anything
            (Null, _) | (_, Null) => Ok(Bool),

            // Same types are always comparable
            (a, b) if a == b => Ok(Bool),

            // Numeric types can be compared
            (a, b) if a.is_numeric() && b.is_numeric() => Ok(Bool),

            // String types can be compared
            (Str, Text) | (Text, Str) => Ok(Bool),

            // Collections can be compared with strings (JSON parsing at runtime)
            (Array(..), Str) | (Str, Array(..)) => Ok(Bool),
            (List(..), Str) | (Str, List(..)) => Ok(Bool),
            (Map(..), Str) | (Str, Map(..)) => Ok(Bool),
            (Struct(..), Str) | (Str, Struct(..)) => Ok(Bool),

            // Date/Time types can be compared with same type
            (Date, Date) | (Time, Time) | (Timestamp, Timestamp) => Ok(Bool),

            // Date/Time types can be compared with strings (parsed at runtime)
            (Date, Str) | (Str, Date) => Ok(Bool),
            (Time, Str) | (Str, Time) => Ok(Bool),
            (Timestamp, Str) | (Str, Timestamp) => Ok(Bool),

            // UUID can be compared with strings (parsed at runtime)
            (Uuid, Str) | (Str, Uuid) => Ok(Bool),

            _ => Err(Error::TypeMismatch {
                expected: format!("{:?}", left),
                found: format!("{:?}", right),
            }),
        }
    }

    fn execute(&self, left: &Value, right: &Value) -> Result<Value> {
        use Value::*;

        // NULL comparison always returns NULL
        match (left, right) {
            (Null, _) | (_, Null) => Ok(Null),
            _ => {
                // Use the compare function from operators module
                let ordering = crate::operators::compare(left, right)?;
                Ok(Bool(ordering == std::cmp::Ordering::Equal))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_equal() {
        let op = EqualOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::I32, &DataType::I32).unwrap(),
            DataType::Bool
        );
        assert_eq!(
            op.validate(&DataType::Str, &DataType::Str).unwrap(),
            DataType::Bool
        );

        // Execution
        assert_eq!(
            op.execute(&Value::I32(5), &Value::I32(5)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            op.execute(&Value::I32(5), &Value::I32(3)).unwrap(),
            Value::Bool(false)
        );

        // NULL handling
        assert_eq!(
            op.execute(&Value::I32(5), &Value::Null).unwrap(),
            Value::Null
        );
    }
}
