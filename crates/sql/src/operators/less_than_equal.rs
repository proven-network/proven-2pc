//! Less than or equal operator implementation

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct LessThanEqualOperator;

impl BinaryOperator for LessThanEqualOperator {
    fn name(&self) -> &'static str {
        "less than or equal"
    }

    fn symbol(&self) -> &'static str {
        "<="
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        false // Less than or equal is NOT commutative
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, _) = unwrap_nullable_pair(left, right);

        // Check if types are comparable
        match (left_inner, right_inner) {
            // Unknown types (parameters) can be compared with anything
            (Unknown, _) | (_, Unknown) => Ok(Bool),

            // Same types are always comparable
            (a, b) if a == b => Ok(Bool),

            // Numeric types can be compared
            (a, b) if a.is_numeric() && b.is_numeric() => Ok(Bool),

            // String types can be compared
            (Str, Text) | (Text, Str) | (Str, Str) | (Text, Text) => Ok(Bool),

            // Date/Time types can be compared with same type
            (Date, Date) | (Time, Time) | (Timestamp, Timestamp) | (Interval, Interval) => Ok(Bool),

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
                Ok(Bool(ordering != std::cmp::Ordering::Greater))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_less_than_equal() {
        let op = LessThanEqualOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::I32, &DataType::I32).unwrap(),
            DataType::Bool
        );

        // Execution
        assert_eq!(
            op.execute(&Value::I32(3), &Value::I32(5)).unwrap(),
            Value::Bool(true)
        );
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
