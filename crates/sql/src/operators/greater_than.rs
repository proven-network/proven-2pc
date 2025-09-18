//! Greater than operator implementation

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct GreaterThanOperator;

impl BinaryOperator for GreaterThanOperator {
    fn name(&self) -> &'static str {
        "greater than"
    }

    fn symbol(&self) -> &'static str {
        ">"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        false // Greater than is NOT commutative
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
        if left.is_null() || right.is_null() {
            return Ok(Null);
        }

        // IEEE 754 semantics: comparisons with NaN always return false
        match (left, right) {
            (F32(a), _) | (_, F32(a)) if a.is_nan() => Ok(Bool(false)),
            (F64(a), _) | (_, F64(a)) if a.is_nan() => Ok(Bool(false)),
            _ => {
                // Use the compare function from operators module
                match crate::operators::compare(left, right) {
                    Ok(std::cmp::Ordering::Greater) => Ok(Bool(true)),
                    Ok(_) => Ok(Bool(false)),
                    Err(_) => Ok(Bool(false)), // Type mismatch returns false
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_greater_than() {
        let op = GreaterThanOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::I32, &DataType::I32).unwrap(),
            DataType::Bool
        );

        // Execution
        assert_eq!(
            op.execute(&Value::I32(5), &Value::I32(3)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            op.execute(&Value::I32(3), &Value::I32(5)).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            op.execute(&Value::I32(5), &Value::I32(5)).unwrap(),
            Value::Bool(false)
        );

        // NULL handling
        assert_eq!(
            op.execute(&Value::I32(5), &Value::Null).unwrap(),
            Value::Null
        );
    }
}
