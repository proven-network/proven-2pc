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
        if left.is_null() || right.is_null() {
            return Ok(Null);
        }

        // Handle special cases for collection/string comparisons
        let (left_val, right_val) = match (left, right) {
            // Map comparison with JSON string
            (Map(_), Str(s)) | (Str(s), Map(_)) if s.starts_with('{') && s.ends_with('}') => {
                if let Ok(parsed) = Value::parse_json_object(s) {
                    if matches!(left, Map(_)) {
                        (left.clone(), parsed)
                    } else {
                        (parsed, right.clone())
                    }
                } else {
                    (left.clone(), right.clone())
                }
            }

            // Array/List comparison with JSON string
            (Array(_) | List(_), Str(s)) | (Str(s), Array(_) | List(_))
                if s.starts_with('[') && s.ends_with(']') =>
            {
                if let Ok(parsed) = Value::parse_json_array(s) {
                    if matches!(left, Array(_) | List(_)) {
                        (left.clone(), parsed)
                    } else {
                        (parsed, right.clone())
                    }
                } else {
                    (left.clone(), right.clone())
                }
            }

            // Struct comparison with JSON string
            (Struct(fields), Str(s)) | (Str(s), Struct(fields))
                if s.starts_with('{') && s.ends_with('}') =>
            {
                if let Ok(parsed) = Value::parse_json_object(s) {
                    // Try to coerce to struct type
                    let schema: Vec<(String, crate::types::DataType)> = fields
                        .iter()
                        .map(|(name, val)| (name.clone(), val.data_type()))
                        .collect();

                    if let Ok(coerced) = crate::coercion::coerce_value(
                        parsed.clone(),
                        &crate::types::DataType::Struct(schema),
                    ) {
                        if matches!(left, Struct(_)) {
                            (left.clone(), coerced)
                        } else {
                            (coerced, right.clone())
                        }
                    } else {
                        (left.clone(), right.clone())
                    }
                } else {
                    (left.clone(), right.clone())
                }
            }

            // UUID comparison with string - parse the string as UUID
            (Uuid(_), Str(s)) | (Str(s), Uuid(_)) => {
                if let Ok(parsed_uuid) = uuid::Uuid::parse_str(s) {
                    if matches!(left, Uuid(_)) {
                        (left.clone(), Uuid(parsed_uuid))
                    } else {
                        (Uuid(parsed_uuid), right.clone())
                    }
                } else {
                    // If parsing fails, comparison will return false
                    (left.clone(), right.clone())
                }
            }

            // Default case - no conversion needed
            _ => (left.clone(), right.clone()),
        };

        // IEEE 754 semantics: NaN is never equal to anything, including itself
        match (&left_val, &right_val) {
            (F32(a), _) | (_, F32(a)) if a.is_nan() => Ok(Bool(false)),
            (F64(a), _) | (_, F64(a)) if a.is_nan() => Ok(Bool(false)),
            _ => {
                // Use the compare function for final comparison
                match crate::operators::compare(&left_val, &right_val) {
                    Ok(std::cmp::Ordering::Equal) => Ok(Bool(true)),
                    Ok(_) => Ok(Bool(false)),
                    Err(_) => Ok(Bool(false)), // Type mismatch means not equal
                }
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
