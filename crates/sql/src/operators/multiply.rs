//! Multiplication operator implementation

use super::helpers::*;
use super::mixed_ops;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};
use rust_decimal::Decimal;

pub struct MultiplyOperator;

impl BinaryOperator for MultiplyOperator {
    fn name(&self) -> &'static str {
        "multiplication"
    }

    fn symbol(&self) -> &'static str {
        "*"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        true // Multiplication is always commutative for supported types
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, nullable) = unwrap_nullable_pair(left, right);

        let result = match (left_inner, right_inner) {
            // Unknown (NULL) with anything returns Unknown
            (Null, _) | (_, Null) => Null,

            // Numeric multiplication only
            (a, b) if a.is_numeric() && b.is_numeric() => promote_numeric_types(a, b)?,

            // Interval * integer (scale interval)
            (Interval, I32) | (I32, Interval) | (Interval, I64) | (I64, Interval) => Interval,

            _ => {
                return Err(Error::InvalidOperation(format!(
                    "Cannot multiply {} and {}",
                    left, right
                )));
            }
        };

        Ok(wrap_nullable(result, nullable))
    }

    fn execute(&self, left: &Value, right: &Value) -> Result<Value> {
        use Value::*;

        match (left, right) {
            // NULL handling
            (Null, _) | (_, Null) => Ok(Null),

            // All integer operations - use generic handler
            (a, b) if a.is_integer() && b.is_integer() => mixed_ops::multiply_integers(a, b),

            // Floats (check for infinity/NaN)
            (F32(a), F32(b)) => {
                let result = a * b;
                if !result.is_finite() {
                    return Err(Error::InvalidValue("Float overflow or NaN".into()));
                }
                Ok(F32(result))
            }
            (F64(a), F64(b)) => {
                let result = a * b;
                if !result.is_finite() {
                    return Err(Error::InvalidValue("Float overflow or NaN".into()));
                }
                Ok(F64(result))
            }

            // Decimal
            (Decimal(a), Decimal(b)) => Ok(Decimal(a * b)),

            // Interval * integer (scale interval)
            (Value::Interval(interval), I32(scale)) | (I32(scale), Value::Interval(interval)) => {
                Ok(Value::Interval(crate::types::data_type::Interval {
                    months: interval.months * scale,
                    days: interval.days * scale,
                    microseconds: interval.microseconds * (*scale as i64),
                }))
            }
            (Value::Interval(interval), I64(scale)) | (I64(scale), Value::Interval(interval)) => {
                Ok(Value::Interval(crate::types::data_type::Interval {
                    months: (interval.months as i64 * scale) as i32,
                    days: (interval.days as i64 * scale) as i32,
                    microseconds: interval.microseconds * scale,
                }))
            }

            // Mixed numeric types - convert to common type
            (a, b) if a.is_numeric() && b.is_numeric() => multiply_mixed_numeric(a, b),

            _ => Err(Error::InvalidOperation(format!(
                "Cannot multiply {:?} and {:?}",
                left, right
            ))),
        }
    }
}

/// Helper function to handle mixed numeric type multiplication
fn multiply_mixed_numeric(left: &Value, right: &Value) -> Result<Value> {
    // Try to convert to Decimal for mixed numeric operations
    match (to_decimal(left), to_decimal(right)) {
        (Some(a), Some(b)) => Ok(Value::Decimal(a * b)),
        _ => Err(Error::InvalidOperation(format!(
            "Cannot multiply {:?} and {:?}",
            left, right
        ))),
    }
}

/// Helper to convert any numeric value to Decimal
fn to_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::I8(n) => Some(Decimal::from(*n)),
        Value::I16(n) => Some(Decimal::from(*n)),
        Value::I32(n) => Some(Decimal::from(*n)),
        Value::I64(n) => Some(Decimal::from(*n)),
        Value::I128(n) => Some(Decimal::from(*n)),
        Value::U8(n) => Some(Decimal::from(*n)),
        Value::U16(n) => Some(Decimal::from(*n)),
        Value::U32(n) => Some(Decimal::from(*n)),
        Value::U64(n) => Some(Decimal::from(*n)),
        Value::U128(n) => Some(Decimal::from(*n)),
        Value::F32(n) => Decimal::from_f32_retain(*n),
        Value::F64(n) => Decimal::from_f64_retain(*n),
        Value::Decimal(d) => Some(*d),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multiply_numeric() {
        let op = MultiplyOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::I32, &DataType::I32).unwrap(),
            DataType::I32
        );

        // Execution
        assert_eq!(
            op.execute(&Value::I32(3), &Value::I32(4)).unwrap(),
            Value::I32(12)
        );

        // Overflow
        assert!(op.execute(&Value::I8(100), &Value::I8(2)).is_err());
    }
}
