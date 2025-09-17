//! Multiplication operator implementation

use super::helpers::*;
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

            // Same-type integer operations with overflow checking
            (I8(a), I8(b)) => a
                .checked_mul(*b)
                .map(I8)
                .ok_or_else(|| Error::InvalidValue("I8 overflow".into())),
            (I16(a), I16(b)) => a
                .checked_mul(*b)
                .map(I16)
                .ok_or_else(|| Error::InvalidValue("I16 overflow".into())),
            (I32(a), I32(b)) => a
                .checked_mul(*b)
                .map(I32)
                .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
            (I64(a), I64(b)) => a
                .checked_mul(*b)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
            (I128(a), I128(b)) => a
                .checked_mul(*b)
                .map(I128)
                .ok_or_else(|| Error::InvalidValue("I128 overflow".into())),

            // Unsigned integers
            (U8(a), U8(b)) => a
                .checked_mul(*b)
                .map(U8)
                .ok_or_else(|| Error::InvalidValue("U8 overflow".into())),
            (U16(a), U16(b)) => a
                .checked_mul(*b)
                .map(U16)
                .ok_or_else(|| Error::InvalidValue("U16 overflow".into())),
            (U32(a), U32(b)) => a
                .checked_mul(*b)
                .map(U32)
                .ok_or_else(|| Error::InvalidValue("U32 overflow".into())),
            (U64(a), U64(b)) => a
                .checked_mul(*b)
                .map(U64)
                .ok_or_else(|| Error::InvalidValue("U64 overflow".into())),
            (U128(a), U128(b)) => a
                .checked_mul(*b)
                .map(U128)
                .ok_or_else(|| Error::InvalidValue("U128 overflow".into())),

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

            // Mixed integer types - promote then multiply
            (I8(a), I16(b)) => (*a as i16)
                .checked_mul(*b)
                .map(I16)
                .ok_or_else(|| Error::InvalidValue("I16 overflow".into())),
            (I16(a), I8(b)) => a
                .checked_mul(*b as i16)
                .map(I16)
                .ok_or_else(|| Error::InvalidValue("I16 overflow".into())),
            (I8(a), I32(b)) => (*a as i32)
                .checked_mul(*b)
                .map(I32)
                .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
            (I32(a), I8(b)) => a
                .checked_mul(*b as i32)
                .map(I32)
                .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
            (I8(a), I64(b)) => (*a as i64)
                .checked_mul(*b)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
            (I64(a), I8(b)) => a
                .checked_mul(*b as i64)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
            (I16(a), I32(b)) => (*a as i32)
                .checked_mul(*b)
                .map(I32)
                .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
            (I32(a), I16(b)) => a
                .checked_mul(*b as i32)
                .map(I32)
                .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
            (I16(a), I64(b)) => (*a as i64)
                .checked_mul(*b)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
            (I64(a), I16(b)) => a
                .checked_mul(*b as i64)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
            (I32(a), I64(b)) => (*a as i64)
                .checked_mul(*b)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
            (I64(a), I32(b)) => a
                .checked_mul(*b as i64)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),

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
