//! Remainder (modulo) operator implementation

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};
use rust_decimal::Decimal;

pub struct RemainderOperator;

impl BinaryOperator for RemainderOperator {
    fn name(&self) -> &'static str {
        "remainder"
    }

    fn symbol(&self) -> &'static str {
        "%"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        false // Remainder is NEVER commutative
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, nullable) = unwrap_nullable_pair(left, right);

        let result = match (left_inner, right_inner) {
            // Integer remainder
            (a, b) if a.is_integer() && b.is_integer() => promote_integer_types(a, b)?,

            // Float remainder
            (F32, F32) => F32,
            (F64, F64) => F64,
            (F32, F64) | (F64, F32) => F64,

            // Decimal remainder
            (Decimal(_, _), Decimal(_, _)) => Decimal(None, None),

            // Mixed numeric - promote appropriately
            (a, b) if a.is_numeric() && b.is_numeric() => {
                if matches!(a, F32 | F64) || matches!(b, F32 | F64) {
                    if matches!(a, F64) || matches!(b, F64) {
                        F64
                    } else {
                        F32
                    }
                } else if matches!(a, Decimal(_, _)) || matches!(b, Decimal(_, _)) {
                    Decimal(None, None)
                } else {
                    promote_integer_types(a, b)?
                }
            }

            _ => {
                return Err(Error::InvalidOperation(format!(
                    "Cannot compute remainder of {} by {}",
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

            // Check for modulo by zero first
            (_, n) if is_zero(n) => Err(Error::InvalidOperation("Modulo by zero".to_string())),

            // Same-type integer operations
            (I8(a), I8(b)) => a
                .checked_rem(*b)
                .map(I8)
                .ok_or_else(|| Error::InvalidValue("I8 remainder error".into())),
            (I16(a), I16(b)) => a
                .checked_rem(*b)
                .map(I16)
                .ok_or_else(|| Error::InvalidValue("I16 remainder error".into())),
            (I32(a), I32(b)) => a
                .checked_rem(*b)
                .map(I32)
                .ok_or_else(|| Error::InvalidValue("I32 remainder error".into())),
            (I64(a), I64(b)) => a
                .checked_rem(*b)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 remainder error".into())),
            (I128(a), I128(b)) => a
                .checked_rem(*b)
                .map(I128)
                .ok_or_else(|| Error::InvalidValue("I128 remainder error".into())),

            // Unsigned integers
            (U8(a), U8(b)) => Ok(U8(a % b)),
            (U16(a), U16(b)) => Ok(U16(a % b)),
            (U32(a), U32(b)) => Ok(U32(a % b)),
            (U64(a), U64(b)) => Ok(U64(a % b)),
            (U128(a), U128(b)) => Ok(U128(a % b)),

            // Floats
            (F32(a), F32(b)) => {
                let result = a % b;
                if !result.is_finite() {
                    return Err(Error::InvalidValue("Float remainder error".into()));
                }
                Ok(F32(result))
            }
            (F64(a), F64(b)) => {
                let result = a % b;
                if !result.is_finite() {
                    return Err(Error::InvalidValue("Float remainder error".into()));
                }
                Ok(F64(result))
            }

            // Decimal
            (Decimal(a), Decimal(b)) => a
                .checked_rem(*b)
                .map(Decimal)
                .ok_or_else(|| Error::InvalidValue("Decimal remainder error".into())),

            // Mixed integer types - promote then compute remainder
            (I8(a), I16(b)) => (*a as i16)
                .checked_rem(*b)
                .map(I16)
                .ok_or_else(|| Error::InvalidValue("I16 remainder error".into())),
            (I16(a), I8(b)) => a
                .checked_rem(*b as i16)
                .map(I16)
                .ok_or_else(|| Error::InvalidValue("I16 remainder error".into())),
            (I8(a), I32(b)) => (*a as i32)
                .checked_rem(*b)
                .map(I32)
                .ok_or_else(|| Error::InvalidValue("I32 remainder error".into())),
            (I32(a), I8(b)) => a
                .checked_rem(*b as i32)
                .map(I32)
                .ok_or_else(|| Error::InvalidValue("I32 remainder error".into())),
            (I8(a), I64(b)) => (*a as i64)
                .checked_rem(*b)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 remainder error".into())),
            (I64(a), I8(b)) => a
                .checked_rem(*b as i64)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 remainder error".into())),
            (I16(a), I32(b)) => (*a as i32)
                .checked_rem(*b)
                .map(I32)
                .ok_or_else(|| Error::InvalidValue("I32 remainder error".into())),
            (I32(a), I16(b)) => a
                .checked_rem(*b as i32)
                .map(I32)
                .ok_or_else(|| Error::InvalidValue("I32 remainder error".into())),
            (I16(a), I64(b)) => (*a as i64)
                .checked_rem(*b)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 remainder error".into())),
            (I64(a), I16(b)) => a
                .checked_rem(*b as i64)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 remainder error".into())),
            (I32(a), I64(b)) => (*a as i64)
                .checked_rem(*b)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 remainder error".into())),
            (I64(a), I32(b)) => a
                .checked_rem(*b as i64)
                .map(I64)
                .ok_or_else(|| Error::InvalidValue("I64 remainder error".into())),

            // Mixed numeric types - convert to common type
            (a, b) if a.is_numeric() && b.is_numeric() => remainder_mixed_numeric(a, b),

            _ => Err(Error::InvalidOperation(format!(
                "Cannot compute remainder of {:?} by {:?}",
                left, right
            ))),
        }
    }
}

/// Check if a value is zero
fn is_zero(value: &Value) -> bool {
    match value {
        Value::I8(n) => *n == 0,
        Value::I16(n) => *n == 0,
        Value::I32(n) => *n == 0,
        Value::I64(n) => *n == 0,
        Value::I128(n) => *n == 0,
        Value::U8(n) => *n == 0,
        Value::U16(n) => *n == 0,
        Value::U32(n) => *n == 0,
        Value::U64(n) => *n == 0,
        Value::U128(n) => *n == 0,
        Value::F32(n) => *n == 0.0,
        Value::F64(n) => *n == 0.0,
        Value::Decimal(d) => d.is_zero(),
        _ => false,
    }
}

/// Helper function to handle mixed numeric type remainder
fn remainder_mixed_numeric(left: &Value, right: &Value) -> Result<Value> {
    // Try to convert to Decimal for mixed numeric operations
    match (to_decimal(left), to_decimal(right)) {
        (Some(a), Some(b)) => a
            .checked_rem(b)
            .map(Value::Decimal)
            .ok_or_else(|| Error::InvalidValue("Decimal remainder error".into())),
        _ => Err(Error::InvalidOperation(format!(
            "Cannot compute remainder of {:?} by {:?}",
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
    fn test_remainder_numeric() {
        let op = RemainderOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::I32, &DataType::I32).unwrap(),
            DataType::I32
        );

        // Execution
        assert_eq!(
            op.execute(&Value::I32(10), &Value::I32(3)).unwrap(),
            Value::I32(1)
        );

        // Modulo by zero
        assert!(op.execute(&Value::I32(10), &Value::I32(0)).is_err());
    }
}
