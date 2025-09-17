//! Remainder (modulo) operator implementation

use super::helpers::*;
use super::mixed_ops;
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
            // Unknown (NULL) with anything returns Unknown
            (Unknown, _) | (_, Unknown) => Unknown,

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

            // All integer operations - use generic handler
            (a, b) if a.is_integer() && b.is_integer() => mixed_ops::remainder_integers(a, b),

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

            // Mixed numeric types - convert to common type
            (a, b) if a.is_numeric() && b.is_numeric() => remainder_mixed_numeric(a, b),

            _ => Err(Error::InvalidOperation(format!(
                "Cannot compute remainder of {:?} by {:?}",
                left, right
            ))),
        }
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
