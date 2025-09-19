//! Remainder (modulo) operator implementation

use super::helpers::*;
use super::mixed_ops;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

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
            (Null, _) | (_, Null) => Null,

            // Decimal with any numeric type -> Decimal (preserve precision, highest priority)
            (Decimal(p1, s1), Decimal(p2, s2)) => {
                // For decimal remainder, result precision and scale follow SQL standard
                Decimal(p1.and(*p2), s1.and(*s2))
            }
            (Decimal(_, _), t) | (t, Decimal(_, _)) if t.is_numeric() => Decimal(None, None),

            // Float64 with any numeric type (except Decimal) -> Float64 (PostgreSQL behavior)
            (F64, t) | (t, F64) if t.is_numeric() => F64,

            // Float32 with any numeric type (except F64 and Decimal) -> Float32
            (F32, t) | (t, F32) if t.is_numeric() => F32,

            // Integer types - use standard promotion
            (a, b) if a.is_integer() && b.is_integer() => promote_integer_types(a, b)?,

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
    use Value::*;

    match (left, right) {
        // Decimal with any numeric -> convert to Decimal (preserve precision, highest priority)
        (Decimal(_), _) | (_, Decimal(_)) => match (to_decimal(left), to_decimal(right)) {
            (Some(a), Some(b)) => a
                .checked_rem(b)
                .map(Decimal)
                .ok_or_else(|| Error::InvalidValue("Decimal remainder error".into())),
            _ => Err(Error::InvalidOperation(format!(
                "Cannot compute remainder of {:?} by {:?}",
                left, right
            ))),
        },

        // F64 with any numeric (except Decimal which is handled above) -> keep as F64
        (F64(a), val) if val.is_numeric() => {
            let b = to_f64(val)?;
            if b == 0.0 {
                return Err(Error::InvalidValue("Division by zero".into()));
            }
            let result = a % b;
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float remainder error".into()));
            }
            Ok(F64(result))
        }
        (val, F64(b)) if val.is_numeric() => {
            let a = to_f64(val)?;
            if *b == 0.0 {
                return Err(Error::InvalidValue("Division by zero".into()));
            }
            let result = a % b;
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float remainder error".into()));
            }
            Ok(F64(result))
        }

        // F32 with numeric (except F64 and Decimal) -> keep as F32
        (F32(a), val) if val.is_numeric() => {
            let b = to_f32(val)?;
            if b == 0.0 {
                return Err(Error::InvalidValue("Division by zero".into()));
            }
            let result = a % b;
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float remainder error".into()));
            }
            Ok(F32(result))
        }
        (val, F32(b)) if val.is_numeric() => {
            let a = to_f32(val)?;
            if *b == 0.0 {
                return Err(Error::InvalidValue("Division by zero".into()));
            }
            let result = a % b;
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float remainder error".into()));
            }
            Ok(F32(result))
        }

        // This shouldn't be reached since integer-integer is handled above
        _ => Err(Error::InvalidOperation(format!(
            "Cannot compute remainder of {:?} by {:?}",
            left, right
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn test_remainder_integers() {
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
        assert_eq!(
            op.execute(&Value::I32(7), &Value::I32(4)).unwrap(),
            Value::I32(3)
        );
        assert_eq!(
            op.execute(&Value::I32(-10), &Value::I32(3)).unwrap(),
            Value::I32(-1)
        );

        // Modulo by zero
        assert!(op.execute(&Value::I32(10), &Value::I32(0)).is_err());
    }

    #[test]
    fn test_remainder_float64_with_integer() {
        let op = RemainderOperator;

        // Type validation - F64 % integer should return F64
        assert_eq!(
            op.validate(&DataType::F64, &DataType::I32).unwrap(),
            DataType::F64
        );
        assert_eq!(
            op.validate(&DataType::I64, &DataType::F64).unwrap(),
            DataType::F64
        );

        // Execution - should stay as F64 (PostgreSQL behavior)
        let result = op.execute(&Value::F64(10.5), &Value::I32(3)).unwrap();
        match result {
            Value::F64(v) => {
                assert!((v - 1.5).abs() < 1e-10, "Expected 1.5, got {}", v);
            }
            _ => panic!("Expected F64, got {:?}", result),
        }

        // Test with integer first
        let result = op.execute(&Value::I32(10), &Value::F64(3.0)).unwrap();
        match result {
            Value::F64(v) => {
                assert!((v - 1.0).abs() < 1e-10, "Expected 1.0, got {}", v);
            }
            _ => panic!("Expected F64, got {:?}", result),
        }
    }

    #[test]
    fn test_remainder_float32_with_integer() {
        let op = RemainderOperator;

        // Type validation - F32 % integer should return F32
        assert_eq!(
            op.validate(&DataType::F32, &DataType::I32).unwrap(),
            DataType::F32
        );

        // Execution
        let result = op.execute(&Value::F32(7.5), &Value::I32(2)).unwrap();
        match result {
            Value::F32(v) => {
                assert!((v - 1.5).abs() < 1e-6, "Expected 1.5, got {}", v);
            }
            _ => panic!("Expected F32, got {:?}", result),
        }

        // Integer % F32
        let result = op.execute(&Value::I16(7), &Value::F32(2.0)).unwrap();
        match result {
            Value::F32(v) => {
                assert!((v - 1.0).abs() < 1e-6, "Expected 1.0, got {}", v);
            }
            _ => panic!("Expected F32, got {:?}", result),
        }
    }

    #[test]
    fn test_remainder_float64_with_float32() {
        let op = RemainderOperator;

        // Type validation - F64 % F32 should return F64
        assert_eq!(
            op.validate(&DataType::F64, &DataType::F32).unwrap(),
            DataType::F64
        );
        assert_eq!(
            op.validate(&DataType::F32, &DataType::F64).unwrap(),
            DataType::F64
        );

        // Execution
        let result = op.execute(&Value::F64(10.0), &Value::F32(3.0)).unwrap();
        match result {
            Value::F64(v) => {
                assert!((v - 1.0).abs() < 1e-10, "Expected 1.0, got {}", v);
            }
            _ => panic!("Expected F64, got {:?}", result),
        }

        let result = op.execute(&Value::F32(10.0), &Value::F64(3.0)).unwrap();
        match result {
            Value::F64(v) => {
                assert!((v - 1.0).abs() < 1e-10, "Expected 1.0, got {}", v);
            }
            _ => panic!("Expected F64, got {:?}", result),
        }
    }

    #[test]
    fn test_remainder_decimal_preserves_precision() {
        let op = RemainderOperator;

        // Type validation - Decimal % anything numeric returns Decimal
        assert_eq!(
            op.validate(&DataType::Decimal(None, None), &DataType::I32)
                .unwrap(),
            DataType::Decimal(None, None)
        );
        assert_eq!(
            op.validate(&DataType::F64, &DataType::Decimal(None, None))
                .unwrap(),
            DataType::Decimal(None, None)
        );

        // Execution - Decimal preserves exact precision
        let decimal_10 = Decimal::from_str_exact("10.0").unwrap();
        let decimal_3 = Decimal::from(3);
        let decimal_1 = Decimal::from(1);
        let result = op
            .execute(&Value::Decimal(decimal_10), &Value::I32(3))
            .unwrap();
        assert_eq!(result, Value::Decimal(decimal_1));

        // F64 % Decimal should convert to Decimal
        let result = op
            .execute(&Value::F64(10.5), &Value::Decimal(decimal_3))
            .unwrap();
        match result {
            Value::Decimal(d) => {
                assert_eq!(d.to_string(), "1.5");
            }
            _ => panic!("Expected Decimal, got {:?}", result),
        }
    }

    #[test]
    fn test_remainder_null_handling() {
        let op = RemainderOperator;

        assert_eq!(
            op.execute(&Value::Null, &Value::I32(5)).unwrap(),
            Value::Null
        );
        assert_eq!(
            op.execute(&Value::F64(10.0), &Value::Null).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_remainder_division_by_zero() {
        let op = RemainderOperator;

        // Integer division by zero
        assert!(op.execute(&Value::I32(10), &Value::I32(0)).is_err());

        // Float division by zero
        assert!(op.execute(&Value::F64(10.0), &Value::F64(0.0)).is_err());
        assert!(op.execute(&Value::F32(10.0), &Value::F32(0.0)).is_err());

        // Mixed type division by zero
        assert!(op.execute(&Value::F64(10.0), &Value::I32(0)).is_err());
        assert!(op.execute(&Value::I32(10), &Value::F64(0.0)).is_err());
    }

    #[test]
    fn test_remainder_not_commutative() {
        let op = RemainderOperator;

        // Remainder is not commutative
        let result1 = op.execute(&Value::I32(10), &Value::I32(3)).unwrap();
        let result2 = op.execute(&Value::I32(3), &Value::I32(10)).unwrap();
        assert_ne!(result1, result2);
        assert_eq!(result1, Value::I32(1)); // 10 % 3 = 1
        assert_eq!(result2, Value::I32(3)); // 3 % 10 = 3
    }
}
