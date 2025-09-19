//! Division operator implementation

use super::helpers::*;
use super::mixed_ops;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct DivideOperator;

impl BinaryOperator for DivideOperator {
    fn name(&self) -> &'static str {
        "division"
    }

    fn symbol(&self) -> &'static str {
        "/"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        false // Division is NEVER commutative
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, nullable) = unwrap_nullable_pair(left, right);

        let result = match (left_inner, right_inner) {
            // Unknown (NULL) with anything returns Unknown
            (Null, _) | (_, Null) => Null,

            // Decimal with any numeric type -> Decimal (preserve precision, highest priority)
            (Decimal(p1, s1), Decimal(p2, s2)) => {
                // For decimal division, result precision and scale follow SQL standard
                Decimal(p1.and(*p2), s1.and(*s2))
            }
            (Decimal(_, _), t) | (t, Decimal(_, _)) if t.is_numeric() => Decimal(None, None),

            // Float64 with any numeric type (except Decimal) -> Float64 (PostgreSQL behavior)
            (F64, t) | (t, F64) if t.is_numeric() => F64,

            // Float32 with any numeric type (except F64 and Decimal) -> Float32
            (F32, t) | (t, F32) if t.is_numeric() => F32,

            // Integer division returns integer (truncating)
            (a, b) if a.is_integer() && b.is_integer() => promote_integer_types(a, b)?,

            // Interval / integer (scale down interval)
            (Interval, I32) | (Interval, I64) => Interval,

            _ => {
                return Err(Error::InvalidOperation(format!(
                    "Cannot divide {} by {}",
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
            (a, b) if a.is_integer() && b.is_integer() => mixed_ops::divide_integers(a, b),

            // Floats
            (F32(a), F32(b)) => {
                let result = a / b;
                if !result.is_finite() {
                    return Err(Error::InvalidValue("Float overflow or NaN".into()));
                }
                Ok(F32(result))
            }
            (F64(a), F64(b)) => {
                let result = a / b;
                if !result.is_finite() {
                    return Err(Error::InvalidValue("Float overflow or NaN".into()));
                }
                Ok(F64(result))
            }

            // Decimal
            (Decimal(a), Decimal(b)) => a
                .checked_div(*b)
                .map(Decimal)
                .ok_or_else(|| Error::InvalidValue("Decimal division error".into())),

            // Interval / integer (scale down interval)
            (Value::Interval(interval), I32(scale)) => {
                Ok(Value::Interval(crate::types::data_type::Interval {
                    months: interval.months / scale,
                    days: interval.days / scale,
                    microseconds: interval.microseconds / (*scale as i64),
                }))
            }
            (Value::Interval(interval), I64(scale)) => {
                Ok(Value::Interval(crate::types::data_type::Interval {
                    months: (interval.months as i64 / scale) as i32,
                    days: (interval.days as i64 / scale) as i32,
                    microseconds: interval.microseconds / scale,
                }))
            }

            // Mixed numeric types - convert to common type
            (a, b) if a.is_numeric() && b.is_numeric() => divide_mixed_numeric(a, b),

            _ => Err(Error::InvalidOperation(format!(
                "Cannot divide {:?} by {:?}",
                left, right
            ))),
        }
    }
}

/// Helper function to handle mixed numeric type division
fn divide_mixed_numeric(left: &Value, right: &Value) -> Result<Value> {
    use Value::*;

    match (left, right) {
        // Decimal with any numeric -> convert to Decimal (preserve precision, highest priority)
        (Decimal(_), _) | (_, Decimal(_)) => match (to_decimal(left), to_decimal(right)) {
            (Some(a), Some(b)) => a
                .checked_div(b)
                .map(Decimal)
                .ok_or_else(|| Error::InvalidValue("Decimal division error".into())),
            _ => Err(Error::InvalidOperation(format!(
                "Cannot divide {:?} by {:?}",
                left, right
            ))),
        },

        // F64 with any numeric (except Decimal which is handled above) -> keep as F64
        (F64(a), _) => {
            let b = to_f64(right)?;
            if b == 0.0 {
                return Err(Error::InvalidValue("Division by zero".into()));
            }
            let result = a / b;
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float overflow or NaN".into()));
            }
            Ok(F64(result))
        }
        (_, F64(b)) => {
            if *b == 0.0 {
                return Err(Error::InvalidValue("Division by zero".into()));
            }
            let a = to_f64(left)?;
            let result = a / b;
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float overflow or NaN".into()));
            }
            Ok(F64(result))
        }

        // F32 with numeric (except F64 and Decimal) -> keep as F32
        (F32(a), _) => {
            let b = to_f32(right)?;
            if b == 0.0 {
                return Err(Error::InvalidValue("Division by zero".into()));
            }
            let result = a / b;
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float overflow or NaN".into()));
            }
            Ok(F32(result))
        }
        (_, F32(b)) => {
            if *b == 0.0 {
                return Err(Error::InvalidValue("Division by zero".into()));
            }
            let a = to_f32(left)?;
            let result = a / b;
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float overflow or NaN".into()));
            }
            Ok(F32(result))
        }

        // This shouldn't be reached since integer-integer is handled above
        _ => Err(Error::InvalidOperation(format!(
            "Cannot divide {:?} by {:?}",
            left, right
        ))),
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn test_divide_integers() {
        let op = DivideOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::I32, &DataType::I32).unwrap(),
            DataType::I32
        );

        // Execution - integer division truncates
        assert_eq!(
            op.execute(&Value::I32(10), &Value::I32(3)).unwrap(),
            Value::I32(3)
        );

        // Division by zero
        assert!(op.execute(&Value::I32(10), &Value::I32(0)).is_err());
    }

    #[test]
    fn test_divide_float64_with_integer() {
        let op = DivideOperator;

        // Type validation - F64 / integer should return F64
        assert_eq!(
            op.validate(&DataType::F64, &DataType::I32).unwrap(),
            DataType::F64
        );
        assert_eq!(
            op.validate(&DataType::I64, &DataType::F64).unwrap(),
            DataType::F64
        );

        // Execution - should stay as F64 (PostgreSQL behavior)
        let result = op.execute(&Value::F64(23.0), &Value::I32(10)).unwrap();
        match result {
            Value::F64(v) => {
                assert!((v - 2.3).abs() < 1e-10, "Expected 2.3, got {}", v);
            }
            _ => panic!("Expected F64, got {:?}", result),
        }

        // Test integer / F64
        let result = op.execute(&Value::I32(25), &Value::F64(10.0)).unwrap();
        assert_eq!(result, Value::F64(2.5));
    }

    #[test]
    fn test_divide_float32_with_integer() {
        let op = DivideOperator;

        // Type validation - F32 / integer should return F32
        assert_eq!(
            op.validate(&DataType::F32, &DataType::I32).unwrap(),
            DataType::F32
        );

        // Execution
        let result = op.execute(&Value::F32(10.0), &Value::I32(4)).unwrap();
        assert_eq!(result, Value::F32(2.5));

        // Integer / F32
        let result = op.execute(&Value::I16(15), &Value::F32(2.0)).unwrap();
        assert_eq!(result, Value::F32(7.5));
    }

    #[test]
    fn test_divide_float64_with_float32() {
        let op = DivideOperator;

        // Type validation - F64 / F32 should return F64
        assert_eq!(
            op.validate(&DataType::F64, &DataType::F32).unwrap(),
            DataType::F64
        );
        assert_eq!(
            op.validate(&DataType::F32, &DataType::F64).unwrap(),
            DataType::F64
        );

        // Execution
        let result = op.execute(&Value::F64(7.0), &Value::F32(2.0)).unwrap();
        assert_eq!(result, Value::F64(3.5));

        let result = op.execute(&Value::F32(10.0), &Value::F64(4.0)).unwrap();
        assert_eq!(result, Value::F64(2.5));
    }

    #[test]
    fn test_divide_decimal_preserves_precision() {
        let op = DivideOperator;

        // Type validation - Decimal / anything numeric returns Decimal
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
        let decimal_23 = Decimal::from_str_exact("23.0").unwrap();
        let decimal_23_div_10 = Decimal::from_str_exact("2.3").unwrap();
        let result = op
            .execute(&Value::Decimal(decimal_23), &Value::I32(10))
            .unwrap();
        assert_eq!(result, Value::Decimal(decimal_23_div_10));

        // F64 / Decimal should convert to Decimal
        let decimal_10 = Decimal::from(10);
        let result = op
            .execute(&Value::F64(23.0), &Value::Decimal(decimal_10))
            .unwrap();
        match result {
            Value::Decimal(d) => {
                println!("F64(23.0) / Decimal(10) = {}", d);
            }
            _ => panic!("Expected Decimal"),
        }
    }

    #[test]
    fn test_divide_null_handling() {
        let op = DivideOperator;

        assert_eq!(
            op.execute(&Value::Null, &Value::I32(5)).unwrap(),
            Value::Null
        );
        assert_eq!(
            op.execute(&Value::F64(2.5), &Value::Null).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_divide_by_zero() {
        let op = DivideOperator;

        // Integer division by zero
        assert!(op.execute(&Value::I32(10), &Value::I32(0)).is_err());

        // Float division by zero (should create infinity, but we check for it)
        assert!(op.execute(&Value::F64(10.0), &Value::F64(0.0)).is_err());
        assert!(op.execute(&Value::F32(10.0), &Value::F32(0.0)).is_err());

        // Mixed type division by zero
        assert!(op.execute(&Value::F64(10.0), &Value::I32(0)).is_err());
    }

    #[test]
    fn test_divide_non_commutative() {
        let op = DivideOperator;

        // Division is NOT commutative
        assert!(!op.is_commutative(&DataType::I32, &DataType::I32));

        // Test that a / b != b / a
        let result1 = op.execute(&Value::I32(10), &Value::I32(2)).unwrap();
        let result2 = op.execute(&Value::I32(2), &Value::I32(10)).unwrap();
        assert_ne!(result1, result2);
        assert_eq!(result1, Value::I32(5));
        assert_eq!(result2, Value::I32(0)); // Integer division truncates
    }
}
