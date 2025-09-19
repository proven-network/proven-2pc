//! Multiplication operator implementation

use super::helpers::{promote_numeric_types, to_decimal, to_f32, to_f64};
use super::helpers::{unwrap_nullable_pair, wrap_nullable};
use super::mixed_ops;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

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

            // Decimal with any numeric type -> Decimal (preserve precision, highest priority)
            (Decimal(p1, s1), Decimal(p2, s2)) => {
                // For decimal multiplication, result precision and scale follow SQL standard
                Decimal(p1.and(*p2), s1.and(*s2))
            }
            (Decimal(_, _), t) | (t, Decimal(_, _)) if t.is_numeric() => Decimal(None, None),

            // Float64 with any numeric type (except Decimal) -> Float64 (PostgreSQL behavior)
            (F64, t) | (t, F64) if t.is_numeric() => F64,

            // Float32 with any numeric type (except F64 and Decimal) -> Float32
            (F32, t) | (t, F32) if t.is_numeric() => F32,

            // Integer types - use standard promotion
            (a, b) if a.is_integer() && b.is_integer() => promote_numeric_types(a, b)?,

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
    use Value::*;

    match (left, right) {
        // Decimal with any numeric -> convert to Decimal (preserve precision, highest priority)
        (Decimal(_), _) | (_, Decimal(_)) => match (to_decimal(left), to_decimal(right)) {
            (Some(a), Some(b)) => Ok(Decimal(a * b)),
            _ => Err(Error::InvalidOperation(format!(
                "Cannot multiply {:?} and {:?}",
                left, right
            ))),
        },

        // F64 with any numeric (except Decimal which is handled above) -> keep as F64
        (F64(f), val) | (val, F64(f)) if val.is_numeric() => {
            let other = to_f64(val)?;
            let result = f * other;
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float overflow or NaN".into()));
            }
            Ok(F64(result))
        }

        // F32 with numeric (except F64 and Decimal) -> keep as F32
        (F32(f), val) | (val, F32(f)) if val.is_numeric() => {
            let other = to_f32(val)?;
            let result = f * other;
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float overflow or NaN".into()));
            }
            Ok(F32(result))
        }

        // This shouldn't be reached since integer-integer is handled above
        _ => Err(Error::InvalidOperation(format!(
            "Cannot multiply {:?} and {:?}",
            left, right
        ))),
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn test_multiply_integers() {
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

    #[test]
    fn test_multiply_float64_with_integer() {
        let op = MultiplyOperator;

        // Type validation - F64 * integer should return F64
        assert_eq!(
            op.validate(&DataType::F64, &DataType::I32).unwrap(),
            DataType::F64
        );
        assert_eq!(
            op.validate(&DataType::I64, &DataType::F64).unwrap(),
            DataType::F64
        );

        // Execution - should stay as F64 (PostgreSQL behavior)
        let result = op.execute(&Value::F64(2.3), &Value::I32(10)).unwrap();
        match result {
            Value::F64(v) => {
                assert!((v - 23.0).abs() < 1e-10, "Expected 23.0, got {}", v);
            }
            _ => panic!("Expected F64, got {:?}", result),
        }

        // Test the problematic case that was converting to decimal
        let result = op.execute(&Value::F64(2.3), &Value::I64(10)).unwrap();
        match result {
            Value::F64(v) => {
                // Should be close to 23, not 22.999...
                assert!((v - 23.0).abs() < 1e-10, "Expected 23.0, got {}", v);
            }
            _ => panic!("Expected F64, got {:?}", result),
        }

        // Test commutative
        let result = op.execute(&Value::I32(10), &Value::F64(2.5)).unwrap();
        assert_eq!(result, Value::F64(25.0));
    }

    #[test]
    fn test_multiply_float32_with_integer() {
        let op = MultiplyOperator;

        // Type validation - F32 * integer should return F32
        assert_eq!(
            op.validate(&DataType::F32, &DataType::I32).unwrap(),
            DataType::F32
        );

        // Execution
        let result = op.execute(&Value::F32(2.5), &Value::I32(4)).unwrap();
        assert_eq!(result, Value::F32(10.0));

        // Commutative
        let result = op.execute(&Value::I16(3), &Value::F32(1.5)).unwrap();
        assert_eq!(result, Value::F32(4.5));
    }

    #[test]
    fn test_multiply_float64_with_float32() {
        let op = MultiplyOperator;

        // Type validation - F64 * F32 should return F64
        assert_eq!(
            op.validate(&DataType::F64, &DataType::F32).unwrap(),
            DataType::F64
        );
        assert_eq!(
            op.validate(&DataType::F32, &DataType::F64).unwrap(),
            DataType::F64
        );

        // Execution
        let result = op.execute(&Value::F64(2.0), &Value::F32(3.5)).unwrap();
        assert_eq!(result, Value::F64(7.0));

        let result = op.execute(&Value::F32(2.5), &Value::F64(4.0)).unwrap();
        assert_eq!(result, Value::F64(10.0));
    }

    #[test]
    fn test_multiply_decimal_preserves_precision() {
        let op = MultiplyOperator;

        // Type validation - Decimal * anything numeric returns Decimal
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
        let decimal_23 = Decimal::from_str_exact("2.3").unwrap();
        let decimal_230 = Decimal::from_str_exact("23.0").unwrap();
        let result = op
            .execute(&Value::Decimal(decimal_23), &Value::I32(10))
            .unwrap();
        assert_eq!(result, Value::Decimal(decimal_230));

        // F64 to Decimal should show exact representation
        let decimal_10 = Decimal::from(10);
        let result = op
            .execute(&Value::F64(2.3), &Value::Decimal(decimal_10))
            .unwrap();
        match result {
            Value::Decimal(d) => {
                // This will show the exact float representation when converted to decimal
                println!("F64(2.3) * Decimal(10) = {}", d);
            }
            _ => panic!("Expected Decimal"),
        }
    }

    #[test]
    fn test_multiply_null_handling() {
        let op = MultiplyOperator;

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
    fn test_multiply_overflow_detection() {
        let op = MultiplyOperator;

        // Float overflow
        assert!(op.execute(&Value::F64(1e308), &Value::F64(10.0)).is_err());
        assert!(op.execute(&Value::F32(1e38), &Value::F32(10.0)).is_err());
    }
}
