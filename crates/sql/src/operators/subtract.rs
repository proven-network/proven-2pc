//! Subtraction operator implementation

use super::helpers::*;
use super::mixed_ops;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};
use chrono::Duration;

pub struct SubtractOperator;

impl BinaryOperator for SubtractOperator {
    fn name(&self) -> &'static str {
        "subtraction"
    }

    fn symbol(&self) -> &'static str {
        "-"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        false // Subtraction is NEVER commutative
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, nullable) = unwrap_nullable_pair(left, right);

        let result = match (left_inner, right_inner) {
            // Unknown (NULL) with anything returns Unknown
            (Null, _) | (_, Null) => Null,

            // Decimal with any numeric type -> Decimal (preserve precision, highest priority)
            (Decimal(p1, s1), Decimal(p2, s2)) => {
                // For decimal operations, result precision and scale follow SQL standard
                Decimal(p1.and(*p2), s1.and(*s2))
            }
            (Decimal(_, _), t) | (t, Decimal(_, _)) if t.is_numeric() => Decimal(None, None),

            // Float64 with any numeric type (except Decimal) -> Float64 (PostgreSQL behavior)
            (F64, t) | (t, F64) if t.is_numeric() => F64,

            // Float32 with any numeric type (except F64 and Decimal) -> Float32
            (F32, t) | (t, F32) if t.is_numeric() => F32,

            // Integer types - use standard promotion
            (a, b) if a.is_integer() && b.is_integer() => promote_numeric_types(a, b)?,

            // Date arithmetic
            (Date, I32) | (Date, I64) => Date,
            (Date, Interval) => Date,
            (Date, Date) => Interval, // Date - Date = Interval

            // Timestamp arithmetic
            (Timestamp, Interval) => Timestamp,
            (Timestamp, Timestamp) => Interval, // Timestamp - Timestamp = Interval

            // Time arithmetic
            (Time, Interval) => Time,
            (Time, Time) => Interval, // Time - Time = Interval

            // Interval arithmetic
            (Interval, Interval) => Interval,

            _ => {
                return Err(Error::InvalidOperation(format!(
                    "Cannot subtract {} from {}",
                    right, left
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
            (a, b) if a.is_integer() && b.is_integer() => mixed_ops::subtract_integers(a, b),

            // Floats (no overflow checking needed)
            (F32(a), F32(b)) => Ok(F32(a - b)),
            (F64(a), F64(b)) => Ok(F64(a - b)),

            // Decimal
            (Decimal(a), Decimal(b)) => Ok(Decimal(a - b)),

            // Date arithmetic
            (Date(date), I32(days)) => {
                let new_date = *date - Duration::days(*days as i64);
                Ok(Date(new_date))
            }
            (Date(date), I64(days)) => {
                let new_date = *date - Duration::days(*days);
                Ok(Date(new_date))
            }
            (Date(a), Date(b)) => {
                let diff = a.signed_duration_since(*b);
                let days = diff.num_days();
                if days > i32::MAX as i64 || days < i32::MIN as i64 {
                    return Err(Error::InvalidValue("Date difference too large".into()));
                }
                // Return an Interval representing the number of days
                Ok(Value::Interval(proven_value::Interval {
                    months: 0,
                    days: days as i32,
                    microseconds: 0,
                }))
            }

            // Date/Time - Interval operations
            (Date(date), Value::Interval(interval)) => {
                let new_date = *date
                    - Duration::days(interval.days as i64)
                    - Duration::days((interval.months * 30) as i64); // Approximate month as 30 days
                Ok(Date(new_date))
            }
            (Time(time), Value::Interval(interval)) => {
                let duration = Duration::microseconds(interval.microseconds)
                    + Duration::days(interval.days as i64)
                    + Duration::days((interval.months * 30) as i64);
                let nanos = duration
                    .num_nanoseconds()
                    .ok_or_else(|| Error::InvalidValue("Interval too large".into()))?;
                let new_time = *time - Duration::nanoseconds(nanos % (24 * 3600 * 1_000_000_000)); // Wrap around 24 hours
                Ok(Time(new_time))
            }
            (Timestamp(ts), Value::Interval(interval)) => {
                let new_ts = *ts
                    - Duration::microseconds(interval.microseconds)
                    - Duration::days(interval.days as i64)
                    - Duration::days((interval.months * 30) as i64); // Approximate month as 30 days
                Ok(Timestamp(new_ts))
            }

            // Timestamp - Timestamp = Interval
            (Timestamp(a), Timestamp(b)) => {
                let diff = a.signed_duration_since(*b);
                let microseconds = diff
                    .num_microseconds()
                    .ok_or_else(|| Error::InvalidValue("Timestamp difference too large".into()))?;
                Ok(Value::Interval(proven_value::Interval {
                    months: 0,
                    days: 0,
                    microseconds,
                }))
            }

            // Time - Time = Interval
            (Time(a), Time(b)) => {
                let diff = a.signed_duration_since(*b);
                let microseconds = diff
                    .num_microseconds()
                    .ok_or_else(|| Error::InvalidValue("Time difference too large".into()))?;
                Ok(Value::Interval(proven_value::Interval {
                    months: 0,
                    days: 0,
                    microseconds,
                }))
            }

            // Interval - Interval
            (Value::Interval(a), Value::Interval(b)) => {
                Ok(Value::Interval(proven_value::Interval {
                    months: a.months - b.months,
                    days: a.days - b.days,
                    microseconds: a.microseconds - b.microseconds,
                }))
            }

            // Mixed numeric types - convert to common type
            (a, b) if a.is_numeric() && b.is_numeric() => subtract_mixed_numeric(a, b),

            _ => Err(Error::InvalidOperation(format!(
                "Cannot subtract {:?} from {:?}",
                right, left
            ))),
        }
    }
}

/// Helper function to handle mixed numeric type subtraction
fn subtract_mixed_numeric(left: &Value, right: &Value) -> Result<Value> {
    use Value::*;

    match (left, right) {
        // Decimal with any numeric -> convert to Decimal (preserve precision, highest priority)
        (Decimal(_), _) | (_, Decimal(_)) => match (to_decimal(left), to_decimal(right)) {
            (Some(a), Some(b)) => Ok(Decimal(a - b)),
            _ => Err(Error::InvalidOperation(format!(
                "Cannot subtract {:?} from {:?}",
                right, left
            ))),
        },

        // F64 with any numeric (except Decimal which is handled above) -> keep as F64
        (F64(f), _) | (_, F64(f)) => {
            // Note: For subtraction, order matters!
            let (left_val, right_val) = if matches!(left, F64(_)) {
                (*f, to_f64(right)?)
            } else {
                (to_f64(left)?, *f)
            };
            Ok(F64(left_val - right_val))
        }

        // F32 with numeric (except F64 and Decimal) -> keep as F32
        (F32(f), _) | (_, F32(f)) => {
            // Note: For subtraction, order matters!
            let (left_val, right_val) = if matches!(left, F32(_)) {
                (*f, to_f32(right)?)
            } else {
                (to_f32(left)?, *f)
            };
            Ok(F32(left_val - right_val))
        }

        // This shouldn't be reached since integer-integer is handled above
        _ => Err(Error::InvalidOperation(format!(
            "Cannot subtract {:?} from {:?}",
            right, left
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use rust_decimal::Decimal;

    #[test]
    fn test_subtract_integers() {
        let op = SubtractOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::I32, &DataType::I32).unwrap(),
            DataType::I32
        );

        // Execution
        assert_eq!(
            op.execute(&Value::I32(10), &Value::I32(3)).unwrap(),
            Value::I32(7)
        );

        // Underflow
        assert!(op.execute(&Value::U8(0), &Value::U8(1)).is_err());
    }

    #[test]
    fn test_subtract_float64_with_integer() {
        let op = SubtractOperator;

        // Type validation - F64 - integer should return F64
        assert_eq!(
            op.validate(&DataType::F64, &DataType::I32).unwrap(),
            DataType::F64
        );
        assert_eq!(
            op.validate(&DataType::I64, &DataType::F64).unwrap(),
            DataType::F64
        );

        // Execution - should stay as F64 (PostgreSQL behavior)
        let result = op.execute(&Value::F64(12.3), &Value::I32(10)).unwrap();
        match result {
            Value::F64(v) => {
                assert!((v - 2.3).abs() < 1e-10, "Expected 2.3, got {}", v);
            }
            _ => panic!("Expected F64, got {:?}", result),
        }

        // Test the case that was converting to decimal
        let result = op.execute(&Value::F64(25.5), &Value::I64(10)).unwrap();
        match result {
            Value::F64(v) => {
                assert!((v - 15.5).abs() < 1e-10, "Expected 15.5, got {}", v);
            }
            _ => panic!("Expected F64, got {:?}", result),
        }

        // Test non-commutative: integer - F64
        let result = op.execute(&Value::I32(10), &Value::F64(2.5)).unwrap();
        assert_eq!(result, Value::F64(7.5));
    }

    #[test]
    fn test_subtract_float32_with_integer() {
        let op = SubtractOperator;

        // Type validation - F32 - integer should return F32
        assert_eq!(
            op.validate(&DataType::F32, &DataType::I32).unwrap(),
            DataType::F32
        );

        // Execution
        let result = op.execute(&Value::F32(10.5), &Value::I32(4)).unwrap();
        assert_eq!(result, Value::F32(6.5));

        // Non-commutative: integer - F32
        let result = op.execute(&Value::I16(8), &Value::F32(1.5)).unwrap();
        assert_eq!(result, Value::F32(6.5));
    }

    #[test]
    fn test_subtract_float64_with_float32() {
        let op = SubtractOperator;

        // Type validation - F64 - F32 should return F64
        assert_eq!(
            op.validate(&DataType::F64, &DataType::F32).unwrap(),
            DataType::F64
        );
        assert_eq!(
            op.validate(&DataType::F32, &DataType::F64).unwrap(),
            DataType::F64
        );

        // Execution
        let result = op.execute(&Value::F64(10.0), &Value::F32(3.5)).unwrap();
        assert_eq!(result, Value::F64(6.5));

        let result = op.execute(&Value::F32(7.5), &Value::F64(2.0)).unwrap();
        assert_eq!(result, Value::F64(5.5));
    }

    #[test]
    fn test_subtract_decimal_preserves_precision() {
        let op = SubtractOperator;

        // Type validation - Decimal - anything numeric returns Decimal
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
        let decimal_123 = Decimal::from_str_exact("12.3").unwrap();
        let decimal_23 = Decimal::from_str_exact("2.3").unwrap();
        let result = op
            .execute(&Value::Decimal(decimal_123), &Value::I32(10))
            .unwrap();
        assert_eq!(result, Value::Decimal(decimal_23));

        // F64 - Decimal should convert to Decimal
        let decimal_10 = Decimal::from(10);
        let result = op
            .execute(&Value::F64(12.3), &Value::Decimal(decimal_10))
            .unwrap();
        match result {
            Value::Decimal(d) => {
                // This will show the exact float representation when converted to decimal
                println!("F64(12.3) - Decimal(10) = {}", d);
            }
            _ => panic!("Expected Decimal"),
        }
    }

    #[test]
    fn test_subtract_null_handling() {
        let op = SubtractOperator;

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
    fn test_subtract_non_commutative() {
        let op = SubtractOperator;

        // Subtraction is NOT commutative
        assert!(!op.is_commutative(&DataType::I32, &DataType::I32));

        // Test that a - b != b - a
        let result1 = op.execute(&Value::I32(10), &Value::I32(3)).unwrap();
        let result2 = op.execute(&Value::I32(3), &Value::I32(10)).unwrap();
        assert_ne!(result1, result2);
        assert_eq!(result1, Value::I32(7));
        assert_eq!(result2, Value::I32(-7));
    }

    #[test]
    fn test_subtract_dates() {
        let op = SubtractOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::Date, &DataType::I32).unwrap(),
            DataType::Date
        );
        assert_eq!(
            op.validate(&DataType::Date, &DataType::Date).unwrap(),
            DataType::Interval
        );

        // Execution - Date - Date = Interval
        let date1 = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        assert_eq!(
            op.execute(&Value::Date(date1), &Value::Date(date2))
                .unwrap(),
            Value::Interval(proven_value::Interval {
                months: 0,
                days: 5,
                microseconds: 0,
            })
        );
    }
}
