//! Addition operator implementation

use super::helpers::*;
use super::mixed_ops;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};
use chrono::Duration;

pub struct AddOperator;

impl BinaryOperator for AddOperator {
    fn name(&self) -> &'static str {
        "addition"
    }

    fn symbol(&self) -> &'static str {
        "+"
    }

    fn is_commutative(&self, left: &DataType, right: &DataType) -> bool {
        use DataType::*;

        // Unwrap nullable types
        let (left_inner, right_inner, _) = unwrap_nullable_pair(left, right);

        match (left_inner, right_inner) {
            // String concatenation is NOT commutative
            (Str, Str) | (Text, Text) | (Str, Text) | (Text, Str) => false,
            // Most other operations are commutative
            _ => true,
        }
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, nullable) = unwrap_nullable_pair(left, right);

        let result = match (left_inner, right_inner) {
            // Unknown (NULL) with anything returns Unknown
            (Null, _) | (_, Null) => Null,

            // Decimal with any numeric type -> Decimal (preserve precision, highest priority)
            (Decimal(p1, s1), Decimal(p2, s2)) => {
                // For decimal addition, result precision and scale follow SQL standard
                Decimal(p1.and(*p2), s1.and(*s2))
            }
            (Decimal(_, _), t) | (t, Decimal(_, _)) if t.is_numeric() => Decimal(None, None),

            // Float64 with any numeric type (except Decimal) -> Float64 (PostgreSQL behavior)
            (F64, t) | (t, F64) if t.is_numeric() => F64,

            // Float32 with any numeric type (except F64 and Decimal) -> Float32
            (F32, t) | (t, F32) if t.is_numeric() => F32,

            // Integer types - use standard promotion
            (a, b) if a.is_integer() && b.is_integer() => promote_numeric_types(a, b)?,

            // Date/Time arithmetic
            (Date, I32) | (I32, Date) => Date,
            (Date, I64) | (I64, Date) => Date,
            (Date, Interval) | (Interval, Date) => Date,
            (Timestamp, Interval) | (Interval, Timestamp) => Timestamp,
            (Time, Interval) | (Interval, Time) => Time,
            (Interval, Interval) => Interval,

            // String concatenation
            (Str, Str) | (Text, Text) | (Str, Text) | (Text, Str) => Str,

            // Array concatenation
            (Array(a, _), Array(b, _)) if a == b => Array(a.clone(), None),
            (List(a), List(b)) if a == b => List(a.clone()),

            _ => {
                return Err(Error::InvalidOperation(format!(
                    "Cannot add {} and {}",
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
            (a, b) if a.is_integer() && b.is_integer() => mixed_ops::add_integers(a, b),

            // Floats (no overflow checking needed)
            (F32(a), F32(b)) => Ok(F32(a + b)),
            (F64(a), F64(b)) => Ok(F64(a + b)),

            // Decimal
            (Decimal(a), Decimal(b)) => Ok(Decimal(a + b)),

            // Date arithmetic
            (Date(date), I32(days)) | (I32(days), Date(date)) => {
                let new_date = *date + Duration::days(*days as i64);
                Ok(Date(new_date))
            }
            (Date(date), I64(days)) | (I64(days), Date(date)) => {
                let new_date = *date + Duration::days(*days);
                Ok(Date(new_date))
            }

            // Date/Time + Interval operations
            (Date(date), Value::Interval(interval)) | (Value::Interval(interval), Date(date)) => {
                let new_date = *date
                    + Duration::days(interval.days as i64)
                    + Duration::days((interval.months * 30) as i64); // Approximate month as 30 days
                Ok(Date(new_date))
            }
            (Time(time), Value::Interval(interval)) | (Value::Interval(interval), Time(time)) => {
                let duration = Duration::microseconds(interval.microseconds)
                    + Duration::days(interval.days as i64)
                    + Duration::days((interval.months * 30) as i64);
                let nanos = duration
                    .num_nanoseconds()
                    .ok_or_else(|| Error::InvalidValue("Interval too large".into()))?;
                let new_time = *time + Duration::nanoseconds(nanos % (24 * 3600 * 1_000_000_000)); // Wrap around 24 hours
                Ok(Time(new_time))
            }
            (Timestamp(ts), Value::Interval(interval))
            | (Value::Interval(interval), Timestamp(ts)) => {
                let new_ts = *ts
                    + Duration::microseconds(interval.microseconds)
                    + Duration::days(interval.days as i64)
                    + Duration::days((interval.months * 30) as i64); // Approximate month as 30 days
                Ok(Timestamp(new_ts))
            }

            // Interval + Interval
            (Value::Interval(a), Value::Interval(b)) => {
                Ok(Value::Interval(crate::types::data_type::Interval {
                    months: a.months + b.months,
                    days: a.days + b.days,
                    microseconds: a.microseconds + b.microseconds,
                }))
            }

            // String concatenation
            (Str(a), Str(b)) => Ok(Str(format!("{}{}", a, b))),

            // Mixed numeric types - convert to common type
            (a, b) if a.is_numeric() && b.is_numeric() => add_mixed_numeric(a, b),

            _ => Err(Error::InvalidOperation(format!(
                "Cannot add {:?} and {:?}",
                left, right
            ))),
        }
    }
}

/// Helper function to handle mixed numeric type addition
fn add_mixed_numeric(left: &Value, right: &Value) -> Result<Value> {
    use Value::*;

    match (left, right) {
        // Decimal with any numeric -> convert to Decimal (preserve precision, highest priority)
        (Decimal(_), _) | (_, Decimal(_)) => match (to_decimal(left), to_decimal(right)) {
            (Some(a), Some(b)) => Ok(Decimal(a + b)),
            _ => Err(Error::InvalidOperation(format!(
                "Cannot add {:?} and {:?}",
                left, right
            ))),
        },

        // F64 with any numeric (except Decimal which is handled above) -> keep as F64
        (F64(f), val) | (val, F64(f)) if val.is_numeric() => {
            let other = to_f64(val)?;
            Ok(F64(f + other))
        }

        // F32 with numeric (except F64 and Decimal) -> keep as F32
        (F32(f), val) | (val, F32(f)) if val.is_numeric() => {
            let other = to_f32(val)?;
            Ok(F32(f + other))
        }

        // This shouldn't be reached since integer-integer is handled above
        _ => Err(Error::InvalidOperation(format!(
            "Cannot add {:?} and {:?}",
            left, right
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use rust_decimal::Decimal;

    #[test]
    fn test_add_integers() {
        let op = AddOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::I32, &DataType::I32).unwrap(),
            DataType::I32
        );
        assert_eq!(
            op.validate(&DataType::I16, &DataType::I32).unwrap(),
            DataType::I32
        );

        // Execution
        assert_eq!(
            op.execute(&Value::I32(5), &Value::I32(3)).unwrap(),
            Value::I32(8)
        );

        // Overflow
        assert!(op.execute(&Value::I8(127), &Value::I8(1)).is_err());
    }

    #[test]
    fn test_add_float64_with_integer() {
        let op = AddOperator;

        // Type validation - F64 + integer should return F64
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
                assert!((v - 12.3).abs() < 1e-10, "Expected 12.3, got {}", v);
            }
            _ => panic!("Expected F64, got {:?}", result),
        }

        // Test commutative
        let result = op.execute(&Value::I32(10), &Value::F64(2.5)).unwrap();
        assert_eq!(result, Value::F64(12.5));
    }

    #[test]
    fn test_add_float32_with_integer() {
        let op = AddOperator;

        // Type validation - F32 + integer should return F32
        assert_eq!(
            op.validate(&DataType::F32, &DataType::I32).unwrap(),
            DataType::F32
        );

        // Execution
        let result = op.execute(&Value::F32(2.5), &Value::I32(4)).unwrap();
        assert_eq!(result, Value::F32(6.5));

        // Commutative
        let result = op.execute(&Value::I16(3), &Value::F32(1.5)).unwrap();
        assert_eq!(result, Value::F32(4.5));
    }

    #[test]
    fn test_add_float64_with_float32() {
        let op = AddOperator;

        // Type validation - F64 + F32 should return F64
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
        assert_eq!(result, Value::F64(5.5));

        let result = op.execute(&Value::F32(2.5), &Value::F64(4.0)).unwrap();
        assert_eq!(result, Value::F64(6.5));
    }

    #[test]
    fn test_add_decimal_preserves_precision() {
        let op = AddOperator;

        // Type validation - Decimal + anything numeric returns Decimal
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
        let decimal_10 = Decimal::from(10);
        let decimal_123 = Decimal::from_str_exact("12.3").unwrap();

        let result = op
            .execute(&Value::Decimal(decimal_23), &Value::I32(10))
            .unwrap();
        assert_eq!(result, Value::Decimal(decimal_123));

        // F64 + Decimal should convert to Decimal
        let result = op
            .execute(&Value::F64(2.3), &Value::Decimal(decimal_10))
            .unwrap();
        match result {
            Value::Decimal(d) => {
                // This will show the exact float representation when converted to decimal
                println!("F64(2.3) + Decimal(10) = {}", d);
            }
            _ => panic!("Expected Decimal"),
        }
    }

    #[test]
    fn test_add_dates() {
        let op = AddOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::Date, &DataType::I32).unwrap(),
            DataType::Date
        );
        assert_eq!(
            op.validate(&DataType::Date, &DataType::Interval).unwrap(),
            DataType::Date
        );

        // Execution
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(
            op.execute(&Value::Date(date), &Value::I32(5)).unwrap(),
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 6).unwrap())
        );
    }

    #[test]
    fn test_add_null_handling() {
        let op = AddOperator;

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
    fn test_add_string_concatenation() {
        let op = AddOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::Str, &DataType::Str).unwrap(),
            DataType::Str
        );

        // Execution
        let result = op
            .execute(&Value::Str("Hello".into()), &Value::Str(" World".into()))
            .unwrap();
        assert_eq!(result, Value::Str("Hello World".into()));
    }
}
