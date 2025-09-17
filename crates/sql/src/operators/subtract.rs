//! Subtraction operator implementation

use super::helpers::*;
use super::mixed_ops;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};
use chrono::Duration;
use rust_decimal::Decimal;

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
            (Unknown, _) | (_, Unknown) => Unknown,

            // Numeric subtraction - use helper for promotion
            (a, b) if a.is_numeric() && b.is_numeric() => promote_numeric_types(a, b)?,

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
                Ok(Value::Interval(crate::types::data_type::Interval {
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
                Ok(Value::Interval(crate::types::data_type::Interval {
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
                Ok(Value::Interval(crate::types::data_type::Interval {
                    months: 0,
                    days: 0,
                    microseconds,
                }))
            }

            // Interval - Interval
            (Value::Interval(a), Value::Interval(b)) => {
                Ok(Value::Interval(crate::types::data_type::Interval {
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
    // Try to convert to Decimal for mixed numeric operations
    match (to_decimal(left), to_decimal(right)) {
        (Some(a), Some(b)) => Ok(Value::Decimal(a - b)),
        _ => Err(Error::InvalidOperation(format!(
            "Cannot subtract {:?} from {:?}",
            right, left
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
    use chrono::NaiveDate;

    #[test]
    fn test_subtract_numeric() {
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
            Value::Interval(crate::types::data_type::Interval {
                months: 0,
                days: 5,
                microseconds: 0,
            })
        );
    }
}
