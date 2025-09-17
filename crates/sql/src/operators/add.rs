//! Addition operator implementation

use super::helpers::*;
use super::mixed_ops;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};
use chrono::Duration;
use rust_decimal::Decimal;

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
            (Unknown, _) | (_, Unknown) => Unknown,

            // Numeric addition - use helper for promotion
            (a, b) if a.is_numeric() && b.is_numeric() => promote_numeric_types(a, b)?,

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
    // Try to convert to Decimal for mixed numeric operations
    match (to_decimal(left), to_decimal(right)) {
        (Some(a), Some(b)) => Ok(Value::Decimal(a + b)),
        _ => Err(Error::InvalidOperation(format!(
            "Cannot add {:?} and {:?}",
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
    use chrono::NaiveDate;

    #[test]
    fn test_add_numeric() {
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
        assert_eq!(
            op.validate(&DataType::F32, &DataType::F64).unwrap(),
            DataType::F64
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
}
