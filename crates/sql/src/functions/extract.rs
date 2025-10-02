//! EXTRACT function - SQL standard datetime field extraction
//!
//! Syntax: EXTRACT(field FROM source)
//! Extracts date/time components from TIMESTAMP, DATE, TIME, and INTERVAL values

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::{DataType, Interval};
use crate::types::value::Value;
use chrono::{Datelike, Timelike};

/// EXTRACT function - extracts datetime components
pub struct ExtractFunction;

impl Function for ExtractFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "EXTRACT",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(
                "EXTRACT takes exactly 2 arguments (field and source)".into(),
            ));
        }
        // Returns an integer (exact numeric value per SQL standard)
        Ok(DataType::I64)
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "EXTRACT takes exactly 2 arguments (field and source)".into(),
            ));
        }

        // First argument is the field name (encoded as a string from the parser)
        let field = match &args[0] {
            Value::Str(f) => f.as_str(),
            _ => {
                return Err(Error::ExecutionError(
                    "EXTRACT field must be a string (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)"
                        .into(),
                ));
            }
        };

        // Second argument is the source value (timestamp, date, time, or interval)
        let source = &args[1];

        // Handle NULL
        if matches!(source, Value::Null) {
            return Ok(Value::Null);
        }

        // Extract the appropriate field based on the source type
        match source {
            Value::Timestamp(ts) => extract_from_timestamp(field, ts),
            Value::Date(date) => extract_from_date(field, date),
            Value::Time(time) => extract_from_time(field, time),
            Value::Interval(interval) => extract_from_interval(field, interval),
            _ => Err(Error::ExecutionError(format!(
                "Cannot extract {} from {}: expected TIMESTAMP, DATE, TIME, or INTERVAL",
                field,
                source.data_type()
            ))),
        }
    }
}

/// Extract field from a timestamp value
fn extract_from_timestamp(field: &str, ts: &chrono::NaiveDateTime) -> Result<Value> {
    let value = match field {
        "YEAR" => ts.year() as i64,
        "MONTH" => ts.month() as i64,
        "DAY" => ts.day() as i64,
        "HOUR" => ts.hour() as i64,
        "MINUTE" => ts.minute() as i64,
        "SECOND" => ts.second() as i64,
        _ => {
            return Err(Error::ExecutionError(format!(
                "Unsupported datetime field for TIMESTAMP: {}. Supported fields: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND",
                field
            )));
        }
    };
    Ok(Value::I64(value))
}

/// Extract field from a date value
fn extract_from_date(field: &str, date: &chrono::NaiveDate) -> Result<Value> {
    let value = match field {
        "YEAR" => date.year() as i64,
        "MONTH" => date.month() as i64,
        "DAY" => date.day() as i64,
        _ => {
            return Err(Error::ExecutionError(format!(
                "Unsupported datetime field for DATE: {}. Supported fields: YEAR, MONTH, DAY",
                field
            )));
        }
    };
    Ok(Value::I64(value))
}

/// Extract field from a time value
fn extract_from_time(field: &str, time: &chrono::NaiveTime) -> Result<Value> {
    let value = match field {
        "HOUR" => time.hour() as i64,
        "MINUTE" => time.minute() as i64,
        "SECOND" => time.second() as i64,
        _ => {
            return Err(Error::ExecutionError(format!(
                "Unsupported datetime field for TIME: {}. Supported fields: HOUR, MINUTE, SECOND",
                field
            )));
        }
    };
    Ok(Value::I64(value))
}

/// Extract field from an interval value
fn extract_from_interval(field: &str, interval: &Interval) -> Result<Value> {
    // Interval stores: months, days, and microseconds
    // We need to convert microseconds to hours, minutes, seconds
    const MICROS_PER_SECOND: i64 = 1_000_000;
    const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
    const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;

    let value = match field {
        "YEAR" => (interval.months / 12) as i64,
        "MONTH" => (interval.months % 12) as i64,
        "DAY" => interval.days as i64,
        "HOUR" => interval.microseconds / MICROS_PER_HOUR,
        "MINUTE" => (interval.microseconds % MICROS_PER_HOUR) / MICROS_PER_MINUTE,
        "SECOND" => (interval.microseconds % MICROS_PER_MINUTE) / MICROS_PER_SECOND,
        _ => {
            return Err(Error::ExecutionError(format!(
                "Unsupported datetime field for INTERVAL: {}. Supported fields: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND",
                field
            )));
        }
    };

    Ok(Value::I64(value))
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(ExtractFunction));
}
