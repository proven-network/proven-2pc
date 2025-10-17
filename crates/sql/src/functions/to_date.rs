//! TO_DATE, TO_TIME, and TO_TIMESTAMP functions - parse strings to temporal types using format strings

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::Value;
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

pub struct ToDateFunction;

impl Function for ToDateFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "TO_DATE",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(format!(
                "TO_DATE takes exactly 2 arguments, got {}",
                arg_types.len()
            )));
        }

        // Both arguments should be string (or nullable string)
        for (i, arg_type) in arg_types.iter().enumerate() {
            match arg_type {
                DataType::Str => {}
                DataType::Nullable(inner) if matches!(**inner, DataType::Str) => {}
                _ => {
                    return Err(Error::ExecutionError(format!(
                        "TO_DATE argument {} must be a string, got {:?}",
                        i + 1,
                        arg_type
                    )));
                }
            }
        }

        Ok(DataType::Date)
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "TO_DATE takes exactly 2 arguments".into(),
            ));
        }

        // Extract the date string
        let date_str = match &args[0] {
            Value::Str(s) => s.as_str(),
            _ => {
                return Err(Error::ExecutionError(
                    "TO_DATE requires first argument to be a string".into(),
                ));
            }
        };

        // Extract the format string
        let format_str = match &args[1] {
            Value::Str(s) => s.as_str(),
            _ => {
                return Err(Error::ExecutionError(
                    "TO_DATE requires second argument to be a string".into(),
                ));
            }
        };

        // Parse the date using the format string
        let date = NaiveDate::parse_from_str(date_str, format_str).map_err(|e| {
            Error::InvalidValue(format!(
                "Failed to parse date '{}' with format '{}': {}",
                date_str, format_str, e
            ))
        })?;

        Ok(Value::Date(date))
    }
}

pub struct ToTimeFunction;

impl Function for ToTimeFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "TO_TIME",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(format!(
                "TO_TIME takes exactly 2 arguments, got {}",
                arg_types.len()
            )));
        }

        // Both arguments should be string (or nullable string)
        for (i, arg_type) in arg_types.iter().enumerate() {
            match arg_type {
                DataType::Str => {}
                DataType::Nullable(inner) if matches!(**inner, DataType::Str) => {}
                _ => {
                    return Err(Error::ExecutionError(format!(
                        "TO_TIME argument {} must be a string, got {:?}",
                        i + 1,
                        arg_type
                    )));
                }
            }
        }

        Ok(DataType::Time)
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "TO_TIME takes exactly 2 arguments".into(),
            ));
        }

        // Extract the time string
        let time_str = match &args[0] {
            Value::Str(s) => s.as_str(),
            _ => {
                return Err(Error::ExecutionError(
                    "TO_TIME requires first argument to be a string".into(),
                ));
            }
        };

        // Extract the format string
        let format_str = match &args[1] {
            Value::Str(s) => s.as_str(),
            _ => {
                return Err(Error::ExecutionError(
                    "TO_TIME requires second argument to be a string".into(),
                ));
            }
        };

        // Parse the time using the format string
        let time = NaiveTime::parse_from_str(time_str, format_str).map_err(|e| {
            Error::InvalidValue(format!(
                "Failed to parse time '{}' with format '{}': {}",
                time_str, format_str, e
            ))
        })?;

        Ok(Value::Time(time))
    }
}

pub struct ToTimestampFunction;

impl Function for ToTimestampFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "TO_TIMESTAMP",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(format!(
                "TO_TIMESTAMP takes exactly 2 arguments, got {}",
                arg_types.len()
            )));
        }

        // Both arguments should be string (or nullable string)
        for (i, arg_type) in arg_types.iter().enumerate() {
            match arg_type {
                DataType::Str => {}
                DataType::Nullable(inner) if matches!(**inner, DataType::Str) => {}
                _ => {
                    return Err(Error::ExecutionError(format!(
                        "TO_TIMESTAMP argument {} must be a string, got {:?}",
                        i + 1,
                        arg_type
                    )));
                }
            }
        }

        Ok(DataType::Timestamp)
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "TO_TIMESTAMP takes exactly 2 arguments".into(),
            ));
        }

        // Extract the timestamp string
        let timestamp_str = match &args[0] {
            Value::Str(s) => s.as_str(),
            _ => {
                return Err(Error::ExecutionError(
                    "TO_TIMESTAMP requires first argument to be a string".into(),
                ));
            }
        };

        // Extract the format string
        let format_str = match &args[1] {
            Value::Str(s) => s.as_str(),
            _ => {
                return Err(Error::ExecutionError(
                    "TO_TIMESTAMP requires second argument to be a string".into(),
                ));
            }
        };

        // Parse the timestamp using the format string
        let timestamp = NaiveDateTime::parse_from_str(timestamp_str, format_str).map_err(|e| {
            Error::InvalidValue(format!(
                "Failed to parse timestamp '{}' with format '{}': {}",
                timestamp_str, format_str, e
            ))
        })?;

        Ok(Value::Timestamp(timestamp))
    }
}

/// Register TO_DATE, TO_TIME, and TO_TIMESTAMP functions
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(ToDateFunction));
    registry.register(Box::new(ToTimeFunction));
    registry.register(Box::new(ToTimestampFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_to_date_signature() {
        let func = ToDateFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "TO_DATE");
        assert!(!sig.is_aggregate);
    }

    #[test]
    fn test_to_date_execute() {
        let func = ToDateFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Valid date
        let result = func
            .execute(
                &[Value::string("2017-06-15"), Value::string("%Y-%m-%d")],
                &context,
            )
            .unwrap();

        match result {
            Value::Date(date) => {
                assert_eq!(date.year(), 2017);
                assert_eq!(date.month(), 6);
                assert_eq!(date.day(), 15);
            }
            _ => panic!("Expected Date value, got {:?}", result),
        }
    }

    #[test]
    fn test_to_date_with_month_name() {
        let func = ToDateFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Date with month abbreviation
        let result = func
            .execute(
                &[Value::string("2017-jun-15"), Value::string("%Y-%b-%d")],
                &context,
            )
            .unwrap();

        match result {
            Value::Date(date) => {
                assert_eq!(date.year(), 2017);
                assert_eq!(date.month(), 6);
                assert_eq!(date.day(), 15);
            }
            _ => panic!("Expected Date value, got {:?}", result),
        }
    }

    #[test]
    fn test_to_date_invalid_format() {
        let func = ToDateFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Invalid date
        let result = func.execute(
            &[Value::string("2015-14-05"), Value::string("%Y-%m-%d")],
            &context,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_to_date_wrong_arg_count() {
        let func = ToDateFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Too few arguments
        let result = func.execute(&[Value::string("2017-06-15")], &context);
        assert!(result.is_err());

        // Too many arguments
        let result = func.execute(
            &[
                Value::string("2017-06-15"),
                Value::string("%Y-%m-%d"),
                Value::string("extra"),
            ],
            &context,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_to_date_requires_string() {
        let func = ToDateFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Non-string first argument
        let result = func.execute(&[Value::I32(20170615), Value::string("%Y-%m-%d")], &context);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_time_signature() {
        let func = ToTimeFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "TO_TIME");
        assert!(!sig.is_aggregate);
    }

    #[test]
    fn test_to_time_execute() {
        let func = ToTimeFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Valid time
        let result = func
            .execute(
                &[Value::string("23:56:04"), Value::string("%H:%M:%S")],
                &context,
            )
            .unwrap();

        match result {
            Value::Time(time) => {
                assert_eq!(time.hour(), 23);
                assert_eq!(time.minute(), 56);
                assert_eq!(time.second(), 4);
            }
            _ => panic!("Expected Time value, got {:?}", result),
        }
    }

    #[test]
    fn test_to_time_invalid_format() {
        let func = ToTimeFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Invalid time (too short)
        let result = func.execute(
            &[Value::string("23:56"), Value::string("%H:%M:%S")],
            &context,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_to_timestamp_signature() {
        let func = ToTimestampFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "TO_TIMESTAMP");
        assert!(!sig.is_aggregate);
    }

    #[test]
    fn test_to_timestamp_execute() {
        let func = ToTimestampFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Valid timestamp
        let result = func
            .execute(
                &[
                    Value::string("2015-09-05 23:56:04"),
                    Value::string("%Y-%m-%d %H:%M:%S"),
                ],
                &context,
            )
            .unwrap();

        match result {
            Value::Timestamp(ts) => {
                assert_eq!(ts.year(), 2015);
                assert_eq!(ts.month(), 9);
                assert_eq!(ts.day(), 5);
                assert_eq!(ts.hour(), 23);
                assert_eq!(ts.minute(), 56);
                assert_eq!(ts.second(), 4);
            }
            _ => panic!("Expected Timestamp value, got {:?}", result),
        }
    }

    #[test]
    fn test_to_timestamp_invalid_format() {
        let func = ToTimestampFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Invalid format string (ends with %)
        let result = func.execute(
            &[
                Value::string("2015-09-05 23:56:04"),
                Value::string("%Y-%m-%d %H:%M:%"),
            ],
            &context,
        );

        assert!(result.is_err());
    }
}
