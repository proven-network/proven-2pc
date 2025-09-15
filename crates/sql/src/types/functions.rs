//! Deterministic SQL functions that use transaction context
//!
//! All SQL functions must be deterministic for consensus - they must produce
//! the same output given the same inputs and transaction context.

use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::value::Value;

/// Evaluate a SQL function call
pub fn evaluate_function(
    name: &str,
    args: &[Value],
    context: &TransactionContext,
) -> Result<Value> {
    match name.to_uppercase().as_str() {
        // CAST function for type conversion
        "CAST" => {
            if args.len() != 2 {
                return Err(Error::ExecutionError(
                    "CAST takes exactly 2 arguments".into(),
                ));
            }

            let value = &args[0];
            let target_type = match &args[1] {
                Value::Str(s) => s.as_str(),
                _ => return Err(Error::ExecutionError("CAST type must be a string".into())),
            };

            cast_value(value, target_type)
        }

        // Time functions - use transaction timestamp for determinism
        "NOW" | "CURRENT_TIMESTAMP" => {
            if !args.is_empty() {
                return Err(Error::ExecutionError(format!(
                    "{} takes no arguments",
                    name
                )));
            }
            // Convert HLC timestamp to SQL timestamp
            use chrono::DateTime;
            let micros = context.timestamp().physical as i64;
            let secs = micros / 1_000_000;
            let nanos = ((micros % 1_000_000) * 1_000) as u32;
            let dt = DateTime::from_timestamp(secs, nanos)
                .ok_or_else(|| Error::InvalidValue("Invalid timestamp".into()))?
                .naive_utc();
            Ok(Value::Timestamp(dt))
        }

        "CURRENT_TIME" => {
            if !args.is_empty() {
                return Err(Error::ExecutionError(format!(
                    "{} takes no arguments",
                    name
                )));
            }
            // Extract time of day from timestamp
            use chrono::NaiveTime;
            let micros = context.timestamp().physical as i64;
            let secs = micros / 1_000_000;
            let nanos = ((micros % 1_000_000) * 1_000) as u32;
            let time = NaiveTime::from_num_seconds_from_midnight_opt((secs % 86400) as u32, nanos)
                .ok_or_else(|| Error::InvalidValue("Invalid time".into()))?;
            Ok(Value::Time(time))
        }

        "CURRENT_DATE" => {
            if !args.is_empty() {
                return Err(Error::ExecutionError(format!(
                    "{} takes no arguments",
                    name
                )));
            }
            // Extract date from timestamp (truncate to day boundary)
            use chrono::NaiveDate;
            let micros = context.timestamp().physical as i64;
            let secs = micros / 1_000_000;
            let days = secs / 86400;
            let date = NaiveDate::from_num_days_from_ce_opt(days as i32 + 719163) // Unix epoch is 719163 days from CE
                .ok_or_else(|| Error::InvalidValue("Invalid date".into()))?;
            Ok(Value::Date(date))
        }

        // COALESCE - returns first non-NULL value
        "COALESCE" | "IFNULL" => {
            if args.is_empty() {
                return Err(Error::ExecutionError(
                    "COALESCE requires at least one argument".into(),
                ));
            }

            // Return the first non-NULL value
            for arg in args {
                if *arg != Value::Null {
                    return Ok(arg.clone());
                }
            }

            // If all are NULL, return NULL
            Ok(Value::Null)
        }

        // UUID generation - deterministic based on transaction ID
        "GEN_UUID" | "UUID" | "GENERATE_UUID" => {
            if !args.is_empty() {
                return Err(Error::ExecutionError(format!(
                    "{} takes no arguments",
                    name
                )));
            }
            Ok(Value::Uuid(context.deterministic_uuid()))
        }

        // Math functions (already deterministic)
        "ABS" => {
            if args.len() != 1 {
                return Err(Error::ExecutionError("ABS takes exactly 1 argument".into()));
            }
            match &args[0] {
                Value::I64(i) => Ok(Value::I64(i.abs())),
                Value::Decimal(d) => Ok(Value::Decimal(d.abs())),
                _ => Err(Error::TypeMismatch {
                    expected: "numeric".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
        }

        "ROUND" => {
            if args.is_empty() || args.len() > 2 {
                return Err(Error::ExecutionError("ROUND takes 1 or 2 arguments".into()));
            }

            let precision = if args.len() == 2 {
                match &args[1] {
                    Value::I64(i) => *i as i32,
                    _ => {
                        return Err(Error::ExecutionError(
                            "ROUND precision must be an integer".into(),
                        ));
                    }
                }
            } else {
                0
            };

            match &args[0] {
                Value::Decimal(d) => {
                    let rounded = d.round_dp(precision as u32);
                    Ok(Value::Decimal(rounded))
                }
                Value::I64(i) => Ok(Value::I64(*i)), // Already rounded
                _ => Err(Error::TypeMismatch {
                    expected: "numeric".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
        }

        // String functions (deterministic)
        "UPPER" => {
            if args.len() != 1 {
                return Err(Error::ExecutionError(
                    "UPPER takes exactly 1 argument".into(),
                ));
            }
            match &args[0] {
                Value::Str(s) => Ok(Value::string(s.to_uppercase())),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "string".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
        }

        "LOWER" => {
            if args.len() != 1 {
                return Err(Error::ExecutionError(
                    "LOWER takes exactly 1 argument".into(),
                ));
            }
            match &args[0] {
                Value::Str(s) => Ok(Value::string(s.to_lowercase())),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "string".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
        }

        "LENGTH" => {
            if args.len() != 1 {
                return Err(Error::ExecutionError(
                    "LENGTH takes exactly 1 argument".into(),
                ));
            }
            match &args[0] {
                Value::Str(s) => Ok(Value::integer(s.len() as i64)),
                Value::Bytea(b) => Ok(Value::integer(b.len() as i64)),
                Value::Array(a) | Value::List(a) => Ok(Value::integer(a.len() as i64)),
                Value::Map(m) => Ok(Value::integer(m.len() as i64)),
                Value::Struct(fields) => Ok(Value::integer(fields.len() as i64)),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "string, blob, or collection".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
        }

        // IS_EMPTY - checks if collection or string is empty
        "IS_EMPTY" => {
            if args.len() != 1 {
                return Err(Error::ExecutionError(
                    "IS_EMPTY takes exactly 1 argument".into(),
                ));
            }
            match &args[0] {
                Value::Str(s) => Ok(Value::boolean(s.is_empty())),
                Value::Array(a) | Value::List(a) => Ok(Value::boolean(a.is_empty())),
                Value::Map(m) => Ok(Value::boolean(m.is_empty())),
                Value::Struct(fields) => Ok(Value::boolean(fields.is_empty())),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "string or collection".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
        }

        // CONTAINS - checks if element/key exists in collection
        "CONTAINS" => {
            if args.len() != 2 {
                return Err(Error::ExecutionError(
                    "CONTAINS takes exactly 2 arguments".into(),
                ));
            }
            match (&args[0], &args[1]) {
                (Value::Array(a) | Value::List(a), elem) => {
                    // Check if element exists in array/list
                    Ok(Value::boolean(a.contains(elem)))
                }
                (Value::Map(m), Value::Str(key)) => {
                    // Check if key exists in map
                    Ok(Value::boolean(m.contains_key(key)))
                }
                (Value::Str(s), Value::Str(substr)) => {
                    // Check if substring exists in string
                    Ok(Value::boolean(s.contains(substr.as_str())))
                }
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "collection with element or map with string key".into(),
                    found: format!("{} and {}", args[0].data_type(), args[1].data_type()),
                }),
            }
        }

        // EXTRACT - universal accessor function for collections
        "EXTRACT" => {
            if args.len() != 2 {
                return Err(Error::ExecutionError(
                    "EXTRACT takes exactly 2 arguments".into(),
                ));
            }
            match (&args[0], &args[1]) {
                (Value::Array(a) | Value::List(a), Value::I32(idx)) => {
                    // Access by index
                    Ok(a.get(*idx as usize).cloned().unwrap_or(Value::Null))
                }
                (Value::Array(a) | Value::List(a), Value::I64(idx)) => {
                    // Access by index
                    Ok(a.get(*idx as usize).cloned().unwrap_or(Value::Null))
                }
                (Value::Map(m), Value::Str(key)) => {
                    // Access by key
                    Ok(m.get(key).cloned().unwrap_or(Value::Null))
                }
                (Value::Struct(fields), Value::Str(field)) => {
                    // Access by field name
                    Ok(fields
                        .iter()
                        .find(|(name, _)| name == field)
                        .map(|(_, val)| val.clone())
                        .unwrap_or(Value::Null))
                }
                (Value::Null, _) => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "collection with appropriate accessor".into(),
                    found: format!("{} and {}", args[0].data_type(), args[1].data_type()),
                }),
            }
        }

        // KEYS - returns all keys from a map as a list
        "KEYS" => {
            if args.len() != 1 {
                return Err(Error::ExecutionError(
                    "KEYS takes exactly 1 argument".into(),
                ));
            }
            match &args[0] {
                Value::Map(m) => {
                    let keys: Vec<Value> = m.keys().map(|k| Value::Str(k.clone())).collect();
                    Ok(Value::List(keys))
                }
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "map".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
        }

        // VALUES - returns all values from a map as a list
        "VALUES" => {
            if args.len() != 1 {
                return Err(Error::ExecutionError(
                    "VALUES takes exactly 1 argument".into(),
                ));
            }
            match &args[0] {
                Value::Map(m) => {
                    let values: Vec<Value> = m.values().cloned().collect();
                    Ok(Value::List(values))
                }
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "map".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
        }

        // UNWRAP - path-based nested access
        "UNWRAP" => {
            if args.len() != 2 {
                return Err(Error::ExecutionError(
                    "UNWRAP takes exactly 2 arguments (collection, path)".into(),
                ));
            }
            let path = match &args[1] {
                Value::Str(s) => s,
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "string path".into(),
                        found: args[1].data_type().to_string(),
                    });
                }
            };
            unwrap_value(&args[0], path)
        }

        _ => Err(Error::ExecutionError(format!("Unknown function: {}", name))),
    }
}

/// Cast a value to a target type
pub fn cast_value(value: &Value, target_type: &str) -> Result<Value> {
    match target_type {
        "TINYINT" => match value {
            Value::I8(v) => Ok(Value::I8(*v)),
            Value::I16(v) => {
                if *v >= i8::MIN as i16 && *v <= i8::MAX as i16 {
                    Ok(Value::I8(*v as i8))
                } else {
                    Err(Error::InvalidValue(format!(
                        "Value {} out of range for TINYINT",
                        v
                    )))
                }
            }
            Value::I32(v) => {
                if *v >= i8::MIN as i32 && *v <= i8::MAX as i32 {
                    Ok(Value::I8(*v as i8))
                } else {
                    Err(Error::InvalidValue(format!(
                        "Value {} out of range for TINYINT",
                        v
                    )))
                }
            }
            Value::I64(v) => {
                if *v >= i8::MIN as i64 && *v <= i8::MAX as i64 {
                    Ok(Value::I8(*v as i8))
                } else {
                    Err(Error::InvalidValue(format!(
                        "Value {} out of range for TINYINT",
                        v
                    )))
                }
            }
            Value::I128(v) => {
                if *v >= i8::MIN as i128 && *v <= i8::MAX as i128 {
                    Ok(Value::I8(*v as i8))
                } else {
                    Err(Error::InvalidValue(format!(
                        "Value {} out of range for TINYINT",
                        v
                    )))
                }
            }
            Value::Str(s) => s
                .parse::<i8>()
                .map(Value::I8)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to TINYINT", s))),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "numeric or string".into(),
                found: value.data_type().to_string(),
            }),
        },

        "SMALLINT" => match value {
            Value::I8(v) => Ok(Value::I16(*v as i16)),
            Value::I16(v) => Ok(Value::I16(*v)),
            Value::I32(v) => {
                if *v >= i16::MIN as i32 && *v <= i16::MAX as i32 {
                    Ok(Value::I16(*v as i16))
                } else {
                    Err(Error::InvalidValue(format!(
                        "Value {} out of range for SMALLINT",
                        v
                    )))
                }
            }
            Value::I64(v) => {
                if *v >= i16::MIN as i64 && *v <= i16::MAX as i64 {
                    Ok(Value::I16(*v as i16))
                } else {
                    Err(Error::InvalidValue(format!(
                        "Value {} out of range for SMALLINT",
                        v
                    )))
                }
            }
            Value::I128(v) => {
                if *v >= i16::MIN as i128 && *v <= i16::MAX as i128 {
                    Ok(Value::I16(*v as i16))
                } else {
                    Err(Error::InvalidValue(format!(
                        "Value {} out of range for SMALLINT",
                        v
                    )))
                }
            }
            Value::Str(s) => s
                .parse::<i16>()
                .map(Value::I16)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to SMALLINT", s))),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "numeric or string".into(),
                found: value.data_type().to_string(),
            }),
        },

        "INT" => match value {
            Value::I8(v) => Ok(Value::I32(*v as i32)),
            Value::I16(v) => Ok(Value::I32(*v as i32)),
            Value::I32(v) => Ok(Value::I32(*v)),
            Value::I64(v) => {
                if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
                    Ok(Value::I32(*v as i32))
                } else {
                    Err(Error::InvalidValue(format!(
                        "Value {} out of range for INT",
                        v
                    )))
                }
            }
            Value::I128(v) => {
                if *v >= i32::MIN as i128 && *v <= i32::MAX as i128 {
                    Ok(Value::I32(*v as i32))
                } else {
                    Err(Error::InvalidValue(format!(
                        "Value {} out of range for INT",
                        v
                    )))
                }
            }
            Value::Str(s) => s
                .parse::<i32>()
                .map(Value::I32)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to INT", s))),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "numeric or string".into(),
                found: value.data_type().to_string(),
            }),
        },

        "BIGINT" => match value {
            Value::I8(v) => Ok(Value::I64(*v as i64)),
            Value::I16(v) => Ok(Value::I64(*v as i64)),
            Value::I32(v) => Ok(Value::I64(*v as i64)),
            Value::I64(v) => Ok(Value::I64(*v)),
            Value::I128(v) => {
                if *v >= i64::MIN as i128 && *v <= i64::MAX as i128 {
                    Ok(Value::I64(*v as i64))
                } else {
                    Err(Error::InvalidValue(format!(
                        "Value {} out of range for BIGINT",
                        v
                    )))
                }
            }
            Value::Str(s) => s
                .parse::<i64>()
                .map(Value::I64)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to BIGINT", s))),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "numeric or string".into(),
                found: value.data_type().to_string(),
            }),
        },

        "HUGEINT" => match value {
            Value::I8(v) => Ok(Value::I128(*v as i128)),
            Value::I16(v) => Ok(Value::I128(*v as i128)),
            Value::I32(v) => Ok(Value::I128(*v as i128)),
            Value::I64(v) => Ok(Value::I128(*v as i128)),
            Value::I128(v) => Ok(Value::I128(*v)),
            Value::Str(s) => s
                .parse::<i128>()
                .map(Value::I128)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to HUGEINT", s))),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "numeric or string".into(),
                found: value.data_type().to_string(),
            }),
        },

        // REAL is F32 (single precision)
        "REAL" => match value {
            Value::I8(v) => Ok(Value::F32(*v as f32)),
            Value::I16(v) => Ok(Value::F32(*v as f32)),
            Value::I32(v) => Ok(Value::F32(*v as f32)),
            Value::I64(v) => Ok(Value::F32(*v as f32)),
            Value::I128(v) => Ok(Value::F32(*v as f32)),
            Value::F32(v) => Ok(Value::F32(*v)),
            Value::F64(v) => Ok(Value::F32(*v as f32)),
            Value::Decimal(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_f32().map(Value::F32).ok_or_else(|| {
                    Error::InvalidValue(format!("Cannot cast decimal {} to REAL", d))
                })
            }
            Value::Str(s) => s
                .parse::<f32>()
                .map(Value::F32)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to REAL", s))),
            _ => Err(Error::TypeMismatch {
                expected: "REAL".into(),
                found: value.data_type().to_string(),
            }),
        },
        // FLOAT is F64 (double precision) - matching column type behavior
        "FLOAT" => match value {
            Value::I8(v) => Ok(Value::F64(*v as f64)),
            Value::I16(v) => Ok(Value::F64(*v as f64)),
            Value::I32(v) => Ok(Value::F64(*v as f64)),
            Value::I64(v) => Ok(Value::F64(*v as f64)),
            Value::I128(v) => Ok(Value::F64(*v as f64)),
            Value::F32(v) => Ok(Value::F64(*v as f64)),
            Value::F64(v) => Ok(Value::F64(*v)),
            Value::Decimal(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_f64().map(Value::F64).ok_or_else(|| {
                    Error::InvalidValue(format!("Cannot cast decimal {} to FLOAT", d))
                })
            }
            Value::Str(s) => s
                .parse::<f64>()
                .map(Value::F64)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to FLOAT", s))),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "numeric or string".into(),
                found: value.data_type().to_string(),
            }),
        },

        "DOUBLE" => match value {
            Value::I8(v) => Ok(Value::F64(*v as f64)),
            Value::I16(v) => Ok(Value::F64(*v as f64)),
            Value::I32(v) => Ok(Value::F64(*v as f64)),
            Value::I64(v) => Ok(Value::F64(*v as f64)),
            Value::I128(v) => Ok(Value::F64(*v as f64)),
            Value::F32(v) => Ok(Value::F64(*v as f64)),
            Value::F64(v) => Ok(Value::F64(*v)),
            Value::Decimal(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_f64().map(Value::F64).ok_or_else(|| {
                    Error::InvalidValue(format!("Cannot cast decimal {} to DOUBLE", d))
                })
            }
            Value::Str(s) => s
                .parse::<f64>()
                .map(Value::F64)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to DOUBLE", s))),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "numeric or string".into(),
                found: value.data_type().to_string(),
            }),
        },

        // Unsigned integer types
        "TINYINT UNSIGNED" => match value {
            Value::U8(v) => Ok(Value::U8(*v)),
            Value::I8(v) if *v >= 0 => Ok(Value::U8(*v as u8)),
            Value::I16(v) if *v >= 0 && *v <= u8::MAX as i16 => Ok(Value::U8(*v as u8)),
            Value::I32(v) if *v >= 0 && *v <= u8::MAX as i32 => Ok(Value::U8(*v as u8)),
            Value::I64(v) if *v >= 0 && *v <= u8::MAX as i64 => Ok(Value::U8(*v as u8)),
            Value::Str(s) => s.parse::<u8>().map(Value::U8).map_err(|_| {
                Error::InvalidValue(format!("Cannot cast '{}' to TINYINT UNSIGNED", s))
            }),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::InvalidValue(format!(
                "Cannot cast {} to TINYINT UNSIGNED",
                value.data_type()
            ))),
        },

        "SMALLINT UNSIGNED" => match value {
            Value::U8(v) => Ok(Value::U16(*v as u16)),
            Value::U16(v) => Ok(Value::U16(*v)),
            Value::I8(v) if *v >= 0 => Ok(Value::U16(*v as u16)),
            Value::I16(v) if *v >= 0 => Ok(Value::U16(*v as u16)),
            Value::I32(v) if *v >= 0 && *v <= u16::MAX as i32 => Ok(Value::U16(*v as u16)),
            Value::I64(v) if *v >= 0 && *v <= u16::MAX as i64 => Ok(Value::U16(*v as u16)),
            Value::Str(s) => s.parse::<u16>().map(Value::U16).map_err(|_| {
                Error::InvalidValue(format!("Cannot cast '{}' to SMALLINT UNSIGNED", s))
            }),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::InvalidValue(format!(
                "Cannot cast {} to SMALLINT UNSIGNED",
                value.data_type()
            ))),
        },

        "INT UNSIGNED" => match value {
            Value::U8(v) => Ok(Value::U32(*v as u32)),
            Value::U16(v) => Ok(Value::U32(*v as u32)),
            Value::U32(v) => Ok(Value::U32(*v)),
            Value::I8(v) if *v >= 0 => Ok(Value::U32(*v as u32)),
            Value::I16(v) if *v >= 0 => Ok(Value::U32(*v as u32)),
            Value::I32(v) if *v >= 0 => Ok(Value::U32(*v as u32)),
            Value::I64(v) if *v >= 0 && *v <= u32::MAX as i64 => Ok(Value::U32(*v as u32)),
            Value::Str(s) => s
                .parse::<u32>()
                .map(Value::U32)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to INT UNSIGNED", s))),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::InvalidValue(format!(
                "Cannot cast {} to INT UNSIGNED",
                value.data_type()
            ))),
        },

        "BIGINT UNSIGNED" => match value {
            Value::U8(v) => Ok(Value::U64(*v as u64)),
            Value::U16(v) => Ok(Value::U64(*v as u64)),
            Value::U32(v) => Ok(Value::U64(*v as u64)),
            Value::U64(v) => Ok(Value::U64(*v)),
            Value::I8(v) if *v >= 0 => Ok(Value::U64(*v as u64)),
            Value::I16(v) if *v >= 0 => Ok(Value::U64(*v as u64)),
            Value::I32(v) if *v >= 0 => Ok(Value::U64(*v as u64)),
            Value::I64(v) if *v >= 0 => Ok(Value::U64(*v as u64)),
            Value::Str(s) => s.parse::<u64>().map(Value::U64).map_err(|_| {
                Error::InvalidValue(format!("Cannot cast '{}' to BIGINT UNSIGNED", s))
            }),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::InvalidValue(format!(
                "Cannot cast {} to BIGINT UNSIGNED",
                value.data_type()
            ))),
        },

        "HUGEINT UNSIGNED" => match value {
            Value::U8(v) => Ok(Value::U128(*v as u128)),
            Value::U16(v) => Ok(Value::U128(*v as u128)),
            Value::U32(v) => Ok(Value::U128(*v as u128)),
            Value::U64(v) => Ok(Value::U128(*v as u128)),
            Value::U128(v) => Ok(Value::U128(*v)),
            Value::I8(v) if *v >= 0 => Ok(Value::U128(*v as u128)),
            Value::I16(v) if *v >= 0 => Ok(Value::U128(*v as u128)),
            Value::I32(v) if *v >= 0 => Ok(Value::U128(*v as u128)),
            Value::I64(v) if *v >= 0 => Ok(Value::U128(*v as u128)),
            Value::I128(v) if *v >= 0 => Ok(Value::U128(*v as u128)),
            Value::Str(s) => s.parse::<u128>().map(Value::U128).map_err(|_| {
                Error::InvalidValue(format!("Cannot cast '{}' to HUGEINT UNSIGNED", s))
            }),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::InvalidValue(format!(
                "Cannot cast {} to HUGEINT UNSIGNED",
                value.data_type()
            ))),
        },

        "TEXT" => match value {
            Value::Str(s) => Ok(Value::Str(s.clone())),
            Value::I8(v) => Ok(Value::Str(v.to_string())),
            Value::I16(v) => Ok(Value::Str(v.to_string())),
            Value::I32(v) => Ok(Value::Str(v.to_string())),
            Value::I64(v) => Ok(Value::Str(v.to_string())),
            Value::I128(v) => Ok(Value::Str(v.to_string())),
            Value::F32(v) => Ok(Value::Str(v.to_string())),
            Value::F64(v) => Ok(Value::Str(v.to_string())),
            Value::Decimal(d) => Ok(Value::Str(d.to_string())),
            Value::Bool(b) => Ok(Value::Str(b.to_string())),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "castable to string".into(),
                found: value.data_type().to_string(),
            }),
        },

        "BOOLEAN" => match value {
            Value::Bool(b) => Ok(Value::Bool(*b)),
            Value::I8(v) => Ok(Value::Bool(*v != 0)),
            Value::I16(v) => Ok(Value::Bool(*v != 0)),
            Value::I32(v) => Ok(Value::Bool(*v != 0)),
            Value::I64(v) => Ok(Value::Bool(*v != 0)),
            Value::I128(v) => Ok(Value::Bool(*v != 0)),
            Value::Str(s) => match s.to_lowercase().as_str() {
                "true" | "t" | "1" => Ok(Value::Bool(true)),
                "false" | "f" | "0" => Ok(Value::Bool(false)),
                _ => Err(Error::InvalidValue(format!(
                    "Cannot cast '{}' to BOOLEAN",
                    s
                ))),
            },
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "castable to boolean".into(),
                found: value.data_type().to_string(),
            }),
        },

        "UUID" => {
            use uuid::Uuid;

            match value {
                Value::Str(s) => {
                    // Try to parse the UUID string (supports standard, URN, and hex formats)
                    Uuid::parse_str(s)
                        .map(Value::Uuid)
                        .map_err(|_| Error::InvalidValue(format!("Failed to parse UUID: {}", s)))
                }
                Value::Uuid(u) => Ok(Value::Uuid(*u)),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::InvalidValue(format!(
                    "Cannot cast {} to UUID",
                    value.data_type()
                ))),
            }
        }

        "DECIMAL" => {
            use rust_decimal::Decimal;
            use std::str::FromStr;

            match value {
                Value::I8(v) => Ok(Value::Decimal(Decimal::from(*v))),
                Value::I16(v) => Ok(Value::Decimal(Decimal::from(*v))),
                Value::I32(v) => Ok(Value::Decimal(Decimal::from(*v))),
                Value::I64(v) => Ok(Value::Decimal(Decimal::from(*v))),
                Value::I128(v) => Ok(Value::Decimal(Decimal::from(*v))),
                Value::F32(v) => Decimal::from_str(&v.to_string())
                    .map(Value::Decimal)
                    .map_err(|_| {
                        Error::InvalidValue(format!("Cannot cast float {} to DECIMAL", v))
                    }),
                Value::F64(v) => Decimal::from_str(&v.to_string())
                    .map(Value::Decimal)
                    .map_err(|_| {
                        Error::InvalidValue(format!("Cannot cast float {} to DECIMAL", v))
                    }),
                Value::Decimal(d) => Ok(Value::Decimal(*d)),
                Value::Str(s) => Decimal::from_str(s)
                    .map(Value::Decimal)
                    .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to DECIMAL", s))),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "numeric or string".into(),
                    found: value.data_type().to_string(),
                }),
            }
        }

        _ => Err(Error::ExecutionError(format!(
            "Unknown cast target type: {}",
            target_type
        ))),
    }
}

/// Helper function for UNWRAP - navigates through nested structures using a path
fn unwrap_value(value: &Value, path: &str) -> Result<Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value.clone();

    for part in parts {
        current = if let Ok(index) = part.parse::<usize>() {
            // Numeric index for array/list
            match current {
                Value::Array(arr) | Value::List(arr) => {
                    arr.get(index).cloned().unwrap_or(Value::Null)
                }
                _ => return Ok(Value::Null),
            }
        } else {
            // String key for map/struct or field name
            match current {
                Value::Map(map) => map.get(part).cloned().unwrap_or(Value::Null),
                Value::Struct(fields) => fields
                    .iter()
                    .find(|(name, _)| name == part)
                    .map(|(_, val)| val.clone())
                    .unwrap_or(Value::Null),
                _ => return Ok(Value::Null),
            }
        };
    }

    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_deterministic_now() {
        let timestamp = HlcTimestamp::new(1_000_000_000, 0, NodeId::new(1));
        let context = TransactionContext::new(timestamp);

        // NOW() should always return the same value within a transaction
        let now1 = evaluate_function("NOW", &[], &context).unwrap();
        let now2 = evaluate_function("CURRENT_TIMESTAMP", &[], &context).unwrap();

        assert_eq!(now1, now2);
        // 1_000_000_000 microseconds = 1000 seconds, 0 nanos
        use chrono::DateTime;
        let expected = DateTime::from_timestamp(1000, 0).unwrap().naive_utc();
        assert_eq!(now1, Value::Timestamp(expected));
    }

    #[test]
    fn test_collection_functions() {
        use crate::stream::transaction::TransactionContext;
        use std::collections::HashMap;

        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // Test LENGTH function with collections
        let list = Value::List(vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
        let result = evaluate_function("LENGTH", &[list], &context).unwrap();
        assert_eq!(result, Value::I64(3));

        let mut map = HashMap::new();
        map.insert("a".to_string(), Value::I64(1));
        map.insert("b".to_string(), Value::I64(2));
        let map_val = Value::Map(map);
        let result = evaluate_function("LENGTH", std::slice::from_ref(&map_val), &context).unwrap();
        assert_eq!(result, Value::I64(2));

        // Test IS_EMPTY function
        let empty_list = Value::List(vec![]);
        let result = evaluate_function("IS_EMPTY", &[empty_list], &context).unwrap();
        assert_eq!(result, Value::Bool(true));

        let non_empty_list = Value::List(vec![Value::I64(1)]);
        let result = evaluate_function("IS_EMPTY", &[non_empty_list], &context).unwrap();
        assert_eq!(result, Value::Bool(false));

        // Test CONTAINS function
        let list = Value::List(vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
        let result = evaluate_function("CONTAINS", &[list, Value::I64(2)], &context).unwrap();
        assert_eq!(result, Value::Bool(true));

        let list = Value::List(vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
        let result = evaluate_function("CONTAINS", &[list, Value::I64(5)], &context).unwrap();
        assert_eq!(result, Value::Bool(false));

        // Test CONTAINS with map
        let result = evaluate_function(
            "CONTAINS",
            &[map_val.clone(), Value::Str("a".to_string())],
            &context,
        )
        .unwrap();
        assert_eq!(result, Value::Bool(true));

        let result = evaluate_function(
            "CONTAINS",
            &[map_val.clone(), Value::Str("c".to_string())],
            &context,
        )
        .unwrap();
        assert_eq!(result, Value::Bool(false));

        // Test EXTRACT function
        let list = Value::List(vec![Value::I64(10), Value::I64(20), Value::I64(30)]);
        let result = evaluate_function("EXTRACT", &[list, Value::I32(1)], &context).unwrap();
        assert_eq!(result, Value::I64(20));

        let result = evaluate_function(
            "EXTRACT",
            &[map_val.clone(), Value::Str("b".to_string())],
            &context,
        )
        .unwrap();
        assert_eq!(result, Value::I64(2));

        // Test KEYS function
        let result = evaluate_function("KEYS", std::slice::from_ref(&map_val), &context).unwrap();
        match result {
            Value::List(keys) => {
                assert_eq!(keys.len(), 2);
                assert!(keys.contains(&Value::Str("a".to_string())));
                assert!(keys.contains(&Value::Str("b".to_string())));
            }
            _ => panic!("Expected List"),
        }

        // Test VALUES function
        let result = evaluate_function("VALUES", std::slice::from_ref(&map_val), &context).unwrap();
        match result {
            Value::List(values) => {
                assert_eq!(values.len(), 2);
                assert!(values.contains(&Value::I64(1)));
                assert!(values.contains(&Value::I64(2)));
            }
            _ => panic!("Expected List"),
        }

        // Test UNWRAP function with nested structures
        let nested = Value::Map({
            let mut m = HashMap::new();
            m.insert(
                "user".to_string(),
                Value::Struct(vec![
                    ("name".to_string(), Value::Str("Alice".to_string())),
                    ("age".to_string(), Value::I64(30)),
                ]),
            );
            m.insert(
                "items".to_string(),
                Value::List(vec![
                    Value::Str("item1".to_string()),
                    Value::Str("item2".to_string()),
                ]),
            );
            m
        });

        let result = evaluate_function(
            "UNWRAP",
            &[nested.clone(), Value::Str("user.name".to_string())],
            &context,
        )
        .unwrap();
        assert_eq!(result, Value::Str("Alice".to_string()));

        let result = evaluate_function(
            "UNWRAP",
            &[nested.clone(), Value::Str("items.0".to_string())],
            &context,
        )
        .unwrap();
        assert_eq!(result, Value::Str("item1".to_string()));

        let result = evaluate_function(
            "UNWRAP",
            &[nested, Value::Str("user.unknown".to_string())],
            &context,
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_deterministic_uuid() {
        let timestamp = HlcTimestamp::new(1_000_000_000, 0, NodeId::new(1));
        let context = TransactionContext::new(timestamp);

        // Each call should produce a different UUID (auto-incrementing sequence)
        let uuid1 = evaluate_function("UUID", &[], &context).unwrap();
        let uuid2 = evaluate_function("UUID", &[], &context).unwrap();
        let uuid3 = evaluate_function("UUID", &[], &context).unwrap();

        // All UUIDs should be different
        assert_ne!(uuid1, uuid2);
        assert_ne!(uuid2, uuid3);
        assert_ne!(uuid1, uuid3);

        // But they should be deterministic - same context produces same sequence
        let context2 = TransactionContext::new(timestamp);
        let uuid1_again = evaluate_function("UUID", &[], &context2).unwrap();
        let uuid2_again = evaluate_function("UUID", &[], &context2).unwrap();

        // Same sequence from a fresh context with same timestamp
        assert_eq!(uuid1, uuid1_again);
        assert_eq!(uuid2, uuid2_again);
    }
}
