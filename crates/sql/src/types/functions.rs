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
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "string or blob".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
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
