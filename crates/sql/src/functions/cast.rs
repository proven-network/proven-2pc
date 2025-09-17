//! CAST function for type conversion

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct CastFunction;

impl Function for CastFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "CAST",
            min_args: 2,
            max_args: Some(2),
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(format!(
                "CAST takes exactly 2 arguments, got {}",
                arg_types.len()
            )));
        }

        // The second argument must be a string literal representing the target type
        // We can't always know the exact type at semantic analysis time,
        // but we try to parse it if possible
        match &arg_types[1] {
            DataType::Text | DataType::Str => {
                // We can't determine the exact type without the literal value
                // Return the source type as the best guess
                Ok(arg_types[0].clone())
            }
            _ => Err(Error::TypeMismatch {
                expected: "string type for cast target".into(),
                found: arg_types[1].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
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
}

/// Cast a value to a target type
fn cast_value(value: &Value, target_type: &str) -> Result<Value> {
    // Handle NULL
    if *value == Value::Null {
        return Ok(Value::Null);
    }

    match target_type.to_uppercase().as_str() {
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
            Value::Decimal(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_i8().map(Value::I8).ok_or_else(|| {
                    Error::InvalidValue(format!("Decimal {} out of range for TINYINT", d))
                })
            }
            Value::Str(s) => s
                .parse::<i8>()
                .map(Value::I8)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to TINYINT", s))),
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
            Value::Decimal(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_i16().map(Value::I16).ok_or_else(|| {
                    Error::InvalidValue(format!("Decimal {} out of range for SMALLINT", d))
                })
            }
            Value::Str(s) => s
                .parse::<i16>()
                .map(Value::I16)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to SMALLINT", s))),
            _ => Err(Error::TypeMismatch {
                expected: "numeric or string".into(),
                found: value.data_type().to_string(),
            }),
        },

        "INT" | "INTEGER" => match value {
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
            Value::F32(v) => Ok(Value::I32(*v as i32)),
            Value::F64(v) => Ok(Value::I32(*v as i32)),
            Value::Decimal(d) => {
                // Try to convert Decimal to i32
                use rust_decimal::prelude::ToPrimitive;
                d.to_i32().map(Value::I32).ok_or_else(|| {
                    Error::InvalidValue(format!("Decimal {} out of range for INT", d))
                })
            }
            Value::Str(s) => s
                .parse::<i32>()
                .map(Value::I32)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to INT", s))),
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
            Value::Decimal(d) => {
                // Try to convert Decimal to i64
                use rust_decimal::prelude::ToPrimitive;
                d.to_i64().map(Value::I64).ok_or_else(|| {
                    Error::InvalidValue(format!("Decimal {} out of range for BIGINT", d))
                })
            }
            Value::Str(s) => s
                .parse::<i64>()
                .map(Value::I64)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to BIGINT", s))),
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
            _ => Err(Error::TypeMismatch {
                expected: "numeric or string".into(),
                found: value.data_type().to_string(),
            }),
        },

        "REAL" => match value {
            Value::I8(v) => Ok(Value::F32(*v as f32)),
            Value::I16(v) => Ok(Value::F32(*v as f32)),
            Value::I32(v) => Ok(Value::F32(*v as f32)),
            Value::I64(v) => Ok(Value::F32(*v as f32)),
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
                expected: "numeric or string".into(),
                found: value.data_type().to_string(),
            }),
        },

        "FLOAT" | "DOUBLE" => match value {
            Value::I8(v) => Ok(Value::F64(*v as f64)),
            Value::I16(v) => Ok(Value::F64(*v as f64)),
            Value::I32(v) => Ok(Value::F64(*v as f64)),
            Value::I64(v) => Ok(Value::F64(*v as f64)),
            Value::F32(v) => Ok(Value::F64(*v as f64)),
            Value::F64(v) => Ok(Value::F64(*v)),
            Value::Str(s) => s
                .parse::<f64>()
                .map(Value::F64)
                .map_err(|_| Error::InvalidValue(format!("Cannot cast '{}' to FLOAT", s))),
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
            _ => Err(Error::InvalidValue(format!(
                "Cannot cast {} to SMALLINT UNSIGNED",
                value.data_type()
            ))),
        },

        "INT UNSIGNED" | "INTEGER UNSIGNED" => match value {
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
            _ => Err(Error::InvalidValue(format!(
                "Cannot cast {} to HUGEINT UNSIGNED",
                value.data_type()
            ))),
        },

        "TEXT" | "VARCHAR" | "STRING" => match value {
            Value::Str(s) => Ok(Value::Str(s.clone())),
            Value::I8(v) => Ok(Value::string(v.to_string())),
            Value::I16(v) => Ok(Value::string(v.to_string())),
            Value::I32(v) => Ok(Value::string(v.to_string())),
            Value::I64(v) => Ok(Value::string(v.to_string())),
            Value::F32(v) => Ok(Value::string(v.to_string())),
            Value::F64(v) => Ok(Value::string(v.to_string())),
            Value::Bool(b) => Ok(Value::string(b.to_string())),
            _ => Err(Error::TypeMismatch {
                expected: "castable to string".into(),
                found: value.data_type().to_string(),
            }),
        },

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
                _ => Err(Error::TypeMismatch {
                    expected: "numeric or string".into(),
                    found: value.data_type().to_string(),
                }),
            }
        }

        "UUID" => {
            use uuid::Uuid;

            match value {
                Value::Str(s) => Uuid::parse_str(s)
                    .map(Value::Uuid)
                    .map_err(|_| Error::InvalidValue(format!("Failed to parse UUID: {}", s))),
                Value::Uuid(u) => Ok(Value::Uuid(*u)),
                _ => Err(Error::InvalidValue(format!(
                    "Cannot cast {} to UUID",
                    value.data_type()
                ))),
            }
        }

        "BOOLEAN" | "BOOL" => match value {
            Value::Bool(b) => Ok(Value::Bool(*b)),
            Value::I8(v) => Ok(Value::Bool(*v != 0)),
            Value::I16(v) => Ok(Value::Bool(*v != 0)),
            Value::I32(v) => Ok(Value::Bool(*v != 0)),
            Value::I64(v) => Ok(Value::Bool(*v != 0)),
            Value::Str(s) => match s.to_lowercase().as_str() {
                "true" | "t" | "1" => Ok(Value::Bool(true)),
                "false" | "f" | "0" => Ok(Value::Bool(false)),
                _ => Err(Error::InvalidValue(format!(
                    "Cannot cast '{}' to BOOLEAN",
                    s
                ))),
            },
            _ => Err(Error::TypeMismatch {
                expected: "castable to boolean".into(),
                found: value.data_type().to_string(),
            }),
        },

        _ => {
            // Check if it's an array type like INT[3] or FLOAT[3]
            if target_type.contains('[') && target_type.contains(']') {
                // Parse array type specification
                let bracket_pos = target_type.find('[').unwrap();
                let element_type_str = &target_type[..bracket_pos];
                let size_str = &target_type[bracket_pos + 1..target_type.len() - 1];
                let expected_size: Option<usize> = if size_str.is_empty() {
                    None
                } else {
                    Some(size_str.parse().map_err(|_| {
                        Error::ExecutionError(format!("Invalid array size: {}", size_str))
                    })?)
                };

                // Parse the input as a JSON array
                let array_value = match value {
                    Value::Str(s) => Value::parse_json_array(s)?,
                    Value::List(_) => value.clone(),
                    _ => {
                        return Err(Error::TypeMismatch {
                            expected: "string or list for array cast".into(),
                            found: value.data_type().to_string(),
                        });
                    }
                };

                // Check array size if specified
                if let Value::List(items) = &array_value {
                    if let Some(expected) = expected_size
                        && items.len() != expected
                    {
                        return Err(Error::TypeMismatch {
                            expected: format!("array with {} elements", expected),
                            found: format!("array with {} elements", items.len()),
                        });
                    }

                    // Cast each element to the target type
                    let mut casted_items = Vec::new();
                    for item in items {
                        // Recursively cast each element
                        let casted = cast_value(item, element_type_str)?;
                        casted_items.push(casted);
                    }

                    // Return Array if size is specified, List otherwise
                    if expected_size.is_some() {
                        Ok(Value::Array(casted_items))
                    } else {
                        Ok(Value::List(casted_items))
                    }
                } else {
                    Err(Error::ExecutionError("Failed to parse as array".into()))
                }
            } else {
                // For other complex types, return an error
                Err(Error::ExecutionError(format!(
                    "Unsupported cast target type: {}",
                    target_type
                )))
            }
        }
    }
}

/// Register the CAST function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(CastFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_cast_to_int() {
        let func = CastFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // String to int
        let result = func
            .execute(&[Value::string("42"), Value::string("INT")], &context)
            .unwrap();
        assert_eq!(result, Value::I32(42));

        // Float to int (truncates)
        let result = func
            .execute(&[Value::F64(42.7), Value::string("INTEGER")], &context)
            .unwrap();
        assert_eq!(result, Value::I32(42));

        // Invalid string
        assert!(
            func.execute(
                &[Value::string("not a number"), Value::string("INT")],
                &context
            )
            .is_err()
        );
    }

    #[test]
    fn test_cast_to_string() {
        let func = CastFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // Int to string
        let result = func
            .execute(&[Value::I32(42), Value::string("TEXT")], &context)
            .unwrap();
        assert_eq!(result, Value::string("42"));

        // Bool to string
        let result = func
            .execute(&[Value::Bool(true), Value::string("VARCHAR")], &context)
            .unwrap();
        assert_eq!(result, Value::string("true"));
    }

    #[test]
    fn test_cast_to_boolean() {
        let func = CastFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // String to bool
        let result = func
            .execute(&[Value::string("true"), Value::string("BOOLEAN")], &context)
            .unwrap();
        assert_eq!(result, Value::Bool(true));

        // Int to bool
        let result = func
            .execute(&[Value::I32(0), Value::string("BOOL")], &context)
            .unwrap();
        assert_eq!(result, Value::Bool(false));

        let result = func
            .execute(&[Value::I32(1), Value::string("BOOL")], &context)
            .unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_cast_null() {
        let func = CastFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // NULL casts to NULL
        let result = func
            .execute(&[Value::Null, Value::string("INT")], &context)
            .unwrap();
        assert_eq!(result, Value::Null);
    }
}
