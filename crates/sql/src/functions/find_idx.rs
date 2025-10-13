//! FIND_IDX function
//! Returns the index of a substring in a string, or an element in a list/array

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

/// FIND_IDX function
pub struct FindIdxFunction;

impl Function for FindIdxFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "FIND_IDX",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        // FIND_IDX(string, substring) or FIND_IDX(string, substring, offset)
        // FIND_IDX(list, element)
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return Err(Error::ExecutionError(format!(
                "FIND_IDX takes 2 or 3 arguments, got {}",
                arg_types.len()
            )));
        }

        // First argument: string, list, or array (can be nullable)
        let base_type = match &arg_types[0] {
            DataType::Nullable(inner) => inner.as_ref(),
            other => other,
        };

        match base_type {
            DataType::Text | DataType::Str => {
                // String mode: second arg must be string (or nullable string, or NULL literal)
                // Accept NULL literal or string types
                if !matches!(
                    arg_types[1],
                    DataType::Null | DataType::Text | DataType::Str | DataType::Nullable(_)
                ) {
                    return Err(Error::TypeMismatch {
                        expected: "string type for substring".into(),
                        found: arg_types[1].to_string(),
                    });
                }

                // If not NULL, check that nullable contains string
                if let DataType::Nullable(inner) = &arg_types[1]
                    && !matches!(
                        inner.as_ref(),
                        DataType::Text | DataType::Str | DataType::Null
                    )
                {
                    return Err(Error::TypeMismatch {
                        expected: "string type for substring".into(),
                        found: arg_types[1].to_string(),
                    });
                }

                // If third argument exists, it must be integer (offset)
                if arg_types.len() == 3
                    && !matches!(
                        arg_types[2],
                        DataType::I8
                            | DataType::I16
                            | DataType::I32
                            | DataType::I64
                            | DataType::U8
                            | DataType::U16
                            | DataType::U32
                            | DataType::U64
                    )
                {
                    return Err(Error::TypeMismatch {
                        expected: "integer type for offset".into(),
                        found: arg_types[2].to_string(),
                    });
                }
            }
            DataType::List(_) | DataType::Array { .. } => {
                // List/array mode: only 2 arguments allowed
                if arg_types.len() != 2 {
                    return Err(Error::ExecutionError(
                        "FIND_IDX on list/array takes exactly 2 arguments".into(),
                    ));
                }
            }
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "string, list, or array".into(),
                    found: arg_types[0].to_string(),
                });
            }
        }

        Ok(DataType::I64)
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() < 2 || args.len() > 3 {
            return Err(Error::ExecutionError(
                "FIND_IDX takes 2 or 3 arguments".into(),
            ));
        }

        // Handle NULL in first argument
        if matches!(args[0], Value::Null) {
            return Ok(Value::Null);
        }

        match &args[0] {
            // String mode
            Value::Str(s) => {
                // Handle NULL substring
                if matches!(args[1], Value::Null) {
                    return Ok(Value::Null);
                }

                // Get substring to search for
                let substring = match &args[1] {
                    Value::Str(sub) => sub.as_str(),
                    _ => {
                        return Err(Error::TypeMismatch {
                            expected: "string".into(),
                            found: args[1].data_type().to_string(),
                        });
                    }
                };

                // Get optional offset (1-based in SQL)
                let start_pos = if args.len() == 3 {
                    let offset_value = match &args[2] {
                        Value::I8(v) => *v as i64,
                        Value::I16(v) => *v as i64,
                        Value::I32(v) => *v as i64,
                        Value::I64(v) => *v,
                        Value::U8(v) => *v as i64,
                        Value::U16(v) => *v as i64,
                        Value::U32(v) => *v as i64,
                        Value::U64(v) => *v as i64,
                        _ => {
                            return Err(Error::TypeMismatch {
                                expected: "integer".into(),
                                found: args[2].data_type().to_string(),
                            });
                        }
                    };

                    // Validate offset is positive (must be >= 1 for 1-based indexing)
                    if offset_value < 1 {
                        return Err(Error::InvalidValue(format!(
                            "FIND_IDX offset must be >= 1, got {}",
                            offset_value
                        )));
                    }

                    offset_value as usize
                } else {
                    1 // Default start position is 1 (first character)
                };

                // Empty substring always found at the start position (0-indexed)
                if substring.is_empty() {
                    return Ok(Value::I64(0));
                }

                // Convert string to bytes for indexing
                // We need to work with byte positions for efficiency
                let bytes = s.as_bytes();

                // Convert 1-based start_pos to 0-based byte offset
                // For now, assume ASCII/single-byte chars (like GlueSQL does)
                let search_start = if start_pos > 1 {
                    (start_pos - 1).min(bytes.len())
                } else {
                    0
                };

                // Search for substring starting at search_start
                if search_start >= bytes.len() {
                    // Start position beyond string length, not found
                    return Ok(Value::I64(0));
                }

                let search_slice = &s[search_start..];

                if let Some(pos) = search_slice.find(substring) {
                    // Return 1-based index: search_start (0-based) + pos (0-based from search_start) + 1
                    Ok(Value::I64((search_start + pos + 1) as i64))
                } else {
                    // Not found, return 0
                    Ok(Value::I64(0))
                }
            }
            // List/array mode
            Value::List(l) | Value::Array(l) => {
                for (i, elem) in l.iter().enumerate() {
                    if elem == &args[1] {
                        return Ok(Value::I64(i as i64));
                    }
                }
                Ok(Value::Null) // Not found
            }
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "string, list, or array".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(FindIdxFunction));
}
