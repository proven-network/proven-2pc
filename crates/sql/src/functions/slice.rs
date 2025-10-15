//! SLICE function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

/// SLICE function
pub struct SliceFunction;

impl Function for SliceFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "SLICE",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return Err(Error::ExecutionError("SLICE takes 2 or 3 arguments".into()));
        }
        Ok(DataType::List(Box::new(DataType::Nullable(Box::new(
            DataType::Text,
        )))))
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() < 2 || args.len() > 3 {
            return Err(Error::ExecutionError(
                "SLICE takes 2 or 3 arguments (list, start, [end])".into(),
            ));
        }

        // Get start index as signed integer (supports negative indices)
        let start_idx = match &args[1] {
            Value::I32(i) => *i as i64,
            Value::I64(i) => *i,
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "integer".into(),
                    found: args[1].data_type().to_string(),
                });
            }
        };

        match &args[0] {
            Value::List(l) | Value::Array(l) => {
                let len = l.len() as i64;

                // DuckDB-style negative index handling for start
                // When start < 0: count from end
                // When start < -length: clamp to 0
                let actual_start = if start_idx < 0 {
                    (len + start_idx).max(0) as usize
                } else {
                    (start_idx as usize).min(l.len())
                };

                // Get end index
                let actual_end = if args.len() == 3 {
                    let end_idx = match &args[2] {
                        Value::I32(i) => *i as i64,
                        Value::I64(i) => *i,
                        _ => {
                            return Err(Error::TypeMismatch {
                                expected: "integer".into(),
                                found: args[2].data_type().to_string(),
                            });
                        }
                    };

                    // DuckDB-style negative index handling for end
                    if end_idx < 0 {
                        (len + end_idx).max(0) as usize
                    } else {
                        (end_idx as usize).min(l.len())
                    }
                } else {
                    l.len()
                };

                // Ensure start <= end
                if actual_start > actual_end {
                    Ok(Value::List(Vec::new()))
                } else {
                    Ok(Value::List(l[actual_start..actual_end].to_vec()))
                }
            }
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "list or array".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(SliceFunction));
}
