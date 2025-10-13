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

        let start = match &args[1] {
            Value::I32(i) => *i as usize,
            Value::I64(i) => *i as usize,
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "integer".into(),
                    found: args[1].data_type().to_string(),
                });
            }
        };

        match &args[0] {
            Value::List(l) | Value::Array(l) => {
                let end = if args.len() == 3 {
                    match &args[2] {
                        Value::I32(i) => *i as usize,
                        Value::I64(i) => *i as usize,
                        _ => {
                            return Err(Error::TypeMismatch {
                                expected: "integer".into(),
                                found: args[2].data_type().to_string(),
                            });
                        }
                    }
                } else {
                    l.len()
                };

                let start = start.min(l.len());
                let end = end.min(l.len());
                Ok(Value::List(l[start..end].to_vec()))
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
