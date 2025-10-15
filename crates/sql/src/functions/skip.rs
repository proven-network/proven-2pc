//! SKIP function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

/// SKIP function
pub struct SkipFunction;

impl Function for SkipFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "SKIP",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(
                "SKIP takes exactly 2 arguments".into(),
            ));
        }
        Ok(DataType::List(Box::new(DataType::Nullable(Box::new(
            DataType::Text,
        )))))
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "SKIP takes exactly 2 arguments (list, count)".into(),
            ));
        }

        // Handle NULL count - SQL standard: NULL in -> NULL out
        if matches!(args[1], Value::Null) {
            return Ok(Value::Null);
        }

        let count = match &args[1] {
            Value::I32(i) => {
                if *i < 0 {
                    return Err(Error::ExecutionError(
                        "SKIP count must be non-negative".into(),
                    ));
                }
                *i as usize
            }
            Value::I64(i) => {
                if *i < 0 {
                    return Err(Error::ExecutionError(
                        "SKIP count must be non-negative".into(),
                    ));
                }
                *i as usize
            }
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "integer".into(),
                    found: args[1].data_type().to_string(),
                });
            }
        };

        match &args[0] {
            Value::List(l) | Value::Array(l) => {
                let skip_count = count.min(l.len());
                Ok(Value::List(l[skip_count..].to_vec()))
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
    registry.register(Box::new(SkipFunction));
}
