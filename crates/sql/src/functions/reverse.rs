//! REVERSE function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// REVERSE function
pub struct ReverseFunction;

impl Function for ReverseFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "REVERSE",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(
                "REVERSE takes exactly 1 argument".into(),
            ));
        }
        Ok(arg_types[0].clone())
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "REVERSE takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::List(l) | Value::Array(l) => {
                let mut reversed = l.clone();
                reversed.reverse();
                Ok(Value::List(reversed))
            }
            Value::Str(s) => Ok(Value::Str(s.chars().rev().collect())),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "list, array, or string".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(ReverseFunction));
}
