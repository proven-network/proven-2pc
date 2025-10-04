//! IS_EMPTY function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// IS_EMPTY function
pub struct IsEmptyFunction;

impl Function for IsEmptyFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "IS_EMPTY",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(
                "IS_EMPTY takes exactly 1 argument".into(),
            ));
        }
        Ok(DataType::Bool)
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "IS_EMPTY takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Str(s) => Ok(Value::Bool(s.is_empty())),
            Value::Array(a) | Value::List(a) => Ok(Value::Bool(a.is_empty())),
            Value::Map(m) => Ok(Value::Bool(m.is_empty())),
            Value::Struct(fields) => Ok(Value::Bool(fields.is_empty())),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "string or collection".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(IsEmptyFunction));
}
