//! ENTRIES function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

/// ENTRIES function
pub struct EntriesFunction;

impl Function for EntriesFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "ENTRIES",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(
                "ENTRIES takes exactly 1 argument".into(),
            ));
        }
        Ok(DataType::List(Box::new(DataType::List(Box::new(
            DataType::Nullable(Box::new(DataType::Text)),
        )))))
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "ENTRIES takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Map(m) => {
                let entries: Vec<Value> = m
                    .iter()
                    .map(|(k, v)| Value::List(vec![Value::Str(k.clone()), v.clone()]))
                    .collect();
                Ok(Value::List(entries))
            }
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "map".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(EntriesFunction));
}
