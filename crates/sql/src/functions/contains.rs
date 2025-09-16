//! CONTAINS function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// CONTAINS function
pub struct ContainsFunction;

impl Function for ContainsFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "CONTAINS",
            min_args: 2,
            max_args: Some(2),
            arg_types: vec![],
            is_deterministic: true,
            is_aggregate: false,
            description: "Checks if element/key exists in collection",
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(
                "CONTAINS takes exactly 2 arguments".into(),
            ));
        }
        Ok(DataType::Bool)
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "CONTAINS takes exactly 2 arguments".into(),
            ));
        }

        match (&args[0], &args[1]) {
            (Value::Array(a) | Value::List(a), elem) => Ok(Value::Bool(a.contains(elem))),
            (Value::Map(m), Value::Str(key)) => Ok(Value::Bool(m.contains_key(key))),
            (Value::Str(s), Value::Str(substr)) => Ok(Value::Bool(s.contains(substr.as_str()))),
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "collection with element or map with string key".into(),
                found: format!("{} and {}", args[0].data_type(), args[1].data_type()),
            }),
        }
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(ContainsFunction));
}
