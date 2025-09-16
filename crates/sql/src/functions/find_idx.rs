//! FIND_IDX function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// FIND_IDX function
pub struct FindIdxFunction;

impl Function for FindIdxFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "FIND_IDX",
            min_args: 2,
            max_args: Some(2),
            arg_types: vec![],
            is_deterministic: true,
            is_aggregate: false,
            description: "Finds the index of an element in a list",
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(
                "FIND_IDX takes exactly 2 arguments".into(),
            ));
        }
        Ok(DataType::Nullable(Box::new(DataType::I64)))
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "FIND_IDX takes exactly 2 arguments (list, element)".into(),
            ));
        }

        match &args[0] {
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
                expected: "list or array".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(FindIdxFunction));
}
