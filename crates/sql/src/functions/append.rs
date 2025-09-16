//! APPEND function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// APPEND function
pub struct AppendFunction;

impl Function for AppendFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "APPEND",
            min_args: 2,
            max_args: Some(2),
            arg_types: vec![],
            is_deterministic: true,
            is_aggregate: false,
            description: "Adds an element to the end of a list",
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(
                "APPEND takes exactly 2 arguments".into(),
            ));
        }
        Ok(DataType::List(Box::new(DataType::Nullable(Box::new(
            DataType::Text,
        )))))
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "APPEND takes exactly 2 arguments (list, element)".into(),
            ));
        }

        match &args[0] {
            Value::List(l) => {
                let mut new_list = l.clone();
                new_list.push(args[1].clone());
                Ok(Value::List(new_list))
            }
            Value::Array(_) => Err(Error::ExecutionError(
                "Cannot APPEND to fixed-size ARRAY, use LIST instead".into(),
            )),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "list".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(AppendFunction));
}
