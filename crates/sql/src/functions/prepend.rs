//! PREPEND function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// PREPEND function
pub struct PrependFunction;

impl Function for PrependFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "PREPEND",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(
                "PREPEND takes exactly 2 arguments".into(),
            ));
        }
        Ok(DataType::List(Box::new(DataType::Nullable(Box::new(
            DataType::Text,
        )))))
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "PREPEND takes exactly 2 arguments (element, list)".into(),
            ));
        }

        match &args[1] {
            Value::List(l) => {
                let mut new_list = vec![args[0].clone()];
                new_list.extend_from_slice(l);
                Ok(Value::List(new_list))
            }
            Value::Array(_) => Err(Error::ExecutionError(
                "Cannot PREPEND to fixed-size ARRAY, use LIST instead".into(),
            )),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "list".into(),
                found: args[1].data_type().to_string(),
            }),
        }
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(PrependFunction));
}
