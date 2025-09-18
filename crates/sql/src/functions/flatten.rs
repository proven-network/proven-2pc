//! FLATTEN function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// FLATTEN function
pub struct FlattenFunction;

impl Function for FlattenFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "FLATTEN",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(
                "FLATTEN takes exactly 1 argument".into(),
            ));
        }
        Ok(DataType::List(Box::new(DataType::Nullable(Box::new(
            DataType::Text,
        )))))
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "FLATTEN takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::List(l) | Value::Array(l) => {
                let mut result = Vec::new();
                for elem in l {
                    match elem {
                        Value::List(inner) | Value::Array(inner) => {
                            result.extend_from_slice(inner);
                        }
                        _ => result.push(elem.clone()),
                    }
                }
                Ok(Value::List(result))
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
    registry.register(Box::new(FlattenFunction));
}
