//! DISTINCT function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// DISTINCT function
pub struct DistinctFunction;

impl Function for DistinctFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "DISTINCT",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(
                "DISTINCT takes exactly 1 argument".into(),
            ));
        }
        Ok(DataType::List(Box::new(DataType::Nullable(Box::new(
            DataType::Text,
        )))))
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "DISTINCT takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::List(l) | Value::Array(l) => {
                let mut seen = Vec::new();
                for elem in l {
                    if !seen.contains(elem) {
                        seen.push(elem.clone());
                    }
                }
                Ok(Value::List(seen))
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
    registry.register(Box::new(DistinctFunction));
}
