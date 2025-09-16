//! EXTRACT function - universal accessor for collections

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// EXTRACT function
pub struct ExtractFunction;

impl Function for ExtractFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "EXTRACT",
            min_args: 2,
            max_args: Some(2),
            arg_types: vec![],
            is_deterministic: true,
            is_aggregate: false,
            description: "Universal accessor for collections",
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(
                "EXTRACT takes exactly 2 arguments".into(),
            ));
        }
        Ok(DataType::Nullable(Box::new(DataType::Text)))
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "EXTRACT takes exactly 2 arguments".into(),
            ));
        }

        match (&args[0], &args[1]) {
            (Value::Array(a) | Value::List(a), Value::I32(idx)) => {
                Ok(a.get(*idx as usize).cloned().unwrap_or(Value::Null))
            }
            (Value::Array(a) | Value::List(a), Value::I64(idx)) => {
                Ok(a.get(*idx as usize).cloned().unwrap_or(Value::Null))
            }
            (Value::Map(m), Value::Str(key)) => {
                Ok(m.get(key).cloned().unwrap_or(Value::Null))
            }
            (Value::Struct(fields), Value::Str(field)) => {
                Ok(fields
                    .iter()
                    .find(|(name, _)| name == field)
                    .map(|(_, val)| val.clone())
                    .unwrap_or(Value::Null))
            }
            (Value::Null, _) => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "collection with appropriate accessor".into(),
                found: format!("{} and {}", args[0].data_type(), args[1].data_type()),
            }),
        }
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(ExtractFunction));
}