//! SORT function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// SORT function
pub struct SortFunction;

impl Function for SortFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "SORT",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return Err(Error::ExecutionError("SORT takes 1 or 2 arguments".into()));
        }
        Ok(DataType::List(Box::new(DataType::Nullable(Box::new(
            DataType::Text,
        )))))
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::ExecutionError(
                "SORT takes 1 or 2 arguments (list, [order])".into(),
            ));
        }

        let order = if args.len() == 2 {
            match &args[1] {
                Value::Str(s) => s.to_uppercase(),
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "string ('ASC' or 'DESC')".into(),
                        found: args[1].data_type().to_string(),
                    });
                }
            }
        } else {
            "ASC".to_string()
        };

        match &args[0] {
            Value::List(l) | Value::Array(l) => {
                let mut sorted = l.clone();
                if order == "DESC" {
                    sorted.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
                } else {
                    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                }
                Ok(Value::List(sorted))
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
    registry.register(Box::new(SortFunction));
}
