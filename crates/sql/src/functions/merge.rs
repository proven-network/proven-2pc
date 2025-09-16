//! MERGE function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;
use std::collections::HashMap;

/// MERGE function
pub struct MergeFunction;

impl Function for MergeFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "MERGE",
            min_args: 1,
            max_args: None, // Variadic
            arg_types: vec![],
            is_deterministic: true,
            is_aggregate: false,
            description: "Merges multiple maps",
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return Err(Error::ExecutionError(
                "MERGE requires at least one argument".into(),
            ));
        }
        Ok(DataType::Map(
            Box::new(DataType::Text),
            Box::new(DataType::Nullable(Box::new(DataType::Text))),
        ))
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::ExecutionError(
                "MERGE requires at least one argument".into(),
            ));
        }

        let mut result = HashMap::new();

        for arg in args {
            match arg {
                Value::Map(m) => {
                    for (k, v) in m {
                        result.insert(k.clone(), v.clone());
                    }
                }
                Value::Null => {} // Skip nulls
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "map".into(),
                        found: arg.data_type().to_string(),
                    });
                }
            }
        }

        Ok(Value::Map(result))
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(MergeFunction));
}
