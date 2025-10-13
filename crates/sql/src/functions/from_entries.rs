//! FROM_ENTRIES function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};
use std::collections::HashMap;

/// FROM_ENTRIES function
pub struct FromEntriesFunction;

impl Function for FromEntriesFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "FROM_ENTRIES",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(
                "FROM_ENTRIES takes exactly 1 argument".into(),
            ));
        }

        // Check if argument is a list/array or nullable
        let base_type = match &arg_types[0] {
            DataType::Nullable(inner) => inner.as_ref(),
            other => other,
        };

        match base_type {
            DataType::List(inner) | DataType::Array(inner, _) => {
                // Try to infer the value type from the inner list structure
                // If it's a list of pairs (lists), extract the value type
                let value_type = match inner.as_ref() {
                    DataType::List(pair_inner) | DataType::Array(pair_inner, _) => {
                        // Assume the second element of the pair determines value type
                        // But since we can't know for sure at validation time, use a generic nullable type
                        Box::new(DataType::Nullable(pair_inner.clone()))
                    }
                    _ => {
                        // Can't determine value type, use generic nullable
                        Box::new(DataType::Nullable(Box::new(DataType::Text)))
                    }
                };

                Ok(DataType::Map(Box::new(DataType::Text), value_type))
            }
            DataType::Null => {
                // NULL input returns NULL
                Ok(DataType::Nullable(Box::new(DataType::Map(
                    Box::new(DataType::Text),
                    Box::new(DataType::Nullable(Box::new(DataType::Text))),
                ))))
            }
            _ => Err(Error::TypeMismatch {
                expected: "list or array of [key, value] pairs".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "FROM_ENTRIES takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::List(entries) | Value::Array(entries) => {
                let mut map = HashMap::new();

                for entry in entries {
                    match entry {
                        Value::List(pair) | Value::Array(pair) if pair.len() == 2 => {
                            let key = match &pair[0] {
                                Value::Str(s) => s.clone(),
                                _ => {
                                    return Err(Error::TypeMismatch {
                                        expected: "string key".into(),
                                        found: pair[0].data_type().to_string(),
                                    });
                                }
                            };
                            map.insert(key, pair[1].clone());
                        }
                        _ => {
                            return Err(Error::ExecutionError(
                                "FROM_ENTRIES expects list of [key, value] pairs".into(),
                            ));
                        }
                    }
                }

                Ok(Value::Map(map))
            }
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "list of pairs".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(FromEntriesFunction));
}
