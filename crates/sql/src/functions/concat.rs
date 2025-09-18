//! CONCAT function - concatenates strings or lists

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct ConcatFunction;

impl Function for ConcatFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "CONCAT",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return Err(Error::ExecutionError(
                "CONCAT requires at least one argument".into(),
            ));
        }

        // Check first argument to determine return type
        match &arg_types[0] {
            DataType::List(_) => Ok(DataType::List(Box::new(DataType::Nullable(Box::new(
                DataType::Text,
            ))))),
            _ => Ok(DataType::Text),
        }
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::ExecutionError(
                "CONCAT requires at least one argument".into(),
            ));
        }

        // Check first argument type to determine operation
        match &args[0] {
            Value::List(_) | Value::Array(_) => {
                let mut result = Vec::new();
                for arg in args {
                    match arg {
                        Value::List(l) | Value::Array(l) => result.extend_from_slice(l),
                        Value::Null => {} // Skip nulls
                        _ => {
                            return Err(Error::TypeMismatch {
                                expected: "list or array".into(),
                                found: arg.data_type().to_string(),
                            });
                        }
                    }
                }
                Ok(Value::List(result))
            }
            Value::Str(_) => {
                let mut result = String::new();
                let mut has_non_null = false;

                for arg in args {
                    match arg {
                        Value::Str(s) => {
                            result.push_str(s);
                            has_non_null = true;
                        }
                        Value::Null => {
                            // NULL values are treated as empty string in CONCAT
                        }
                        // Convert other types to string
                        v => {
                            result.push_str(&v.to_string());
                            has_non_null = true;
                        }
                    }
                }

                // If all values were NULL, return NULL
                if !has_non_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::string(result))
                }
            }
            Value::Null => Ok(Value::Null),
            _ => {
                // Default to string concatenation
                let mut result = String::new();
                for arg in args {
                    match arg {
                        Value::Null => {}
                        v => result.push_str(&v.to_string()),
                    }
                }
                Ok(Value::string(result))
            }
        }
    }
}

/// Register the CONCAT function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(ConcatFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_concat_signature() {
        let func = ConcatFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "CONCAT");
        assert!(!sig.is_aggregate);
    }

    #[test]
    fn test_concat_execute() {
        let func = ConcatFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // Simple concatenation
        let result = func
            .execute(
                &[
                    Value::string("Hello"),
                    Value::string(" "),
                    Value::string("World"),
                ],
                &context,
            )
            .unwrap();
        assert_eq!(result, Value::string("Hello World"));

        // With NULL values (treated as empty)
        let result = func
            .execute(
                &[Value::string("Hello"), Value::Null, Value::string("World")],
                &context,
            )
            .unwrap();
        assert_eq!(result, Value::string("HelloWorld"));

        // Mixed types
        let result = func
            .execute(&[Value::string("Count: "), Value::I32(42)], &context)
            .unwrap();
        assert_eq!(result, Value::string("Count: 42"));

        // All NULL
        let result = func.execute(&[Value::Null, Value::Null], &context).unwrap();
        assert_eq!(result, Value::Null);
    }
}
