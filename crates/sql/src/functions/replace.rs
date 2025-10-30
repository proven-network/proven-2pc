//! REPLACE function - replaces occurrences of a substring

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

pub struct ReplaceFunction;

impl Function for ReplaceFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "REPLACE",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        // REPLACE(string, search, replacement)
        if arg_types.len() != 3 {
            return Err(Error::ExecutionError(format!(
                "REPLACE takes exactly 3 arguments, got {}",
                arg_types.len()
            )));
        }

        // All arguments must be strings
        for (i, arg_type) in arg_types.iter().enumerate() {
            match arg_type {
                DataType::Text | DataType::Str => {}
                DataType::Nullable(inner) => match inner.as_ref() {
                    DataType::Text | DataType::Str => {
                        // If any argument is nullable, result is nullable
                        if i == 0 {
                            return Ok(DataType::Nullable(Box::new(DataType::Text)));
                        }
                    }
                    _ => {
                        return Err(Error::TypeMismatch {
                            expected: "string type".into(),
                            found: arg_type.to_string(),
                        });
                    }
                },
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "string type".into(),
                        found: arg_type.to_string(),
                    });
                }
            }
        }

        // Check if first argument is nullable
        if matches!(arg_types[0], DataType::Nullable(_)) {
            Ok(DataType::Nullable(Box::new(DataType::Text)))
        } else {
            Ok(DataType::Text)
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 3 {
            return Err(Error::ExecutionError(
                "REPLACE takes exactly 3 arguments".into(),
            ));
        }

        // Handle NULL inputs - if any argument is NULL, return NULL
        if args.iter().any(|v| matches!(v, Value::Null)) {
            return Ok(Value::Null);
        }

        // Get string values
        let text = match &args[0] {
            Value::Str(s) => s.as_str(),
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "string".into(),
                    found: args[0].data_type().to_string(),
                });
            }
        };

        let search = match &args[1] {
            Value::Str(s) => s.as_str(),
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "string".into(),
                    found: args[1].data_type().to_string(),
                });
            }
        };

        let replacement = match &args[2] {
            Value::Str(s) => s.as_str(),
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "string".into(),
                    found: args[2].data_type().to_string(),
                });
            }
        };

        // Perform replacement
        // If search string is empty, return original text (SQL standard behavior)
        let result = if search.is_empty() {
            text.to_string()
        } else {
            text.replace(search, replacement)
        };
        Ok(Value::string(result))
    }
}

/// Register the REPLACE function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(ReplaceFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_common::TransactionId;
    use uuid::Uuid;

    #[test]
    fn test_replace_signature() {
        let func = ReplaceFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "REPLACE");
        assert!(!sig.is_aggregate);
    }

    #[test]
    fn test_replace_validate() {
        let func = ReplaceFunction;

        // Valid call
        assert_eq!(
            func.validate(&[DataType::Text, DataType::Text, DataType::Text])
                .unwrap(),
            DataType::Text
        );

        // Nullable first argument
        assert_eq!(
            func.validate(&[
                DataType::Nullable(Box::new(DataType::Text)),
                DataType::Text,
                DataType::Text
            ])
            .unwrap(),
            DataType::Nullable(Box::new(DataType::Text))
        );

        // Wrong number of arguments
        assert!(func.validate(&[DataType::Text]).is_err());
        assert!(func.validate(&[DataType::Text, DataType::Text]).is_err());

        // Wrong types
        assert!(
            func.validate(&[DataType::I32, DataType::Text, DataType::Text])
                .is_err()
        );
    }

    #[test]
    fn test_replace_execute() {
        let func = ReplaceFunction;
        let context = ExecutionContext::new(TransactionId::from_uuid(Uuid::from_u128(0)), 0);

        // Basic replacement
        let result = func
            .execute(
                &[
                    Value::string("hello world"),
                    Value::string("world"),
                    Value::string("rust"),
                ],
                &context,
            )
            .unwrap();
        assert_eq!(result, Value::string("hello rust"));

        // Multiple occurrences
        let result = func
            .execute(
                &[
                    Value::string("foo bar foo baz"),
                    Value::string("foo"),
                    Value::string("test"),
                ],
                &context,
            )
            .unwrap();
        assert_eq!(result, Value::string("test bar test baz"));

        // No match
        let result = func
            .execute(
                &[
                    Value::string("hello world"),
                    Value::string("xyz"),
                    Value::string("abc"),
                ],
                &context,
            )
            .unwrap();
        assert_eq!(result, Value::string("hello world"));

        // Empty search string (replaces nothing)
        let result = func
            .execute(
                &[
                    Value::string("hello"),
                    Value::string(""),
                    Value::string("X"),
                ],
                &context,
            )
            .unwrap();
        assert_eq!(result, Value::string("hello"));

        // Replace with empty string (removes occurrences)
        let result = func
            .execute(
                &[
                    Value::string("hello world"),
                    Value::string("world"),
                    Value::string(""),
                ],
                &context,
            )
            .unwrap();
        assert_eq!(result, Value::string("hello "));

        // NULL handling
        let result = func
            .execute(
                &[
                    Value::Null,
                    Value::string("search"),
                    Value::string("replace"),
                ],
                &context,
            )
            .unwrap();
        assert_eq!(result, Value::Null);
    }
}
