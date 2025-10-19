//! APPEND function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::coercion::coerce_value;
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

/// APPEND function
pub struct AppendFunction;

impl Function for AppendFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "APPEND",
            is_aggregate: false,
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

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "APPEND takes exactly 2 arguments (list, element)".into(),
            ));
        }

        match &args[0] {
            Value::List(l) => {
                let mut new_list = l.clone();

                // Infer element type from existing list elements
                let element_type = if let Some(first) = l.first() {
                    first.data_type()
                } else {
                    // Empty list - use the element's type as-is
                    args[1].data_type()
                };

                // Coerce the new element to match the list's element type
                let coerced_element = coerce_value(args[1].clone(), &element_type)?;
                new_list.push(coerced_element);
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

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    fn test_context() -> ExecutionContext {
        ExecutionContext::new(HlcTimestamp::new(0, 0, NodeId::new(1)), 0)
    }

    #[test]
    fn test_append_integer_to_integer_list() {
        let func = AppendFunction;
        let list = Value::List(vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
        let element = Value::I64(4);
        let ctx = test_context();

        let result = func.execute(&[list, element], &ctx).unwrap();
        match result {
            Value::List(v) => {
                assert_eq!(v.len(), 4);
                assert_eq!(v[3], Value::I64(4));
            }
            _ => panic!("Expected List"),
        }
    }

    #[test]
    fn test_append_coercible_type() {
        let func = AppendFunction;
        // I32 should coerce to I64
        let list = Value::List(vec![Value::I64(1), Value::I64(2)]);
        let element = Value::I32(3);
        let ctx = test_context();

        let result = func.execute(&[list, element], &ctx).unwrap();
        match result {
            Value::List(v) => {
                assert_eq!(v.len(), 3);
                assert_eq!(v[2], Value::I64(3)); // Should be coerced to I64
            }
            _ => panic!("Expected List"),
        }
    }

    #[test]
    fn test_append_incompatible_type_should_error() {
        let func = AppendFunction;
        // TEXT cannot coerce to INTEGER
        let list = Value::List(vec![Value::I64(1), Value::I64(2)]);
        let element = Value::Str("hello".to_string());
        let ctx = test_context();

        let result = func.execute(&[list, element], &ctx);
        assert!(
            result.is_err(),
            "Should error when appending incompatible type"
        );
    }

    #[test]
    fn test_append_to_empty_list() {
        let func = AppendFunction;
        let list = Value::List(vec![]);
        let element = Value::I64(1);
        let ctx = test_context();

        let result = func.execute(&[list, element], &ctx).unwrap();
        match result {
            Value::List(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0], Value::I64(1));
            }
            _ => panic!("Expected List"),
        }
    }

    #[test]
    fn test_append_to_array_should_error() {
        let func = AppendFunction;
        let array = Value::Array(vec![Value::I64(1), Value::I64(2)]);
        let element = Value::I64(3);
        let ctx = test_context();

        let result = func.execute(&[array, element], &ctx);
        assert!(result.is_err(), "Cannot append to fixed-size ARRAY");
    }

    #[test]
    fn test_append_null_list() {
        let func = AppendFunction;
        let list = Value::Null;
        let element = Value::I64(1);
        let ctx = test_context();

        let result = func.execute(&[list, element], &ctx).unwrap();
        assert_eq!(result, Value::Null);
    }
}
