//! PREPEND function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

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

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "PREPEND takes exactly 2 arguments (element, list)".into(),
            ));
        }

        match &args[1] {
            Value::List(l) => {
                // Infer element type from existing list elements
                let element_type = if let Some(first) = l.first() {
                    first.data_type()
                } else {
                    // Empty list - use the element's type as-is
                    args[0].data_type()
                };

                // Coerce the new element to match the list's element type
                let coerced_element =
                    crate::coercion::coerce_value(args[0].clone(), &element_type)?;
                let mut new_list = vec![coerced_element];
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

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    fn test_context() -> ExecutionContext {
        ExecutionContext::new(HlcTimestamp::new(0, 0, NodeId::new(1)), 0)
    }

    #[test]
    fn test_prepend_integer_to_integer_list() {
        let func = PrependFunction;
        let element = Value::I64(0);
        let list = Value::List(vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
        let ctx = test_context();

        let result = func.execute(&[element, list], &ctx).unwrap();
        match result {
            Value::List(v) => {
                assert_eq!(v.len(), 4);
                assert_eq!(v[0], Value::I64(0));
                assert_eq!(v[1], Value::I64(1));
            }
            _ => panic!("Expected List"),
        }
    }

    #[test]
    fn test_prepend_coercible_type() {
        let func = PrependFunction;
        // I32 should coerce to I64
        let element = Value::I32(0);
        let list = Value::List(vec![Value::I64(1), Value::I64(2)]);
        let ctx = test_context();

        let result = func.execute(&[element, list], &ctx).unwrap();
        match result {
            Value::List(v) => {
                assert_eq!(v.len(), 3);
                assert_eq!(v[0], Value::I64(0)); // Should be coerced to I64
            }
            _ => panic!("Expected List"),
        }
    }

    #[test]
    fn test_prepend_incompatible_type_should_error() {
        let func = PrependFunction;
        // TEXT cannot coerce to INTEGER
        let element = Value::Str("hello".to_string());
        let list = Value::List(vec![Value::I64(1), Value::I64(2)]);
        let ctx = test_context();

        let result = func.execute(&[element, list], &ctx);
        assert!(
            result.is_err(),
            "Should error when prepending incompatible type"
        );
    }

    #[test]
    fn test_prepend_to_empty_list() {
        let func = PrependFunction;
        let element = Value::I64(1);
        let list = Value::List(vec![]);
        let ctx = test_context();

        let result = func.execute(&[element, list], &ctx).unwrap();
        match result {
            Value::List(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0], Value::I64(1));
            }
            _ => panic!("Expected List"),
        }
    }

    #[test]
    fn test_prepend_to_array_should_error() {
        let func = PrependFunction;
        let element = Value::I64(0);
        let array = Value::Array(vec![Value::I64(1), Value::I64(2)]);
        let ctx = test_context();

        let result = func.execute(&[element, array], &ctx);
        assert!(result.is_err(), "Cannot prepend to fixed-size ARRAY");
    }

    #[test]
    fn test_prepend_null_list() {
        let func = PrependFunction;
        let element = Value::I64(1);
        let list = Value::Null;
        let ctx = test_context();

        let result = func.execute(&[element, list], &ctx).unwrap();
        assert_eq!(result, Value::Null);
    }
}
