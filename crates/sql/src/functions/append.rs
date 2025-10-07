//! APPEND function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::coercion::coerce_value;
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

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
