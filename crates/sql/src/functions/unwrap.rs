//! UNWRAP function - path-based nested access

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// UNWRAP function
pub struct UnwrapFunction;

impl Function for UnwrapFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "UNWRAP",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(
                "UNWRAP takes exactly 2 arguments (collection, path)".into(),
            ));
        }
        Ok(DataType::Nullable(Box::new(DataType::Text)))
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "UNWRAP takes exactly 2 arguments (collection, path)".into(),
            ));
        }

        let path = match &args[1] {
            Value::Str(s) => s,
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "string path".into(),
                    found: args[1].data_type().to_string(),
                });
            }
        };

        unwrap_value(&args[0], path)
    }
}

/// Helper function for UNWRAP - navigates through nested structures using a path
fn unwrap_value(value: &Value, path: &str) -> Result<Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value.clone();

    for part in parts {
        current = if let Ok(index) = part.parse::<usize>() {
            // Numeric index for array/list
            match current {
                Value::Array(arr) | Value::List(arr) => {
                    arr.get(index).cloned().unwrap_or(Value::Null)
                }
                _ => return Ok(Value::Null),
            }
        } else {
            // String key for map/struct or field name
            match current {
                Value::Map(map) => map.get(part).cloned().unwrap_or(Value::Null),
                Value::Struct(fields) => fields
                    .iter()
                    .find(|(name, _)| name == part)
                    .map(|(_, val)| val.clone())
                    .unwrap_or(Value::Null),
                _ => return Ok(Value::Null),
            }
        };
    }

    Ok(current)
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(UnwrapFunction));
}
