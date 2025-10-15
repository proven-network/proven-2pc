//! VALUES and MAP_VALUES functions

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

/// VALUES function
pub struct ValuesFunction;

impl Function for ValuesFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "VALUES",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(
                "VALUES takes exactly 1 argument".into(),
            ));
        }
        // Validate that the argument is a map type
        match &arg_types[0] {
            DataType::Map(_key_type, value_type) => {
                // Return a list of the map's value type
                Ok(DataType::List(value_type.clone()))
            }
            DataType::Nullable(inner) => {
                if let DataType::Map(_key_type, value_type) = inner.as_ref() {
                    // Return a nullable list of the map's value type
                    Ok(DataType::List(value_type.clone()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "map".into(),
                        found: arg_types[0].to_string(),
                    })
                }
            }
            _ => Err(Error::TypeMismatch {
                expected: "map".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "VALUES takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Map(m) => {
                let values: Vec<Value> = m.values().cloned().collect();
                Ok(Value::List(values))
            }
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "map".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// MAP_VALUES function - alias for VALUES
pub struct MapValuesFunction;

impl Function for MapValuesFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "MAP_VALUES",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        ValuesFunction.validate(arg_types)
    }

    fn execute(&self, args: &[Value], context: &ExecutionContext) -> Result<Value> {
        ValuesFunction.execute(args, context)
    }
}

/// Register the functions
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(ValuesFunction));
    registry.register(Box::new(MapValuesFunction));
}
