//! VALUES and MAP_VALUES functions

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// VALUES function
pub struct ValuesFunction;

impl Function for ValuesFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "VALUES",
            min_args: 1,
            max_args: Some(1),
            arg_types: vec![],
            is_deterministic: true,
            is_aggregate: false,
            description: "Returns all values from a map as a list",
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(
                "VALUES takes exactly 1 argument".into(),
            ));
        }
        Ok(DataType::List(Box::new(DataType::Nullable(Box::new(DataType::Text)))))
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
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
            min_args: 1,
            max_args: Some(1),
            arg_types: vec![],
            is_deterministic: true,
            is_aggregate: false,
            description: "Returns all values from a map as a list (alias for VALUES)",
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        ValuesFunction.validate(arg_types)
    }

    fn execute(&self, args: &[Value], context: &TransactionContext) -> Result<Value> {
        ValuesFunction.execute(args, context)
    }
}

/// Register the functions
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(ValuesFunction));
    registry.register(Box::new(MapValuesFunction));
}