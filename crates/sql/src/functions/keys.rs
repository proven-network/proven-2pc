//! KEYS and MAP_KEYS functions

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// KEYS function
pub struct KeysFunction;

impl Function for KeysFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "KEYS",
            min_args: 1,
            max_args: Some(1),
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(
                "KEYS takes exactly 1 argument".into(),
            ));
        }
        Ok(DataType::List(Box::new(DataType::Text)))
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "KEYS takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Map(m) => {
                let keys: Vec<Value> = m.keys().map(|k| Value::Str(k.clone())).collect();
                Ok(Value::List(keys))
            }
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "map".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// MAP_KEYS function - alias for KEYS
pub struct MapKeysFunction;

impl Function for MapKeysFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "MAP_KEYS",
            min_args: 1,
            max_args: Some(1),
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        KeysFunction.validate(arg_types)
    }

    fn execute(&self, args: &[Value], context: &TransactionContext) -> Result<Value> {
        KeysFunction.execute(args, context)
    }
}

/// Register the functions
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(KeysFunction));
    registry.register(Box::new(MapKeysFunction));
}
