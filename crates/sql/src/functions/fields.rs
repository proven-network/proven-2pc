//! FIELDS function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// FIELDS function
pub struct FieldsFunction;

impl Function for FieldsFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "FIELDS",
            min_args: 1,
            max_args: Some(1),
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(
                "FIELDS takes exactly 1 argument".into(),
            ));
        }
        Ok(DataType::List(Box::new(DataType::Text)))
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "FIELDS takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Struct(fields) => {
                let field_names: Vec<Value> = fields
                    .iter()
                    .map(|(name, _)| Value::Str(name.clone()))
                    .collect();
                Ok(Value::List(field_names))
            }
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "struct".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(FieldsFunction));
}
