//! LOWER function - converts string to lowercase

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct LowerFunction;

impl Function for LowerFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "LOWER",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "LOWER takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            DataType::Text | DataType::Str => Ok(DataType::Text),
            DataType::Nullable(inner) => match inner.as_ref() {
                DataType::Text | DataType::Str => Ok(DataType::Nullable(Box::new(DataType::Text))),
                _ => Err(Error::TypeMismatch {
                    expected: "string type".into(),
                    found: arg_types[0].to_string(),
                }),
            },
            _ => Err(Error::TypeMismatch {
                expected: "string type".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "LOWER takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Str(s) => Ok(Value::string(s.to_lowercase())),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "string".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the LOWER function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(LowerFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_lower_execute() {
        let func = LowerFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Lowercase conversion
        let result = func
            .execute(&[Value::string("HELLO WORLD")], &context)
            .unwrap();
        assert_eq!(result, Value::string("hello world"));

        // Mixed case
        let result = func
            .execute(&[Value::string("MiXeD CaSe")], &context)
            .unwrap();
        assert_eq!(result, Value::string("mixed case"));

        // NULL handling
        let result = func.execute(&[Value::Null], &context).unwrap();
        assert_eq!(result, Value::Null);
    }
}
