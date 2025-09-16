//! UPPER function - converts string to uppercase

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct UpperFunction;

impl Function for UpperFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "UPPER",
            min_args: 1,
            max_args: Some(1),
            arg_types: vec![], // Will be validated in validate()
            is_deterministic: true,
            is_aggregate: false,
            description: "Converts a string to uppercase",
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "UPPER takes exactly 1 argument, got {}",
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

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "UPPER takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Str(s) => Ok(Value::string(s.to_uppercase())),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "string".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the UPPER function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(UpperFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_upper_signature() {
        let func = UpperFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "UPPER");
        assert_eq!(sig.min_args, 1);
        assert_eq!(sig.max_args, Some(1));
        assert!(sig.is_deterministic);
        assert!(!sig.is_aggregate);
    }

    #[test]
    fn test_upper_validate() {
        let func = UpperFunction;

        // Valid string type
        assert_eq!(func.validate(&[DataType::Text]).unwrap(), DataType::Text);

        // Valid Str type
        assert_eq!(func.validate(&[DataType::Str]).unwrap(), DataType::Text);

        // Nullable string type
        assert_eq!(
            func.validate(&[DataType::Nullable(Box::new(DataType::Text))])
                .unwrap(),
            DataType::Nullable(Box::new(DataType::Text))
        );

        // Invalid type
        assert!(func.validate(&[DataType::I32]).is_err());

        // Wrong number of arguments
        assert!(func.validate(&[]).is_err());
        assert!(func.validate(&[DataType::Text, DataType::Text]).is_err());
    }

    #[test]
    fn test_upper_execute() {
        let func = UpperFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // Uppercase conversion
        let result = func
            .execute(&[Value::string("hello world")], &context)
            .unwrap();
        assert_eq!(result, Value::string("HELLO WORLD"));

        // Already uppercase
        let result = func
            .execute(&[Value::string("ALREADY UPPER")], &context)
            .unwrap();
        assert_eq!(result, Value::string("ALREADY UPPER"));

        // Empty string
        let result = func.execute(&[Value::string("")], &context).unwrap();
        assert_eq!(result, Value::string(""));

        // NULL handling
        let result = func.execute(&[Value::Null], &context).unwrap();
        assert_eq!(result, Value::Null);

        // Invalid type should error
        assert!(func.execute(&[Value::I32(42)], &context).is_err());
    }
}
