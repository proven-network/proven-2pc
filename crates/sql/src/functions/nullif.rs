//! NULLIF function - returns NULL if arguments are equal, otherwise returns first argument

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::Value;
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;

pub struct NullifFunction;

impl Function for NullifFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "NULLIF",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(format!(
                "NULLIF takes exactly 2 arguments, got {}",
                arg_types.len()
            )));
        }

        // Return type is nullable version of the first argument type
        // since we may return NULL if the values are equal
        match &arg_types[0] {
            DataType::Nullable(_) => Ok(arg_types[0].clone()),
            dt => Ok(DataType::Nullable(Box::new(dt.clone()))),
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "NULLIF takes exactly 2 arguments".into(),
            ));
        }

        // If the two arguments are equal, return NULL
        // Otherwise, return the first argument
        if args[0] == args[1] {
            Ok(Value::Null)
        } else {
            Ok(args[0].clone())
        }
    }
}

/// Register NULLIF function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(NullifFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_common::TransactionId;
    use uuid::Uuid;

    #[test]
    fn test_nullif_signature() {
        let func = NullifFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "NULLIF");
        assert!(!sig.is_aggregate);
    }

    #[test]
    fn test_nullif_execute_equal() {
        let func = NullifFunction;
        let context = ExecutionContext::new(TransactionId::from_uuid(Uuid::from_u128(0)), 0);

        // Equal integers return NULL
        let result = func
            .execute(&[Value::I32(42), Value::I32(42)], &context)
            .unwrap();
        assert_eq!(result, Value::Null);

        // Equal strings return NULL
        let result = func
            .execute(&[Value::string("hello"), Value::string("hello")], &context)
            .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_nullif_execute_different() {
        let func = NullifFunction;
        let context = ExecutionContext::new(TransactionId::from_uuid(Uuid::from_u128(0)), 0);

        // Different integers return first argument
        let result = func
            .execute(&[Value::I32(42), Value::I32(100)], &context)
            .unwrap();
        assert_eq!(result, Value::I32(42));

        // Different strings return first argument
        let result = func
            .execute(&[Value::string("hello"), Value::string("world")], &context)
            .unwrap();
        assert_eq!(result, Value::string("hello"));
    }

    #[test]
    fn test_nullif_wrong_arg_count() {
        let func = NullifFunction;
        let context = ExecutionContext::new(TransactionId::from_uuid(Uuid::from_u128(0)), 0);

        // Too few arguments
        let result = func.execute(&[Value::I32(42)], &context);
        assert!(result.is_err());

        // Too many arguments
        let result = func.execute(
            &[Value::I32(42), Value::I32(100), Value::I32(200)],
            &context,
        );
        assert!(result.is_err());
    }
}
