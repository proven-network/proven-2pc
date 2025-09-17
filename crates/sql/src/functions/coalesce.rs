//! COALESCE function - returns first non-NULL value

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct CoalesceFunction;

impl Function for CoalesceFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "COALESCE",
            min_args: 1,
            max_args: None, // Variadic
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return Err(Error::ExecutionError(
                "COALESCE requires at least one argument".into(),
            ));
        }

        // Find the common type among all arguments
        // For simplicity, we'll use the first non-nullable type we find
        // In a full implementation, we'd find the least common supertype
        let mut result_type = None;

        for arg_type in arg_types {
            match arg_type {
                DataType::Nullable(inner) => {
                    if result_type.is_none() {
                        result_type = Some((**inner).clone());
                    }
                }
                dt => {
                    // Non-nullable type - this is our result type
                    return Ok(dt.clone());
                }
            }
        }

        // If all types are nullable, return a nullable version of the common type
        match result_type {
            Some(t) => Ok(DataType::Nullable(Box::new(t))),
            None => Ok(DataType::Nullable(Box::new(DataType::Text))), // Default fallback
        }
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::ExecutionError(
                "COALESCE requires at least one argument".into(),
            ));
        }

        // Return the first non-NULL value
        for arg in args {
            if *arg != Value::Null {
                return Ok(arg.clone());
            }
        }

        // If all are NULL, return NULL
        Ok(Value::Null)
    }
}

// IFNULL is an alias for COALESCE with exactly 2 arguments
pub struct IfNullFunction;

impl Function for IfNullFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "IFNULL",
            min_args: 2,
            max_args: Some(2),
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(format!(
                "IFNULL takes exactly 2 arguments, got {}",
                arg_types.len()
            )));
        }

        CoalesceFunction.validate(arg_types)
    }

    fn execute(&self, args: &[Value], context: &TransactionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "IFNULL takes exactly 2 arguments".into(),
            ));
        }

        CoalesceFunction.execute(args, context)
    }
}

/// Register COALESCE and IFNULL functions
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(CoalesceFunction));
    registry.register(Box::new(IfNullFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_coalesce_signature() {
        let func = CoalesceFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "COALESCE");
        assert_eq!(sig.min_args, 1);
        assert_eq!(sig.max_args, None); // Variadic
        assert!(!sig.is_aggregate);
    }

    #[test]
    fn test_coalesce_execute() {
        let func = CoalesceFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // First non-null value
        let result = func
            .execute(&[Value::Null, Value::I32(42), Value::I32(100)], &context)
            .unwrap();
        assert_eq!(result, Value::I32(42));

        // All nulls
        let result = func.execute(&[Value::Null, Value::Null], &context).unwrap();
        assert_eq!(result, Value::Null);

        // First value is not null
        let result = func
            .execute(&[Value::string("hello"), Value::Null], &context)
            .unwrap();
        assert_eq!(result, Value::string("hello"));
    }

    #[test]
    fn test_ifnull() {
        let func = IfNullFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // First is null, return second
        let result = func
            .execute(&[Value::Null, Value::I32(42)], &context)
            .unwrap();
        assert_eq!(result, Value::I32(42));

        // First is not null, return first
        let result = func
            .execute(&[Value::string("hello"), Value::I32(42)], &context)
            .unwrap();
        assert_eq!(result, Value::string("hello"));
    }
}
