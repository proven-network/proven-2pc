//! LEAST function - returns the least (minimum) value from a list of arguments

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct LeastFunction;

impl Function for LeastFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "LEAST",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() < 2 {
            return Err(Error::ExecutionError(format!(
                "LEAST requires at least 2 arguments, got {}",
                arg_types.len()
            )));
        }

        // Check that all arguments are of the same type and not NULL
        let first_type = &arg_types[0];

        // NULL arguments are not allowed
        if matches!(first_type, DataType::Null) {
            return Err(Error::ExecutionError(
                "LEAST does not accept NULL arguments".into(),
            ));
        }

        // Get the base type (unwrap nullable)
        let base_type = match first_type {
            DataType::Nullable(_) => {
                return Err(Error::ExecutionError(
                    "LEAST does not accept NULL arguments".into(),
                ));
            }
            other => other,
        };

        // Check all arguments have the same base type
        for arg_type in &arg_types[1..] {
            if matches!(arg_type, DataType::Null | DataType::Nullable(_)) {
                return Err(Error::ExecutionError(
                    "LEAST does not accept NULL arguments".into(),
                ));
            }

            if arg_type != base_type {
                return Err(Error::ExecutionError(format!(
                    "LEAST arguments must be of the same type, got {:?} and {:?}",
                    first_type, arg_type
                )));
            }
        }

        Ok(first_type.clone())
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::ExecutionError(format!(
                "LEAST requires at least 2 arguments, got {}",
                args.len()
            )));
        }

        // Check for NULL values
        for arg in args {
            if matches!(arg, Value::Null) {
                return Err(Error::ExecutionError(
                    "LEAST does not accept NULL arguments".into(),
                ));
            }
        }

        // Find the minimum value
        let mut min_value = &args[0];

        for arg in &args[1..] {
            // Compare values - we need to handle different types
            if compare_values(arg, min_value)? {
                min_value = arg;
            }
        }

        Ok(min_value.clone())
    }
}

/// Compare two values, returns true if a < b
fn compare_values(a: &Value, b: &Value) -> Result<bool> {
    match (a, b) {
        // Integers
        (Value::I8(a), Value::I8(b)) => Ok(a < b),
        (Value::I16(a), Value::I16(b)) => Ok(a < b),
        (Value::I32(a), Value::I32(b)) => Ok(a < b),
        (Value::I64(a), Value::I64(b)) => Ok(a < b),
        (Value::I128(a), Value::I128(b)) => Ok(a < b),
        (Value::U8(a), Value::U8(b)) => Ok(a < b),
        (Value::U16(a), Value::U16(b)) => Ok(a < b),
        (Value::U32(a), Value::U32(b)) => Ok(a < b),
        (Value::U64(a), Value::U64(b)) => Ok(a < b),
        (Value::U128(a), Value::U128(b)) => Ok(a < b),

        // Floats
        (Value::F32(a), Value::F32(b)) => Ok(a < b),
        (Value::F64(a), Value::F64(b)) => Ok(a < b),

        // Decimals
        (Value::Decimal(a), Value::Decimal(b)) => Ok(a < b),

        // Strings
        (Value::Str(a), Value::Str(b)) => Ok(a < b),

        // Booleans
        (Value::Bool(a), Value::Bool(b)) => Ok(a < b),

        // Dates
        (Value::Date(a), Value::Date(b)) => Ok(a < b),
        (Value::Time(a), Value::Time(b)) => Ok(a < b),
        (Value::Timestamp(a), Value::Timestamp(b)) => Ok(a < b),

        // Incompatible types
        _ => Err(Error::ExecutionError(
            "LEAST arguments are not comparable".into(),
        )),
    }
}

/// Register the LEAST function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(LeastFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_least_signature() {
        let func = LeastFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "LEAST");
        assert!(!sig.is_aggregate);
    }

    #[test]
    fn test_least_validate() {
        let func = LeastFunction;

        // Valid - same types
        assert_eq!(
            func.validate(&[DataType::I32, DataType::I32, DataType::I32])
                .unwrap(),
            DataType::I32
        );

        // Invalid - too few arguments
        assert!(func.validate(&[DataType::I32]).is_err());

        // Invalid - empty
        assert!(func.validate(&[]).is_err());

        // Invalid - mixed types
        assert!(func.validate(&[DataType::I32, DataType::F64]).is_err());

        // Invalid - NULL
        assert!(func.validate(&[DataType::Null, DataType::I32]).is_err());
    }

    #[test]
    fn test_least_execute() {
        let func = LeastFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // Integers
        let result = func
            .execute(&[Value::I64(6), Value::I64(1), Value::I64(3)], &context)
            .unwrap();
        assert_eq!(result, Value::I64(1));

        // Strings
        let result = func
            .execute(
                &[
                    Value::string("cherry"),
                    Value::string("banana"),
                    Value::string("apple"),
                ],
                &context,
            )
            .unwrap();
        assert_eq!(result, Value::string("apple"));

        // Booleans
        let result = func
            .execute(&[Value::Bool(true), Value::Bool(false)], &context)
            .unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_least_null_error() {
        let func = LeastFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        let result = func.execute(&[Value::Null, Value::I64(1)], &context);
        assert!(result.is_err());
    }
}
