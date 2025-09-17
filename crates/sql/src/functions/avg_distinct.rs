//! AVG_DISTINCT aggregate function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct AvgDistinctFunction;

impl Function for AvgDistinctFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "AVG_DISTINCT",
            min_args: 1,
            max_args: Some(1),
            is_aggregate: true,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "AVG_DISTINCT takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        // Check that the argument is numeric
        match &arg_types[0] {
            DataType::I8
            | DataType::I16
            | DataType::I32
            | DataType::I64
            | DataType::I128
            | DataType::U8
            | DataType::U16
            | DataType::U32
            | DataType::U64
            | DataType::U128
            | DataType::F32
            | DataType::F64
            | DataType::Decimal(_, _) => {
                // AVG always returns F64 for precision
                Ok(DataType::F64)
            }
            DataType::Nullable(inner) => {
                // Recursively validate the inner type
                self.validate(&[(**inner).clone()])?;
                Ok(DataType::Nullable(Box::new(DataType::F64)))
            }
            DataType::Unknown => {
                // Parameters - can't validate yet
                Ok(DataType::Unknown)
            }
            _ => Err(Error::TypeMismatch {
                expected: "numeric type".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        // This is typically not called directly for aggregate functions
        // The executor handles the actual aggregation logic including DISTINCT
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "AVG_DISTINCT takes exactly 1 argument".into(),
            ));
        }

        // For aggregate functions, we typically just return the value
        // The aggregation logic with DISTINCT is handled by the executor
        match &args[0] {
            Value::Null => Ok(Value::Null),
            v if v.is_numeric() => Ok(v.clone()),
            _ => Err(Error::TypeMismatch {
                expected: "numeric value".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the AVG_DISTINCT function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(AvgDistinctFunction));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avg_distinct_signature() {
        let func = AvgDistinctFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "AVG_DISTINCT");
        assert_eq!(sig.min_args, 1);
        assert_eq!(sig.max_args, Some(1));
        assert!(sig.is_aggregate);
    }

    #[test]
    fn test_avg_distinct_validate() {
        let func = AvgDistinctFunction;

        // Valid numeric types
        assert_eq!(func.validate(&[DataType::I32]).unwrap(), DataType::F64);
        assert_eq!(func.validate(&[DataType::F64]).unwrap(), DataType::F64);

        // Invalid non-numeric type
        assert!(func.validate(&[DataType::Text]).is_err());

        // Wrong number of arguments
        assert!(func.validate(&[]).is_err());
        assert!(func.validate(&[DataType::I32, DataType::I32]).is_err());
    }
}
