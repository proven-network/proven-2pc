//! SUM aggregate function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct SumFunction;

impl Function for SumFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "SUM",
            is_aggregate: true,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "SUM takes exactly 1 argument, got {}",
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
                // SUM returns the same type for integers, F64 for floats
                match &arg_types[0] {
                    DataType::F32 | DataType::F64 => Ok(DataType::F64),
                    DataType::Decimal(_, _) => Ok(DataType::Decimal(None, None)),
                    _ => Ok(DataType::I64), // Promote smaller ints to I64
                }
            }
            DataType::Nullable(inner) => {
                // Recursively validate the inner type
                let inner_result = self.validate(&[(**inner).clone()])?;
                Ok(DataType::Nullable(Box::new(inner_result)))
            }
            _ => Err(Error::TypeMismatch {
                expected: "numeric type".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        // Note: This is the single-row execution
        // The actual aggregation happens in the executor
        // This just validates and returns the value for aggregation

        if args.len() != 1 {
            return Err(Error::ExecutionError("SUM takes exactly 1 argument".into()));
        }

        // For aggregate functions, we typically just return the value
        // The aggregation logic is handled by the executor
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

/// Register the SUM function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(SumFunction));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_signature() {
        let func = SumFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "SUM");
        assert!(sig.is_aggregate);
    }

    #[test]
    fn test_sum_validate() {
        let func = SumFunction;

        // Integer types
        assert_eq!(func.validate(&[DataType::I32]).unwrap(), DataType::I64);
        assert_eq!(func.validate(&[DataType::I64]).unwrap(), DataType::I64);

        // Float types
        assert_eq!(func.validate(&[DataType::F32]).unwrap(), DataType::F64);
        assert_eq!(func.validate(&[DataType::F64]).unwrap(), DataType::F64);

        // Decimal
        assert_eq!(
            func.validate(&[DataType::Decimal(Some(10), Some(2))])
                .unwrap(),
            DataType::Decimal(None, None)
        );

        // Non-numeric type should error
        assert!(func.validate(&[DataType::Text]).is_err());

        // Wrong number of arguments
        assert!(func.validate(&[]).is_err());
        assert!(func.validate(&[DataType::I32, DataType::I32]).is_err());
    }
}
