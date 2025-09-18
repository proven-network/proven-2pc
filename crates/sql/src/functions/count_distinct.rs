//! COUNT_DISTINCT aggregate function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct CountDistinctFunction;

impl Function for CountDistinctFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "COUNT_DISTINCT",
            is_aggregate: true,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "COUNT_DISTINCT takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        // COUNT_DISTINCT accepts any type and always returns I64
        // Even with Unknown types (parameters)
        Ok(DataType::I64)
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        // This is typically not called directly for aggregate functions
        // The executor handles the actual aggregation logic including DISTINCT
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "COUNT_DISTINCT takes exactly 1 argument".into(),
            ));
        }

        // For aggregate functions, we typically just return the value
        // The aggregation logic with DISTINCT is handled by the executor
        Ok(args[0].clone())
    }
}

/// Register the COUNT_DISTINCT function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(CountDistinctFunction));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_distinct_signature() {
        let func = CountDistinctFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "COUNT_DISTINCT");
        assert!(sig.is_aggregate);
    }

    #[test]
    fn test_count_distinct_validate() {
        let func = CountDistinctFunction;

        // Accepts any type
        assert_eq!(func.validate(&[DataType::I32]).unwrap(), DataType::I64);
        assert_eq!(func.validate(&[DataType::Text]).unwrap(), DataType::I64);
        assert_eq!(func.validate(&[DataType::Bool]).unwrap(), DataType::I64);
        // COUNT works with any type, even NULL
        assert_eq!(func.validate(&[DataType::Null]).unwrap(), DataType::I64);

        // Wrong number of arguments
        assert!(func.validate(&[]).is_err());
        assert!(func.validate(&[DataType::I32, DataType::I32]).is_err());
    }
}
