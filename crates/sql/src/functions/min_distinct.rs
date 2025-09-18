//! MIN_DISTINCT aggregate function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct MinDistinctFunction;

impl Function for MinDistinctFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "MIN_DISTINCT",
            is_aggregate: true,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "MIN_DISTINCT takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        // MIN_DISTINCT works with any comparable type
        // Returns the same type as input
        match &arg_types[0] {
            DataType::Nullable(inner) => Ok(DataType::Nullable(inner.clone())),
            dt => Ok(dt.clone()),
        }
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        // This is typically not called directly for aggregate functions
        // The executor handles the actual aggregation logic including DISTINCT
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "MIN_DISTINCT takes exactly 1 argument".into(),
            ));
        }

        // For aggregate functions, we typically just return the value
        // The aggregation logic with DISTINCT is handled by the executor
        Ok(args[0].clone())
    }
}

/// Register the MIN_DISTINCT function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(MinDistinctFunction));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_distinct_signature() {
        let func = MinDistinctFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "MIN_DISTINCT");
        assert!(sig.is_aggregate);
    }

    #[test]
    fn test_min_distinct_validate() {
        let func = MinDistinctFunction;

        // Accepts any type and returns same type
        assert_eq!(func.validate(&[DataType::I32]).unwrap(), DataType::I32);
        assert_eq!(func.validate(&[DataType::Text]).unwrap(), DataType::Text);
        assert_eq!(func.validate(&[DataType::F64]).unwrap(), DataType::F64);
        assert_eq!(func.validate(&[DataType::Date]).unwrap(), DataType::Date);
        // MIN can work with Null (returns Null)
        assert_eq!(func.validate(&[DataType::Null]).unwrap(), DataType::Null);

        // Wrong number of arguments
        assert!(func.validate(&[]).is_err());
        assert!(func.validate(&[DataType::I32, DataType::I32]).is_err());
    }
}
