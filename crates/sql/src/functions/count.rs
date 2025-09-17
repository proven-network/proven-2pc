//! COUNT aggregate function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct CountFunction;

impl Function for CountFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "COUNT",
            min_args: 1,
            max_args: Some(1),
            is_aggregate: true,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "COUNT takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        // COUNT always returns I64, regardless of input type
        // COUNT(*) is handled specially by the parser
        Ok(DataType::I64)
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        // Note: This is the single-row execution for COUNT
        // The actual aggregation logic happens in the executor/aggregator
        // This is called for each row to determine if it should be counted

        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "COUNT takes exactly 1 argument".into(),
            ));
        }

        // For aggregate functions, we typically just validate the value here
        // The actual counting happens in the aggregation layer
        // Return 1 for non-null values, 0 for null
        match &args[0] {
            Value::Null => Ok(Value::I64(0)),
            _ => Ok(Value::I64(1)),
        }
    }
}

/// Register the COUNT function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(CountFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_count_signature() {
        let func = CountFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "COUNT");
        assert_eq!(sig.min_args, 1);
        assert_eq!(sig.max_args, Some(1));
        assert!(sig.is_aggregate);
    }

    #[test]
    fn test_count_validate() {
        let func = CountFunction;

        // Any type is valid for COUNT
        assert_eq!(func.validate(&[DataType::Text]).unwrap(), DataType::I64);
        assert_eq!(func.validate(&[DataType::I32]).unwrap(), DataType::I64);
        assert_eq!(
            func.validate(&[DataType::Nullable(Box::new(DataType::Text))])
                .unwrap(),
            DataType::I64
        );

        // Wrong number of arguments
        assert!(func.validate(&[]).is_err());
        assert!(func.validate(&[DataType::Text, DataType::Text]).is_err());
    }

    #[test]
    fn test_count_execute() {
        let func = CountFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // Non-null values return 1
        assert_eq!(
            func.execute(&[Value::string("test")], &context).unwrap(),
            Value::I64(1)
        );
        assert_eq!(
            func.execute(&[Value::I32(42)], &context).unwrap(),
            Value::I64(1)
        );

        // Null values return 0
        assert_eq!(
            func.execute(&[Value::Null], &context).unwrap(),
            Value::I64(0)
        );
    }
}
