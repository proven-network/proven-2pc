//! ARRAY_AGG aggregate function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::Value;
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;

pub struct ArrayAggFunction;

impl Function for ArrayAggFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "ARRAY_AGG",
            is_aggregate: true,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "ARRAY_AGG takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        // ARRAY_AGG accepts any type and returns a List of that type
        // For now, we return a generic List type
        Ok(DataType::List(Box::new(arg_types[0].clone())))
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        // Note: This is the single-row execution
        // The actual aggregation happens in the executor
        // This just validates and returns the value for aggregation

        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "ARRAY_AGG takes exactly 1 argument".into(),
            ));
        }

        // For aggregate functions, we just return the value
        // The aggregation logic is handled by the executor
        Ok(args[0].clone())
    }
}

/// Register the ARRAY_AGG function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(ArrayAggFunction));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_agg_signature() {
        let func = ArrayAggFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "ARRAY_AGG");
        assert!(sig.is_aggregate);
    }

    #[test]
    fn test_array_agg_validate() {
        let func = ArrayAggFunction;

        // Should accept any type and return List of that type
        assert_eq!(
            func.validate(&[DataType::I32]).unwrap(),
            DataType::List(Box::new(DataType::I32))
        );
        assert_eq!(
            func.validate(&[DataType::Text]).unwrap(),
            DataType::List(Box::new(DataType::Text))
        );

        // Wrong number of arguments
        assert!(func.validate(&[]).is_err());
        assert!(func.validate(&[DataType::I32, DataType::I32]).is_err());
    }
}
