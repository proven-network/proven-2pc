//! MIN aggregate function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct MinFunction;

impl Function for MinFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "MIN",
            min_args: 1,
            max_args: Some(1),
            is_aggregate: true,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "MIN takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        // MIN returns the same type as input for any comparable type
        // All types are comparable in SQL (even strings)
        match &arg_types[0] {
            DataType::Nullable(inner) => Ok(DataType::Nullable(inner.clone())),
            _ => Ok(arg_types[0].clone()),
        }
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError("MIN takes exactly 1 argument".into()));
        }

        // For aggregate functions, just return the value
        // The actual min computation happens in the executor
        Ok(args[0].clone())
    }
}

/// Register the MIN function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(MinFunction));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_signature() {
        let func = MinFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "MIN");
        assert_eq!(sig.min_args, 1);
        assert_eq!(sig.max_args, Some(1));
        assert!(sig.is_aggregate);
    }

    #[test]
    fn test_min_validate() {
        let func = MinFunction;

        // Returns same type as input
        assert_eq!(func.validate(&[DataType::I32]).unwrap(), DataType::I32);
        assert_eq!(func.validate(&[DataType::F64]).unwrap(), DataType::F64);
        assert_eq!(func.validate(&[DataType::Text]).unwrap(), DataType::Text);
        assert_eq!(func.validate(&[DataType::Date]).unwrap(), DataType::Date);

        // Nullable types
        assert_eq!(
            func.validate(&[DataType::Nullable(Box::new(DataType::I32))])
                .unwrap(),
            DataType::Nullable(Box::new(DataType::I32))
        );

        // Wrong number of arguments
        assert!(func.validate(&[]).is_err());
    }
}
