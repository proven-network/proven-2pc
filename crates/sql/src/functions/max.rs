//! MAX aggregate function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::Value;
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;

pub struct MaxFunction;

impl Function for MaxFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "MAX",
            is_aggregate: true,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "MAX takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        // MAX returns the same type as input for any comparable type
        match &arg_types[0] {
            DataType::Nullable(inner) => Ok(DataType::Nullable(inner.clone())),
            _ => Ok(arg_types[0].clone()),
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError("MAX takes exactly 1 argument".into()));
        }

        // For aggregate functions, just return the value
        // The actual max computation happens in the executor
        Ok(args[0].clone())
    }
}

/// Register the MAX function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(MaxFunction));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_signature() {
        let func = MaxFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "MAX");
        assert!(sig.is_aggregate);
    }

    #[test]
    fn test_max_validate() {
        let func = MaxFunction;

        // Returns same type as input
        assert_eq!(func.validate(&[DataType::I32]).unwrap(), DataType::I32);
        assert_eq!(func.validate(&[DataType::F64]).unwrap(), DataType::F64);
        assert_eq!(func.validate(&[DataType::Text]).unwrap(), DataType::Text);
        assert_eq!(
            func.validate(&[DataType::Timestamp]).unwrap(),
            DataType::Timestamp
        );

        // Wrong number of arguments
        assert!(func.validate(&[]).is_err());
    }
}
