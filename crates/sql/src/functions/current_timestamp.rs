//! CURRENT_TIMESTAMP function - alias for NOW

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::Result;
use crate::types::Value;
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;

/// CURRENT_TIMESTAMP function
pub struct CurrentTimestampFunction;

impl Function for CurrentTimestampFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "CURRENT_TIMESTAMP",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        super::now::NowFunction.validate(arg_types)
    }

    fn execute(&self, args: &[Value], context: &ExecutionContext) -> Result<Value> {
        super::now::NowFunction.execute(args, context)
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(CurrentTimestampFunction));
}
