//! IFNULL function - alias for COALESCE

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::Result;
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// IFNULL function
pub struct IfnullFunction;

impl Function for IfnullFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "IFNULL",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        super::coalesce::CoalesceFunction.validate(arg_types)
    }

    fn execute(&self, args: &[Value], context: &ExecutionContext) -> Result<Value> {
        super::coalesce::CoalesceFunction.execute(args, context)
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(IfnullFunction));
}
