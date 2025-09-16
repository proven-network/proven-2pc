//! IFNULL function - alias for COALESCE

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::Result;
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// IFNULL function
pub struct IfnullFunction;

impl Function for IfnullFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "IFNULL",
            min_args: 2,
            max_args: Some(2),
            arg_types: vec![],
            is_deterministic: true,
            is_aggregate: false,
            description: "Returns first non-null value (alias for COALESCE)",
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        super::coalesce::CoalesceFunction.validate(arg_types)
    }

    fn execute(&self, args: &[Value], context: &TransactionContext) -> Result<Value> {
        super::coalesce::CoalesceFunction.execute(args, context)
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(IfnullFunction));
}