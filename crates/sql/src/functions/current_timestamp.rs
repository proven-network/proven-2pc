//! CURRENT_TIMESTAMP function - alias for NOW

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::Result;
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// CURRENT_TIMESTAMP function
pub struct CurrentTimestampFunction;

impl Function for CurrentTimestampFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "CURRENT_TIMESTAMP",
            min_args: 0,
            max_args: Some(0),
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        super::now::NowFunction.validate(arg_types)
    }

    fn execute(&self, args: &[Value], context: &TransactionContext) -> Result<Value> {
        super::now::NowFunction.execute(args, context)
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(CurrentTimestampFunction));
}
