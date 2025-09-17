//! GENERATE_UUID function - generates deterministic UUIDs

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// GENERATE_UUID function
pub struct GenerateUuidFunction;

impl Function for GenerateUuidFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "GENERATE_UUID",
            min_args: 0,
            max_args: Some(0),
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types.is_empty() {
            return Err(Error::ExecutionError(format!(
                "GENERATE_UUID takes no arguments, got {}",
                arg_types.len()
            )));
        }
        Ok(DataType::Uuid)
    }

    fn execute(&self, args: &[Value], context: &TransactionContext) -> Result<Value> {
        if !args.is_empty() {
            return Err(Error::ExecutionError(format!(
                "GENERATE_UUID takes no arguments, got {}",
                args.len()
            )));
        }
        // Use the transaction context to generate a deterministic UUID
        Ok(Value::Uuid(context.deterministic_uuid()))
    }
}

/// Register the GENERATE_UUID function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(GenerateUuidFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_generate_uuid() {
        let func = GenerateUuidFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        let result = func.execute(&[], &context).unwrap();
        match result {
            Value::Uuid(_) => {
                // Success - generated a UUID
            }
            _ => panic!("Expected UUID value"),
        }
    }
}
