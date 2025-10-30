//! CURRENT_DATE function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::Value;
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use chrono::NaiveDate;

/// CURRENT_DATE function
pub struct CurrentDateFunction;

impl Function for CurrentDateFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "CURRENT_DATE",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types.is_empty() {
            return Err(Error::ExecutionError(
                "CURRENT_DATE takes no arguments".into(),
            ));
        }
        Ok(DataType::Date)
    }

    fn execute(&self, args: &[Value], context: &ExecutionContext) -> Result<Value> {
        if !args.is_empty() {
            return Err(Error::ExecutionError(
                "CURRENT_DATE takes no arguments".into(),
            ));
        }

        // Extract timestamp from UUIDv7
        let uuid = context.timestamp().as_uuid();
        let timestamp_ms = uuid
            .get_timestamp()
            .ok_or_else(|| Error::InvalidValue("Transaction ID does not contain timestamp".into()))?
            .to_unix();
        let (secs, _nanos) = timestamp_ms;
        let days = secs / 86400;
        let date = NaiveDate::from_num_days_from_ce_opt(days as i32 + 719163)
            .ok_or_else(|| Error::InvalidValue("Invalid date".into()))?;
        Ok(Value::Date(date))
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(CurrentDateFunction));
}
