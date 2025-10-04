//! CURRENT_TIME function

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;
use chrono::NaiveTime;

/// CURRENT_TIME function
pub struct CurrentTimeFunction;

impl Function for CurrentTimeFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "CURRENT_TIME",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types.is_empty() {
            return Err(Error::ExecutionError(
                "CURRENT_TIME takes no arguments".into(),
            ));
        }
        Ok(DataType::Time)
    }

    fn execute(&self, args: &[Value], context: &ExecutionContext) -> Result<Value> {
        if !args.is_empty() {
            return Err(Error::ExecutionError(
                "CURRENT_TIME takes no arguments".into(),
            ));
        }

        let micros = context.timestamp().physical as i64;
        let secs = micros / 1_000_000;
        let nanos = ((micros % 1_000_000) * 1_000) as u32;
        let time = NaiveTime::from_num_seconds_from_midnight_opt((secs % 86400) as u32, nanos)
            .ok_or_else(|| Error::InvalidValue("Invalid time".into()))?;
        Ok(Value::Time(time))
    }
}

/// Register the function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(CurrentTimeFunction));
}
