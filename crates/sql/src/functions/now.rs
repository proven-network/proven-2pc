//! NOW function - returns current timestamp

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::Value;
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;

pub struct NowFunction;

impl Function for NowFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "NOW",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types.is_empty() {
            return Err(Error::ExecutionError(format!(
                "NOW takes no arguments, got {}",
                arg_types.len()
            )));
        }

        Ok(DataType::Timestamp)
    }

    fn execute(&self, args: &[Value], context: &ExecutionContext) -> Result<Value> {
        if !args.is_empty() {
            return Err(Error::ExecutionError(format!(
                "NOW takes no arguments, got {}",
                args.len()
            )));
        }

        // Extract timestamp from UUIDv7 (embedded in first 48 bits as milliseconds)
        use chrono::DateTime;
        let uuid = context.timestamp().as_uuid();
        let timestamp_ms = uuid
            .get_timestamp()
            .ok_or_else(|| Error::InvalidValue("Transaction ID does not contain timestamp".into()))?
            .to_unix();
        let (secs, nanos) = timestamp_ms;
        let dt = DateTime::from_timestamp(secs as i64, nanos)
            .ok_or_else(|| Error::InvalidValue("Invalid timestamp".into()))?
            .naive_utc();
        Ok(Value::Timestamp(dt))
    }
}

// Also implement CURRENT_TIMESTAMP as an alias
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
        NowFunction.validate(arg_types)
    }

    fn execute(&self, args: &[Value], context: &ExecutionContext) -> Result<Value> {
        NowFunction.execute(args, context)
    }
}

/// Register NOW and CURRENT_TIMESTAMP functions
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(NowFunction));
    registry.register(Box::new(CurrentTimestampFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_common::TransactionId;

    #[test]
    fn test_now_signature() {
        let func = NowFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "NOW");
        assert!(!sig.is_aggregate);
    }

    #[test]
    fn test_now_validate() {
        let func = NowFunction;

        // No arguments is valid
        assert_eq!(func.validate(&[]).unwrap(), DataType::Timestamp);

        // Any arguments is invalid
        assert!(func.validate(&[DataType::Text]).is_err());
    }

    #[test]
    fn test_now_deterministic() {
        // Use a real UUIDv7 which has embedded timestamp
        let timestamp = TransactionId::new();
        let context = ExecutionContext::new(timestamp, 0);

        // NOW() should always return the same value within a transaction
        let now1 = NowFunction.execute(&[], &context).unwrap();
        let now2 = NowFunction.execute(&[], &context).unwrap();
        let current = CurrentTimestampFunction.execute(&[], &context).unwrap();

        // All calls should return the same deterministic timestamp
        assert_eq!(now1, now2);
        assert_eq!(now1, current);

        // Verify it's a valid timestamp value
        assert!(matches!(now1, Value::Timestamp(_)));
    }
}
