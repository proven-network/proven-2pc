//! NOW function - returns current timestamp

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct NowFunction;

impl Function for NowFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "NOW",
            min_args: 0,
            max_args: Some(0),
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

    fn execute(&self, args: &[Value], context: &TransactionContext) -> Result<Value> {
        if !args.is_empty() {
            return Err(Error::ExecutionError(format!(
                "NOW takes no arguments, got {}",
                args.len()
            )));
        }

        // Convert HLC timestamp to SQL timestamp
        use chrono::DateTime;
        let micros = context.timestamp().physical as i64;
        let secs = micros / 1_000_000;
        let nanos = ((micros % 1_000_000) * 1_000) as u32;
        let dt = DateTime::from_timestamp(secs, nanos)
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
            min_args: 0,
            max_args: Some(0),
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        NowFunction.validate(arg_types)
    }

    fn execute(&self, args: &[Value], context: &TransactionContext) -> Result<Value> {
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
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_now_signature() {
        let func = NowFunction;
        let sig = func.signature();
        assert_eq!(sig.name, "NOW");
        assert_eq!(sig.min_args, 0);
        assert_eq!(sig.max_args, Some(0));
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
        let timestamp = HlcTimestamp::new(1_000_000_000, 0, NodeId::new(1));
        let context = TransactionContext::new(timestamp);

        // NOW() should always return the same value within a transaction
        let now1 = NowFunction.execute(&[], &context).unwrap();
        let now2 = NowFunction.execute(&[], &context).unwrap();
        let current = CurrentTimestampFunction.execute(&[], &context).unwrap();

        assert_eq!(now1, now2);
        assert_eq!(now1, current);

        // Verify the timestamp conversion
        use chrono::DateTime;
        let expected = DateTime::from_timestamp(1000, 0).unwrap().naive_utc();
        assert_eq!(now1, Value::Timestamp(expected));
    }
}
