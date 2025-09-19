//! CEIL and FLOOR functions - rounding functions

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct CeilFunction;
pub struct FloorFunction;

impl Function for CeilFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "CEIL",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "CEIL takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            // Integer types just return themselves
            DataType::I8
            | DataType::I16
            | DataType::I32
            | DataType::I64
            | DataType::I128
            | DataType::U8
            | DataType::U16
            | DataType::U32
            | DataType::U64
            | DataType::U128 => Ok(arg_types[0].clone()),
            // Float types return I64
            DataType::F32 | DataType::F64 => Ok(DataType::I64),
            DataType::Decimal(_, _) => Ok(DataType::I64),
            DataType::Nullable(inner) => {
                let inner_result = self.validate(&[(**inner).clone()])?;
                Ok(DataType::Nullable(Box::new(inner_result)))
            }
            _ => Err(Error::TypeMismatch {
                expected: "numeric type".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "CEIL takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            // Integer types return unchanged
            Value::I8(_)
            | Value::I16(_)
            | Value::I32(_)
            | Value::I64(_)
            | Value::I128(_)
            | Value::U8(_)
            | Value::U16(_)
            | Value::U32(_)
            | Value::U64(_)
            | Value::U128(_) => Ok(args[0].clone()),
            // Float types get ceiling
            Value::F32(v) => Ok(Value::I64(v.ceil() as i64)),
            Value::F64(v) => Ok(Value::I64(v.ceil() as i64)),
            Value::Decimal(d) => Ok(Value::I64(d.ceil().try_into().unwrap_or(i64::MAX))),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "numeric".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

impl Function for FloorFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "FLOOR",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "FLOOR takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            // Integer types just return themselves
            DataType::I8
            | DataType::I16
            | DataType::I32
            | DataType::I64
            | DataType::I128
            | DataType::U8
            | DataType::U16
            | DataType::U32
            | DataType::U64
            | DataType::U128 => Ok(arg_types[0].clone()),
            // Float types return I64
            DataType::F32 | DataType::F64 => Ok(DataType::I64),
            DataType::Decimal(_, _) => Ok(DataType::I64),
            DataType::Nullable(inner) => {
                let inner_result = self.validate(&[(**inner).clone()])?;
                Ok(DataType::Nullable(Box::new(inner_result)))
            }
            _ => Err(Error::TypeMismatch {
                expected: "numeric type".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "FLOOR takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            // Integer types return unchanged
            Value::I8(_)
            | Value::I16(_)
            | Value::I32(_)
            | Value::I64(_)
            | Value::I128(_)
            | Value::U8(_)
            | Value::U16(_)
            | Value::U32(_)
            | Value::U64(_)
            | Value::U128(_) => Ok(args[0].clone()),
            // Float types get floor
            Value::F32(v) => Ok(Value::I64(v.floor() as i64)),
            Value::F64(v) => Ok(Value::I64(v.floor() as i64)),
            Value::Decimal(d) => Ok(Value::I64(d.floor().try_into().unwrap_or(i64::MIN))),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "numeric".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the CEIL and FLOOR functions
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(CeilFunction));
    registry.register(Box::new(FloorFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn test_ceil_execute() {
        let func = CeilFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // Integer stays the same
        assert_eq!(
            func.execute(&[Value::I32(42)], &context).unwrap(),
            Value::I32(42)
        );

        // Positive float rounds up
        assert_eq!(
            func.execute(&[Value::F64(42.3)], &context).unwrap(),
            Value::I64(43)
        );

        // Negative float rounds towards zero
        assert_eq!(
            func.execute(&[Value::F64(-42.7)], &context).unwrap(),
            Value::I64(-42)
        );

        // Decimal
        let d = Decimal::from_str("42.3").unwrap();
        assert_eq!(
            func.execute(&[Value::Decimal(d)], &context).unwrap(),
            Value::I64(43)
        );

        // NULL handling
        assert_eq!(func.execute(&[Value::Null], &context).unwrap(), Value::Null);
    }

    #[test]
    fn test_floor_execute() {
        let func = FloorFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // Integer stays the same
        assert_eq!(
            func.execute(&[Value::I32(42)], &context).unwrap(),
            Value::I32(42)
        );

        // Positive float rounds down
        assert_eq!(
            func.execute(&[Value::F64(42.7)], &context).unwrap(),
            Value::I64(42)
        );

        // Negative float rounds away from zero
        assert_eq!(
            func.execute(&[Value::F64(-42.3)], &context).unwrap(),
            Value::I64(-43)
        );

        // Decimal
        let d = Decimal::from_str("42.7").unwrap();
        assert_eq!(
            func.execute(&[Value::Decimal(d)], &context).unwrap(),
            Value::I64(42)
        );

        // NULL handling
        assert_eq!(func.execute(&[Value::Null], &context).unwrap(), Value::Null);
    }
}
