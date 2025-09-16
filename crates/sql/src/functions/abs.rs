//! ABS function - returns absolute value

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct AbsFunction;

impl Function for AbsFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "ABS",
            min_args: 1,
            max_args: Some(1),
            arg_types: vec![],
            is_deterministic: true,
            is_aggregate: false,
            description: "Returns the absolute value of a number",
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "ABS takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            DataType::I8
            | DataType::I16
            | DataType::I32
            | DataType::I64
            | DataType::I128
            | DataType::F32
            | DataType::F64
            | DataType::Decimal(_, _) => {
                // ABS returns the same type as input
                Ok(arg_types[0].clone())
            }
            DataType::Nullable(inner) => {
                // Recursively validate the inner type
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
            return Err(Error::ExecutionError("ABS takes exactly 1 argument".into()));
        }

        match &args[0] {
            Value::I8(v) => Ok(Value::I8(v.abs())),
            Value::I16(v) => Ok(Value::I16(v.abs())),
            Value::I32(v) => Ok(Value::I32(v.abs())),
            Value::I64(v) => Ok(Value::I64(v.abs())),
            Value::I128(v) => Ok(Value::I128(v.abs())),
            Value::F32(v) => Ok(Value::F32(v.abs())),
            Value::F64(v) => Ok(Value::F64(v.abs())),
            Value::Decimal(d) => Ok(Value::Decimal(d.abs())),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "numeric".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the ABS function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(AbsFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn test_abs_execute() {
        let func = AbsFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // Positive number stays positive
        assert_eq!(
            func.execute(&[Value::I32(42)], &context).unwrap(),
            Value::I32(42)
        );

        // Negative number becomes positive
        assert_eq!(
            func.execute(&[Value::I32(-42)], &context).unwrap(),
            Value::I32(42)
        );

        // Float
        assert_eq!(
            func.execute(&[Value::F64(-std::f64::consts::PI)], &context)
                .unwrap(),
            Value::F64(std::f64::consts::PI)
        );

        // Decimal
        let neg_decimal = Decimal::from_str("-123.45").unwrap();
        let pos_decimal = Decimal::from_str("123.45").unwrap();
        assert_eq!(
            func.execute(&[Value::Decimal(neg_decimal)], &context)
                .unwrap(),
            Value::Decimal(pos_decimal)
        );

        // NULL
        assert_eq!(func.execute(&[Value::Null], &context).unwrap(), Value::Null);
    }
}
