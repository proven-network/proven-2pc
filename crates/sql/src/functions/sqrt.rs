//! SQRT and POWER functions - mathematical functions

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::data_type::DataType;
use crate::types::value::Value;

pub struct SqrtFunction;
pub struct PowerFunction;

impl Function for SqrtFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "SQRT",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "SQRT takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            DataType::I8
            | DataType::I16
            | DataType::I32
            | DataType::I64
            | DataType::I128
            | DataType::U8
            | DataType::U16
            | DataType::U32
            | DataType::U64
            | DataType::U128
            | DataType::F32
            | DataType::F64
            | DataType::Decimal(_, _) => {
                // SQRT always returns F64
                Ok(DataType::F64)
            }
            DataType::Nullable(inner) => {
                // Check inner type is numeric
                self.validate(&[(**inner).clone()])?;
                Ok(DataType::Nullable(Box::new(DataType::F64)))
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
                "SQRT takes exactly 1 argument".into(),
            ));
        }

        let value = match &args[0] {
            Value::I8(v) => *v as f64,
            Value::I16(v) => *v as f64,
            Value::I32(v) => *v as f64,
            Value::I64(v) => *v as f64,
            Value::I128(v) => *v as f64,
            Value::U8(v) => *v as f64,
            Value::U16(v) => *v as f64,
            Value::U32(v) => *v as f64,
            Value::U64(v) => *v as f64,
            Value::U128(v) => *v as f64,
            Value::F32(v) => *v as f64,
            Value::F64(v) => *v,
            Value::Decimal(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_f64().unwrap_or(0.0)
            }
            Value::Null => return Ok(Value::Null),
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "numeric".into(),
                    found: args[0].data_type().to_string(),
                });
            }
        };

        if value < 0.0 {
            return Err(Error::ExecutionError("SQRT of negative number".into()));
        }

        Ok(Value::F64(value.sqrt()))
    }
}

impl Function for PowerFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "POWER",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(Error::ExecutionError(format!(
                "POWER takes exactly 2 arguments, got {}",
                arg_types.len()
            )));
        }

        // Both arguments must be numeric
        for arg_type in arg_types {
            match arg_type {
                DataType::I8
                | DataType::I16
                | DataType::I32
                | DataType::I64
                | DataType::I128
                | DataType::U8
                | DataType::U16
                | DataType::U32
                | DataType::U64
                | DataType::U128
                | DataType::F32
                | DataType::F64
                | DataType::Decimal(_, _) => {}
                DataType::Nullable(inner) => match inner.as_ref() {
                    DataType::I8
                    | DataType::I16
                    | DataType::I32
                    | DataType::I64
                    | DataType::I128
                    | DataType::U8
                    | DataType::U16
                    | DataType::U32
                    | DataType::U64
                    | DataType::U128
                    | DataType::F32
                    | DataType::F64
                    | DataType::Decimal(_, _) => {
                        return Ok(DataType::Nullable(Box::new(DataType::F64)));
                    }
                    _ => {
                        return Err(Error::TypeMismatch {
                            expected: "numeric type".into(),
                            found: arg_type.to_string(),
                        });
                    }
                },
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "numeric type".into(),
                        found: arg_type.to_string(),
                    });
                }
            }
        }

        // POWER always returns F64
        Ok(DataType::F64)
    }

    fn execute(&self, args: &[Value], _context: &TransactionContext) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::ExecutionError(
                "POWER takes exactly 2 arguments".into(),
            ));
        }

        // Handle NULLs
        if args.iter().any(|v| matches!(v, Value::Null)) {
            return Ok(Value::Null);
        }

        let base = match &args[0] {
            Value::I8(v) => *v as f64,
            Value::I16(v) => *v as f64,
            Value::I32(v) => *v as f64,
            Value::I64(v) => *v as f64,
            Value::I128(v) => *v as f64,
            Value::U8(v) => *v as f64,
            Value::U16(v) => *v as f64,
            Value::U32(v) => *v as f64,
            Value::U64(v) => *v as f64,
            Value::U128(v) => *v as f64,
            Value::F32(v) => *v as f64,
            Value::F64(v) => *v,
            Value::Decimal(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_f64().unwrap_or(0.0)
            }
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "numeric".into(),
                    found: args[0].data_type().to_string(),
                });
            }
        };

        let exponent = match &args[1] {
            Value::I8(v) => *v as f64,
            Value::I16(v) => *v as f64,
            Value::I32(v) => *v as f64,
            Value::I64(v) => *v as f64,
            Value::I128(v) => *v as f64,
            Value::U8(v) => *v as f64,
            Value::U16(v) => *v as f64,
            Value::U32(v) => *v as f64,
            Value::U64(v) => *v as f64,
            Value::U128(v) => *v as f64,
            Value::F32(v) => *v as f64,
            Value::F64(v) => *v,
            Value::Decimal(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_f64().unwrap_or(0.0)
            }
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "numeric".into(),
                    found: args[1].data_type().to_string(),
                });
            }
        };

        Ok(Value::F64(base.powf(exponent)))
    }
}

/// Register the SQRT and POWER functions
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(SqrtFunction));
    registry.register(Box::new(PowerFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_sqrt_execute() {
        let func = SqrtFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // Perfect square
        assert_eq!(
            func.execute(&[Value::I32(9)], &context).unwrap(),
            Value::F64(3.0)
        );

        // Non-perfect square
        assert_eq!(
            func.execute(&[Value::I32(2)], &context).unwrap(),
            Value::F64(std::f64::consts::SQRT_2)
        );

        // Zero
        assert_eq!(
            func.execute(&[Value::I32(0)], &context).unwrap(),
            Value::F64(0.0)
        );

        // Float input
        assert_eq!(
            func.execute(&[Value::F64(16.0)], &context).unwrap(),
            Value::F64(4.0)
        );

        // Negative number should error
        assert!(func.execute(&[Value::I32(-1)], &context).is_err());

        // NULL handling
        assert_eq!(func.execute(&[Value::Null], &context).unwrap(), Value::Null);
    }

    #[test]
    fn test_power_execute() {
        let func = PowerFunction;
        let context = TransactionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)));

        // Integer power
        assert_eq!(
            func.execute(&[Value::I32(2), Value::I32(3)], &context)
                .unwrap(),
            Value::F64(8.0)
        );

        // Float power
        assert_eq!(
            func.execute(&[Value::F64(2.0), Value::F64(0.5)], &context)
                .unwrap(),
            Value::F64(std::f64::consts::SQRT_2)
        );

        // Zero exponent
        assert_eq!(
            func.execute(&[Value::I32(5), Value::I32(0)], &context)
                .unwrap(),
            Value::F64(1.0)
        );

        // Negative exponent
        assert_eq!(
            func.execute(&[Value::I32(2), Value::I32(-2)], &context)
                .unwrap(),
            Value::F64(0.25)
        );

        // NULL handling
        assert_eq!(
            func.execute(&[Value::Null, Value::I32(2)], &context)
                .unwrap(),
            Value::Null
        );
    }
}
