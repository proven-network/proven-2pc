//! ROUND function - rounds numeric values

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

pub struct RoundFunction;

impl Function for RoundFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "ROUND",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return Err(Error::ExecutionError(format!(
                "ROUND takes 1 or 2 arguments, got {}",
                arg_types.len()
            )));
        }

        // First argument must be numeric or NULL
        match &arg_types[0] {
            DataType::I8
            | DataType::I16
            | DataType::I32
            | DataType::I64
            | DataType::I128
            | DataType::F32
            | DataType::F64
            | DataType::Decimal(_, _) => {
                // Second argument (if present) must be integer
                if arg_types.len() == 2 {
                    match &arg_types[1] {
                        DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 => {}
                        DataType::Nullable(inner) => match inner.as_ref() {
                            DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 => {}
                            _ => {
                                return Err(Error::TypeMismatch {
                                    expected: "integer for precision".into(),
                                    found: arg_types[1].to_string(),
                                });
                            }
                        },
                        _ => {
                            return Err(Error::TypeMismatch {
                                expected: "integer for precision".into(),
                                found: arg_types[1].to_string(),
                            });
                        }
                    }
                }
                Ok(arg_types[0].clone())
            }
            DataType::Null => {
                // NULL literal returns nullable numeric
                Ok(DataType::Nullable(Box::new(DataType::F64)))
            }
            DataType::Nullable(inner) => {
                // Recursively validate
                match inner.as_ref() {
                    DataType::Null => Ok(DataType::Nullable(Box::new(DataType::F64))),
                    _ => {
                        let inner_types = if arg_types.len() == 2 {
                            vec![(**inner).clone(), arg_types[1].clone()]
                        } else {
                            vec![(**inner).clone()]
                        };
                        let inner_result = self.validate(&inner_types)?;
                        Ok(DataType::Nullable(Box::new(inner_result)))
                    }
                }
            }
            _ => Err(Error::TypeMismatch {
                expected: "numeric type".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::ExecutionError("ROUND takes 1 or 2 arguments".into()));
        }

        let precision = if args.len() == 2 {
            match &args[1] {
                Value::I8(i) => *i as i32,
                Value::I16(i) => *i as i32,
                Value::I32(i) => *i,
                Value::I64(i) => *i as i32,
                Value::Null => return Ok(Value::Null),
                _ => {
                    return Err(Error::ExecutionError(
                        "ROUND precision must be an integer".into(),
                    ));
                }
            }
        } else {
            0
        };

        match &args[0] {
            Value::Decimal(d) => {
                let rounded = d.round_dp(precision.max(0) as u32);
                Ok(Value::Decimal(rounded))
            }
            Value::F32(f) => {
                let multiplier = 10_f32.powi(precision);
                let rounded = (f * multiplier).round() / multiplier;
                Ok(Value::F32(rounded))
            }
            Value::F64(f) => {
                let multiplier = 10_f64.powi(precision);
                let rounded = (f * multiplier).round() / multiplier;
                Ok(Value::F64(rounded))
            }
            // Integers are already rounded
            Value::I8(i) => Ok(Value::I8(*i)),
            Value::I16(i) => Ok(Value::I16(*i)),
            Value::I32(i) => Ok(Value::I32(*i)),
            Value::I64(i) => Ok(Value::I64(*i)),
            Value::I128(i) => Ok(Value::I128(*i)),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "numeric".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the ROUND function
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(RoundFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn test_round_execute() {
        let func = RoundFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Round float to integer
        assert_eq!(
            func.execute(&[Value::F64(3.7)], &context).unwrap(),
            Value::F64(4.0)
        );

        assert_eq!(
            func.execute(&[Value::F64(3.2)], &context).unwrap(),
            Value::F64(3.0)
        );

        // Round to 2 decimal places
        assert_eq!(
            func.execute(&[Value::F64(42.453), Value::I32(2)], &context)
                .unwrap(),
            Value::F64(42.45)
        );

        // Round decimal
        let decimal = Decimal::from_str("123.456").unwrap();
        let rounded = Decimal::from_str("123.46").unwrap();
        assert_eq!(
            func.execute(&[Value::Decimal(decimal), Value::I32(2)], &context)
                .unwrap(),
            Value::Decimal(rounded)
        );

        // Integer stays the same
        assert_eq!(
            func.execute(&[Value::I32(42)], &context).unwrap(),
            Value::I32(42)
        );

        // NULL
        assert_eq!(func.execute(&[Value::Null], &context).unwrap(), Value::Null);
    }
}
