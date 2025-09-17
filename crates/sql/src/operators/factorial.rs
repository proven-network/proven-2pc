//! Factorial operator implementation

use super::helpers::{unwrap_nullable, wrap_nullable};
use super::traits::UnaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct FactorialOperator;

impl UnaryOperator for FactorialOperator {
    fn name(&self) -> &'static str {
        "factorial"
    }

    fn symbol(&self) -> &'static str {
        "!"
    }

    fn validate(&self, operand: &DataType) -> Result<DataType> {
        use DataType::*;

        let (inner, nullable) = unwrap_nullable(operand);

        let result = match inner {
            // Only non-negative integers can have factorial
            // Result is promoted to larger type to handle growth
            I8 | I16 | I32 => I64,
            I64 => I128,
            I128 => I128, // Can't promote further
            U8 | U16 | U32 => U64,
            U64 => U128,
            U128 => U128, // Can't promote further

            _ => {
                return Err(Error::InvalidValue(format!(
                    "Factorial requires integer type, got: {}",
                    operand
                )));
            }
        };

        Ok(wrap_nullable(result, nullable))
    }

    fn execute(&self, operand: &Value) -> Result<Value> {
        use Value::*;

        match operand {
            // NULL handling
            Null => Ok(Null),

            // Signed integers - check for non-negative
            I8(n) => {
                if *n < 0 {
                    return Err(Error::InvalidValue("Factorial of negative number".into()));
                }
                factorial_i64(*n as i64)
            }
            I16(n) => {
                if *n < 0 {
                    return Err(Error::InvalidValue("Factorial of negative number".into()));
                }
                factorial_i64(*n as i64)
            }
            I32(n) => {
                if *n < 0 {
                    return Err(Error::InvalidValue("Factorial of negative number".into()));
                }
                factorial_i64(*n as i64)
            }
            I64(n) => {
                if *n < 0 {
                    return Err(Error::InvalidValue("Factorial of negative number".into()));
                }
                factorial_i128(*n as i128)
            }
            I128(n) => {
                if *n < 0 {
                    return Err(Error::InvalidValue("Factorial of negative number".into()));
                }
                factorial_i128(*n)
            }

            // Unsigned integers
            U8(n) => factorial_u64(*n as u64),
            U16(n) => factorial_u64(*n as u64),
            U32(n) => factorial_u64(*n as u64),
            U64(n) => factorial_u128(*n as u128),
            U128(n) => factorial_u128(*n),

            _ => Err(Error::InvalidValue(format!(
                "Cannot compute factorial of non-integer value: {:?}",
                operand
            ))),
        }
    }
}

/// Compute factorial for i64 with overflow checking
fn factorial_i64(n: i64) -> Result<Value> {
    if n < 0 {
        return Err(Error::InvalidValue("Factorial of negative number".into()));
    }
    if n > 20 {
        return Err(Error::InvalidValue("Factorial overflow (n > 20)".into()));
    }

    let mut result = 1i64;
    for i in 2..=n {
        result = result
            .checked_mul(i)
            .ok_or_else(|| Error::InvalidValue("Factorial overflow".into()))?;
    }
    Ok(Value::I64(result))
}

/// Compute factorial for i128 with overflow checking
fn factorial_i128(n: i128) -> Result<Value> {
    if n < 0 {
        return Err(Error::InvalidValue("Factorial of negative number".into()));
    }
    if n > 33 {
        return Err(Error::InvalidValue("Factorial overflow (n > 33)".into()));
    }

    let mut result = 1i128;
    for i in 2..=n {
        result = result
            .checked_mul(i)
            .ok_or_else(|| Error::InvalidValue("Factorial overflow".into()))?;
    }
    Ok(Value::I128(result))
}

/// Compute factorial for u64 with overflow checking
fn factorial_u64(n: u64) -> Result<Value> {
    if n > 20 {
        return Err(Error::InvalidValue("Factorial overflow (n > 20)".into()));
    }

    let mut result = 1u64;
    for i in 2..=n {
        result = result
            .checked_mul(i)
            .ok_or_else(|| Error::InvalidValue("Factorial overflow".into()))?;
    }
    Ok(Value::U64(result))
}

/// Compute factorial for u128 with overflow checking
fn factorial_u128(n: u128) -> Result<Value> {
    if n > 34 {
        return Err(Error::InvalidValue("Factorial overflow (n > 34)".into()));
    }

    let mut result = 1u128;
    for i in 2..=n {
        result = result
            .checked_mul(i)
            .ok_or_else(|| Error::InvalidValue("Factorial overflow".into()))?;
    }
    Ok(Value::U128(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factorial() {
        let op = FactorialOperator;

        // Type validation
        assert_eq!(op.validate(&DataType::I32).unwrap(), DataType::I64);
        assert_eq!(op.validate(&DataType::U32).unwrap(), DataType::U64);

        // Execution
        assert_eq!(op.execute(&Value::I32(0)).unwrap(), Value::I64(1));
        assert_eq!(op.execute(&Value::I32(1)).unwrap(), Value::I64(1));
        assert_eq!(op.execute(&Value::I32(5)).unwrap(), Value::I64(120));
        assert_eq!(op.execute(&Value::U8(6)).unwrap(), Value::U64(720));

        // Negative number error
        assert!(op.execute(&Value::I32(-1)).is_err());

        // Overflow error
        assert!(op.execute(&Value::U64(100)).is_err());
    }
}
