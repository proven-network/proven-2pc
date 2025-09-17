//! Exponentiation (power) operator implementation

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};
use rust_decimal::prelude::*;

pub struct ExponentiateOperator;

impl BinaryOperator for ExponentiateOperator {
    fn name(&self) -> &'static str {
        "exponentiation"
    }

    fn symbol(&self) -> &'static str {
        "^"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        false // Exponentiation is NOT commutative (a^b != b^a)
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, nullable) = unwrap_nullable_pair(left, right);

        let result = match (left_inner, right_inner) {
            // Integer exponentiation - promote to larger int or float for large exponents
            (I8 | I16 | I32, I8 | I16 | I32) => I64,
            (I64 | I128, _) if right_inner.is_integer() => I128,
            (_, I64 | I128) if left_inner.is_integer() => I128,

            // Unsigned integer exponentiation
            (U8 | U16 | U32, U8 | U16 | U32) => U64,
            (U64 | U128, _) if right_inner.is_unsigned_integer() => U128,
            (_, U64 | U128) if left_inner.is_unsigned_integer() => U128,

            // Float exponentiation
            (F32, F32) => F32,
            (F64, F64) => F64,
            (F32, F64) | (F64, F32) => F64,

            // Decimal exponentiation - not well supported, convert to F64
            (Decimal(_, _), _) | (_, Decimal(_, _))
                if left_inner.is_numeric() && right_inner.is_numeric() =>
            {
                F64
            }

            // Any numeric type - promote to float
            (a, b) if a.is_numeric() && b.is_numeric() => F64,

            _ => {
                return Err(Error::InvalidValue(format!(
                    "Cannot compute {} to the power of {}",
                    left, right
                )));
            }
        };

        Ok(wrap_nullable(result, nullable))
    }

    fn execute(&self, left: &Value, right: &Value) -> Result<Value> {
        use Value::*;

        match (left, right) {
            // NULL handling
            (Null, _) | (_, Null) => Ok(Null),

            // Special cases
            (_, n) if is_zero(n) => {
                // Any number to the power of 0 is 1
                match left {
                    I8(_) => Ok(I8(1)),
                    I16(_) => Ok(I16(1)),
                    I32(_) => Ok(I32(1)),
                    I64(_) => Ok(I64(1)),
                    I128(_) => Ok(I128(1)),
                    U8(_) => Ok(U8(1)),
                    U16(_) => Ok(U16(1)),
                    U32(_) => Ok(U32(1)),
                    U64(_) => Ok(U64(1)),
                    U128(_) => Ok(U128(1)),
                    F32(_) => Ok(F32(1.0)),
                    F64(_) => Ok(F64(1.0)),
                    Decimal(_) => Ok(Decimal(rust_decimal::Decimal::ONE)),
                    _ => Ok(I32(1)),
                }
            }
            (n, _) if is_zero(n) => {
                // 0 to any positive power is 0
                match right {
                    I8(e) if *e > 0 => Ok(I8(0)),
                    I16(e) if *e > 0 => Ok(I16(0)),
                    I32(e) if *e > 0 => Ok(I32(0)),
                    I64(e) if *e > 0 => Ok(I64(0)),
                    I128(e) if *e > 0 => Ok(I128(0)),
                    U8(_) | U16(_) | U32(_) | U64(_) | U128(_) => Ok(n.clone()),
                    F32(e) if *e > 0.0 => Ok(F32(0.0)),
                    F64(e) if *e > 0.0 => Ok(F64(0.0)),
                    _ => {
                        // 0 to negative power is undefined (would be infinity)
                        Err(Error::InvalidValue(
                            "0 raised to negative power".to_string(),
                        ))
                    }
                }
            }

            // Integer exponentiation for small values
            (I32(base), I32(exp)) if *exp >= 0 && *exp < 20 => pow_i64(*base as i64, *exp as u32),
            (I64(base), I32(exp)) if *exp >= 0 && *exp < 40 => pow_i128(*base as i128, *exp as u32),

            // Float exponentiation
            (F32(a), F32(b)) => {
                let result = a.powf(*b);
                if !result.is_finite() {
                    return Err(Error::InvalidValue("Float overflow or NaN".into()));
                }
                Ok(F32(result))
            }
            (F64(a), F64(b)) => {
                let result = a.powf(*b);
                if !result.is_finite() {
                    return Err(Error::InvalidValue("Float overflow or NaN".into()));
                }
                Ok(F64(result))
            }

            // Convert to float for mixed or complex cases
            (a, b) if a.is_numeric() && b.is_numeric() => {
                let base = to_f64(a).ok_or_else(|| {
                    Error::InvalidValue(format!("Cannot convert {:?} to float", a))
                })?;
                let exp = to_f64(b).ok_or_else(|| {
                    Error::InvalidValue(format!("Cannot convert {:?} to float", b))
                })?;
                let result = base.powf(exp);
                if !result.is_finite() {
                    return Err(Error::InvalidValue("Float overflow or NaN".into()));
                }
                Ok(F64(result))
            }

            _ => Err(Error::InvalidValue(format!(
                "Cannot compute {:?} to the power of {:?}",
                left, right
            ))),
        }
    }
}

/// Helper function for integer exponentiation with overflow checking
fn pow_i64(base: i64, exp: u32) -> Result<Value> {
    let mut result = 1i64;
    let mut b = base;
    let mut e = exp;

    while e > 0 {
        if e & 1 == 1 {
            result = result
                .checked_mul(b)
                .ok_or_else(|| Error::InvalidValue("Integer overflow in exponentiation".into()))?;
        }
        e >>= 1;
        if e > 0 {
            b = b
                .checked_mul(b)
                .ok_or_else(|| Error::InvalidValue("Integer overflow in exponentiation".into()))?;
        }
    }

    Ok(Value::I64(result))
}

/// Helper function for i128 exponentiation with overflow checking
fn pow_i128(base: i128, exp: u32) -> Result<Value> {
    let mut result = 1i128;
    let mut b = base;
    let mut e = exp;

    while e > 0 {
        if e & 1 == 1 {
            result = result
                .checked_mul(b)
                .ok_or_else(|| Error::InvalidValue("Integer overflow in exponentiation".into()))?;
        }
        e >>= 1;
        if e > 0 {
            b = b
                .checked_mul(b)
                .ok_or_else(|| Error::InvalidValue("Integer overflow in exponentiation".into()))?;
        }
    }

    Ok(Value::I128(result))
}

/// Check if a value is zero
fn is_zero(value: &Value) -> bool {
    match value {
        Value::I8(n) => *n == 0,
        Value::I16(n) => *n == 0,
        Value::I32(n) => *n == 0,
        Value::I64(n) => *n == 0,
        Value::I128(n) => *n == 0,
        Value::U8(n) => *n == 0,
        Value::U16(n) => *n == 0,
        Value::U32(n) => *n == 0,
        Value::U64(n) => *n == 0,
        Value::U128(n) => *n == 0,
        Value::F32(n) => *n == 0.0,
        Value::F64(n) => *n == 0.0,
        Value::Decimal(d) => d.is_zero(),
        _ => false,
    }
}

/// Helper to convert any numeric value to f64
fn to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::I8(n) => Some(*n as f64),
        Value::I16(n) => Some(*n as f64),
        Value::I32(n) => Some(*n as f64),
        Value::I64(n) => Some(*n as f64),
        Value::I128(n) => Some(*n as f64),
        Value::U8(n) => Some(*n as f64),
        Value::U16(n) => Some(*n as f64),
        Value::U32(n) => Some(*n as f64),
        Value::U64(n) => Some(*n as f64),
        Value::U128(n) => Some(*n as f64),
        Value::F32(n) => Some(*n as f64),
        Value::F64(n) => Some(*n),
        Value::Decimal(d) => d.to_f64(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponentiate_numeric() {
        let op = ExponentiateOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::I32, &DataType::I32).unwrap(),
            DataType::I64
        );
        assert_eq!(
            op.validate(&DataType::F32, &DataType::F32).unwrap(),
            DataType::F32
        );

        // Execution
        assert_eq!(
            op.execute(&Value::I32(2), &Value::I32(3)).unwrap(),
            Value::I64(8)
        );
        assert_eq!(
            op.execute(&Value::I32(5), &Value::I32(0)).unwrap(),
            Value::I32(1)
        );

        // 0^0 is typically defined as 1 in SQL
        assert_eq!(
            op.execute(&Value::I32(0), &Value::I32(0)).unwrap(),
            Value::I32(1)
        );
    }
}
