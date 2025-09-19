//! Exponentiation (power) operator implementation

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

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
            // NULL with anything returns NULL
            (Null, _) | (_, Null) => Null,

            // Decimal or numeric types - exponentiation typically converts to F64
            // since Decimal doesn't have native pow support
            (Decimal(_, _), _) | (_, Decimal(_, _))
                if left_inner.is_numeric() && right_inner.is_numeric() =>
            {
                F64
            }

            // Float64 with any numeric type -> Float64 (PostgreSQL behavior)
            (F64, t) | (t, F64) if t.is_numeric() => F64,

            // Float32 with any numeric type (except F64) -> Float32
            (F32, t) | (t, F32) if t.is_numeric() => F32,

            // Integer exponentiation - for small integer exponents, can stay integer
            // but for safety and PostgreSQL compatibility, promote to float
            (a, b) if a.is_integer() && b.is_integer() => F64,

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

            // Mixed types - handle based on PostgreSQL behavior
            (a, b) if a.is_numeric() && b.is_numeric() => exponentiate_mixed_numeric(a, b),

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

/// Helper function to handle mixed numeric type exponentiation
fn exponentiate_mixed_numeric(base: &Value, exp: &Value) -> Result<Value> {
    use Value::*;

    // Exponentiation is complex, so we typically convert to float
    // Following PostgreSQL behavior: preserve F64/F32 when mixed with integers

    match (base, exp) {
        // F64 base with any numeric exponent -> F64
        (F64(b), e) if e.is_numeric() => {
            let exponent = to_f64(e)?;
            let result = b.powf(exponent);
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float overflow or NaN".into()));
            }
            Ok(F64(result))
        }

        // Any numeric base with F64 exponent -> F64
        (b, F64(e)) if b.is_numeric() => {
            let base_val = to_f64(b)?;
            let result = base_val.powf(*e);
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float overflow or NaN".into()));
            }
            Ok(F64(result))
        }

        // F32 base with numeric (non-F64) exponent -> F32
        (F32(b), e) if e.is_numeric() => {
            let exponent = to_f32(e)?;
            let result = b.powf(exponent);
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float overflow or NaN".into()));
            }
            Ok(F32(result))
        }

        // Numeric base with F32 exponent -> F32
        (b, F32(e)) if b.is_numeric() && !matches!(b, F64(_)) => {
            let base_val = to_f32(b)?;
            let result = base_val.powf(*e);
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float overflow or NaN".into()));
            }
            Ok(F32(result))
        }

        // All other cases (including Decimal, integer^integer) -> convert to F64
        _ => {
            let base_val = to_f64(base)?;
            let exp_val = to_f64(exp)?;
            let result = base_val.powf(exp_val);
            if !result.is_finite() {
                return Err(Error::InvalidValue("Float overflow or NaN".into()));
            }
            Ok(F64(result))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponentiate_integers() {
        let op = ExponentiateOperator;

        // Type validation - integer ^ integer -> F64 for safety
        assert_eq!(
            op.validate(&DataType::I32, &DataType::I32).unwrap(),
            DataType::F64
        );

        // Execution - small integer powers are handled specially
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

        // Larger powers convert to float
        let result = op.execute(&Value::I32(2), &Value::I32(50)).unwrap();
        match result {
            Value::F64(v) => {
                assert!((v - 2_f64.powi(50)).abs() < 1e-10);
            }
            _ => panic!("Expected F64 for large exponent"),
        }
    }

    #[test]
    fn test_exponentiate_float64_with_integer() {
        let op = ExponentiateOperator;

        // Type validation - F64 ^ integer -> F64
        assert_eq!(
            op.validate(&DataType::F64, &DataType::I32).unwrap(),
            DataType::F64
        );
        assert_eq!(
            op.validate(&DataType::I32, &DataType::F64).unwrap(),
            DataType::F64
        );

        // Execution - should stay as F64
        let result = op.execute(&Value::F64(2.5), &Value::I32(2)).unwrap();
        assert_eq!(result, Value::F64(6.25));

        let result = op.execute(&Value::I32(2), &Value::F64(3.0)).unwrap();
        assert_eq!(result, Value::F64(8.0));

        // Test with non-integer exponent
        let result = op.execute(&Value::F64(4.0), &Value::F64(0.5)).unwrap();
        assert_eq!(result, Value::F64(2.0)); // Square root of 4
    }

    #[test]
    fn test_exponentiate_float32_with_integer() {
        let op = ExponentiateOperator;

        // Type validation - F32 ^ integer -> F32
        assert_eq!(
            op.validate(&DataType::F32, &DataType::I32).unwrap(),
            DataType::F32
        );

        // Execution
        let result = op.execute(&Value::F32(2.0), &Value::I32(3)).unwrap();
        assert_eq!(result, Value::F32(8.0));

        let result = op.execute(&Value::I16(3), &Value::F32(2.0)).unwrap();
        assert_eq!(result, Value::F32(9.0));
    }

    #[test]
    fn test_exponentiate_float64_with_float32() {
        let op = ExponentiateOperator;

        // Type validation - F64 ^ F32 -> F64
        assert_eq!(
            op.validate(&DataType::F64, &DataType::F32).unwrap(),
            DataType::F64
        );
        assert_eq!(
            op.validate(&DataType::F32, &DataType::F64).unwrap(),
            DataType::F64
        );

        // Execution
        let result = op.execute(&Value::F64(2.0), &Value::F32(3.0)).unwrap();
        assert_eq!(result, Value::F64(8.0));

        let result = op.execute(&Value::F32(4.0), &Value::F64(0.5)).unwrap();
        assert_eq!(result, Value::F64(2.0));
    }

    #[test]
    fn test_exponentiate_decimal_converts_to_float() {
        let op = ExponentiateOperator;
        use rust_decimal::Decimal;

        // Type validation - Decimal ^ anything -> F64
        assert_eq!(
            op.validate(&DataType::Decimal(None, None), &DataType::I32)
                .unwrap(),
            DataType::F64
        );

        // Execution - Decimal base converts to F64
        let decimal_2 = Decimal::from(2);
        let result = op
            .execute(&Value::Decimal(decimal_2), &Value::I32(3))
            .unwrap();
        match result {
            Value::F64(v) => {
                assert_eq!(v, 8.0);
            }
            _ => panic!("Expected F64 for Decimal exponentiation"),
        }

        // Decimal exponent also converts to F64
        let decimal_3 = Decimal::from(3);
        let result = op
            .execute(&Value::I32(2), &Value::Decimal(decimal_3))
            .unwrap();
        match result {
            Value::F64(v) => {
                assert_eq!(v, 8.0);
            }
            _ => panic!("Expected F64 for Decimal exponent"),
        }
    }

    #[test]
    fn test_exponentiate_special_cases() {
        let op = ExponentiateOperator;

        // x^0 = 1
        assert_eq!(
            op.execute(&Value::F64(5.5), &Value::I32(0)).unwrap(),
            Value::F64(1.0)
        );

        // 0^x where x > 0 = 0
        let result = op.execute(&Value::I32(0), &Value::I32(5)).unwrap();
        match result {
            Value::I32(0) | Value::I64(0) => {}
            Value::F64(0.0) => {}
            _ => panic!("Expected 0, got {:?}", result),
        }

        // 0^negative should error (would be infinity)
        assert!(op.execute(&Value::I32(0), &Value::I32(-1)).is_err());

        // 1^anything = 1
        assert_eq!(
            op.execute(&Value::F64(1.0), &Value::F64(999.0)).unwrap(),
            Value::F64(1.0)
        );
    }

    #[test]
    fn test_exponentiate_null_handling() {
        let op = ExponentiateOperator;

        assert_eq!(
            op.execute(&Value::Null, &Value::I32(2)).unwrap(),
            Value::Null
        );
        assert_eq!(
            op.execute(&Value::F64(2.0), &Value::Null).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_exponentiate_overflow_detection() {
        let op = ExponentiateOperator;

        // Large exponentiation should error on overflow
        assert!(op.execute(&Value::F64(10.0), &Value::F64(1000.0)).is_err());
        assert!(op.execute(&Value::F32(10.0), &Value::F32(100.0)).is_err());

        // Integer overflow
        assert!(op.execute(&Value::I32(2), &Value::I32(100)).is_ok()); // Converts to float
        assert!(op.execute(&Value::I64(9999), &Value::I32(9999)).is_err()); // Would overflow even as float
    }
}
