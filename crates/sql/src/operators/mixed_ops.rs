//! Generic helpers for mixed integer type operations
//!
//! This module provides systematic handling of operations between different integer types,
//! especially unsigned + signed combinations, preserving the smallest type that can hold the result.

use crate::error::{Error, Result};
use crate::types::Value;

/// Add two integer values with proper type promotion
pub fn add_integers(left: &Value, right: &Value) -> Result<Value> {
    use Value::*;

    match (left, right) {
        // Same type operations - straightforward
        (I8(a), I8(b)) => a
            .checked_add(*b)
            .map(I8)
            .ok_or_else(|| Error::InvalidValue("I8 overflow".into())),
        (I16(a), I16(b)) => a
            .checked_add(*b)
            .map(I16)
            .ok_or_else(|| Error::InvalidValue("I16 overflow".into())),
        (I32(a), I32(b)) => a
            .checked_add(*b)
            .map(I32)
            .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
        (I64(a), I64(b)) => a
            .checked_add(*b)
            .map(I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (I128(a), I128(b)) => a
            .checked_add(*b)
            .map(I128)
            .ok_or_else(|| Error::InvalidValue("I128 overflow".into())),

        (U8(a), U8(b)) => a
            .checked_add(*b)
            .map(U8)
            .ok_or_else(|| Error::InvalidValue("U8 overflow".into())),
        (U16(a), U16(b)) => a
            .checked_add(*b)
            .map(U16)
            .ok_or_else(|| Error::InvalidValue("U16 overflow".into())),
        (U32(a), U32(b)) => a
            .checked_add(*b)
            .map(U32)
            .ok_or_else(|| Error::InvalidValue("U32 overflow".into())),
        (U64(a), U64(b)) => a
            .checked_add(*b)
            .map(U64)
            .ok_or_else(|| Error::InvalidValue("U64 overflow".into())),
        (U128(a), U128(b)) => a
            .checked_add(*b)
            .map(U128)
            .ok_or_else(|| Error::InvalidValue("U128 overflow".into())),

        // Unsigned + Signed: preserve unsigned type when safe
        (U8(a), b) | (b, U8(a)) if b.is_signed_integer() => {
            add_unsigned_signed(*a as u128, to_i128_value(b)?, 8)
        }
        (U16(a), b) | (b, U16(a)) if b.is_signed_integer() => {
            add_unsigned_signed(*a as u128, to_i128_value(b)?, 16)
        }
        (U32(a), b) | (b, U32(a)) if b.is_signed_integer() => {
            add_unsigned_signed(*a as u128, to_i128_value(b)?, 32)
        }
        (U64(a), b) | (b, U64(a)) if b.is_signed_integer() => {
            add_unsigned_signed(*a as u128, to_i128_value(b)?, 64)
        }
        (U128(a), b) | (b, U128(a)) if b.is_signed_integer() => {
            let b_val = to_i128_value(b)?;
            if b_val < 0 {
                return Err(Error::InvalidValue("Cannot add negative to U128".into()));
            }
            a.checked_add(b_val as u128)
                .map(U128)
                .ok_or_else(|| Error::InvalidValue("U128 overflow".into()))
        }

        // Mixed signed types - promote to larger
        (I8(a), I16(b)) => (*a as i16)
            .checked_add(*b)
            .map(I16)
            .ok_or_else(|| Error::InvalidValue("I16 overflow".into())),
        (I16(a), I8(b)) => a
            .checked_add(*b as i16)
            .map(I16)
            .ok_or_else(|| Error::InvalidValue("I16 overflow".into())),
        (I8(a), I32(b)) => (*a as i32)
            .checked_add(*b)
            .map(I32)
            .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
        (I32(a), I8(b)) => a
            .checked_add(*b as i32)
            .map(I32)
            .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
        (I8(a), I64(b)) => (*a as i64)
            .checked_add(*b)
            .map(I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (I64(a), I8(b)) => a
            .checked_add(*b as i64)
            .map(I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (I16(a), I32(b)) => (*a as i32)
            .checked_add(*b)
            .map(I32)
            .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
        (I32(a), I16(b)) => a
            .checked_add(*b as i32)
            .map(I32)
            .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
        (I16(a), I64(b)) => (*a as i64)
            .checked_add(*b)
            .map(I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (I64(a), I16(b)) => a
            .checked_add(*b as i64)
            .map(I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (I32(a), I64(b)) => (*a as i64)
            .checked_add(*b)
            .map(I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (I64(a), I32(b)) => a
            .checked_add(*b as i64)
            .map(I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),

        // Mixed unsigned types - promote to larger
        (U8(a), U16(b)) => (*a as u16)
            .checked_add(*b)
            .map(U16)
            .ok_or_else(|| Error::InvalidValue("U16 overflow".into())),
        (U16(a), U8(b)) => a
            .checked_add(*b as u16)
            .map(U16)
            .ok_or_else(|| Error::InvalidValue("U16 overflow".into())),
        (U8(a), U32(b)) => (*a as u32)
            .checked_add(*b)
            .map(U32)
            .ok_or_else(|| Error::InvalidValue("U32 overflow".into())),
        (U32(a), U8(b)) => a
            .checked_add(*b as u32)
            .map(U32)
            .ok_or_else(|| Error::InvalidValue("U32 overflow".into())),
        (U8(a), U64(b)) => (*a as u64)
            .checked_add(*b)
            .map(U64)
            .ok_or_else(|| Error::InvalidValue("U64 overflow".into())),
        (U64(a), U8(b)) => a
            .checked_add(*b as u64)
            .map(U64)
            .ok_or_else(|| Error::InvalidValue("U64 overflow".into())),
        (U16(a), U32(b)) => (*a as u32)
            .checked_add(*b)
            .map(U32)
            .ok_or_else(|| Error::InvalidValue("U32 overflow".into())),
        (U32(a), U16(b)) => a
            .checked_add(*b as u32)
            .map(U32)
            .ok_or_else(|| Error::InvalidValue("U32 overflow".into())),
        (U16(a), U64(b)) => (*a as u64)
            .checked_add(*b)
            .map(U64)
            .ok_or_else(|| Error::InvalidValue("U64 overflow".into())),
        (U64(a), U16(b)) => a
            .checked_add(*b as u64)
            .map(U64)
            .ok_or_else(|| Error::InvalidValue("U64 overflow".into())),
        (U32(a), U64(b)) => (*a as u64)
            .checked_add(*b)
            .map(U64)
            .ok_or_else(|| Error::InvalidValue("U64 overflow".into())),
        (U64(a), U32(b)) => a
            .checked_add(*b as u64)
            .map(U64)
            .ok_or_else(|| Error::InvalidValue("U64 overflow".into())),

        _ => Err(Error::InvalidOperation(format!(
            "Cannot add {:?} and {:?}",
            left, right
        ))),
    }
}

/// Subtract right from left with proper type promotion
pub fn subtract_integers(left: &Value, right: &Value) -> Result<Value> {
    use Value::*;

    match (left, right) {
        // Same type operations
        (I8(a), I8(b)) => a
            .checked_sub(*b)
            .map(I8)
            .ok_or_else(|| Error::InvalidValue("I8 underflow".into())),
        (I16(a), I16(b)) => a
            .checked_sub(*b)
            .map(I16)
            .ok_or_else(|| Error::InvalidValue("I16 underflow".into())),
        (I32(a), I32(b)) => a
            .checked_sub(*b)
            .map(I32)
            .ok_or_else(|| Error::InvalidValue("I32 underflow".into())),
        (I64(a), I64(b)) => a
            .checked_sub(*b)
            .map(I64)
            .ok_or_else(|| Error::InvalidValue("I64 underflow".into())),
        (I128(a), I128(b)) => a
            .checked_sub(*b)
            .map(I128)
            .ok_or_else(|| Error::InvalidValue("I128 underflow".into())),

        (U8(a), U8(b)) => a
            .checked_sub(*b)
            .map(U8)
            .ok_or_else(|| Error::InvalidValue("U8 underflow".into())),
        (U16(a), U16(b)) => a
            .checked_sub(*b)
            .map(U16)
            .ok_or_else(|| Error::InvalidValue("U16 underflow".into())),
        (U32(a), U32(b)) => a
            .checked_sub(*b)
            .map(U32)
            .ok_or_else(|| Error::InvalidValue("U32 underflow".into())),
        (U64(a), U64(b)) => a
            .checked_sub(*b)
            .map(U64)
            .ok_or_else(|| Error::InvalidValue("U64 underflow".into())),
        (U128(a), U128(b)) => a
            .checked_sub(*b)
            .map(U128)
            .ok_or_else(|| Error::InvalidValue("U128 underflow".into())),

        // Unsigned - Signed
        (U8(a), b) if b.is_signed_integer() => {
            subtract_unsigned_signed(*a as u128, to_i128_value(b)?, 8)
        }
        (U16(a), b) if b.is_signed_integer() => {
            subtract_unsigned_signed(*a as u128, to_i128_value(b)?, 16)
        }
        (U32(a), b) if b.is_signed_integer() => {
            subtract_unsigned_signed(*a as u128, to_i128_value(b)?, 32)
        }
        (U64(a), b) if b.is_signed_integer() => {
            subtract_unsigned_signed(*a as u128, to_i128_value(b)?, 64)
        }

        // Signed - Unsigned (need careful handling)
        (a, U8(b)) if a.is_signed_integer() => {
            let a_val = to_i128_value(a)?;
            let result = a_val
                .checked_sub(*b as i128)
                .ok_or_else(|| Error::InvalidValue("Integer underflow".into()))?;
            fit_signed_result(result)
        }
        (a, U16(b)) if a.is_signed_integer() => {
            let a_val = to_i128_value(a)?;
            let result = a_val
                .checked_sub(*b as i128)
                .ok_or_else(|| Error::InvalidValue("Integer underflow".into()))?;
            fit_signed_result(result)
        }
        (a, U32(b)) if a.is_signed_integer() => {
            let a_val = to_i128_value(a)?;
            let result = a_val
                .checked_sub(*b as i128)
                .ok_or_else(|| Error::InvalidValue("Integer underflow".into()))?;
            fit_signed_result(result)
        }
        (a, U64(b)) if a.is_signed_integer() => {
            let a_val = to_i128_value(a)?;
            let result = a_val
                .checked_sub(*b as i128)
                .ok_or_else(|| Error::InvalidValue("Integer underflow".into()))?;
            fit_signed_result(result)
        }

        // Mixed signed/unsigned promotions (similar patterns as add)
        _ => Err(Error::InvalidOperation(format!(
            "Cannot subtract {:?} from {:?}",
            right, left
        ))),
    }
}

/// Multiply two integer values with proper type promotion
pub fn multiply_integers(left: &Value, right: &Value) -> Result<Value> {
    use Value::*;

    match (left, right) {
        // Same type operations
        (I8(a), I8(b)) => a
            .checked_mul(*b)
            .map(I8)
            .ok_or_else(|| Error::InvalidValue("I8 overflow".into())),
        (I16(a), I16(b)) => a
            .checked_mul(*b)
            .map(I16)
            .ok_or_else(|| Error::InvalidValue("I16 overflow".into())),
        (I32(a), I32(b)) => a
            .checked_mul(*b)
            .map(I32)
            .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
        (I64(a), I64(b)) => a
            .checked_mul(*b)
            .map(I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (I128(a), I128(b)) => a
            .checked_mul(*b)
            .map(I128)
            .ok_or_else(|| Error::InvalidValue("I128 overflow".into())),

        (U8(a), U8(b)) => a
            .checked_mul(*b)
            .map(U8)
            .ok_or_else(|| Error::InvalidValue("U8 overflow".into())),
        (U16(a), U16(b)) => a
            .checked_mul(*b)
            .map(U16)
            .ok_or_else(|| Error::InvalidValue("U16 overflow".into())),
        (U32(a), U32(b)) => a
            .checked_mul(*b)
            .map(U32)
            .ok_or_else(|| Error::InvalidValue("U32 overflow".into())),
        (U64(a), U64(b)) => a
            .checked_mul(*b)
            .map(U64)
            .ok_or_else(|| Error::InvalidValue("U64 overflow".into())),
        (U128(a), U128(b)) => a
            .checked_mul(*b)
            .map(U128)
            .ok_or_else(|| Error::InvalidValue("U128 overflow".into())),

        // Unsigned * Signed
        (U8(a), b) | (b, U8(a)) if b.is_signed_integer() => {
            multiply_unsigned_signed(*a as u128, to_i128_value(b)?, 8)
        }
        (U16(a), b) | (b, U16(a)) if b.is_signed_integer() => {
            multiply_unsigned_signed(*a as u128, to_i128_value(b)?, 16)
        }
        (U32(a), b) | (b, U32(a)) if b.is_signed_integer() => {
            multiply_unsigned_signed(*a as u128, to_i128_value(b)?, 32)
        }
        (U64(a), b) | (b, U64(a)) if b.is_signed_integer() => {
            multiply_unsigned_signed(*a as u128, to_i128_value(b)?, 64)
        }

        // Mixed signed/unsigned promotions (add remaining cases)
        _ => add_integers(left, right).map_err(|_| {
            Error::InvalidOperation(format!("Cannot multiply {:?} and {:?}", left, right))
        }),
    }
}

/// Divide left by right with proper type promotion
pub fn divide_integers(left: &Value, right: &Value) -> Result<Value> {
    use Value::*;

    // Check for division by zero first
    if is_zero(right) {
        return Err(Error::InvalidOperation("Division by zero".into()));
    }

    match (left, right) {
        // Same type operations
        (I8(a), I8(b)) => a
            .checked_div(*b)
            .map(I8)
            .ok_or_else(|| Error::InvalidValue("I8 overflow".into())),
        (I16(a), I16(b)) => a
            .checked_div(*b)
            .map(I16)
            .ok_or_else(|| Error::InvalidValue("I16 overflow".into())),
        (I32(a), I32(b)) => a
            .checked_div(*b)
            .map(I32)
            .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
        (I64(a), I64(b)) => a
            .checked_div(*b)
            .map(I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (I128(a), I128(b)) => a
            .checked_div(*b)
            .map(I128)
            .ok_or_else(|| Error::InvalidValue("I128 overflow".into())),

        (U8(a), U8(b)) => a
            .checked_div(*b)
            .map(U8)
            .ok_or_else(|| Error::InvalidValue("U8 overflow".into())),
        (U16(a), U16(b)) => a
            .checked_div(*b)
            .map(U16)
            .ok_or_else(|| Error::InvalidValue("U16 overflow".into())),
        (U32(a), U32(b)) => a
            .checked_div(*b)
            .map(U32)
            .ok_or_else(|| Error::InvalidValue("U32 overflow".into())),
        (U64(a), U64(b)) => a
            .checked_div(*b)
            .map(U64)
            .ok_or_else(|| Error::InvalidValue("U64 overflow".into())),
        (U128(a), U128(b)) => a
            .checked_div(*b)
            .map(U128)
            .ok_or_else(|| Error::InvalidValue("U128 overflow".into())),

        // Unsigned / Signed
        (U8(a), b) if b.is_signed_integer() => {
            divide_unsigned_signed(*a as u128, to_i128_value(b)?, 8)
        }
        (U16(a), b) if b.is_signed_integer() => {
            divide_unsigned_signed(*a as u128, to_i128_value(b)?, 16)
        }
        (U32(a), b) if b.is_signed_integer() => {
            divide_unsigned_signed(*a as u128, to_i128_value(b)?, 32)
        }
        (U64(a), b) if b.is_signed_integer() => {
            divide_unsigned_signed(*a as u128, to_i128_value(b)?, 64)
        }

        // Signed / Unsigned
        (a, U8(b)) if a.is_signed_integer() => {
            let a_val = to_i128_value(a)?;
            if a_val < 0 {
                return Err(Error::InvalidValue(
                    "Cannot divide negative by unsigned".into(),
                ));
            }
            let result = (a_val as u128) / (*b as u128);
            fit_unsigned_result(result, 8)
        }
        (a, U16(b)) if a.is_signed_integer() => {
            let a_val = to_i128_value(a)?;
            if a_val < 0 {
                return Err(Error::InvalidValue(
                    "Cannot divide negative by unsigned".into(),
                ));
            }
            let result = (a_val as u128) / (*b as u128);
            fit_unsigned_result(result, 16)
        }
        (a, U32(b)) if a.is_signed_integer() => {
            let a_val = to_i128_value(a)?;
            if a_val < 0 {
                return Err(Error::InvalidValue(
                    "Cannot divide negative by unsigned".into(),
                ));
            }
            let result = (a_val as u128) / (*b as u128);
            fit_unsigned_result(result, 32)
        }
        (a, U64(b)) if a.is_signed_integer() => {
            let a_val = to_i128_value(a)?;
            if a_val < 0 {
                return Err(Error::InvalidValue(
                    "Cannot divide negative by unsigned".into(),
                ));
            }
            let result = (a_val as u128) / (*b as u128);
            fit_unsigned_result(result, 64)
        }

        // Mixed integer promotions
        _ => Err(Error::InvalidOperation(format!(
            "Cannot divide {:?} by {:?}",
            left, right
        ))),
    }
}

/// Compute remainder of left / right with proper type promotion
pub fn remainder_integers(left: &Value, right: &Value) -> Result<Value> {
    use Value::*;

    // Check for modulo by zero first
    if is_zero(right) {
        return Err(Error::InvalidOperation("Modulo by zero".into()));
    }

    match (left, right) {
        // Same type operations
        (I8(a), I8(b)) => a
            .checked_rem(*b)
            .map(I8)
            .ok_or_else(|| Error::InvalidValue("I8 remainder error".into())),
        (I16(a), I16(b)) => a
            .checked_rem(*b)
            .map(I16)
            .ok_or_else(|| Error::InvalidValue("I16 remainder error".into())),
        (I32(a), I32(b)) => a
            .checked_rem(*b)
            .map(I32)
            .ok_or_else(|| Error::InvalidValue("I32 remainder error".into())),
        (I64(a), I64(b)) => a
            .checked_rem(*b)
            .map(I64)
            .ok_or_else(|| Error::InvalidValue("I64 remainder error".into())),
        (I128(a), I128(b)) => a
            .checked_rem(*b)
            .map(I128)
            .ok_or_else(|| Error::InvalidValue("I128 remainder error".into())),

        (U8(a), U8(b)) => Ok(U8(a % b)),
        (U16(a), U16(b)) => Ok(U16(a % b)),
        (U32(a), U32(b)) => Ok(U32(a % b)),
        (U64(a), U64(b)) => Ok(U64(a % b)),
        (U128(a), U128(b)) => Ok(U128(a % b)),

        // Unsigned % Signed
        (U8(a), b) if b.is_signed_integer() => {
            remainder_unsigned_signed(*a as u128, to_i128_value(b)?, 8)
        }
        (U16(a), b) if b.is_signed_integer() => {
            remainder_unsigned_signed(*a as u128, to_i128_value(b)?, 16)
        }
        (U32(a), b) if b.is_signed_integer() => {
            remainder_unsigned_signed(*a as u128, to_i128_value(b)?, 32)
        }
        (U64(a), b) if b.is_signed_integer() => {
            remainder_unsigned_signed(*a as u128, to_i128_value(b)?, 64)
        }

        // Signed % Unsigned
        (a, U8(b)) if a.is_signed_integer() => {
            let a_val = to_i128_value(a)?;
            if a_val < 0 {
                return Err(Error::InvalidValue(
                    "Cannot compute remainder of negative by unsigned".into(),
                ));
            }
            let result = (a_val as u128) % (*b as u128);
            fit_unsigned_result(result, 8)
        }
        (a, U16(b)) if a.is_signed_integer() => {
            let a_val = to_i128_value(a)?;
            if a_val < 0 {
                return Err(Error::InvalidValue(
                    "Cannot compute remainder of negative by unsigned".into(),
                ));
            }
            let result = (a_val as u128) % (*b as u128);
            fit_unsigned_result(result, 16)
        }
        (a, U32(b)) if a.is_signed_integer() => {
            let a_val = to_i128_value(a)?;
            if a_val < 0 {
                return Err(Error::InvalidValue(
                    "Cannot compute remainder of negative by unsigned".into(),
                ));
            }
            let result = (a_val as u128) % (*b as u128);
            fit_unsigned_result(result, 32)
        }
        (a, U64(b)) if a.is_signed_integer() => {
            let a_val = to_i128_value(a)?;
            if a_val < 0 {
                return Err(Error::InvalidValue(
                    "Cannot compute remainder of negative by unsigned".into(),
                ));
            }
            let result = (a_val as u128) % (*b as u128);
            fit_unsigned_result(result, 64)
        }

        _ => Err(Error::InvalidOperation(format!(
            "Cannot compute remainder of {:?} by {:?}",
            left, right
        ))),
    }
}

// Helper functions

fn add_unsigned_signed(unsigned: u128, signed: i128, unsigned_bits: u8) -> Result<Value> {
    if signed < 0 {
        // Subtracting from unsigned
        let abs_signed = signed.unsigned_abs();
        if abs_signed > unsigned {
            // Result would be negative
            return Err(Error::InvalidValue("Result would be negative".into()));
        }
        let result = unsigned - abs_signed;
        fit_unsigned_result(result, unsigned_bits)
    } else {
        // Adding to unsigned
        let result = unsigned
            .checked_add(signed as u128)
            .ok_or_else(|| Error::InvalidValue("Integer overflow".into()))?;
        fit_unsigned_result(result, unsigned_bits)
    }
}

fn subtract_unsigned_signed(unsigned: u128, signed: i128, unsigned_bits: u8) -> Result<Value> {
    if signed < 0 {
        // Subtracting negative = adding positive
        let abs_signed = signed.unsigned_abs();
        let result = unsigned
            .checked_add(abs_signed)
            .ok_or_else(|| Error::InvalidValue("Integer overflow".into()))?;
        fit_unsigned_result(result, unsigned_bits)
    } else {
        // Subtracting positive
        let signed_u128 = signed as u128;
        if signed_u128 > unsigned {
            // Result would be negative
            return Err(Error::InvalidValue("Result would be negative".into()));
        }
        let result = unsigned - signed_u128;
        fit_unsigned_result(result, unsigned_bits)
    }
}

fn multiply_unsigned_signed(unsigned: u128, signed: i128, unsigned_bits: u8) -> Result<Value> {
    if signed < 0 {
        return Err(Error::InvalidValue(
            "Cannot multiply unsigned by negative".into(),
        ));
    }

    let result = unsigned
        .checked_mul(signed as u128)
        .ok_or_else(|| Error::InvalidValue("Integer overflow".into()))?;
    fit_unsigned_result(result, unsigned_bits)
}

fn divide_unsigned_signed(unsigned: u128, signed: i128, unsigned_bits: u8) -> Result<Value> {
    if signed < 0 {
        return Err(Error::InvalidValue(
            "Cannot divide unsigned by negative".into(),
        ));
    }
    if signed == 0 {
        return Err(Error::InvalidOperation("Division by zero".into()));
    }

    let result = unsigned / (signed as u128);
    fit_unsigned_result(result, unsigned_bits)
}

fn remainder_unsigned_signed(unsigned: u128, signed: i128, unsigned_bits: u8) -> Result<Value> {
    if signed < 0 {
        return Err(Error::InvalidValue(
            "Cannot compute remainder with negative divisor".into(),
        ));
    }
    if signed == 0 {
        return Err(Error::InvalidOperation("Modulo by zero".into()));
    }

    let result = unsigned % (signed as u128);
    fit_unsigned_result(result, unsigned_bits)
}

fn is_zero(value: &Value) -> bool {
    use Value::*;
    match value {
        I8(n) => *n == 0,
        I16(n) => *n == 0,
        I32(n) => *n == 0,
        I64(n) => *n == 0,
        I128(n) => *n == 0,
        U8(n) => *n == 0,
        U16(n) => *n == 0,
        U32(n) => *n == 0,
        U64(n) => *n == 0,
        U128(n) => *n == 0,
        _ => false,
    }
}

fn fit_unsigned_result(value: u128, prefer_bits: u8) -> Result<Value> {
    use Value::*;

    // Try to fit in the preferred size first, then go larger as needed
    match prefer_bits {
        8 => {
            if value <= u8::MAX as u128 {
                return Ok(U8(value as u8));
            }
            if value <= u16::MAX as u128 {
                return Ok(U16(value as u16));
            }
            if value <= u32::MAX as u128 {
                return Ok(U32(value as u32));
            }
            if value <= u64::MAX as u128 {
                return Ok(U64(value as u64));
            }
            Ok(U128(value))
        }
        16 => {
            if value <= u16::MAX as u128 {
                return Ok(U16(value as u16));
            }
            if value <= u32::MAX as u128 {
                return Ok(U32(value as u32));
            }
            if value <= u64::MAX as u128 {
                return Ok(U64(value as u64));
            }
            Ok(U128(value))
        }
        32 => {
            if value <= u32::MAX as u128 {
                return Ok(U32(value as u32));
            }
            if value <= u64::MAX as u128 {
                return Ok(U64(value as u64));
            }
            Ok(U128(value))
        }
        64 => {
            if value <= u64::MAX as u128 {
                return Ok(U64(value as u64));
            }
            Ok(U128(value))
        }
        _ => Ok(U128(value)),
    }
}

fn fit_signed_result(value: i128) -> Result<Value> {
    use Value::*;

    // Find the smallest signed type that fits
    if value >= i8::MIN as i128 && value <= i8::MAX as i128 {
        Ok(I8(value as i8))
    } else if value >= i16::MIN as i128 && value <= i16::MAX as i128 {
        Ok(I16(value as i16))
    } else if value >= i32::MIN as i128 && value <= i32::MAX as i128 {
        Ok(I32(value as i32))
    } else if value >= i64::MIN as i128 && value <= i64::MAX as i128 {
        Ok(I64(value as i64))
    } else {
        Ok(I128(value))
    }
}

fn to_i128_value(value: &Value) -> Result<i128> {
    use Value::*;

    match value {
        I8(n) => Ok(*n as i128),
        I16(n) => Ok(*n as i128),
        I32(n) => Ok(*n as i128),
        I64(n) => Ok(*n as i128),
        I128(n) => Ok(*n),
        _ => Err(Error::InvalidValue("Not a signed integer".into())),
    }
}
