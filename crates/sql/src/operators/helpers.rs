//! Helper functions for operator implementations

use crate::error::{Error, Result};
use crate::types::{DataType, Value};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// Unwrap nullable type to get inner type and nullability flag
pub fn unwrap_nullable(dt: &DataType) -> (&DataType, bool) {
    match dt {
        DataType::Nullable(inner) => (&**inner, true),
        other => (other, false),
    }
}

/// Unwrap nullable types for a pair
pub fn unwrap_nullable_pair<'a>(
    left: &'a DataType,
    right: &'a DataType,
) -> (&'a DataType, &'a DataType, bool) {
    let (left_inner, left_null) = unwrap_nullable(left);
    let (right_inner, right_null) = unwrap_nullable(right);
    (left_inner, right_inner, left_null || right_null)
}

/// Wrap type in nullable if needed
pub fn wrap_nullable(dt: DataType, nullable: bool) -> DataType {
    if nullable {
        DataType::Nullable(Box::new(dt))
    } else {
        dt
    }
}

/// Promote numeric types to a common type for operations
pub fn promote_numeric_types(left: &DataType, right: &DataType) -> Result<DataType> {
    use DataType::*;

    // Check both are numeric
    if !left.is_numeric() || !right.is_numeric() {
        return Err(Error::TypeMismatch {
            expected: "numeric types".into(),
            found: format!("{:?} and {:?}", left, right),
        });
    }

    let result = match (left, right) {
        // Same types
        (I8, I8) => I8,
        (I16, I16) => I16,
        (I32, I32) => I32,
        (I64, I64) => I64,
        (I128, I128) => I128,
        (U8, U8) => U8,
        (U16, U16) => U16,
        (U32, U32) => U32,
        (U64, U64) => U64,
        (U128, U128) => U128,
        (F32, F32) => F32,
        (F64, F64) => F64,
        (Decimal(p1, s1), Decimal(p2, s2)) => {
            // Use the larger precision/scale
            let precision = match (p1, p2) {
                (Some(p1), Some(p2)) => Some((*p1).max(*p2)),
                (Some(p), None) | (None, Some(p)) => Some(*p),
                (None, None) => None,
            };
            let scale = match (s1, s2) {
                (Some(s1), Some(s2)) => Some((*s1).max(*s2)),
                (Some(s), None) | (None, Some(s)) => Some(*s),
                (None, None) => None,
            };
            Decimal(precision, scale)
        }

        // Signed integer promotions
        (I8, I16) | (I16, I8) => I16,
        (I8, I32) | (I32, I8) | (I16, I32) | (I32, I16) => I32,
        (I8, I64) | (I64, I8) | (I16, I64) | (I64, I16) | (I32, I64) | (I64, I32) => I64,
        (I8, I128)
        | (I128, I8)
        | (I16, I128)
        | (I128, I16)
        | (I32, I128)
        | (I128, I32)
        | (I64, I128)
        | (I128, I64) => I128,

        // Unsigned integer promotions
        (U8, U16) | (U16, U8) => U16,
        (U8, U32) | (U32, U8) | (U16, U32) | (U32, U16) => U32,
        (U8, U64) | (U64, U8) | (U16, U64) | (U64, U16) | (U32, U64) | (U64, U32) => U64,
        (U8, U128)
        | (U128, U8)
        | (U16, U128)
        | (U128, U16)
        | (U32, U128)
        | (U128, U32)
        | (U64, U128)
        | (U128, U64) => U128,

        // Mixed signed/unsigned - promote to larger signed type
        (I8, U8) | (U8, I8) => I16,
        (I16, U8) | (U8, I16) | (I16, U16) | (U16, I16) => I32,
        (I32, U8) | (U8, I32) | (I32, U16) | (U16, I32) | (I32, U32) | (U32, I32) => I64,
        (I64, U8)
        | (U8, I64)
        | (I64, U16)
        | (U16, I64)
        | (I64, U32)
        | (U32, I64)
        | (I64, U64)
        | (U64, I64) => I128,

        // I128 with smaller unsigned - promote to I128
        (I128, U8)
        | (U8, I128)
        | (I128, U16)
        | (U16, I128)
        | (I128, U32)
        | (U32, I128)
        | (I128, U64)
        | (U64, I128) => I128,

        // I128 with U128 - special case: if I128 is non-negative literal, allow U128
        // This handles cases like adding positive literals to U128 columns
        (I128, U128) | (U128, I128) => U128,

        // Float promotions
        (F32, F64) | (F64, F32) => F64,

        // Integer to float
        (I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64, F32)
        | (F32, I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64) => F32,
        (I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64 | I128 | U128, F64)
        | (F64, I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64 | I128 | U128) => F64,

        // Integer/Float to Decimal
        (I8 | I16 | I32 | I64 | I128 | U8 | U16 | U32 | U64 | U128, Decimal(_, _))
        | (Decimal(_, _), I8 | I16 | I32 | I64 | I128 | U8 | U16 | U32 | U64 | U128) => {
            Decimal(None, None)
        }
        (F32 | F64, Decimal(_, _)) | (Decimal(_, _), F32 | F64) => Decimal(None, None),

        _ => {
            return Err(Error::TypeMismatch {
                expected: format!("{:?}", left),
                found: format!("{:?}", right),
            });
        }
    };

    Ok(result)
}

/// Promote integer types only (no floats/decimals)
pub fn promote_integer_types(left: &DataType, right: &DataType) -> Result<DataType> {
    use DataType::*;

    // Check both are integers
    if !left.is_integer() || !right.is_integer() {
        return Err(Error::TypeMismatch {
            expected: "integer types".into(),
            found: format!("{:?} and {:?}", left, right),
        });
    }

    Ok(match (left, right) {
        // Same types
        (I8, I8) => I8,
        (I16, I16) => I16,
        (I32, I32) => I32,
        (I64, I64) => I64,
        (I128, I128) => I128,
        (U8, U8) => U8,
        (U16, U16) => U16,
        (U32, U32) => U32,
        (U64, U64) => U64,
        (U128, U128) => U128,

        // Signed integer promotions
        (I8, I16) | (I16, I8) => I16,
        (I8, I32) | (I32, I8) | (I16, I32) | (I32, I16) => I32,
        (I8, I64) | (I64, I8) | (I16, I64) | (I64, I16) | (I32, I64) | (I64, I32) => I64,
        (I8, I128)
        | (I128, I8)
        | (I16, I128)
        | (I128, I16)
        | (I32, I128)
        | (I128, I32)
        | (I64, I128)
        | (I128, I64) => I128,

        // Unsigned integer promotions
        (U8, U16) | (U16, U8) => U16,
        (U8, U32) | (U32, U8) | (U16, U32) | (U32, U16) => U32,
        (U8, U64) | (U64, U8) | (U16, U64) | (U64, U16) | (U32, U64) | (U64, U32) => U64,
        (U8, U128)
        | (U128, U8)
        | (U16, U128)
        | (U128, U16)
        | (U32, U128)
        | (U128, U32)
        | (U64, U128)
        | (U128, U64) => U128,

        // Mixed signed/unsigned - promote to larger signed type
        (I8, U8) | (U8, I8) => I16,
        (I16, U8) | (U8, I16) | (I16, U16) | (U16, I16) => I32,
        (I32, U8) | (U8, I32) | (I32, U16) | (U16, I32) | (I32, U32) | (U32, I32) => I64,
        (I64, U8)
        | (U8, I64)
        | (I64, U16)
        | (U16, I64)
        | (I64, U32)
        | (U32, I64)
        | (I64, U64)
        | (U64, I64) => I128,

        _ => {
            return Err(Error::TypeMismatch {
                expected: format!("{:?}", left),
                found: format!("{:?}", right),
            });
        }
    })
}

/// Helper to convert any numeric value to f32
pub fn to_f32(value: &Value) -> Result<f32> {
    use Value::*;
    Ok(match value {
        I8(n) => *n as f32,
        I16(n) => *n as f32,
        I32(n) => *n as f32,
        I64(n) => *n as f32,
        I128(n) => *n as f32,
        U8(n) => *n as f32,
        U16(n) => *n as f32,
        U32(n) => *n as f32,
        U64(n) => *n as f32,
        U128(n) => *n as f32,
        F32(n) => *n,
        F64(n) => *n as f32,
        Decimal(d) => d
            .to_f32()
            .ok_or_else(|| Error::InvalidValue("Decimal to f32 conversion failed".into()))?,
        _ => return Err(Error::InvalidValue("Cannot convert to f32".into())),
    })
}

/// Helper to convert any numeric value to f64
pub fn to_f64(value: &Value) -> Result<f64> {
    use Value::*;
    Ok(match value {
        I8(n) => *n as f64,
        I16(n) => *n as f64,
        I32(n) => *n as f64,
        I64(n) => *n as f64,
        I128(n) => *n as f64,
        U8(n) => *n as f64,
        U16(n) => *n as f64,
        U32(n) => *n as f64,
        U64(n) => *n as f64,
        U128(n) => *n as f64,
        F32(n) => *n as f64,
        F64(n) => *n,
        Decimal(d) => d
            .to_f64()
            .ok_or_else(|| Error::InvalidValue("Decimal to f64 conversion failed".into()))?,
        _ => return Err(Error::InvalidValue("Cannot convert to f64".into())),
    })
}

/// Helper to convert any numeric value to Decimal
pub fn to_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::I8(n) => Some(Decimal::from(*n)),
        Value::I16(n) => Some(Decimal::from(*n)),
        Value::I32(n) => Some(Decimal::from(*n)),
        Value::I64(n) => Some(Decimal::from(*n)),
        Value::I128(n) => Some(Decimal::from(*n)),
        Value::U8(n) => Some(Decimal::from(*n)),
        Value::U16(n) => Some(Decimal::from(*n)),
        Value::U32(n) => Some(Decimal::from(*n)),
        Value::U64(n) => Some(Decimal::from(*n)),
        Value::U128(n) => Some(Decimal::from(*n)),
        Value::F32(n) => Decimal::from_f32_retain(*n),
        Value::F64(n) => Decimal::from_f64_retain(*n),
        Value::Decimal(d) => Some(*d),
        _ => None,
    }
}
