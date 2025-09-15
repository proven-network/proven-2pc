//! Value evaluation operations
//!
//! This module provides arithmetic and comparison operations for SQL values,
//! keeping the Value type as pure data representation.

use super::DataType;
use super::value::Value;
use crate::error::{Error, Result};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::cmp::Ordering;

/// Macro to handle mixed integer type arithmetic operations
macro_rules! mixed_int_op {
    ($left:expr, $right:expr, $op:tt, $checked_op:ident, $op_name:expr) => {
        match ($left, $right) {
            // Mixed integer types - promote to larger type
            (Value::I8(a), Value::I16(b)) => (*a as i16)
                .$checked_op(*b)
                .map(Value::I16)
                .ok_or_else(|| Error::InvalidValue(format!("I16 {}", $op_name))),
            (Value::I16(a), Value::I8(b)) => a
                .$checked_op(*b as i16)
                .map(Value::I16)
                .ok_or_else(|| Error::InvalidValue(format!("I16 {}", $op_name))),
            (Value::I8(a), Value::I32(b)) => (*a as i32)
                .$checked_op(*b)
                .map(Value::I32)
                .ok_or_else(|| Error::InvalidValue(format!("I32 {}", $op_name))),
            (Value::I32(a), Value::I8(b)) => a
                .$checked_op(*b as i32)
                .map(Value::I32)
                .ok_or_else(|| Error::InvalidValue(format!("I32 {}", $op_name))),
            (Value::I8(a), Value::I64(b)) => (*a as i64)
                .$checked_op(*b)
                .map(Value::I64)
                .ok_or_else(|| Error::InvalidValue(format!("I64 {}", $op_name))),
            (Value::I64(a), Value::I8(b)) => a
                .$checked_op(*b as i64)
                .map(Value::I64)
                .ok_or_else(|| Error::InvalidValue(format!("I64 {}", $op_name))),
            (Value::I16(a), Value::I32(b)) => (*a as i32)
                .$checked_op(*b)
                .map(Value::I32)
                .ok_or_else(|| Error::InvalidValue(format!("I32 {}", $op_name))),
            (Value::I32(a), Value::I16(b)) => a
                .$checked_op(*b as i32)
                .map(Value::I32)
                .ok_or_else(|| Error::InvalidValue(format!("I32 {}", $op_name))),
            (Value::I16(a), Value::I64(b)) => (*a as i64)
                .$checked_op(*b)
                .map(Value::I64)
                .ok_or_else(|| Error::InvalidValue(format!("I64 {}", $op_name))),
            (Value::I64(a), Value::I16(b)) => a
                .$checked_op(*b as i64)
                .map(Value::I64)
                .ok_or_else(|| Error::InvalidValue(format!("I64 {}", $op_name))),
            (Value::I32(a), Value::I64(b)) => (*a as i64)
                .$checked_op(*b)
                .map(Value::I64)
                .ok_or_else(|| Error::InvalidValue(format!("I64 {}", $op_name))),
            (Value::I64(a), Value::I32(b)) => a
                .$checked_op(*b as i64)
                .map(Value::I64)
                .ok_or_else(|| Error::InvalidValue(format!("I64 {}", $op_name))),
            _ => Err(Error::TypeMismatch {
                expected: "matching integer types".into(),
                found: format!("{:?} and {:?}", $left, $right),
            }),
        }
    };
}

/// Helper to convert any numeric value to Decimal for mixed-type operations
fn to_decimal(value: &Value) -> Option<Decimal> {
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

/// Helper to convert any numeric value to f32
fn to_f32(value: &Value) -> Option<f32> {
    match value {
        Value::I8(n) => Some(*n as f32),
        Value::I16(n) => Some(*n as f32),
        Value::I32(n) => Some(*n as f32),
        Value::I64(n) => Some(*n as f32),
        Value::I128(n) => Some(*n as f32),
        Value::U8(n) => Some(*n as f32),
        Value::U16(n) => Some(*n as f32),
        Value::U32(n) => Some(*n as f32),
        Value::U64(n) => Some(*n as f32),
        Value::U128(n) => Some(*n as f32),
        Value::F32(n) => Some(*n),
        Value::F64(n) => Some(*n as f32),
        Value::Decimal(d) => d.to_f32(),
        _ => None,
    }
}

/// Check if a value is numeric
fn is_numeric(value: &Value) -> bool {
    matches!(
        value,
        Value::I8(_)
            | Value::I16(_)
            | Value::I32(_)
            | Value::I64(_)
            | Value::I128(_)
            | Value::U8(_)
            | Value::U16(_)
            | Value::U32(_)
            | Value::U64(_)
            | Value::U128(_)
            | Value::F32(_)
            | Value::F64(_)
            | Value::Decimal(_)
    )
}

/// Performs addition on two values
pub fn add(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),

        // String concatenation
        (Value::Str(a), Value::Str(b)) => Ok(Value::Str(format!("{}{}", a, b))),

        // Same-type integer operations with overflow checking
        (Value::I8(a), Value::I8(b)) => a
            .checked_add(*b)
            .map(Value::I8)
            .ok_or_else(|| Error::InvalidValue("I8 overflow".into())),
        (Value::I16(a), Value::I16(b)) => a
            .checked_add(*b)
            .map(Value::I16)
            .ok_or_else(|| Error::InvalidValue("I16 overflow".into())),
        (Value::I32(a), Value::I32(b)) => a
            .checked_add(*b)
            .map(Value::I32)
            .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
        (Value::I64(a), Value::I64(b)) => a
            .checked_add(*b)
            .map(Value::I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (Value::I128(a), Value::I128(b)) => a
            .checked_add(*b)
            .map(Value::I128)
            .ok_or_else(|| Error::InvalidValue("I128 overflow".into())),
        (Value::U8(a), Value::U8(b)) => a
            .checked_add(*b)
            .map(Value::U8)
            .ok_or_else(|| Error::InvalidValue("U8 overflow".into())),
        (Value::U16(a), Value::U16(b)) => a
            .checked_add(*b)
            .map(Value::U16)
            .ok_or_else(|| Error::InvalidValue("U16 overflow".into())),
        (Value::U32(a), Value::U32(b)) => a
            .checked_add(*b)
            .map(Value::U32)
            .ok_or_else(|| Error::InvalidValue("U32 overflow".into())),
        (Value::U64(a), Value::U64(b)) => a
            .checked_add(*b)
            .map(Value::U64)
            .ok_or_else(|| Error::InvalidValue("U64 overflow".into())),
        (Value::U128(a), Value::U128(b)) => a
            .checked_add(*b)
            .map(Value::U128)
            .ok_or_else(|| Error::InvalidValue("U128 overflow".into())),

        // Float operations
        (Value::F32(a), Value::F32(b)) => Ok(Value::F32(a + b)),
        (Value::F64(a), Value::F64(b)) => Ok(Value::F64(a + b)),

        // Decimal operations
        (Value::Decimal(a), Value::Decimal(b)) => Ok(Value::Decimal(a + b)),

        // Mixed unsigned + signed integer operations - preserve unsigned type when possible
        // U8 + signed
        (Value::U8(a), Value::I8(b)) if *b >= 0 => {
            let b_u8 = *b as u8;
            a.checked_add(b_u8)
                .map(Value::U8)
                .ok_or_else(|| Error::InvalidValue("U8 overflow".into()))
        }
        (Value::I8(a), Value::U8(b)) if *a >= 0 => {
            let a_u8 = *a as u8;
            b.checked_add(a_u8)
                .map(Value::U8)
                .ok_or_else(|| Error::InvalidValue("U8 overflow".into()))
        }
        (Value::U8(a), Value::I16(b)) if *b >= 0 && *b <= 255 => {
            let b_u8 = *b as u8;
            a.checked_add(b_u8)
                .map(Value::U8)
                .ok_or_else(|| Error::InvalidValue("U8 overflow".into()))
        }
        (Value::U8(a), Value::I32(b)) if *b >= 0 && *b <= 255 => {
            let b_u8 = *b as u8;
            a.checked_add(b_u8)
                .map(Value::U8)
                .ok_or_else(|| Error::InvalidValue("U8 overflow".into()))
        }
        (Value::U8(a), Value::I64(b)) if *b >= 0 && *b <= 255 => {
            let b_u8 = *b as u8;
            a.checked_add(b_u8)
                .map(Value::U8)
                .ok_or_else(|| Error::InvalidValue("U8 overflow".into()))
        }
        (Value::U8(a), Value::I128(b)) if *b >= 0 && *b <= 255 => {
            let b_u8 = *b as u8;
            a.checked_add(b_u8)
                .map(Value::U8)
                .ok_or_else(|| Error::InvalidValue("U8 overflow".into()))
        }

        // U16 + signed
        (Value::U16(a), Value::I8(b)) if *b >= 0 => {
            let b_u16 = *b as u16;
            a.checked_add(b_u16)
                .map(Value::U16)
                .ok_or_else(|| Error::InvalidValue("U16 overflow".into()))
        }
        (Value::U16(a), Value::I16(b)) if *b >= 0 => {
            let b_u16 = *b as u16;
            a.checked_add(b_u16)
                .map(Value::U16)
                .ok_or_else(|| Error::InvalidValue("U16 overflow".into()))
        }
        (Value::U16(a), Value::I32(b)) if *b >= 0 && *b <= 65535 => {
            let b_u16 = *b as u16;
            a.checked_add(b_u16)
                .map(Value::U16)
                .ok_or_else(|| Error::InvalidValue("U16 overflow".into()))
        }
        (Value::U16(a), Value::I64(b)) if *b >= 0 && *b <= 65535 => {
            let b_u16 = *b as u16;
            a.checked_add(b_u16)
                .map(Value::U16)
                .ok_or_else(|| Error::InvalidValue("U16 overflow".into()))
        }
        (Value::U16(a), Value::I128(b)) if *b >= 0 && *b <= 65535 => {
            let b_u16 = *b as u16;
            a.checked_add(b_u16)
                .map(Value::U16)
                .ok_or_else(|| Error::InvalidValue("U16 overflow".into()))
        }

        // U32 + signed
        (Value::U32(a), Value::I8(b)) if *b >= 0 => {
            let b_u32 = *b as u32;
            a.checked_add(b_u32)
                .map(Value::U32)
                .ok_or_else(|| Error::InvalidValue("U32 overflow".into()))
        }
        (Value::U32(a), Value::I16(b)) if *b >= 0 => {
            let b_u32 = *b as u32;
            a.checked_add(b_u32)
                .map(Value::U32)
                .ok_or_else(|| Error::InvalidValue("U32 overflow".into()))
        }
        (Value::U32(a), Value::I32(b)) if *b >= 0 => {
            let b_u32 = *b as u32;
            a.checked_add(b_u32)
                .map(Value::U32)
                .ok_or_else(|| Error::InvalidValue("U32 overflow".into()))
        }
        (Value::U32(a), Value::I64(b)) if *b >= 0 && *b <= u32::MAX as i64 => {
            let b_u32 = *b as u32;
            a.checked_add(b_u32)
                .map(Value::U32)
                .ok_or_else(|| Error::InvalidValue("U32 overflow".into()))
        }
        (Value::U32(a), Value::I128(b)) if *b >= 0 && *b <= u32::MAX as i128 => {
            let b_u32 = *b as u32;
            a.checked_add(b_u32)
                .map(Value::U32)
                .ok_or_else(|| Error::InvalidValue("U32 overflow".into()))
        }

        // U64 + signed
        (Value::U64(a), Value::I8(b)) if *b >= 0 => {
            let b_u64 = *b as u64;
            a.checked_add(b_u64)
                .map(Value::U64)
                .ok_or_else(|| Error::InvalidValue("U64 overflow".into()))
        }
        (Value::U64(a), Value::I16(b)) if *b >= 0 => {
            let b_u64 = *b as u64;
            a.checked_add(b_u64)
                .map(Value::U64)
                .ok_or_else(|| Error::InvalidValue("U64 overflow".into()))
        }
        (Value::U64(a), Value::I32(b)) if *b >= 0 => {
            let b_u64 = *b as u64;
            a.checked_add(b_u64)
                .map(Value::U64)
                .ok_or_else(|| Error::InvalidValue("U64 overflow".into()))
        }
        (Value::U64(a), Value::I64(b)) if *b >= 0 => {
            let b_u64 = *b as u64;
            a.checked_add(b_u64)
                .map(Value::U64)
                .ok_or_else(|| Error::InvalidValue("U64 overflow".into()))
        }
        (Value::U64(a), Value::I128(b)) if *b >= 0 && *b <= u64::MAX as i128 => {
            let b_u64 = *b as u64;
            a.checked_add(b_u64)
                .map(Value::U64)
                .ok_or_else(|| Error::InvalidValue("U64 overflow".into()))
        }

        // U128 + signed
        (Value::U128(a), Value::I8(b)) if *b >= 0 => {
            let b_u128 = *b as u128;
            a.checked_add(b_u128)
                .map(Value::U128)
                .ok_or_else(|| Error::InvalidValue("U128 overflow".into()))
        }
        (Value::U128(a), Value::I16(b)) if *b >= 0 => {
            let b_u128 = *b as u128;
            a.checked_add(b_u128)
                .map(Value::U128)
                .ok_or_else(|| Error::InvalidValue("U128 overflow".into()))
        }
        (Value::U128(a), Value::I32(b)) if *b >= 0 => {
            let b_u128 = *b as u128;
            a.checked_add(b_u128)
                .map(Value::U128)
                .ok_or_else(|| Error::InvalidValue("U128 overflow".into()))
        }
        (Value::U128(a), Value::I64(b)) if *b >= 0 => {
            let b_u128 = *b as u128;
            a.checked_add(b_u128)
                .map(Value::U128)
                .ok_or_else(|| Error::InvalidValue("U128 overflow".into()))
        }
        (Value::U128(a), Value::I128(b)) if *b >= 0 => {
            let b_u128 = *b as u128;
            a.checked_add(b_u128)
                .map(Value::U128)
                .ok_or_else(|| Error::InvalidValue("U128 overflow".into()))
        }

        // Mixed signed integer types - use macro
        (l, r)
            if matches!(
                (l, r),
                (Value::I8(_), Value::I16(_))
                    | (Value::I16(_), Value::I8(_))
                    | (Value::I8(_), Value::I32(_))
                    | (Value::I32(_), Value::I8(_))
                    | (Value::I8(_), Value::I64(_))
                    | (Value::I64(_), Value::I8(_))
                    | (Value::I16(_), Value::I32(_))
                    | (Value::I32(_), Value::I16(_))
                    | (Value::I16(_), Value::I64(_))
                    | (Value::I64(_), Value::I16(_))
                    | (Value::I32(_), Value::I64(_))
                    | (Value::I64(_), Value::I32(_))
            ) =>
        {
            mixed_int_op!(l, r, +, checked_add, "overflow")
        }

        // Date/Time + Interval operations
        (Value::Date(date), Value::Interval(interval))
        | (Value::Interval(interval), Value::Date(date)) => {
            use chrono::Duration;
            let new_date = *date
                + Duration::days(interval.days as i64)
                + Duration::days((interval.months * 30) as i64); // Approximate month as 30 days
            Ok(Value::Date(new_date))
        }
        (Value::Time(time), Value::Interval(interval))
        | (Value::Interval(interval), Value::Time(time)) => {
            use chrono::Duration;
            let duration = Duration::microseconds(interval.microseconds)
                + Duration::days(interval.days as i64)
                + Duration::days((interval.months * 30) as i64);
            let nanos = duration
                .num_nanoseconds()
                .ok_or_else(|| Error::InvalidValue("Interval too large".into()))?;
            let new_time = *time + Duration::nanoseconds(nanos % (24 * 3600 * 1_000_000_000)); // Wrap around 24 hours
            Ok(Value::Time(new_time))
        }
        (Value::Timestamp(ts), Value::Interval(interval))
        | (Value::Interval(interval), Value::Timestamp(ts)) => {
            use chrono::Duration;
            let new_ts = *ts
                + Duration::microseconds(interval.microseconds)
                + Duration::days(interval.days as i64)
                + Duration::days((interval.months * 30) as i64); // Approximate month as 30 days
            Ok(Value::Timestamp(new_ts))
        }

        // Interval + Interval
        (Value::Interval(a), Value::Interval(b)) => {
            Ok(Value::Interval(super::data_type::Interval {
                months: a.months + b.months,
                days: a.days + b.days,
                microseconds: a.microseconds + b.microseconds,
            }))
        }

        // Mixed numeric types - convert to Decimal
        (l, r) if is_numeric(l) && is_numeric(r) => {
            let l_dec = to_decimal(l)
                .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
            let r_dec = to_decimal(r)
                .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
            Ok(Value::Decimal(l_dec + r_dec))
        }

        _ => Err(Error::TypeMismatch {
            expected: "numeric, string, or date/time with interval".into(),
            found: format!("{:?} and {:?}", left, right),
        }),
    }
}

/// Performs subtraction on two values
pub fn subtract(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),

        // Same-type integer operations with overflow checking
        (Value::I8(a), Value::I8(b)) => a
            .checked_sub(*b)
            .map(Value::I8)
            .ok_or_else(|| Error::InvalidValue("I8 underflow".into())),
        (Value::I16(a), Value::I16(b)) => a
            .checked_sub(*b)
            .map(Value::I16)
            .ok_or_else(|| Error::InvalidValue("I16 underflow".into())),
        (Value::I32(a), Value::I32(b)) => a
            .checked_sub(*b)
            .map(Value::I32)
            .ok_or_else(|| Error::InvalidValue("I32 underflow".into())),
        (Value::I64(a), Value::I64(b)) => a
            .checked_sub(*b)
            .map(Value::I64)
            .ok_or_else(|| Error::InvalidValue("I64 underflow".into())),
        (Value::I128(a), Value::I128(b)) => a
            .checked_sub(*b)
            .map(Value::I128)
            .ok_or_else(|| Error::InvalidValue("I128 underflow".into())),
        (Value::U8(a), Value::U8(b)) => a
            .checked_sub(*b)
            .map(Value::U8)
            .ok_or_else(|| Error::InvalidValue("U8 underflow".into())),
        (Value::U16(a), Value::U16(b)) => a
            .checked_sub(*b)
            .map(Value::U16)
            .ok_or_else(|| Error::InvalidValue("U16 underflow".into())),
        (Value::U32(a), Value::U32(b)) => a
            .checked_sub(*b)
            .map(Value::U32)
            .ok_or_else(|| Error::InvalidValue("U32 underflow".into())),
        (Value::U64(a), Value::U64(b)) => a
            .checked_sub(*b)
            .map(Value::U64)
            .ok_or_else(|| Error::InvalidValue("U64 underflow".into())),
        (Value::U128(a), Value::U128(b)) => a
            .checked_sub(*b)
            .map(Value::U128)
            .ok_or_else(|| Error::InvalidValue("U128 underflow".into())),

        // Float operations
        (Value::F32(a), Value::F32(b)) => Ok(Value::F32(a - b)),
        (Value::F64(a), Value::F64(b)) => Ok(Value::F64(a - b)),

        // Decimal operations
        (Value::Decimal(a), Value::Decimal(b)) => Ok(Value::Decimal(a - b)),

        // Mixed unsigned - signed integer operations - preserve unsigned type when possible
        // U8 - signed
        (Value::U8(a), Value::I8(b)) if *b >= 0 && *b as u8 <= *a => {
            let b_u8 = *b as u8;
            a.checked_sub(b_u8)
                .map(Value::U8)
                .ok_or_else(|| Error::InvalidValue("U8 underflow".into()))
        }
        (Value::U8(a), Value::I16(b)) if *b >= 0 && *b <= 255 && *b as u8 <= *a => {
            let b_u8 = *b as u8;
            a.checked_sub(b_u8)
                .map(Value::U8)
                .ok_or_else(|| Error::InvalidValue("U8 underflow".into()))
        }
        (Value::U8(a), Value::I32(b)) if *b >= 0 && *b <= 255 && *b as u8 <= *a => {
            let b_u8 = *b as u8;
            a.checked_sub(b_u8)
                .map(Value::U8)
                .ok_or_else(|| Error::InvalidValue("U8 underflow".into()))
        }
        (Value::U8(a), Value::I64(b)) if *b >= 0 && *b <= 255 && *b as u8 <= *a => {
            let b_u8 = *b as u8;
            a.checked_sub(b_u8)
                .map(Value::U8)
                .ok_or_else(|| Error::InvalidValue("U8 underflow".into()))
        }
        (Value::U8(a), Value::I128(b)) if *b >= 0 && *b <= 255 && *b as u8 <= *a => {
            let b_u8 = *b as u8;
            a.checked_sub(b_u8)
                .map(Value::U8)
                .ok_or_else(|| Error::InvalidValue("U8 underflow".into()))
        }

        // U16 - signed
        (Value::U16(a), Value::I8(b)) if *b >= 0 => {
            let b_u16 = *b as u16;
            a.checked_sub(b_u16)
                .map(Value::U16)
                .ok_or_else(|| Error::InvalidValue("U16 underflow".into()))
        }
        (Value::U16(a), Value::I16(b)) if *b >= 0 => {
            let b_u16 = *b as u16;
            a.checked_sub(b_u16)
                .map(Value::U16)
                .ok_or_else(|| Error::InvalidValue("U16 underflow".into()))
        }
        (Value::U16(a), Value::I32(b)) if *b >= 0 && *b <= 65535 => {
            let b_u16 = *b as u16;
            a.checked_sub(b_u16)
                .map(Value::U16)
                .ok_or_else(|| Error::InvalidValue("U16 underflow".into()))
        }
        (Value::U16(a), Value::I64(b)) if *b >= 0 && *b <= 65535 => {
            let b_u16 = *b as u16;
            a.checked_sub(b_u16)
                .map(Value::U16)
                .ok_or_else(|| Error::InvalidValue("U16 underflow".into()))
        }
        (Value::U16(a), Value::I128(b)) if *b >= 0 && *b <= 65535 => {
            let b_u16 = *b as u16;
            a.checked_sub(b_u16)
                .map(Value::U16)
                .ok_or_else(|| Error::InvalidValue("U16 underflow".into()))
        }

        // U32 - signed
        (Value::U32(a), Value::I8(b)) if *b >= 0 => {
            let b_u32 = *b as u32;
            a.checked_sub(b_u32)
                .map(Value::U32)
                .ok_or_else(|| Error::InvalidValue("U32 underflow".into()))
        }
        (Value::U32(a), Value::I16(b)) if *b >= 0 => {
            let b_u32 = *b as u32;
            a.checked_sub(b_u32)
                .map(Value::U32)
                .ok_or_else(|| Error::InvalidValue("U32 underflow".into()))
        }
        (Value::U32(a), Value::I32(b)) if *b >= 0 => {
            let b_u32 = *b as u32;
            a.checked_sub(b_u32)
                .map(Value::U32)
                .ok_or_else(|| Error::InvalidValue("U32 underflow".into()))
        }
        (Value::U32(a), Value::I64(b)) if *b >= 0 && *b <= u32::MAX as i64 => {
            let b_u32 = *b as u32;
            a.checked_sub(b_u32)
                .map(Value::U32)
                .ok_or_else(|| Error::InvalidValue("U32 underflow".into()))
        }
        (Value::U32(a), Value::I128(b)) if *b >= 0 && *b <= u32::MAX as i128 => {
            let b_u32 = *b as u32;
            a.checked_sub(b_u32)
                .map(Value::U32)
                .ok_or_else(|| Error::InvalidValue("U32 underflow".into()))
        }

        // U64 - signed
        (Value::U64(a), Value::I8(b)) if *b >= 0 => {
            let b_u64 = *b as u64;
            a.checked_sub(b_u64)
                .map(Value::U64)
                .ok_or_else(|| Error::InvalidValue("U64 underflow".into()))
        }
        (Value::U64(a), Value::I16(b)) if *b >= 0 => {
            let b_u64 = *b as u64;
            a.checked_sub(b_u64)
                .map(Value::U64)
                .ok_or_else(|| Error::InvalidValue("U64 underflow".into()))
        }
        (Value::U64(a), Value::I32(b)) if *b >= 0 => {
            let b_u64 = *b as u64;
            a.checked_sub(b_u64)
                .map(Value::U64)
                .ok_or_else(|| Error::InvalidValue("U64 underflow".into()))
        }
        (Value::U64(a), Value::I64(b)) if *b >= 0 => {
            let b_u64 = *b as u64;
            a.checked_sub(b_u64)
                .map(Value::U64)
                .ok_or_else(|| Error::InvalidValue("U64 underflow".into()))
        }
        (Value::U64(a), Value::I128(b)) if *b >= 0 && *b <= u64::MAX as i128 => {
            let b_u64 = *b as u64;
            a.checked_sub(b_u64)
                .map(Value::U64)
                .ok_or_else(|| Error::InvalidValue("U64 underflow".into()))
        }

        // U128 - signed
        (Value::U128(a), Value::I8(b)) if *b >= 0 => {
            let b_u128 = *b as u128;
            a.checked_sub(b_u128)
                .map(Value::U128)
                .ok_or_else(|| Error::InvalidValue("U128 underflow".into()))
        }
        (Value::U128(a), Value::I16(b)) if *b >= 0 => {
            let b_u128 = *b as u128;
            a.checked_sub(b_u128)
                .map(Value::U128)
                .ok_or_else(|| Error::InvalidValue("U128 underflow".into()))
        }
        (Value::U128(a), Value::I32(b)) if *b >= 0 => {
            let b_u128 = *b as u128;
            a.checked_sub(b_u128)
                .map(Value::U128)
                .ok_or_else(|| Error::InvalidValue("U128 underflow".into()))
        }
        (Value::U128(a), Value::I64(b)) if *b >= 0 => {
            let b_u128 = *b as u128;
            a.checked_sub(b_u128)
                .map(Value::U128)
                .ok_or_else(|| Error::InvalidValue("U128 underflow".into()))
        }
        (Value::U128(a), Value::I128(b)) if *b >= 0 => {
            let b_u128 = *b as u128;
            a.checked_sub(b_u128)
                .map(Value::U128)
                .ok_or_else(|| Error::InvalidValue("U128 underflow".into()))
        }

        // Mixed signed integer types - use macro
        (l, r)
            if matches!(
                (l, r),
                (Value::I8(_), Value::I16(_))
                    | (Value::I16(_), Value::I8(_))
                    | (Value::I8(_), Value::I32(_))
                    | (Value::I32(_), Value::I8(_))
                    | (Value::I8(_), Value::I64(_))
                    | (Value::I64(_), Value::I8(_))
                    | (Value::I16(_), Value::I32(_))
                    | (Value::I32(_), Value::I16(_))
                    | (Value::I16(_), Value::I64(_))
                    | (Value::I64(_), Value::I16(_))
                    | (Value::I32(_), Value::I64(_))
                    | (Value::I64(_), Value::I32(_))
            ) =>
        {
            mixed_int_op!(l, r, -, checked_sub, "underflow")
        }

        // Date/Time - Interval operations
        (Value::Date(date), Value::Interval(interval)) => {
            use chrono::Duration;
            let new_date = *date
                - Duration::days(interval.days as i64)
                - Duration::days((interval.months * 30) as i64); // Approximate month as 30 days
            Ok(Value::Date(new_date))
        }
        (Value::Time(time), Value::Interval(interval)) => {
            use chrono::Duration;
            let duration = Duration::microseconds(interval.microseconds)
                + Duration::days(interval.days as i64)
                + Duration::days((interval.months * 30) as i64);
            let nanos = duration
                .num_nanoseconds()
                .ok_or_else(|| Error::InvalidValue("Interval too large".into()))?;
            let new_time = *time - Duration::nanoseconds(nanos % (24 * 3600 * 1_000_000_000)); // Wrap around 24 hours
            Ok(Value::Time(new_time))
        }
        (Value::Timestamp(ts), Value::Interval(interval)) => {
            use chrono::Duration;
            let new_ts = *ts
                - Duration::microseconds(interval.microseconds)
                - Duration::days(interval.days as i64)
                - Duration::days((interval.months * 30) as i64); // Approximate month as 30 days
            Ok(Value::Timestamp(new_ts))
        }

        // Date - Date = Interval (in days)
        (Value::Date(a), Value::Date(b)) => {
            let days = (*a - *b).num_days() as i32;
            Ok(Value::Interval(super::data_type::Interval {
                months: 0,
                days,
                microseconds: 0,
            }))
        }

        // Timestamp - Timestamp = Interval
        (Value::Timestamp(a), Value::Timestamp(b)) => {
            let duration = *a - *b;
            let days = duration.num_days() as i32;
            let microseconds = (duration - chrono::Duration::days(days as i64))
                .num_microseconds()
                .ok_or_else(|| Error::InvalidValue("Duration too large".into()))?;
            Ok(Value::Interval(super::data_type::Interval {
                months: 0,
                days,
                microseconds,
            }))
        }

        // Time - Time = Interval
        (Value::Time(a), Value::Time(b)) => {
            let duration = *a - *b;
            let microseconds = duration
                .num_microseconds()
                .ok_or_else(|| Error::InvalidValue("Duration too large".into()))?;
            Ok(Value::Interval(super::data_type::Interval {
                months: 0,
                days: 0,
                microseconds,
            }))
        }

        // Interval - Interval
        (Value::Interval(a), Value::Interval(b)) => {
            Ok(Value::Interval(super::data_type::Interval {
                months: a.months - b.months,
                days: a.days - b.days,
                microseconds: a.microseconds - b.microseconds,
            }))
        }

        // Mixed numeric types - convert to Decimal
        (l, r) if is_numeric(l) && is_numeric(r) => {
            let l_dec = to_decimal(l)
                .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
            let r_dec = to_decimal(r)
                .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
            Ok(Value::Decimal(l_dec - r_dec))
        }

        _ => Err(Error::TypeMismatch {
            expected: "numeric or date/time types".into(),
            found: format!("{:?} and {:?}", left, right),
        }),
    }
}

/// Performs multiplication on two values
pub fn multiply(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),

        // Same-type integer operations with overflow checking
        (Value::I8(a), Value::I8(b)) => a
            .checked_mul(*b)
            .map(Value::I8)
            .ok_or_else(|| Error::InvalidValue("I8 overflow".into())),
        (Value::I16(a), Value::I16(b)) => a
            .checked_mul(*b)
            .map(Value::I16)
            .ok_or_else(|| Error::InvalidValue("I16 overflow".into())),
        (Value::I32(a), Value::I32(b)) => a
            .checked_mul(*b)
            .map(Value::I32)
            .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
        (Value::I64(a), Value::I64(b)) => a
            .checked_mul(*b)
            .map(Value::I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (Value::I128(a), Value::I128(b)) => a
            .checked_mul(*b)
            .map(Value::I128)
            .ok_or_else(|| Error::InvalidValue("I128 overflow".into())),
        (Value::U8(a), Value::U8(b)) => a
            .checked_mul(*b)
            .map(Value::U8)
            .ok_or_else(|| Error::InvalidValue("U8 overflow".into())),
        (Value::U16(a), Value::U16(b)) => a
            .checked_mul(*b)
            .map(Value::U16)
            .ok_or_else(|| Error::InvalidValue("U16 overflow".into())),
        (Value::U32(a), Value::U32(b)) => a
            .checked_mul(*b)
            .map(Value::U32)
            .ok_or_else(|| Error::InvalidValue("U32 overflow".into())),
        (Value::U64(a), Value::U64(b)) => a
            .checked_mul(*b)
            .map(Value::U64)
            .ok_or_else(|| Error::InvalidValue("U64 overflow".into())),
        (Value::U128(a), Value::U128(b)) => a
            .checked_mul(*b)
            .map(Value::U128)
            .ok_or_else(|| Error::InvalidValue("U128 overflow".into())),

        // Float operations
        (Value::F32(a), Value::F32(b)) => Ok(Value::F32(a * b)),
        (Value::F64(a), Value::F64(b)) => Ok(Value::F64(a * b)),

        // Decimal operations
        (Value::Decimal(a), Value::Decimal(b)) => Ok(Value::Decimal(a * b)),

        // Mixed integer types - promote to larger type
        (Value::I8(a), Value::I16(b)) => (*a as i16)
            .checked_mul(*b)
            .map(Value::I16)
            .ok_or_else(|| Error::InvalidValue("I16 overflow".into())),
        (Value::I16(a), Value::I8(b)) => a
            .checked_mul(*b as i16)
            .map(Value::I16)
            .ok_or_else(|| Error::InvalidValue("I16 overflow".into())),
        (Value::I8(a), Value::I32(b)) => (*a as i32)
            .checked_mul(*b)
            .map(Value::I32)
            .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
        (Value::I32(a), Value::I8(b)) => a
            .checked_mul(*b as i32)
            .map(Value::I32)
            .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
        (Value::I8(a), Value::I64(b)) => (*a as i64)
            .checked_mul(*b)
            .map(Value::I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (Value::I64(a), Value::I8(b)) => a
            .checked_mul(*b as i64)
            .map(Value::I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (Value::I16(a), Value::I32(b)) => (*a as i32)
            .checked_mul(*b)
            .map(Value::I32)
            .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
        (Value::I32(a), Value::I16(b)) => a
            .checked_mul(*b as i32)
            .map(Value::I32)
            .ok_or_else(|| Error::InvalidValue("I32 overflow".into())),
        (Value::I16(a), Value::I64(b)) => (*a as i64)
            .checked_mul(*b)
            .map(Value::I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (Value::I64(a), Value::I16(b)) => a
            .checked_mul(*b as i64)
            .map(Value::I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (Value::I32(a), Value::I64(b)) => (*a as i64)
            .checked_mul(*b)
            .map(Value::I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),
        (Value::I64(a), Value::I32(b)) => a
            .checked_mul(*b as i64)
            .map(Value::I64)
            .ok_or_else(|| Error::InvalidValue("I64 overflow".into())),

        // Mixed numeric types involving floats or decimals - convert to appropriate type
        (l, r) if is_numeric(l) && is_numeric(r) => {
            // If either is a decimal, convert both to decimal
            if matches!(l, Value::Decimal(_)) || matches!(r, Value::Decimal(_)) {
                let l_dec = to_decimal(l)
                    .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
                let r_dec = to_decimal(r)
                    .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
                Ok(Value::Decimal(l_dec * r_dec))
            }
            // If either is a float, use float
            else if matches!(l, Value::F64(_)) || matches!(r, Value::F64(_)) {
                let l_f64 = match l {
                    Value::F64(f) => *f,
                    Value::F32(f) => *f as f64,
                    Value::I8(i) => *i as f64,
                    Value::I16(i) => *i as f64,
                    Value::I32(i) => *i as f64,
                    Value::I64(i) => *i as f64,
                    Value::I128(i) => *i as f64,
                    _ => return Err(Error::InvalidValue("Cannot convert to f64".into())),
                };
                let r_f64 = match r {
                    Value::F64(f) => *f,
                    Value::F32(f) => *f as f64,
                    Value::I8(i) => *i as f64,
                    Value::I16(i) => *i as f64,
                    Value::I32(i) => *i as f64,
                    Value::I64(i) => *i as f64,
                    Value::I128(i) => *i as f64,
                    _ => return Err(Error::InvalidValue("Cannot convert to f64".into())),
                };
                Ok(Value::F64(l_f64 * r_f64))
            } else if matches!(l, Value::F32(_)) || matches!(r, Value::F32(_)) {
                let l_f32 = match l {
                    Value::F32(f) => *f,
                    Value::I8(i) => *i as f32,
                    Value::I16(i) => *i as f32,
                    Value::I32(i) => *i as f32,
                    _ => return Err(Error::InvalidValue("Cannot convert to f32".into())),
                };
                let r_f32 = match r {
                    Value::F32(f) => *f,
                    Value::I8(i) => *i as f32,
                    Value::I16(i) => *i as f32,
                    Value::I32(i) => *i as f32,
                    _ => return Err(Error::InvalidValue("Cannot convert to f32".into())),
                };
                Ok(Value::F32(l_f32 * r_f32))
            } else {
                // Shouldn't reach here if we've handled all cases correctly
                let l_dec = to_decimal(l)
                    .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
                let r_dec = to_decimal(r)
                    .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
                Ok(Value::Decimal(l_dec * r_dec))
            }
        }

        _ => Err(Error::TypeMismatch {
            expected: "numeric".into(),
            found: format!("{:?} and {:?}", left, right),
        }),
    }
}

/// Performs division on two values
pub fn divide(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),

        // Check for division by zero for integers
        (_, Value::I8(0))
        | (_, Value::I16(0))
        | (_, Value::I32(0))
        | (_, Value::I64(0))
        | (_, Value::I128(0))
        | (_, Value::U8(0))
        | (_, Value::U16(0))
        | (_, Value::U32(0))
        | (_, Value::U64(0))
        | (_, Value::U128(0)) => Err(Error::InvalidValue("Division by zero".into())),

        // Integer division (truncating)
        (Value::I8(a), Value::I8(b)) => Ok(Value::I8(a / b)),
        (Value::I16(a), Value::I16(b)) => Ok(Value::I16(a / b)),
        (Value::I32(a), Value::I32(b)) => Ok(Value::I32(a / b)),
        (Value::I64(a), Value::I64(b)) => Ok(Value::I64(a / b)),
        (Value::I128(a), Value::I128(b)) => Ok(Value::I128(a / b)),
        (Value::U8(a), Value::U8(b)) => Ok(Value::U8(a / b)),
        (Value::U16(a), Value::U16(b)) => Ok(Value::U16(a / b)),
        (Value::U32(a), Value::U32(b)) => Ok(Value::U32(a / b)),
        (Value::U64(a), Value::U64(b)) => Ok(Value::U64(a / b)),
        (Value::U128(a), Value::U128(b)) => Ok(Value::U128(a / b)),

        // Float division
        (Value::F32(a), Value::F32(b)) => {
            if *b == 0.0 {
                Err(Error::InvalidValue("Division by zero".into()))
            } else {
                Ok(Value::F32(a / b))
            }
        }
        (Value::F64(a), Value::F64(b)) => {
            if *b == 0.0 {
                Err(Error::InvalidValue("Division by zero".into()))
            } else {
                Ok(Value::F64(a / b))
            }
        }

        // Decimal division
        (Value::Decimal(a), Value::Decimal(b)) => {
            if b.is_zero() {
                Err(Error::InvalidValue("Division by zero".into()))
            } else {
                Ok(Value::Decimal(a / b))
            }
        }

        // Mixed numeric types - convert to Decimal
        (l, r) if is_numeric(l) && is_numeric(r) => {
            let r_dec = to_decimal(r)
                .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
            if r_dec.is_zero() {
                return Err(Error::InvalidValue("Division by zero".into()));
            }
            let l_dec = to_decimal(l)
                .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
            Ok(Value::Decimal(l_dec / r_dec))
        }

        _ => Err(Error::TypeMismatch {
            expected: "numeric".into(),
            found: format!("{:?} and {:?}", left, right),
        }),
    }
}

/// Performs remainder/modulo operation on two values
pub fn remainder(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),

        // Check for modulo by zero
        (_, Value::I8(0))
        | (_, Value::I16(0))
        | (_, Value::I32(0))
        | (_, Value::I64(0))
        | (_, Value::I128(0))
        | (_, Value::U8(0))
        | (_, Value::U16(0))
        | (_, Value::U32(0))
        | (_, Value::U64(0))
        | (_, Value::U128(0)) => Err(Error::InvalidValue("Division by zero".into())),

        // Integer remainder
        (Value::I8(a), Value::I8(b)) => Ok(Value::I8(a % b)),
        (Value::I16(a), Value::I16(b)) => Ok(Value::I16(a % b)),
        (Value::I32(a), Value::I32(b)) => Ok(Value::I32(a % b)),
        (Value::I64(a), Value::I64(b)) => Ok(Value::I64(a % b)),
        (Value::I128(a), Value::I128(b)) => Ok(Value::I128(a % b)),
        (Value::U8(a), Value::U8(b)) => Ok(Value::U8(a % b)),
        (Value::U16(a), Value::U16(b)) => Ok(Value::U16(a % b)),
        (Value::U32(a), Value::U32(b)) => Ok(Value::U32(a % b)),
        (Value::U64(a), Value::U64(b)) => Ok(Value::U64(a % b)),
        (Value::U128(a), Value::U128(b)) => Ok(Value::U128(a % b)),

        // Float remainder
        (Value::F32(a), Value::F32(b)) => {
            if *b == 0.0 {
                Err(Error::InvalidValue("Division by zero".into()))
            } else {
                Ok(Value::F32(a % b))
            }
        }
        (Value::F64(a), Value::F64(b)) => {
            if *b == 0.0 {
                Err(Error::InvalidValue("Division by zero".into()))
            } else {
                Ok(Value::F64(a % b))
            }
        }

        // Decimal remainder
        (Value::Decimal(a), Value::Decimal(b)) => {
            if b.is_zero() {
                Err(Error::InvalidValue("Division by zero".into()))
            } else {
                Ok(Value::Decimal(a % b))
            }
        }

        // Mixed numeric types - convert to appropriate type
        (l, r) if is_numeric(l) && is_numeric(r) => {
            // For mixed integer types, promote to larger type
            match (l, r) {
                (Value::I8(a), Value::I16(b)) => Ok(Value::I16(*a as i16 % b)),
                (Value::I16(a), Value::I8(b)) => Ok(Value::I16(a % *b as i16)),
                (Value::I8(a), Value::I32(b)) => Ok(Value::I32(*a as i32 % b)),
                (Value::I32(a), Value::I8(b)) => Ok(Value::I32(a % *b as i32)),
                (Value::I8(a), Value::I64(b)) => Ok(Value::I64(*a as i64 % b)),
                (Value::I64(a), Value::I8(b)) => Ok(Value::I64(a % *b as i64)),
                (Value::I16(a), Value::I32(b)) => Ok(Value::I32(*a as i32 % b)),
                (Value::I32(a), Value::I16(b)) => Ok(Value::I32(a % *b as i32)),
                (Value::I16(a), Value::I64(b)) => Ok(Value::I64(*a as i64 % b)),
                (Value::I64(a), Value::I16(b)) => Ok(Value::I64(a % *b as i64)),
                (Value::I32(a), Value::I64(b)) => Ok(Value::I64(*a as i64 % b)),
                (Value::I64(a), Value::I32(b)) => Ok(Value::I64(a % *b as i64)),
                _ => {
                    // Fall back to decimal for complex cases
                    let l_dec = to_decimal(l)
                        .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
                    let r_dec = to_decimal(r)
                        .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
                    if r_dec.is_zero() {
                        return Err(Error::InvalidValue("Division by zero".into()));
                    }
                    Ok(Value::Decimal(l_dec % r_dec))
                }
            }
        }

        _ => Err(Error::TypeMismatch {
            expected: "numeric".into(),
            found: format!("{:?} and {:?}", left, right),
        }),
    }
}

/// Performs logical AND on two values
pub fn and(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a && *b)),
        (Value::Bool(false), Value::Null) | (Value::Null, Value::Bool(false)) => {
            Ok(Value::Bool(false))
        }
        (Value::Bool(true), Value::Null) | (Value::Null, Value::Bool(true)) => Ok(Value::Null),
        (Value::Null, Value::Null) => Ok(Value::Null),
        _ => Err(Error::TypeMismatch {
            expected: "boolean".into(),
            found: format!("{:?} and {:?}", left, right),
        }),
    }
}

/// Performs logical OR on two values
pub fn or(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a || *b)),
        (Value::Bool(true), Value::Null) | (Value::Null, Value::Bool(true)) => {
            Ok(Value::Bool(true))
        }
        (Value::Bool(false), Value::Null) | (Value::Null, Value::Bool(false)) => Ok(Value::Null),
        (Value::Null, Value::Null) => Ok(Value::Null),
        _ => Err(Error::TypeMismatch {
            expected: "boolean".into(),
            found: format!("{:?} and {:?}", left, right),
        }),
    }
}

/// Performs logical NOT on a value
pub fn not(value: &Value) -> Result<Value> {
    match value {
        Value::Bool(b) => Ok(Value::Bool(!b)),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::TypeMismatch {
            expected: "boolean".into(),
            found: format!("{:?}", value),
        }),
    }
}

/// Compares two values for ordering
pub fn compare(left: &Value, right: &Value) -> Result<Ordering> {
    match (left, right) {
        // NULL comparisons - NULL is less than any value
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Null, _) => Ok(Ordering::Less),
        (_, Value::Null) => Ok(Ordering::Greater),

        // Boolean comparisons (false < true)
        (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),

        // Same-type integer comparisons
        (Value::I8(a), Value::I8(b)) => Ok(a.cmp(b)),
        (Value::I16(a), Value::I16(b)) => Ok(a.cmp(b)),
        (Value::I32(a), Value::I32(b)) => Ok(a.cmp(b)),
        (Value::I64(a), Value::I64(b)) => Ok(a.cmp(b)),
        (Value::I128(a), Value::I128(b)) => Ok(a.cmp(b)),
        (Value::U8(a), Value::U8(b)) => Ok(a.cmp(b)),
        (Value::U16(a), Value::U16(b)) => Ok(a.cmp(b)),
        (Value::U32(a), Value::U32(b)) => Ok(a.cmp(b)),
        (Value::U64(a), Value::U64(b)) => Ok(a.cmp(b)),
        (Value::U128(a), Value::U128(b)) => Ok(a.cmp(b)),

        // Float comparisons - handle NaN specially
        (Value::F32(a), Value::F32(b)) => {
            // NaN comparisons: NaN is never equal to anything, including itself
            // For ordering (ORDER BY), treat NaN as greater than all other values
            match a.partial_cmp(b) {
                Some(ord) => Ok(ord),
                None => {
                    // One or both are NaN
                    if a.is_nan() && b.is_nan() {
                        Ok(Ordering::Equal) // Two NaNs are considered equal for sorting
                    } else if a.is_nan() {
                        Ok(Ordering::Greater) // NaN sorts after regular numbers
                    } else {
                        Ok(Ordering::Less) // Regular number sorts before NaN
                    }
                }
            }
        }
        (Value::F64(a), Value::F64(b)) => {
            // Same NaN handling for F64
            match a.partial_cmp(b) {
                Some(ord) => Ok(ord),
                None => {
                    if a.is_nan() && b.is_nan() {
                        Ok(Ordering::Equal)
                    } else if a.is_nan() {
                        Ok(Ordering::Greater)
                    } else {
                        Ok(Ordering::Less)
                    }
                }
            }
        }

        // Decimal comparisons
        (Value::Decimal(a), Value::Decimal(b)) => Ok(a.cmp(b)),

        // Mixed numeric comparisons
        // If either side is a float, convert both to float for comparison
        // This preserves float semantics and avoids precision issues
        (l, r) if is_numeric(l) && is_numeric(r) => {
            // Check if either value is F64
            if matches!(l, Value::F64(_)) || matches!(r, Value::F64(_)) {
                let l_f64 =
                    to_f64(l).ok_or_else(|| Error::InvalidValue("Cannot convert to f64".into()))?;
                let r_f64 =
                    to_f64(r).ok_or_else(|| Error::InvalidValue("Cannot convert to f64".into()))?;
                // Handle NaN properly
                match l_f64.partial_cmp(&r_f64) {
                    Some(ord) => Ok(ord),
                    None => {
                        if l_f64.is_nan() && r_f64.is_nan() {
                            Ok(Ordering::Equal)
                        } else if l_f64.is_nan() {
                            Ok(Ordering::Greater)
                        } else {
                            Ok(Ordering::Less)
                        }
                    }
                }
            }
            // Check if either value is F32
            else if matches!(l, Value::F32(_)) || matches!(r, Value::F32(_)) {
                let l_f32 =
                    to_f32(l).ok_or_else(|| Error::InvalidValue("Cannot convert to f32".into()))?;
                let r_f32 =
                    to_f32(r).ok_or_else(|| Error::InvalidValue("Cannot convert to f32".into()))?;
                // Handle NaN properly
                match l_f32.partial_cmp(&r_f32) {
                    Some(ord) => Ok(ord),
                    None => {
                        if l_f32.is_nan() && r_f32.is_nan() {
                            Ok(Ordering::Equal)
                        } else if l_f32.is_nan() {
                            Ok(Ordering::Greater)
                        } else {
                            Ok(Ordering::Less)
                        }
                    }
                }
            }
            // Otherwise convert to Decimal for exact comparison
            else {
                let l_dec = to_decimal(l)
                    .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
                let r_dec = to_decimal(r)
                    .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
                Ok(l_dec.cmp(&r_dec))
            }
        }

        // String comparisons
        (Value::Str(a), Value::Str(b)) => Ok(a.cmp(b)),

        // Date comparisons
        (Value::Date(a), Value::Date(b)) => Ok(a.cmp(b)),

        // Date with String - try to parse the string as Date
        (Value::Date(a), Value::Str(s)) | (Value::Str(s), Value::Date(a)) => {
            use chrono::NaiveDate;
            match NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                Ok(parsed_date) => {
                    if left.data_type() == DataType::Date {
                        Ok(a.cmp(&parsed_date))
                    } else {
                        Ok(parsed_date.cmp(a))
                    }
                }
                Err(_) => {
                    // If string cannot be parsed as Date, treat as type mismatch
                    Err(Error::TypeMismatch {
                        expected: "comparable types".into(),
                        found: format!("Date and invalid date string '{}'", s),
                    })
                }
            }
        }

        // Time comparisons
        (Value::Time(a), Value::Time(b)) => Ok(a.cmp(b)),

        // Time with String - try to parse the string as Time
        (Value::Time(a), Value::Str(s)) | (Value::Str(s), Value::Time(a)) => {
            use chrono::NaiveTime;
            // Try multiple time formats
            let parsed_time = NaiveTime::parse_from_str(s, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"));

            match parsed_time {
                Ok(parsed) => {
                    if left.data_type() == DataType::Time {
                        Ok(a.cmp(&parsed))
                    } else {
                        Ok(parsed.cmp(a))
                    }
                }
                Err(_) => Err(Error::TypeMismatch {
                    expected: "comparable types".into(),
                    found: format!("Time and invalid time string '{}'", s),
                }),
            }
        }

        // Timestamp comparisons
        (Value::Timestamp(a), Value::Timestamp(b)) => Ok(a.cmp(b)),

        // Timestamp with String - try to parse the string as Timestamp
        (Value::Timestamp(a), Value::Str(s)) | (Value::Str(s), Value::Timestamp(a)) => {
            use chrono::NaiveDateTime;
            // Try multiple timestamp formats
            let parsed_timestamp = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"));

            match parsed_timestamp {
                Ok(parsed) => {
                    if left.data_type() == DataType::Timestamp {
                        Ok(a.cmp(&parsed))
                    } else {
                        Ok(parsed.cmp(a))
                    }
                }
                Err(_) => Err(Error::TypeMismatch {
                    expected: "comparable types".into(),
                    found: format!("Timestamp and invalid timestamp string '{}'", s),
                }),
            }
        }

        // Interval comparisons
        (Value::Interval(a), Value::Interval(b)) => Ok(a.cmp(b)),

        // UUID comparisons
        (Value::Uuid(a), Value::Uuid(b)) => Ok(a.cmp(b)),

        // UUID with String - try to parse the string as UUID
        (Value::Uuid(a), Value::Str(s)) | (Value::Str(s), Value::Uuid(a)) => {
            use uuid::Uuid;
            match Uuid::parse_str(s) {
                Ok(parsed_uuid) => {
                    if left.data_type() == DataType::Uuid {
                        Ok(a.cmp(&parsed_uuid))
                    } else {
                        Ok(parsed_uuid.cmp(a))
                    }
                }
                Err(_) => {
                    // If string cannot be parsed as UUID, treat as unequal
                    // Return Greater/Less based on type ordering (UUID > String in type hierarchy)
                    if left.data_type() == DataType::Uuid {
                        Ok(Ordering::Greater)
                    } else {
                        Ok(Ordering::Less)
                    }
                }
            }
        }

        // Bytea comparisons
        (Value::Bytea(a), Value::Bytea(b)) => Ok(a.cmp(b)),

        // Inet comparisons
        (Value::Inet(a), Value::Inet(b)) => Ok(a.cmp(b)),

        // Point comparisons
        (Value::Point(a), Value::Point(b)) => Ok(a.cmp(b)),

        // Type mismatches
        _ => Err(Error::TypeMismatch {
            expected: "comparable types".into(),
            found: format!("{:?} and {:?}", left, right),
        }),
    }
}

/// Compares composite keys (multiple values)
pub fn compare_composite(left: &[Value], right: &[Value]) -> Result<Ordering> {
    for (l, r) in left.iter().zip(right.iter()) {
        match compare(l, r)? {
            Ordering::Equal => continue,
            other => return Ok(other),
        }
    }
    Ok(left.len().cmp(&right.len()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn test_add() {
        // Integer addition
        assert_eq!(add(&Value::I64(5), &Value::I64(3)).unwrap(), Value::I64(8));

        // Mixed types
        assert_eq!(
            add(&Value::I32(5), &Value::F32(3.5)).unwrap(),
            Value::Decimal(Decimal::from_str("8.5").unwrap())
        );

        // String concatenation
        assert_eq!(
            add(
                &Value::Str("hello".to_string()),
                &Value::Str(" world".to_string())
            )
            .unwrap(),
            Value::Str("hello world".to_string())
        );

        // NULL handling
        assert_eq!(add(&Value::Null, &Value::I64(5)).unwrap(), Value::Null);
    }

    #[test]
    fn test_compare() {
        // Integer comparison
        assert_eq!(
            compare(&Value::I64(5), &Value::I64(3)).unwrap(),
            Ordering::Greater
        );

        // Mixed numeric comparison
        assert_eq!(
            compare(&Value::I32(5), &Value::F64(5.0)).unwrap(),
            Ordering::Equal
        );

        // String comparison
        assert_eq!(
            compare(
                &Value::Str("apple".to_string()),
                &Value::Str("banana".to_string())
            )
            .unwrap(),
            Ordering::Less
        );

        // NULL comparison
        assert_eq!(
            compare(&Value::Null, &Value::I64(5)).unwrap(),
            Ordering::Less
        );
    }
}
