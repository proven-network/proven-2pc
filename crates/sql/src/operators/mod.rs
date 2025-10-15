//! SQL operator implementations
//!
//! This module provides a unified implementation for all SQL operators,
//! ensuring consistency between type checking and execution.

pub mod helpers;
pub mod mixed_ops;
pub mod traits;

// Arithmetic operators
mod add;
mod divide;
mod exponentiate;
mod multiply;
mod remainder;
mod subtract;

// Comparison operators
mod equal;
mod greater_than;
mod greater_than_equal;
mod less_than;
mod less_than_equal;
mod not_equal;

// Logical operators
mod and;
mod not;
mod or;
mod xor;

// Unary operators
mod factorial;
mod identity;
mod negate;

// Pattern matching operators
mod ilike;
mod like;

// String operations
mod concat;

// Re-export commonly used items
pub use traits::{BinaryOperator, UnaryOperator};

use crate::error::Result;
use crate::types::{DataType, Value, ValueExt};
use std::cmp::Ordering;

// Arithmetic operators

/// Execute addition operation
pub fn execute_add(left: &Value, right: &Value) -> Result<Value> {
    static OP: add::AddOperator = add::AddOperator;
    OP.execute(left, right)
}

/// Validate addition operation types
pub fn validate_add(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: add::AddOperator = add::AddOperator;
    OP.validate(left, right)
}

/// Execute subtraction operation
pub fn execute_subtract(left: &Value, right: &Value) -> Result<Value> {
    static OP: subtract::SubtractOperator = subtract::SubtractOperator;
    OP.execute(left, right)
}

/// Validate subtraction operation types
pub fn validate_subtract(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: subtract::SubtractOperator = subtract::SubtractOperator;
    OP.validate(left, right)
}

/// Execute multiplication operation
pub fn execute_multiply(left: &Value, right: &Value) -> Result<Value> {
    static OP: multiply::MultiplyOperator = multiply::MultiplyOperator;
    OP.execute(left, right)
}

/// Validate multiplication operation types
pub fn validate_multiply(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: multiply::MultiplyOperator = multiply::MultiplyOperator;
    OP.validate(left, right)
}

/// Execute division operation
pub fn execute_divide(left: &Value, right: &Value) -> Result<Value> {
    static OP: divide::DivideOperator = divide::DivideOperator;
    OP.execute(left, right)
}

/// Validate division operation types
pub fn validate_divide(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: divide::DivideOperator = divide::DivideOperator;
    OP.validate(left, right)
}

/// Execute remainder operation
pub fn execute_remainder(left: &Value, right: &Value) -> Result<Value> {
    static OP: remainder::RemainderOperator = remainder::RemainderOperator;
    OP.execute(left, right)
}

/// Validate remainder operation types
pub fn validate_remainder(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: remainder::RemainderOperator = remainder::RemainderOperator;
    OP.validate(left, right)
}

/// Execute exponentiation operation
pub fn execute_exponentiate(left: &Value, right: &Value) -> Result<Value> {
    static OP: exponentiate::ExponentiateOperator = exponentiate::ExponentiateOperator;
    OP.execute(left, right)
}

/// Validate exponentiation operation types
pub fn validate_exponentiate(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: exponentiate::ExponentiateOperator = exponentiate::ExponentiateOperator;
    OP.validate(left, right)
}

// Logical operators

/// Execute AND operation
pub fn execute_and(left: &Value, right: &Value) -> Result<Value> {
    static OP: and::AndOperator = and::AndOperator;
    OP.execute(left, right)
}

/// Validate AND operation types
pub fn validate_and(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: and::AndOperator = and::AndOperator;
    OP.validate(left, right)
}

/// Execute OR operation
pub fn execute_or(left: &Value, right: &Value) -> Result<Value> {
    static OP: or::OrOperator = or::OrOperator;
    OP.execute(left, right)
}

/// Validate OR operation types
pub fn validate_or(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: or::OrOperator = or::OrOperator;
    OP.validate(left, right)
}

/// Execute XOR operation
pub fn execute_xor(left: &Value, right: &Value) -> Result<Value> {
    static OP: xor::XorOperator = xor::XorOperator;
    OP.execute(left, right)
}

/// Validate XOR operation types
pub fn validate_xor(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: xor::XorOperator = xor::XorOperator;
    OP.validate(left, right)
}

/// Execute NOT operation
pub fn execute_not(value: &Value) -> Result<Value> {
    static OP: not::NotOperator = not::NotOperator;
    OP.execute(value)
}

/// Validate NOT operation types
pub fn validate_not(operand: &DataType) -> Result<DataType> {
    static OP: not::NotOperator = not::NotOperator;
    OP.validate(operand)
}

// Unary arithmetic operators

/// Execute negation operation
pub fn execute_negate(value: &Value) -> Result<Value> {
    static OP: negate::NegateOperator = negate::NegateOperator;
    OP.execute(value)
}

/// Validate negation operation types
pub fn validate_negate(operand: &DataType) -> Result<DataType> {
    static OP: negate::NegateOperator = negate::NegateOperator;
    OP.validate(operand)
}

/// Execute identity operation
pub fn execute_identity(value: &Value) -> Result<Value> {
    static OP: identity::IdentityOperator = identity::IdentityOperator;
    OP.execute(value)
}

/// Validate identity operation types
pub fn validate_identity(operand: &DataType) -> Result<DataType> {
    static OP: identity::IdentityOperator = identity::IdentityOperator;
    OP.validate(operand)
}

/// Execute factorial operation
pub fn execute_factorial(value: &Value) -> Result<Value> {
    static OP: factorial::FactorialOperator = factorial::FactorialOperator;
    OP.execute(value)
}

/// Validate factorial operation types
pub fn validate_factorial(operand: &DataType) -> Result<DataType> {
    static OP: factorial::FactorialOperator = factorial::FactorialOperator;
    OP.validate(operand)
}

// Pattern matching

/// Execute LIKE operation
pub fn execute_like(value: &Value, pattern: &Value) -> Result<Value> {
    static OP: like::LikeOperator = like::LikeOperator;
    OP.execute(value, pattern)
}

/// Validate LIKE operation types
pub fn validate_like(value: &DataType, pattern: &DataType) -> Result<DataType> {
    static OP: like::LikeOperator = like::LikeOperator;
    OP.validate(value, pattern)
}

/// Execute ILIKE operation
pub fn execute_ilike(value: &Value, pattern: &Value) -> Result<Value> {
    static OP: ilike::ILikeOperator = ilike::ILikeOperator;
    OP.execute(value, pattern)
}

/// Validate ILIKE operation types
pub fn validate_ilike(value: &DataType, pattern: &DataType) -> Result<DataType> {
    static OP: ilike::ILikeOperator = ilike::ILikeOperator;
    OP.validate(value, pattern)
}

// String operations

/// Execute string concatenation operation
pub fn execute_concat(left: &Value, right: &Value) -> Result<Value> {
    static OP: concat::ConcatOperator = concat::ConcatOperator;
    OP.execute(left, right)
}

/// Validate string concatenation operation types
pub fn validate_concat(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: concat::ConcatOperator = concat::ConcatOperator;
    OP.validate(left, right)
}

// Comparison operators

/// Execute equality comparison
pub fn execute_equal(left: &Value, right: &Value) -> Result<Value> {
    static OP: equal::EqualOperator = equal::EqualOperator;
    OP.execute(left, right)
}

/// Validate equality comparison types
pub fn validate_equal(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: equal::EqualOperator = equal::EqualOperator;
    OP.validate(left, right)
}

/// Execute not equal comparison
pub fn execute_not_equal(left: &Value, right: &Value) -> Result<Value> {
    static OP: not_equal::NotEqualOperator = not_equal::NotEqualOperator;
    OP.execute(left, right)
}

/// Validate not equal comparison types
pub fn validate_not_equal(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: not_equal::NotEqualOperator = not_equal::NotEqualOperator;
    OP.validate(left, right)
}

/// Execute less than comparison
pub fn execute_less_than(left: &Value, right: &Value) -> Result<Value> {
    static OP: less_than::LessThanOperator = less_than::LessThanOperator;
    OP.execute(left, right)
}

/// Validate less than comparison types
pub fn validate_less_than(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: less_than::LessThanOperator = less_than::LessThanOperator;
    OP.validate(left, right)
}

/// Execute less than or equal comparison
pub fn execute_less_than_equal(left: &Value, right: &Value) -> Result<Value> {
    static OP: less_than_equal::LessThanEqualOperator = less_than_equal::LessThanEqualOperator;
    OP.execute(left, right)
}

/// Validate less than or equal comparison types
pub fn validate_less_than_equal(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: less_than_equal::LessThanEqualOperator = less_than_equal::LessThanEqualOperator;
    OP.validate(left, right)
}

/// Execute greater than comparison
pub fn execute_greater_than(left: &Value, right: &Value) -> Result<Value> {
    static OP: greater_than::GreaterThanOperator = greater_than::GreaterThanOperator;
    OP.execute(left, right)
}

/// Validate greater than comparison types
pub fn validate_greater_than(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: greater_than::GreaterThanOperator = greater_than::GreaterThanOperator;
    OP.validate(left, right)
}

/// Execute greater than or equal comparison
pub fn execute_greater_than_equal(left: &Value, right: &Value) -> Result<Value> {
    static OP: greater_than_equal::GreaterThanEqualOperator =
        greater_than_equal::GreaterThanEqualOperator;
    OP.execute(left, right)
}

/// Validate greater than or equal comparison types
pub fn validate_greater_than_equal(left: &DataType, right: &DataType) -> Result<DataType> {
    static OP: greater_than_equal::GreaterThanEqualOperator =
        greater_than_equal::GreaterThanEqualOperator;
    OP.validate(left, right)
}

// Comparison helpers and unified compare function

/// Compare two values using SQL semantics
/// Returns None if values are incomparable (different types)
pub fn compare(left: &Value, right: &Value) -> Result<Ordering> {
    use crate::error::Error;
    use Value::*;

    // NULL handling - NULLs are equal to each other but less than all other values
    match (left, right) {
        (Null, Null) => return Ok(Ordering::Equal),
        (Null, _) => return Ok(Ordering::Less),
        (_, Null) => return Ok(Ordering::Greater),
        _ => {}
    }

    // Type-specific comparisons
    Ok(match (left, right) {
        // Boolean
        (Bool(a), Bool(b)) => a.cmp(b),

        // Integers - same type
        (I8(a), I8(b)) => a.cmp(b),
        (I16(a), I16(b)) => a.cmp(b),
        (I32(a), I32(b)) => a.cmp(b),
        (I64(a), I64(b)) => a.cmp(b),
        (I128(a), I128(b)) => a.cmp(b),
        (U8(a), U8(b)) => a.cmp(b),
        (U16(a), U16(b)) => a.cmp(b),
        (U32(a), U32(b)) => a.cmp(b),
        (U64(a), U64(b)) => a.cmp(b),
        (U128(a), U128(b)) => a.cmp(b),

        // Floats
        (F32(a), F32(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| Error::InvalidValue("Cannot compare NaN values".into()))?,
        (F64(a), F64(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| Error::InvalidValue("Cannot compare NaN values".into()))?,

        // Decimal
        (Decimal(a), Decimal(b)) => a.cmp(b),

        // Strings
        (Str(a), Str(b)) => a.cmp(b),

        // Date/Time types
        (Date(a), Date(b)) => a.cmp(b),
        (Time(a), Time(b)) => a.cmp(b),
        (Timestamp(a), Timestamp(b)) => a.cmp(b),
        (Interval(a), Interval(b)) => a.cmp(b),

        // Date/Time vs String comparisons - parse string at runtime
        (Date(date), Str(s)) | (Str(s), Date(date)) => {
            use chrono::NaiveDate;
            let parsed = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| Error::InvalidValue(format!("Cannot parse '{}' as date", s)))?;
            if matches!(left, Date(_)) {
                date.cmp(&parsed)
            } else {
                parsed.cmp(date)
            }
        }
        (Time(time), Str(s)) | (Str(s), Time(time)) => {
            use chrono::NaiveTime;
            let parsed = NaiveTime::parse_from_str(s, "%H:%M:%S")
                .map_err(|_| Error::InvalidValue(format!("Cannot parse '{}' as time", s)))?;
            if matches!(left, Time(_)) {
                time.cmp(&parsed)
            } else {
                parsed.cmp(time)
            }
        }
        (Timestamp(ts), Str(s)) | (Str(s), Timestamp(ts)) => {
            use chrono::NaiveDateTime;
            let parsed = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .map_err(|_| Error::InvalidValue(format!("Cannot parse '{}' as timestamp", s)))?;
            if matches!(left, Timestamp(_)) {
                ts.cmp(&parsed)
            } else {
                parsed.cmp(ts)
            }
        }

        // UUID vs String comparisons - parse string at runtime
        (Uuid(uuid), Str(s)) | (Str(s), Uuid(uuid)) => {
            use uuid::Uuid as UuidType;
            let parsed = UuidType::parse_str(s)
                .map_err(|_| Error::InvalidValue(format!("Cannot parse '{}' as UUID", s)))?;
            if matches!(left, Uuid(_)) {
                uuid.cmp(&parsed)
            } else {
                parsed.cmp(uuid)
            }
        }

        // UUID
        (Uuid(a), Uuid(b)) => a.cmp(b),

        // Bytea
        (Bytea(a), Bytea(b)) => a.cmp(b),

        // INET vs String comparisons - parse string at runtime
        (Inet(inet), Str(s)) | (Str(s), Inet(inet)) => {
            use std::net::IpAddr;
            let parsed = s
                .parse::<IpAddr>()
                .map_err(|_| Error::InvalidValue(format!("Cannot parse '{}' as IP address", s)))?;
            if matches!(left, Inet(_)) {
                inet.cmp(&parsed)
            } else {
                parsed.cmp(inet)
            }
        }

        // Inet
        (Inet(a), Inet(b)) => a.cmp(b),

        // POINT vs String comparisons - parse string at runtime
        (Point(point), Str(s)) | (Str(s), Point(point)) => {
            use crate::coercion::coerce_value;
            use crate::types::DataType;
            let parsed_val = coerce_value(Str(s.clone()), &DataType::Point)
                .map_err(|_| Error::InvalidValue(format!("Cannot parse '{}' as POINT", s)))?;
            if let Value::Point(parsed_point) = parsed_val {
                if matches!(left, Point(_)) {
                    point.cmp(&parsed_point)
                } else {
                    parsed_point.cmp(point)
                }
            } else {
                return Err(Error::InvalidValue(format!("Failed to parse '{}' as POINT", s)));
            }
        }

        // Point
        (Point(a), Point(b)) => a.cmp(b),

        // Collections - Arrays and Lists are comparable
        (Array(a), Array(b)) | (List(a), List(b)) => a.cmp(b),
        (Array(a), List(b)) | (List(a), Array(b)) => a.cmp(b),
        (Map(a), Map(b)) => {
            // Compare maps by converting to sorted vectors
            let mut a_vec: Vec<_> = a.iter().collect();
            let mut b_vec: Vec<_> = b.iter().collect();
            a_vec.sort_by_key(|(k, _)| *k);
            b_vec.sort_by_key(|(k, _)| *k);
            a_vec.cmp(&b_vec)
        }
        (Struct(a), Struct(b)) => a.cmp(b),

        // Collection vs String comparisons - parse JSON at runtime
        (Array(arr), Str(s)) | (Str(s), Array(arr)) => {
            let parsed = Value::parse_json_array(s)
                .map_err(|_| Error::InvalidValue(format!("Cannot parse '{}' as array", s)))?;
            if let List(parsed_list) = parsed {
                if matches!(left, Array(_)) {
                    arr.cmp(&parsed_list)
                } else {
                    parsed_list.cmp(arr)
                }
            } else {
                return Err(Error::InvalidValue(
                    "parse_json_array didn't return a List".into(),
                ));
            }
        }
        (List(lst), Str(s)) | (Str(s), List(lst)) => {
            let parsed = Value::parse_json_array(s)
                .map_err(|_| Error::InvalidValue(format!("Cannot parse '{}' as list", s)))?;
            if let List(parsed_list) = parsed {
                if matches!(left, List(_)) {
                    lst.cmp(&parsed_list)
                } else {
                    parsed_list.cmp(lst)
                }
            } else {
                return Err(Error::InvalidValue(
                    "parse_json_array didn't return a List".into(),
                ));
            }
        }
        (Map(map), Str(s)) | (Str(s), Map(map)) => {
            let parsed = Value::parse_json_object(s)
                .map_err(|_| Error::InvalidValue(format!("Cannot parse '{}' as map", s)))?;
            if let Map(parsed_map) = parsed {
                // Convert both to sorted vectors for comparison
                let mut map_vec: Vec<_> = map.iter().collect();
                let mut parsed_vec: Vec<_> = parsed_map.iter().collect();
                map_vec.sort_by_key(|(k, _)| *k);
                parsed_vec.sort_by_key(|(k, _)| *k);
                if matches!(left, Map(_)) {
                    map_vec.cmp(&parsed_vec)
                } else {
                    parsed_vec.cmp(&map_vec)
                }
            } else {
                return Err(Error::InvalidValue(
                    "parse_json_object didn't return a Map".into(),
                ));
            }
        }

        // Mixed numeric types - convert and compare
        (a, b) if a.is_numeric() && b.is_numeric() => compare_mixed_numeric(a, b)?,

        _ => {
            return Err(Error::TypeMismatch {
                expected: format!("{:?}", left.data_type()),
                found: format!("{:?}", right.data_type()),
            });
        }
    })
}

/// Helper for comparing mixed numeric types
fn compare_mixed_numeric(left: &Value, right: &Value) -> Result<Ordering> {
    use crate::error::Error;
    use Value::*;

    // Try to convert both to the wider type
    match (left, right) {
        // Mixed signed integers - promote to wider type
        (I8(a), I16(b)) => Ok((*a as i16).cmp(b)),
        (I16(a), I8(b)) => Ok(a.cmp(&(*b as i16))),
        (I8(a), I32(b)) => Ok((*a as i32).cmp(b)),
        (I32(a), I8(b)) => Ok(a.cmp(&(*b as i32))),
        (I8(a), I64(b)) => Ok((*a as i64).cmp(b)),
        (I64(a), I8(b)) => Ok(a.cmp(&(*b as i64))),
        (I16(a), I32(b)) => Ok((*a as i32).cmp(b)),
        (I32(a), I16(b)) => Ok(a.cmp(&(*b as i32))),
        (I16(a), I64(b)) => Ok((*a as i64).cmp(b)),
        (I64(a), I16(b)) => Ok(a.cmp(&(*b as i64))),
        (I32(a), I64(b)) => Ok((*a as i64).cmp(b)),
        (I64(a), I32(b)) => Ok(a.cmp(&(*b as i64))),

        // Integer to float comparisons
        (a, F32(b)) if a.is_integer() => {
            let a_float = integer_to_f32(a)?;
            a_float
                .partial_cmp(b)
                .ok_or_else(|| Error::InvalidValue("Cannot compare with NaN".into()))
        }
        (F32(a), b) if b.is_integer() => {
            let b_float = integer_to_f32(b)?;
            a.partial_cmp(&b_float)
                .ok_or_else(|| Error::InvalidValue("Cannot compare with NaN".into()))
        }
        (a, F64(b)) if a.is_integer() => {
            let a_float = integer_to_f64(a)?;
            a_float
                .partial_cmp(b)
                .ok_or_else(|| Error::InvalidValue("Cannot compare with NaN".into()))
        }
        (F64(a), b) if b.is_integer() => {
            let b_float = integer_to_f64(b)?;
            a.partial_cmp(&b_float)
                .ok_or_else(|| Error::InvalidValue("Cannot compare with NaN".into()))
        }

        // Float to float
        (F32(a), F64(b)) => (*a as f64)
            .partial_cmp(b)
            .ok_or_else(|| Error::InvalidValue("Cannot compare with NaN".into())),
        (F64(a), F32(b)) => a
            .partial_cmp(&(*b as f64))
            .ok_or_else(|| Error::InvalidValue("Cannot compare with NaN".into())),

        // Decimal comparisons - convert everything to decimal
        _ => {
            let a_dec = to_decimal(left)
                .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
            let b_dec = to_decimal(right)
                .ok_or_else(|| Error::InvalidValue("Cannot convert to decimal".into()))?;
            Ok(a_dec.cmp(&b_dec))
        }
    }
}

/// Convert integer to f32
fn integer_to_f32(value: &Value) -> Result<f32> {
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
        _ => return Err(crate::error::Error::InvalidValue("Not an integer".into())),
    })
}

/// Convert integer to f64
fn integer_to_f64(value: &Value) -> Result<f64> {
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
        _ => return Err(crate::error::Error::InvalidValue("Not an integer".into())),
    })
}

/// Convert any numeric value to Decimal
fn to_decimal(value: &Value) -> Option<rust_decimal::Decimal> {
    use Value::*;
    use rust_decimal::Decimal as RustDecimal;

    match value {
        I8(n) => Some(RustDecimal::from(*n)),
        I16(n) => Some(RustDecimal::from(*n)),
        I32(n) => Some(RustDecimal::from(*n)),
        I64(n) => Some(RustDecimal::from(*n)),
        I128(n) => Some(RustDecimal::from(*n)),
        U8(n) => Some(RustDecimal::from(*n)),
        U16(n) => Some(RustDecimal::from(*n)),
        U32(n) => Some(RustDecimal::from(*n)),
        U64(n) => Some(RustDecimal::from(*n)),
        U128(n) => Some(RustDecimal::from(*n)),
        F32(n) => RustDecimal::from_f32_retain(*n),
        F64(n) => RustDecimal::from_f64_retain(*n),
        Decimal(d) => Some(*d),
        _ => None,
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
