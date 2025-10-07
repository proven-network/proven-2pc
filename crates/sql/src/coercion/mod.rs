//! Type coercion for SQL values
//! Simplified coercion module that handles type conversions and compatibility checks

use crate::error::Result;
use crate::types::{DataType, Value};

mod value;
pub use value::coerce_row;

/// Check if a value type can be coerced to a target type
pub fn can_coerce(from: &DataType, to: &DataType) -> bool {
    // Same type is always valid
    if from == to {
        return true;
    }

    // Text and Str are interchangeable
    if matches!(
        (from, to),
        (DataType::Text, DataType::Str) | (DataType::Str, DataType::Text)
    ) {
        return true;
    }

    match (from, to) {
        // NULL can go into anything
        (_, _) if matches!(from, DataType::Nullable(_)) => true,

        // Integer widening is always safe
        (DataType::I8, DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128) => true,
        (DataType::I16, DataType::I32 | DataType::I64 | DataType::I128) => true,
        (DataType::I32, DataType::I64 | DataType::I128) => true,
        (DataType::I64, DataType::I128) => true,

        // Unsigned widening is safe
        (DataType::U8, DataType::U16 | DataType::U32 | DataType::U64 | DataType::U128) => true,
        (DataType::U16, DataType::U32 | DataType::U64 | DataType::U128) => true,
        (DataType::U32, DataType::U64 | DataType::U128) => true,
        (DataType::U64, DataType::U128) => true,

        // Small unsigned to larger signed is safe
        (DataType::U8, DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128) => true,
        (DataType::U16, DataType::I32 | DataType::I64 | DataType::I128) => true,
        (DataType::U32, DataType::I64 | DataType::I128) => true,
        (DataType::U64, DataType::I128) => true,

        // Float conversions
        (DataType::F32, DataType::F64) => true, // Widening is always safe
        (DataType::F64, DataType::F32) => true, // Narrowing allowed (runtime will check range)

        // Integer to float (may lose precision but allowed)
        (DataType::I8 | DataType::I16 | DataType::I32, DataType::F32 | DataType::F64) => true,
        (DataType::I64, DataType::F64) => true,

        // Integer to Decimal
        (
            DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128,
            DataType::Decimal(_, _),
        ) => true,

        // Float to Decimal
        (DataType::F32 | DataType::F64, DataType::Decimal(_, _)) => true,

        // Float to unsigned integers (for large literals parsed as float)
        // This happens when literals exceed I128::MAX
        (
            DataType::F64,
            DataType::U128 | DataType::U64 | DataType::U32 | DataType::U16 | DataType::U8,
        ) => true,
        (DataType::F32, DataType::U64 | DataType::U32 | DataType::U16 | DataType::U8) => true,

        // String to date/time types (parsing)
        (DataType::Str, DataType::Date | DataType::Time | DataType::Timestamp | DataType::Uuid) => {
            true
        }

        // String to collections (JSON parsing)
        (
            DataType::Str,
            DataType::List(_)
            | DataType::Array(_, _)
            | DataType::Map(_, _)
            | DataType::Struct(_)
            | DataType::Json,
        ) => true,

        // JSON to String conversion
        (DataType::Json, DataType::Str | DataType::Text) => true,

        // List/Array interchangeability
        (DataType::List(_), DataType::Array(_, _)) => true,
        (DataType::Array(_, _), DataType::List(_)) => true,

        // Map to Struct conversion
        (DataType::Map(_, _), DataType::Struct(_)) => true,

        // For parameterized queries: allow narrowing with runtime checks
        // This is important for parameter binding where we don't know the exact value
        (DataType::I128, DataType::I64 | DataType::I32 | DataType::I16 | DataType::I8) => true,
        (DataType::I64, DataType::I32 | DataType::I16 | DataType::I8) => true,
        (DataType::I32, DataType::I16 | DataType::I8) => true,
        (DataType::I16, DataType::I8) => true,

        // Allow signed to unsigned with runtime checks for parameters
        (
            DataType::I128,
            DataType::U128 | DataType::U64 | DataType::U32 | DataType::U16 | DataType::U8,
        ) => true,
        (
            DataType::I64,
            DataType::U128 | DataType::U64 | DataType::U32 | DataType::U16 | DataType::U8,
        ) => true,
        (
            DataType::I32,
            DataType::U128 | DataType::U64 | DataType::U32 | DataType::U16 | DataType::U8,
        ) => true,
        (
            DataType::I16,
            DataType::U128 | DataType::U64 | DataType::U32 | DataType::U16 | DataType::U8,
        ) => true,
        (
            DataType::I8,
            DataType::U128 | DataType::U64 | DataType::U32 | DataType::U16 | DataType::U8,
        ) => true,

        _ => false,
    }
}

/// Coerce a value to the target type
pub fn coerce_value(value: Value, target_type: &DataType) -> Result<Value> {
    value::coerce_value_impl(value, target_type)
}

#[cfg(test)]
mod tests;
