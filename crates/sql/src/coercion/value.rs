//! Value coercion implementation
//!
//! This module orchestrates all type coercions by delegating to specialized helper modules.

use crate::error::{Error, Result};
use crate::types::{DataType, Value, ValueExt};

use super::{collection, numeric, special, string, temporal};

/// Coerce a value to match the target data type
/// This handles implicit type conversions that are safe and expected in SQL
pub fn coerce_value_impl(value: Value, target_type: &DataType) -> Result<Value> {
    // Fast path: same type (except Struct which needs field-level coercion)
    if !matches!(value, Value::Struct(_)) && value.data_type() == *target_type {
        return Ok(value);
    }

    // Special handling for Map values that need element coercion
    if let (Value::Map(map), DataType::Map(_key_type, value_type)) = (&value, target_type) {
        return collection::coerce_map(map.clone(), value_type);
    }

    match (&value, target_type) {
        // ========== NULL HANDLING ==========
        (Value::Null, _) => Ok(Value::Null),

        // Non-null value to Nullable type - unwrap the Nullable and try to coerce
        (_, DataType::Nullable(inner_type)) if !matches!(value, Value::Null) => {
            coerce_value_impl(value, inner_type)
        }

        // ========== SIGNED INTEGER COERCIONS ==========
        (Value::I8(v), target)
            if matches!(
                target,
                DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128
            ) =>
        {
            numeric::coerce_signed_widen(*v as i128, target)
        }
        (Value::I16(v), target)
            if matches!(
                target,
                DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128
            ) =>
        {
            numeric::coerce_signed_widen(*v as i128, target)
        }
        (Value::I32(v), target)
            if matches!(
                target,
                DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128
            ) =>
        {
            numeric::coerce_signed_widen(*v as i128, target)
        }
        (Value::I64(v), target)
            if matches!(
                target,
                DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128
            ) =>
        {
            numeric::coerce_signed_widen(*v as i128, target)
        }
        (Value::I128(v), target)
            if matches!(
                target,
                DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128
            ) =>
        {
            numeric::coerce_signed_widen(*v, target)
        }

        // ========== SIGNED TO UNSIGNED ==========
        (Value::I8(v), target)
            if matches!(
                target,
                DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::U128
            ) =>
        {
            numeric::coerce_signed_to_unsigned(*v as i128, target)
        }
        (Value::I16(v), target)
            if matches!(
                target,
                DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::U128
            ) =>
        {
            numeric::coerce_signed_to_unsigned(*v as i128, target)
        }
        (Value::I32(v), target)
            if matches!(
                target,
                DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::U128
            ) =>
        {
            numeric::coerce_signed_to_unsigned(*v as i128, target)
        }
        (Value::I64(v), target)
            if matches!(
                target,
                DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::U128
            ) =>
        {
            numeric::coerce_signed_to_unsigned(*v as i128, target)
        }
        (Value::I128(v), target)
            if matches!(
                target,
                DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::U128
            ) =>
        {
            numeric::coerce_signed_to_unsigned(*v, target)
        }

        // ========== UNSIGNED INTEGER COERCIONS ==========
        (Value::U8(v), target)
            if matches!(
                target,
                DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::U128
            ) =>
        {
            numeric::coerce_unsigned_widen(*v as u128, target)
        }
        (Value::U16(v), target)
            if matches!(
                target,
                DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::U128
            ) =>
        {
            numeric::coerce_unsigned_widen(*v as u128, target)
        }
        (Value::U32(v), target)
            if matches!(
                target,
                DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::U128
            ) =>
        {
            numeric::coerce_unsigned_widen(*v as u128, target)
        }
        (Value::U64(v), target)
            if matches!(
                target,
                DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::U128
            ) =>
        {
            numeric::coerce_unsigned_widen(*v as u128, target)
        }
        (Value::U128(v), target)
            if matches!(
                target,
                DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::U128
            ) =>
        {
            numeric::coerce_unsigned_widen(*v, target)
        }

        // ========== UNSIGNED TO SIGNED ==========
        (Value::U8(v), target)
            if matches!(
                target,
                DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128
            ) =>
        {
            numeric::coerce_unsigned_to_signed(*v as u128, target)
        }
        (Value::U16(v), target)
            if matches!(
                target,
                DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128
            ) =>
        {
            numeric::coerce_unsigned_to_signed(*v as u128, target)
        }
        (Value::U32(v), target)
            if matches!(
                target,
                DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128
            ) =>
        {
            numeric::coerce_unsigned_to_signed(*v as u128, target)
        }
        (Value::U64(v), target)
            if matches!(
                target,
                DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128
            ) =>
        {
            numeric::coerce_unsigned_to_signed(*v as u128, target)
        }
        (Value::U128(v), target)
            if matches!(
                target,
                DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128
            ) =>
        {
            numeric::coerce_unsigned_to_signed(*v, target)
        }

        // ========== FLOAT COERCIONS ==========
        (Value::F32(v), DataType::F64) => Ok(Value::F64(*v as f64)),
        (Value::F64(v), DataType::F32) => Ok(Value::F32(*v as f32)),

        // ========== INTEGER TO FLOAT ==========
        (Value::I8(v), target @ (DataType::F32 | DataType::F64)) => {
            numeric::coerce_int_to_float(*v as i128, target)
        }
        (Value::I16(v), target @ (DataType::F32 | DataType::F64)) => {
            numeric::coerce_int_to_float(*v as i128, target)
        }
        (Value::I32(v), target @ (DataType::F32 | DataType::F64)) => {
            numeric::coerce_int_to_float(*v as i128, target)
        }
        (Value::I64(v), target @ (DataType::F32 | DataType::F64)) => {
            numeric::coerce_int_to_float(*v as i128, target)
        }
        (Value::I128(v), target @ (DataType::F32 | DataType::F64)) => {
            numeric::coerce_int_to_float(*v, target)
        }
        (Value::U8(v), target @ (DataType::F32 | DataType::F64)) => {
            numeric::coerce_int_to_float(*v as i128, target)
        }
        (Value::U16(v), target @ (DataType::F32 | DataType::F64)) => {
            numeric::coerce_int_to_float(*v as i128, target)
        }
        (Value::U32(v), target @ (DataType::F32 | DataType::F64)) => {
            numeric::coerce_int_to_float(*v as i128, target)
        }
        (Value::U64(v), target @ (DataType::F32 | DataType::F64)) => {
            numeric::coerce_int_to_float(*v as i128, target)
        }
        (Value::U128(v), target @ (DataType::F32 | DataType::F64)) => {
            // U128 might overflow i128, handle specially
            if *v <= i128::MAX as u128 {
                numeric::coerce_int_to_float(*v as i128, target)
            } else {
                // Large U128 value - convert directly to float
                match target {
                    DataType::F32 => Ok(Value::F32(*v as f32)),
                    DataType::F64 => Ok(Value::F64(*v as f64)),
                    _ => unreachable!(),
                }
            }
        }

        // ========== FLOAT TO INTEGER ==========
        (Value::F32(v), target)
            if matches!(
                target,
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
            ) =>
        {
            numeric::coerce_float_to_int(*v as f64, target)
        }
        (Value::F64(v), target)
            if matches!(
                target,
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
            ) =>
        {
            numeric::coerce_float_to_int(*v, target)
        }

        // ========== DECIMAL COERCIONS ==========
        (Value::Decimal(d), DataType::Decimal(precision, scale)) => {
            numeric::enforce_decimal_precision(*d, *precision, *scale)
        }
        (Value::Decimal(d), DataType::F32 | DataType::F64) => {
            numeric::coerce_decimal_to_float(d, target_type)
        }
        (Value::Decimal(d), target)
            if matches!(
                target,
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
            ) =>
        {
            numeric::coerce_decimal_to_int(d, target)
        }

        // Integer to Decimal
        (Value::I8(v), DataType::Decimal(_, _)) => numeric::coerce_int_to_decimal(*v as i128),
        (Value::I16(v), DataType::Decimal(_, _)) => numeric::coerce_int_to_decimal(*v as i128),
        (Value::I32(v), DataType::Decimal(_, _)) => numeric::coerce_int_to_decimal(*v as i128),
        (Value::I64(v), DataType::Decimal(_, _)) => numeric::coerce_int_to_decimal(*v as i128),
        (Value::I128(v), DataType::Decimal(_, _)) => numeric::coerce_int_to_decimal(*v),
        (Value::U8(v), DataType::Decimal(_, _)) => numeric::coerce_int_to_decimal(*v as i128),
        (Value::U16(v), DataType::Decimal(_, _)) => numeric::coerce_int_to_decimal(*v as i128),
        (Value::U32(v), DataType::Decimal(_, _)) => numeric::coerce_int_to_decimal(*v as i128),
        (Value::U64(v), DataType::Decimal(_, _)) => numeric::coerce_int_to_decimal(*v as i128),
        (Value::U128(v), DataType::Decimal(_, _)) => {
            if *v <= i128::MAX as u128 {
                numeric::coerce_int_to_decimal(*v as i128)
            } else {
                Err(Error::InvalidValue(
                    "U128 value too large for Decimal".into(),
                ))
            }
        }

        // Float to Decimal
        (Value::F32(v), DataType::Decimal(_, _)) => numeric::coerce_float_to_decimal(*v as f64),
        (Value::F64(v), DataType::Decimal(_, _)) => numeric::coerce_float_to_decimal(*v),

        // ========== BOOLEAN COERCIONS ==========
        (Value::Bool(_), DataType::Bool) => Ok(value),
        (Value::Bool(b), DataType::I8) => Ok(Value::I8(if *b { 1 } else { 0 })),
        (Value::Bool(b), DataType::I16) => Ok(Value::I16(if *b { 1 } else { 0 })),
        (Value::Bool(b), DataType::I32) => Ok(Value::I32(if *b { 1 } else { 0 })),
        (Value::Bool(b), DataType::I64) => Ok(Value::I64(if *b { 1 } else { 0 })),
        (Value::Bool(b), DataType::I128) => Ok(Value::I128(if *b { 1 } else { 0 })),
        (Value::Bool(b), DataType::U8) => Ok(Value::U8(if *b { 1 } else { 0 })),
        (Value::Bool(b), DataType::U16) => Ok(Value::U16(if *b { 1 } else { 0 })),
        (Value::Bool(b), DataType::U32) => Ok(Value::U32(if *b { 1 } else { 0 })),
        (Value::Bool(b), DataType::U64) => Ok(Value::U64(if *b { 1 } else { 0 })),
        (Value::Bool(b), DataType::U128) => Ok(Value::U128(if *b { 1 } else { 0 })),

        (Value::I8(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),
        (Value::I16(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),
        (Value::I32(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),
        (Value::I64(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),
        (Value::I128(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),
        (Value::U8(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),
        (Value::U16(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),
        (Value::U32(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),
        (Value::U64(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),
        (Value::U128(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),

        // ========== STRING COERCIONS ==========
        (Value::Str(_), DataType::Text | DataType::Str) => Ok(value),

        // String to numeric types
        (
            Value::Str(s),
            target
            @ (DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 | DataType::I128),
        ) => string::parse_string_to_signed(s, target),
        (
            Value::Str(s),
            target
            @ (DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::U128),
        ) => string::parse_string_to_unsigned(s, target),
        (Value::Str(s), target @ (DataType::F32 | DataType::F64)) => {
            string::parse_string_to_float(s, target)
        }
        (Value::Str(s), DataType::Decimal(_, _)) => string::parse_string_to_decimal(s),
        (Value::Str(s), DataType::Bool) => string::parse_string_to_bool(s),

        // String to temporal types
        (Value::Str(s), DataType::Date) => temporal::parse_string_to_date(s),
        (Value::Str(s), DataType::Time) => temporal::parse_string_to_time(s),
        (Value::Str(s), DataType::Timestamp) => temporal::parse_string_to_timestamp(s),
        (Value::Str(s), DataType::Interval) => temporal::parse_string_to_interval(s),

        // String to special types
        (Value::Str(s), DataType::Uuid) => special::parse_string_to_uuid(s),
        (Value::Str(s), DataType::Inet) => special::parse_string_to_inet(s),
        (Value::Str(s), DataType::Point) => special::parse_string_to_point(s),
        (Value::Str(s), DataType::Bytea) => special::parse_string_to_bytea(s),
        (Value::Str(s), DataType::Json) => special::parse_string_to_json(s),

        // String to collection types
        (Value::Str(s), DataType::List(elem_type)) => collection::parse_json_to_list(s, elem_type),
        (Value::Str(s), DataType::Array(elem_type, size)) => {
            collection::parse_json_to_array(s, elem_type, *size)
        }
        (Value::Str(s), DataType::Map(key_type, value_type)) => {
            collection::parse_json_to_map(s, key_type, value_type)
        }
        (Value::Str(s), DataType::Struct(schema_fields)) => {
            collection::parse_json_to_struct(s, schema_fields)
        }

        // ========== SPECIAL TYPE COERCIONS ==========
        (Value::Uuid(_), DataType::Uuid) => Ok(value),
        (Value::Inet(_), DataType::Inet) => Ok(value),
        (Value::Point(_), DataType::Point) => Ok(value),
        (Value::Bytea(bytes), DataType::Str | DataType::Text) => {
            special::coerce_bytea_to_string(bytes)
        }

        // Integer to INET
        (Value::U8(v), DataType::Inet) => special::coerce_int_to_inet(*v as u128),
        (Value::U16(v), DataType::Inet) => special::coerce_int_to_inet(*v as u128),
        (Value::U32(v), DataType::Inet) => special::coerce_int_to_inet(*v as u128),
        (Value::U64(v), DataType::Inet) => special::coerce_int_to_inet(*v as u128),
        (Value::U128(v), DataType::Inet) => special::coerce_int_to_inet(*v),
        (Value::I8(v), DataType::Inet) => {
            if *v < 0 {
                Err(Error::InvalidValue(format!(
                    "Cannot convert negative value {} to INET",
                    v
                )))
            } else {
                special::coerce_int_to_inet(*v as u128)
            }
        }
        (Value::I16(v), DataType::Inet) => {
            if *v < 0 {
                Err(Error::InvalidValue(format!(
                    "Cannot convert negative value {} to INET",
                    v
                )))
            } else {
                special::coerce_int_to_inet(*v as u128)
            }
        }
        (Value::I32(v), DataType::Inet) => {
            if *v < 0 {
                Err(Error::InvalidValue(format!(
                    "Cannot convert negative value {} to INET",
                    v
                )))
            } else {
                special::coerce_int_to_inet(*v as u128)
            }
        }
        (Value::I64(v), DataType::Inet) => {
            if *v < 0 {
                Err(Error::InvalidValue(format!(
                    "Cannot convert negative value {} to INET",
                    v
                )))
            } else {
                special::coerce_int_to_inet(*v as u128)
            }
        }
        (Value::I128(v), DataType::Inet) => {
            if *v < 0 {
                Err(Error::InvalidValue(format!(
                    "Cannot convert negative value {} to INET",
                    v
                )))
            } else {
                special::coerce_int_to_inet(*v as u128)
            }
        }

        // JSON coercions
        (Value::Json(j), DataType::Str | DataType::Text) => special::coerce_json_to_string(j),
        (Value::Json(_), DataType::Json) => Ok(value),

        // ========== COLLECTION COERCIONS ==========
        (Value::List(items), DataType::List(elem_type)) => {
            collection::coerce_list(items.clone(), elem_type)
        }
        (Value::List(items), DataType::Array(elem_type, size)) => {
            collection::coerce_list_to_array(items.clone(), elem_type, *size)
        }
        (Value::Array(items), DataType::List(elem_type)) => {
            collection::coerce_array_to_list(items.clone(), elem_type)
        }
        (Value::Array(items), DataType::Array(elem_type, size)) => {
            collection::coerce_list_to_array(items.clone(), elem_type, *size)
        }
        (Value::Map(map), DataType::Struct(schema_fields)) => {
            collection::coerce_map_to_struct(map.clone(), schema_fields)
        }
        (Value::Struct(fields), DataType::Struct(schema_fields)) => {
            collection::coerce_struct(fields.clone(), schema_fields)
        }

        // ========== TEMPORAL TYPE SELF-COERCIONS ==========
        (Value::Date(_), DataType::Date) => Ok(value),
        (Value::Time(_), DataType::Time) => Ok(value),
        (Value::Timestamp(_), DataType::Timestamp) => Ok(value),
        (Value::Interval(_), DataType::Interval) => Ok(value),

        // ========== ANY TYPE TO STRING ==========
        (_, DataType::Str | DataType::Text) => Ok(Value::Str(value.to_string())),

        // ========== NO COERCION POSSIBLE ==========
        _ => Err(Error::TypeMismatch {
            expected: target_type.to_string(),
            found: value.data_type().to_string(),
        }),
    }
}

/// Coerce a row of values to match a table schema
pub fn coerce_row(row: Vec<Value>, schema: &crate::types::schema::Table) -> Result<Vec<Value>> {
    if row.len() != schema.columns.len() {
        return Err(Error::ExecutionError(format!(
            "Row has {} values but table has {} columns",
            row.len(),
            schema.columns.len()
        )));
    }

    row.into_iter()
        .zip(&schema.columns)
        .map(|(value, column)| coerce_value_impl(value, &column.data_type))
        .collect()
}
