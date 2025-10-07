//! Encoding utilities for storage
//!
//! This module provides three types of encoding:
//! 1. Index keys - Sortable encoding for index values
//! 2. Row values - Schema-aware compact encoding

use crate::error::{Error, Result};
use crate::types::data_type::{DataType, Interval, Point};
use crate::types::schema::Table;
use crate::types::value::Value;
use chrono::{NaiveDate, NaiveTime, Timelike};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::net::IpAddr;
use uuid::Uuid;

// ============================================================================
// Index Key Encoding (Sortable)
// ============================================================================

/// Encode an index key with row_id
pub fn encode_index_key(index_values: &[Value], row_id: u64) -> Vec<u8> {
    let mut key = Vec::new();

    // Encode each value in a sortable way
    for value in index_values {
        encode_value_sortable(value, &mut key);
    }

    // Append row_id for uniqueness
    key.extend_from_slice(&row_id.to_be_bytes());
    key
}

/// Encode a value in a sortable binary format (for index keys)
pub fn encode_value_sortable(value: &Value, output: &mut Vec<u8>) {
    match value {
        Value::Null => {
            output.push(0x00); // NULL sorts first
        }
        Value::Bool(b) => {
            output.push(0x01);
            output.push(if *b { 1 } else { 0 });
        }
        // Integer types - sorted by type then value
        Value::I8(i) => {
            output.push(0x02);
            let u = (*i as u8) ^ (1u8 << 7);
            output.push(u);
        }
        Value::I16(i) => {
            output.push(0x03);
            let u = (*i as u16) ^ (1u16 << 15);
            output.extend_from_slice(&u.to_be_bytes());
        }
        Value::I32(i) => {
            output.push(0x04);
            let u = (*i as u32) ^ (1u32 << 31);
            output.extend_from_slice(&u.to_be_bytes());
        }
        Value::I64(i) => {
            output.push(0x05);
            let u = (*i as u64) ^ (1u64 << 63);
            output.extend_from_slice(&u.to_be_bytes());
        }
        Value::I128(i) => {
            output.push(0x06);
            let u = (*i as u128) ^ (1u128 << 127);
            output.extend_from_slice(&u.to_be_bytes());
        }
        Value::U8(u) => {
            output.push(0x07);
            output.push(*u);
        }
        Value::U16(u) => {
            output.push(0x08);
            output.extend_from_slice(&u.to_be_bytes());
        }
        Value::U32(u) => {
            output.push(0x09);
            output.extend_from_slice(&u.to_be_bytes());
        }
        Value::U64(u) => {
            output.push(0x0A);
            output.extend_from_slice(&u.to_be_bytes());
        }
        Value::U128(u) => {
            output.push(0x0B);
            output.extend_from_slice(&u.to_be_bytes());
        }
        // Float types
        Value::F32(f) => {
            output.push(0x0C);
            let bits = f.to_bits();
            let sortable = if f.is_sign_negative() {
                !bits
            } else {
                bits ^ (1u32 << 31)
            };
            output.extend_from_slice(&sortable.to_be_bytes());
        }
        Value::F64(f) => {
            output.push(0x0D);
            let bits = f.to_bits();
            let sortable = if f.is_sign_negative() {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            output.extend_from_slice(&sortable.to_be_bytes());
        }
        // Decimal - encode mantissa and scale directly for sortability
        Value::Decimal(d) => {
            output.push(0x0E);
            // For sorting, we need to handle sign and magnitude
            let mantissa = d.mantissa();
            let scale = d.scale();
            // XOR with sign bit for proper signed sorting
            let sortable = (mantissa as u128) ^ (1u128 << 127);
            output.extend_from_slice(&sortable.to_be_bytes());
            output.extend_from_slice(&scale.to_be_bytes());
        }
        // String
        Value::Str(s) => {
            output.push(0x0F);
            let bytes = s.as_bytes();
            output.extend_from_slice(bytes);
            output.push(0x00); // Null terminator for sorting
        }
        // Date/Time types - encode as numeric values for sorting
        Value::Date(d) => {
            output.push(0x10);
            let days = d
                .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                .num_days();
            output.extend_from_slice(&days.to_be_bytes());
        }
        Value::Time(t) => {
            output.push(0x11);
            let nanos =
                t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64;
            output.extend_from_slice(&nanos.to_be_bytes());
        }
        Value::Timestamp(ts) => {
            output.push(0x12);
            let timestamp = ts.and_utc().timestamp();
            let nanos = ts.and_utc().timestamp_subsec_nanos();
            output.extend_from_slice(&timestamp.to_be_bytes());
            output.extend_from_slice(&nanos.to_be_bytes());
        }
        Value::Interval(i) => {
            output.push(0x13);
            // Intervals are complex - months, days, microseconds
            output.extend_from_slice(&i.months.to_be_bytes());
            output.extend_from_slice(&i.days.to_be_bytes());
            output.extend_from_slice(&i.microseconds.to_be_bytes());
        }
        // Special types
        Value::Uuid(u) => {
            output.push(0x14);
            output.extend_from_slice(u.as_bytes());
        }
        Value::Bytea(b) => {
            output.push(0x15);
            output.extend_from_slice(&(b.len() as u32).to_be_bytes());
            output.extend_from_slice(b);
        }
        Value::Inet(ip) => {
            output.push(0x16);
            match ip {
                IpAddr::V4(v4) => {
                    output.push(4);
                    output.extend_from_slice(&v4.octets());
                }
                IpAddr::V6(v6) => {
                    output.push(6);
                    output.extend_from_slice(&v6.octets());
                }
            }
        }
        Value::Point(p) => {
            output.push(0x17);
            output.extend_from_slice(&p.x.to_bits().to_be_bytes());
            output.extend_from_slice(&p.y.to_bits().to_be_bytes());
        }
        // Collection types - encode recursively for proper sorting
        Value::Array(arr) | Value::List(arr) => {
            output.push(0x18);
            output.extend_from_slice(&(arr.len() as u32).to_be_bytes());
            for value in arr {
                encode_value_sortable(value, output);
            }
        }
        Value::Map(m) => {
            output.push(0x19);
            output.extend_from_slice(&(m.len() as u32).to_be_bytes());
            // Sort keys for deterministic ordering
            let mut sorted_keys: Vec<_> = m.keys().collect();
            sorted_keys.sort();
            for key in sorted_keys {
                let key_bytes = key.as_bytes();
                output.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
                output.extend_from_slice(key_bytes);
                encode_value_sortable(&m[key], output);
            }
        }
        Value::Struct(fields) => {
            output.push(0x1A);
            output.extend_from_slice(&(fields.len() as u32).to_be_bytes());
            // Encode fields in the order they appear (assumes consistent ordering)
            for (name, value) in fields {
                let name_bytes = name.as_bytes();
                output.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
                output.extend_from_slice(name_bytes);
                encode_value_sortable(value, output);
            }
        }
        Value::Json(j) => {
            output.push(0x1B);
            // For sorting, encode JSON as its string representation
            let json_str = j.to_string();
            let bytes = json_str.as_bytes();
            output.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            output.extend_from_slice(bytes);
        }
    }
}

// ============================================================================
// Row Value Encoding (Schema-Aware Compact Format)
// ============================================================================

/// Current row encoding format version
pub const ROW_FORMAT_VERSION: u8 = 1;

/// Encode a row using schema-aware format (30-50% smaller than bincode)
///
/// Format: [version:1][schema_id:2][null_bitmap:ceil(n/8)][packed_values...]
pub fn encode_row(values: &[Value], schema: &Table) -> Result<Vec<u8>> {
    if values.len() != schema.columns.len() {
        return Err(Error::InvalidValue(format!(
            "Row has {} columns but schema has {}",
            values.len(),
            schema.columns.len()
        )));
    }

    let mut buf = Vec::new();

    // Write version
    buf.push(ROW_FORMAT_VERSION);

    // Write schema_version
    buf.extend_from_slice(&schema.schema_version.to_le_bytes());

    // Build null bitmap
    let num_bytes = schema.columns.len().div_ceil(8);
    let mut null_bitmap = vec![0u8; num_bytes];

    for (i, value) in values.iter().enumerate() {
        if value.is_null() {
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            null_bitmap[byte_idx] |= 1 << bit_idx;
        }
    }

    buf.extend_from_slice(&null_bitmap);

    // Encode non-null values
    for (i, value) in values.iter().enumerate() {
        if !value.is_null() {
            let expected_type = &schema.columns[i].data_type;
            encode_value_compact(value, expected_type, &mut buf)?;
        }
    }

    Ok(buf)
}

/// Decode a row using schema-aware format
pub fn decode_row(bytes: &[u8], schema: &Table) -> Result<Vec<Value>> {
    let mut cursor = Cursor::new(bytes);

    // Read and verify version
    let mut version = [0u8; 1];
    cursor.read_exact(&mut version)?;
    if version[0] != ROW_FORMAT_VERSION {
        return Err(Error::InvalidValue(format!(
            "Unsupported row format version: {}",
            version[0]
        )));
    }

    // Read and verify schema_version
    let mut schema_version_bytes = [0u8; 4];
    cursor.read_exact(&mut schema_version_bytes)?;
    let schema_version = u32::from_le_bytes(schema_version_bytes);

    if schema_version != schema.schema_version {
        return Err(Error::InvalidValue(format!(
            "Schema version mismatch: expected {}, got {}",
            schema.schema_version, schema_version
        )));
    }

    // Read null bitmap
    let num_bytes = schema.columns.len().div_ceil(8);
    let mut null_bitmap = vec![0u8; num_bytes];
    cursor.read_exact(&mut null_bitmap)?;

    // Decode values
    let mut values = Vec::with_capacity(schema.columns.len());

    for (i, column) in schema.columns.iter().enumerate() {
        let byte_idx = i / 8;
        let bit_idx = i % 8;
        let is_null = (null_bitmap[byte_idx] & (1 << bit_idx)) != 0;

        if is_null {
            values.push(Value::Null);
        } else {
            let value = decode_value_compact(&mut cursor, &column.data_type)?;
            values.push(value);
        }
    }

    Ok(values)
}

/// Encode a single value without type tag (type is known from schema)
fn encode_value_compact(value: &Value, expected_type: &DataType, buf: &mut Vec<u8>) -> Result<()> {
    match (value, expected_type.base_type()) {
        (Value::Null, _) => {
            return Err(Error::InvalidValue(
                "Cannot encode null in compact format (use null bitmap)".into(),
            ));
        }

        // Integers
        (Value::I8(v), DataType::I8) => buf.extend_from_slice(&v.to_le_bytes()),
        (Value::I16(v), DataType::I16) => buf.extend_from_slice(&v.to_le_bytes()),
        (Value::I32(v), DataType::I32) => buf.extend_from_slice(&v.to_le_bytes()),
        (Value::I64(v), DataType::I64) => buf.extend_from_slice(&v.to_le_bytes()),
        (Value::I128(v), DataType::I128) => buf.extend_from_slice(&v.to_le_bytes()),

        (Value::U8(v), DataType::U8) => buf.push(*v),
        (Value::U16(v), DataType::U16) => buf.extend_from_slice(&v.to_le_bytes()),
        (Value::U32(v), DataType::U32) => buf.extend_from_slice(&v.to_le_bytes()),
        (Value::U64(v), DataType::U64) => buf.extend_from_slice(&v.to_le_bytes()),
        (Value::U128(v), DataType::U128) => buf.extend_from_slice(&v.to_le_bytes()),

        // Floats
        (Value::F32(v), DataType::F32) => buf.extend_from_slice(&v.to_bits().to_le_bytes()),
        (Value::F64(v), DataType::F64) => buf.extend_from_slice(&v.to_bits().to_le_bytes()),

        // Boolean
        (Value::Bool(b), DataType::Bool) => buf.push(if *b { 1 } else { 0 }),

        // Variable-length types need length prefix
        (Value::Str(s), DataType::Str | DataType::Text) => {
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }

        (Value::Bytea(b), DataType::Bytea) => {
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }

        // Decimal
        (Value::Decimal(d), DataType::Decimal(_, _)) => {
            let mantissa = d.mantissa();
            let scale = d.scale();
            buf.extend_from_slice(&mantissa.to_le_bytes());
            buf.extend_from_slice(&scale.to_le_bytes());
        }

        (Value::Uuid(u), DataType::Uuid) => {
            buf.extend_from_slice(u.as_bytes());
        }

        // Date/Time types
        (Value::Date(d), DataType::Date) => {
            let days = d
                .signed_duration_since(
                    NaiveDate::from_ymd_opt(1970, 1, 1)
                        .ok_or_else(|| Error::InvalidValue("Invalid epoch date".into()))?,
                )
                .num_days();
            buf.extend_from_slice(&days.to_le_bytes());
        }

        (Value::Time(t), DataType::Time) => {
            let nanos =
                t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64;
            buf.extend_from_slice(&nanos.to_le_bytes());
        }

        (Value::Timestamp(ts), DataType::Timestamp) => {
            let timestamp = ts.and_utc().timestamp();
            let nanos = ts.and_utc().timestamp_subsec_nanos();
            buf.extend_from_slice(&timestamp.to_le_bytes());
            buf.extend_from_slice(&nanos.to_le_bytes());
        }

        (Value::Interval(i), DataType::Interval) => {
            buf.extend_from_slice(&i.months.to_le_bytes());
            buf.extend_from_slice(&i.days.to_le_bytes());
            buf.extend_from_slice(&i.microseconds.to_le_bytes());
        }

        (Value::Inet(ip), DataType::Inet) => match ip {
            IpAddr::V4(v4) => {
                buf.push(4);
                buf.extend_from_slice(&v4.octets());
            }
            IpAddr::V6(v6) => {
                buf.push(6);
                buf.extend_from_slice(&v6.octets());
            }
        },

        (Value::Point(p), DataType::Point) => {
            buf.extend_from_slice(&p.x.to_bits().to_le_bytes());
            buf.extend_from_slice(&p.y.to_bits().to_le_bytes());
        }

        // Array/List: encode count + null bitmap + elements recursively
        (Value::Array(arr), DataType::Array(elem_type, _)) => {
            buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());

            // Encode null bitmap for array elements
            let num_bytes = arr.len().div_ceil(8);
            let mut null_bitmap = vec![0u8; num_bytes];
            for (i, value) in arr.iter().enumerate() {
                if value.is_null() {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    null_bitmap[byte_idx] |= 1 << bit_idx;
                }
            }
            buf.extend_from_slice(&null_bitmap);

            // Encode non-null values
            for value in arr {
                if !value.is_null() {
                    encode_value_compact(value, elem_type, buf)?;
                }
            }
        }

        (Value::List(arr), DataType::List(elem_type)) => {
            buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());

            // Encode null bitmap for list elements
            let num_bytes = arr.len().div_ceil(8);
            let mut null_bitmap = vec![0u8; num_bytes];
            for (i, value) in arr.iter().enumerate() {
                if value.is_null() {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    null_bitmap[byte_idx] |= 1 << bit_idx;
                }
            }
            buf.extend_from_slice(&null_bitmap);

            // Encode non-null values
            for value in arr {
                if !value.is_null() {
                    encode_value_compact(value, elem_type, buf)?;
                }
            }
        }

        // Handle Array value with List type (should not happen but be defensive)
        (Value::Array(arr), DataType::List(elem_type)) => {
            buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());

            // Encode null bitmap for array elements
            let num_bytes = arr.len().div_ceil(8);
            let mut null_bitmap = vec![0u8; num_bytes];
            for (i, value) in arr.iter().enumerate() {
                if value.is_null() {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    null_bitmap[byte_idx] |= 1 << bit_idx;
                }
            }
            buf.extend_from_slice(&null_bitmap);

            // Encode non-null values
            for value in arr {
                if !value.is_null() {
                    encode_value_compact(value, elem_type, buf)?;
                }
            }
        }

        // Map: encode count + key-value pairs recursively
        (Value::Map(m), DataType::Map(_key_type, value_type)) => {
            buf.extend_from_slice(&(m.len() as u32).to_le_bytes());
            for (key, value) in m {
                // Keys are always strings in our Map type
                let key_bytes = key.as_bytes();
                buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(key_bytes);
                encode_value_compact(value, value_type, buf)?;
            }
        }

        // Struct: encode values in schema order (no field names needed!)
        (Value::Struct(fields), DataType::Struct(field_types)) => {
            // Build a null bitmap for struct fields
            let num_bytes = field_types.len().div_ceil(8);
            let mut null_bitmap = vec![0u8; num_bytes];

            // Create a map for fast lookup
            let field_map: HashMap<&str, &Value> =
                fields.iter().map(|(k, v)| (k.as_str(), v)).collect();

            // Check which fields are null
            for (i, (field_name, _field_type)) in field_types.iter().enumerate() {
                if let Some(value) = field_map.get(field_name.as_str()) {
                    if value.is_null() {
                        let byte_idx = i / 8;
                        let bit_idx = i % 8;
                        null_bitmap[byte_idx] |= 1 << bit_idx;
                    }
                } else {
                    // Missing field is treated as null
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    null_bitmap[byte_idx] |= 1 << bit_idx;
                }
            }

            // Write null bitmap
            buf.extend_from_slice(&null_bitmap);

            // Encode non-null values in schema order
            for (field_name, field_type) in field_types {
                if let Some(value) = field_map.get(field_name.as_str())
                    && !value.is_null()
                {
                    encode_value_compact(value, field_type, buf)?;
                }
            }
        }

        // JSON: encode using compact binary format
        (Value::Json(j), DataType::Json) => {
            encode_json_compact(j, buf)?;
        }

        _ => {
            return Err(Error::InvalidValue(format!(
                "Type mismatch: value {:?} doesn't match schema type {:?}",
                value, expected_type
            )));
        }
    }

    Ok(())
}

/// Encode JSON value in compact binary format
fn encode_json_compact(json: &serde_json::Value, buf: &mut Vec<u8>) -> Result<()> {
    match json {
        serde_json::Value::Null => {
            buf.push(0x00); // Null tag
        }
        serde_json::Value::Bool(b) => {
            buf.push(0x01); // Bool tag
            buf.push(if *b { 1 } else { 0 });
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                buf.push(0x02); // i64 tag
                buf.extend_from_slice(&i.to_le_bytes());
            } else if let Some(f) = n.as_f64() {
                buf.push(0x03); // f64 tag
                buf.extend_from_slice(&f.to_bits().to_le_bytes());
            } else {
                return Err(Error::InvalidValue(format!("Invalid JSON number: {}", n)));
            }
        }
        serde_json::Value::String(s) => {
            buf.push(0x04); // String tag
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        serde_json::Value::Array(arr) => {
            buf.push(0x05); // Array tag
            buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());
            for val in arr {
                encode_json_compact(val, buf)?;
            }
        }
        serde_json::Value::Object(obj) => {
            buf.push(0x06); // Object tag
            buf.extend_from_slice(&(obj.len() as u32).to_le_bytes());
            for (key, val) in obj {
                // Encode key
                let key_bytes = key.as_bytes();
                buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(key_bytes);
                // Encode value
                encode_json_compact(val, buf)?;
            }
        }
    }
    Ok(())
}

/// Decode a single value (type is known from schema)
fn decode_value_compact(cursor: &mut Cursor<&[u8]>, expected_type: &DataType) -> Result<Value> {
    let value = match expected_type.base_type() {
        // Integers
        DataType::I8 => {
            let mut bytes = [0u8; 1];
            cursor.read_exact(&mut bytes)?;
            Value::I8(i8::from_le_bytes(bytes))
        }
        DataType::I16 => {
            let mut bytes = [0u8; 2];
            cursor.read_exact(&mut bytes)?;
            Value::I16(i16::from_le_bytes(bytes))
        }
        DataType::I32 => {
            let mut bytes = [0u8; 4];
            cursor.read_exact(&mut bytes)?;
            Value::I32(i32::from_le_bytes(bytes))
        }
        DataType::I64 => {
            let mut bytes = [0u8; 8];
            cursor.read_exact(&mut bytes)?;
            Value::I64(i64::from_le_bytes(bytes))
        }
        DataType::I128 => {
            let mut bytes = [0u8; 16];
            cursor.read_exact(&mut bytes)?;
            Value::I128(i128::from_le_bytes(bytes))
        }

        DataType::U8 => {
            let mut bytes = [0u8; 1];
            cursor.read_exact(&mut bytes)?;
            Value::U8(bytes[0])
        }
        DataType::U16 => {
            let mut bytes = [0u8; 2];
            cursor.read_exact(&mut bytes)?;
            Value::U16(u16::from_le_bytes(bytes))
        }
        DataType::U32 => {
            let mut bytes = [0u8; 4];
            cursor.read_exact(&mut bytes)?;
            Value::U32(u32::from_le_bytes(bytes))
        }
        DataType::U64 => {
            let mut bytes = [0u8; 8];
            cursor.read_exact(&mut bytes)?;
            Value::U64(u64::from_le_bytes(bytes))
        }
        DataType::U128 => {
            let mut bytes = [0u8; 16];
            cursor.read_exact(&mut bytes)?;
            Value::U128(u128::from_le_bytes(bytes))
        }

        // Floats
        DataType::F32 => {
            let mut bytes = [0u8; 4];
            cursor.read_exact(&mut bytes)?;
            Value::F32(f32::from_bits(u32::from_le_bytes(bytes)))
        }
        DataType::F64 => {
            let mut bytes = [0u8; 8];
            cursor.read_exact(&mut bytes)?;
            Value::F64(f64::from_bits(u64::from_le_bytes(bytes)))
        }

        // Boolean
        DataType::Bool => {
            let mut b = [0u8; 1];
            cursor.read_exact(&mut b)?;
            Value::Bool(b[0] != 0)
        }

        // Variable-length types
        DataType::Str | DataType::Text => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut bytes = vec![0u8; len];
            cursor.read_exact(&mut bytes)?;
            Value::Str(
                String::from_utf8(bytes)
                    .map_err(|e| Error::InvalidValue(format!("Invalid UTF-8: {}", e)))?,
            )
        }

        DataType::Bytea => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut bytes = vec![0u8; len];
            cursor.read_exact(&mut bytes)?;
            Value::Bytea(bytes)
        }

        // Decimal
        DataType::Decimal(_, _) => {
            let mut mantissa_bytes = [0u8; 16];
            cursor.read_exact(&mut mantissa_bytes)?;
            let mantissa = i128::from_le_bytes(mantissa_bytes);

            let mut scale_bytes = [0u8; 4];
            cursor.read_exact(&mut scale_bytes)?;
            let scale = u32::from_le_bytes(scale_bytes);

            Value::Decimal(Decimal::from_i128_with_scale(mantissa, scale))
        }

        DataType::Uuid => {
            let mut bytes = [0u8; 16];
            cursor.read_exact(&mut bytes)?;
            Value::Uuid(Uuid::from_bytes(bytes))
        }

        DataType::Date => {
            let mut bytes = [0u8; 8];
            cursor.read_exact(&mut bytes)?;
            let days = i64::from_le_bytes(bytes);

            let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                .ok_or_else(|| Error::InvalidValue("Invalid epoch date".into()))?;

            let date = unix_epoch
                .checked_add_signed(chrono::Duration::days(days))
                .ok_or_else(|| Error::InvalidValue(format!("Invalid date: {} days", days)))?;

            Value::Date(date)
        }

        DataType::Time => {
            let mut bytes = [0u8; 8];
            cursor.read_exact(&mut bytes)?;
            let nanos = i64::from_le_bytes(bytes);

            let seconds = (nanos / 1_000_000_000) as u32;
            let nanoseconds = (nanos % 1_000_000_000) as u32;

            let time = NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanoseconds)
                .ok_or_else(|| Error::InvalidValue(format!("Invalid time: {} nanos", nanos)))?;

            Value::Time(time)
        }

        DataType::Timestamp => {
            let mut timestamp_bytes = [0u8; 8];
            cursor.read_exact(&mut timestamp_bytes)?;
            let timestamp = i64::from_le_bytes(timestamp_bytes);

            let mut nanos_bytes = [0u8; 4];
            cursor.read_exact(&mut nanos_bytes)?;
            let nanos = u32::from_le_bytes(nanos_bytes);

            let dt = chrono::DateTime::from_timestamp(timestamp, nanos).ok_or_else(|| {
                Error::InvalidValue(format!("Invalid timestamp: {}s + {}ns", timestamp, nanos))
            })?;

            Value::Timestamp(dt.naive_utc())
        }

        DataType::Interval => {
            let mut months_bytes = [0u8; 4];
            cursor.read_exact(&mut months_bytes)?;
            let months = i32::from_le_bytes(months_bytes);

            let mut days_bytes = [0u8; 4];
            cursor.read_exact(&mut days_bytes)?;
            let days = i32::from_le_bytes(days_bytes);

            let mut micros_bytes = [0u8; 8];
            cursor.read_exact(&mut micros_bytes)?;
            let microseconds = i64::from_le_bytes(micros_bytes);

            Value::Interval(Interval {
                months,
                days,
                microseconds,
            })
        }

        DataType::Inet => {
            let mut tag = [0u8; 1];
            cursor.read_exact(&mut tag)?;

            match tag[0] {
                4 => {
                    let mut octets = [0u8; 4];
                    cursor.read_exact(&mut octets)?;
                    Value::Inet(IpAddr::from(octets))
                }
                6 => {
                    let mut octets = [0u8; 16];
                    cursor.read_exact(&mut octets)?;
                    Value::Inet(IpAddr::from(octets))
                }
                v => {
                    return Err(Error::InvalidValue(format!("Invalid IP version: {}", v)));
                }
            }
        }

        DataType::Point => {
            let mut x_bytes = [0u8; 8];
            cursor.read_exact(&mut x_bytes)?;
            let x = f64::from_bits(u64::from_le_bytes(x_bytes));

            let mut y_bytes = [0u8; 8];
            cursor.read_exact(&mut y_bytes)?;
            let y = f64::from_bits(u64::from_le_bytes(y_bytes));

            Value::Point(Point { x, y })
        }

        // Array/List: decode count + null bitmap + elements recursively
        DataType::Array(elem_type, _) => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;

            // Read null bitmap
            let num_bytes = len.div_ceil(8);
            let mut null_bitmap = vec![0u8; num_bytes];
            cursor.read_exact(&mut null_bitmap)?;

            // Decode values
            let mut arr = Vec::with_capacity(len);
            for i in 0..len {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                let is_null = (null_bitmap[byte_idx] & (1 << bit_idx)) != 0;

                if is_null {
                    arr.push(Value::Null);
                } else {
                    arr.push(decode_value_compact(cursor, elem_type)?);
                }
            }

            Value::Array(arr)
        }

        DataType::List(elem_type) => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;

            // Read null bitmap
            let num_bytes = len.div_ceil(8);
            let mut null_bitmap = vec![0u8; num_bytes];
            cursor.read_exact(&mut null_bitmap)?;

            // Decode values
            let mut arr = Vec::with_capacity(len);
            for i in 0..len {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                let is_null = (null_bitmap[byte_idx] & (1 << bit_idx)) != 0;

                if is_null {
                    arr.push(Value::Null);
                } else {
                    arr.push(decode_value_compact(cursor, elem_type)?);
                }
            }

            Value::List(arr)
        }

        DataType::Map(_key_type, value_type) => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;

            let mut map = HashMap::with_capacity(len);
            for _ in 0..len {
                // Decode key (always string)
                let mut key_len_bytes = [0u8; 4];
                cursor.read_exact(&mut key_len_bytes)?;
                let key_len = u32::from_le_bytes(key_len_bytes) as usize;

                let mut key_bytes = vec![0u8; key_len];
                cursor.read_exact(&mut key_bytes)?;
                let key = String::from_utf8(key_bytes)
                    .map_err(|e| Error::InvalidValue(format!("Invalid UTF-8 in map key: {}", e)))?;

                // Decode value
                let value = decode_value_compact(cursor, value_type)?;
                map.insert(key, value);
            }

            Value::Map(map)
        }

        DataType::Struct(field_types) => {
            // Read null bitmap
            let num_bytes = field_types.len().div_ceil(8);
            let mut null_bitmap = vec![0u8; num_bytes];
            cursor.read_exact(&mut null_bitmap)?;

            // Decode values in schema order
            let mut fields = Vec::with_capacity(field_types.len());

            for (i, (field_name, field_type)) in field_types.iter().enumerate() {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                let is_null = (null_bitmap[byte_idx] & (1 << bit_idx)) != 0;

                if is_null {
                    fields.push((field_name.clone(), Value::Null));
                } else {
                    let value = decode_value_compact(cursor, field_type)?;
                    fields.push((field_name.clone(), value));
                }
            }

            Value::Struct(fields)
        }

        DataType::Json => {
            let json = decode_json_compact(cursor)?;
            Value::Json(json)
        }

        DataType::Nullable(inner) => decode_value_compact(cursor, inner)?,

        DataType::Null => Value::Null,
    };

    Ok(value)
}

/// Decode JSON value from compact binary format
fn decode_json_compact(cursor: &mut Cursor<&[u8]>) -> Result<serde_json::Value> {
    let mut tag = [0u8; 1];
    cursor.read_exact(&mut tag)?;

    match tag[0] {
        0x00 => Ok(serde_json::Value::Null),
        0x01 => {
            let mut b = [0u8; 1];
            cursor.read_exact(&mut b)?;
            Ok(serde_json::Value::Bool(b[0] != 0))
        }
        0x02 => {
            let mut bytes = [0u8; 8];
            cursor.read_exact(&mut bytes)?;
            let i = i64::from_le_bytes(bytes);
            Ok(serde_json::Value::Number(i.into()))
        }
        0x03 => {
            let mut bytes = [0u8; 8];
            cursor.read_exact(&mut bytes)?;
            let f = f64::from_bits(u64::from_le_bytes(bytes));
            Ok(serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null))
        }
        0x04 => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;

            let mut bytes = vec![0u8; len];
            cursor.read_exact(&mut bytes)?;
            let s = String::from_utf8(bytes)
                .map_err(|e| Error::InvalidValue(format!("Invalid UTF-8 in JSON string: {}", e)))?;
            Ok(serde_json::Value::String(s))
        }
        0x05 => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;

            let mut arr = Vec::with_capacity(len);
            for _ in 0..len {
                arr.push(decode_json_compact(cursor)?);
            }
            Ok(serde_json::Value::Array(arr))
        }
        0x06 => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;

            let mut obj = serde_json::Map::new();
            for _ in 0..len {
                // Decode key
                let mut key_len_bytes = [0u8; 4];
                cursor.read_exact(&mut key_len_bytes)?;
                let key_len = u32::from_le_bytes(key_len_bytes) as usize;

                let mut key_bytes = vec![0u8; key_len];
                cursor.read_exact(&mut key_bytes)?;
                let key = String::from_utf8(key_bytes).map_err(|e| {
                    Error::InvalidValue(format!("Invalid UTF-8 in JSON key: {}", e))
                })?;

                // Decode value
                let val = decode_json_compact(cursor)?;
                obj.insert(key, val);
            }
            Ok(serde_json::Value::Object(obj))
        }
        tag => Err(Error::InvalidValue(format!(
            "Invalid JSON type tag: {}",
            tag
        ))),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::schema::{Column, Table};

    #[test]
    fn test_index_key_sorting() {
        let values1 = vec![Value::I64(10), Value::Str("apple".to_string())];
        let values2 = vec![Value::I64(10), Value::Str("banana".to_string())];
        let values3 = vec![Value::I64(20), Value::Str("apple".to_string())];

        let row_id = 1u64;

        let key1 = encode_index_key(&values1, row_id);
        let key2 = encode_index_key(&values2, row_id);
        let key3 = encode_index_key(&values3, row_id);

        assert!(key1 < key2);
        assert!(key1 < key3);
        assert!(key2 < key3);
    }

    fn create_test_schema() -> Table {
        let columns = vec![
            Column::new("id".to_string(), DataType::I64).primary_key(),
            Column::new("name".to_string(), DataType::Str),
            Column::new("age".to_string(), DataType::I32).nullable(true),
            Column::new("active".to_string(), DataType::Bool),
        ];
        let mut table = Table::new("users".to_string(), columns).unwrap();
        table.schema_version = 42;
        table
    }

    #[test]
    fn test_compact_row_encoding() {
        let schema = create_test_schema();
        let row = vec![
            Value::I64(1),
            Value::Str("Alice".to_string()),
            Value::I32(30),
            Value::Bool(true),
        ];

        let encoded = encode_row(&row, &schema).unwrap();
        let decoded = decode_row(&encoded, &schema).unwrap();

        assert_eq!(row, decoded);
    }

    #[test]
    fn test_compact_encoding_with_nulls() {
        let schema = create_test_schema();
        let row = vec![
            Value::I64(2),
            Value::Str("Bob".to_string()),
            Value::Null, // nullable age
            Value::Bool(false),
        ];

        let encoded = encode_row(&row, &schema).unwrap();
        let decoded = decode_row(&encoded, &schema).unwrap();

        assert_eq!(row, decoded);
    }

    #[test]
    fn test_compact_encoding_smaller_than_bincode() {
        let schema = create_test_schema();
        let row = vec![
            Value::I64(1),
            Value::Str("Alice".to_string()),
            Value::I32(30),
            Value::Bool(true),
        ];

        let compact = encode_row(&row, &schema).unwrap();
        let bincode_result = bincode::serialize(&row)
            .map_err(|e| Error::Serialization(e.to_string()))
            .unwrap();

        // Compact encoding should be smaller
        assert!(compact.len() < bincode_result.len());
    }
}
