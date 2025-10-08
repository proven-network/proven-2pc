//! Binary encoding for Value
//!
//! Provides efficient sortable binary encoding with type tags.
//! This is based on the SQL crate's sortable index encoding.

use crate::types::{Interval, Point, Value};
use chrono::{NaiveDate, Timelike};
use std::io::{Cursor, Read};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Encoding error: {0}")]
    Encoding(String),

    #[error("Decoding error: {0}")]
    Decoding(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid value: {0}")]
    InvalidValue(String),
}

// ============================================================================
// Encoding (Sortable Binary Format with Type Tags)
// ============================================================================

/// Encode a value in sortable binary format with type tag
pub fn encode_value(value: &Value) -> Vec<u8> {
    let mut output = Vec::new();
    encode_value_internal(value, &mut output);
    output
}

fn encode_value_internal(value: &Value, output: &mut Vec<u8>) {
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
        // Decimal
        Value::Decimal(d) => {
            output.push(0x0E);
            let mantissa = d.mantissa();
            let scale = d.scale();
            let sortable = (mantissa as u128) ^ (1u128 << 127);
            output.extend_from_slice(&sortable.to_be_bytes());
            output.extend_from_slice(&scale.to_be_bytes());
        }
        // String
        Value::Str(s) => {
            output.push(0x0F);
            let bytes = s.as_bytes();
            output.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            output.extend_from_slice(bytes);
        }
        // Date/Time types
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
            let ip_str = ip.to_string();
            let bytes = ip_str.as_bytes();
            output.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            output.extend_from_slice(bytes);
        }
        Value::Point(p) => {
            output.push(0x17);
            output.extend_from_slice(&p.x.to_be_bytes());
            output.extend_from_slice(&p.y.to_be_bytes());
        }
        // Collection types
        Value::Array(arr) | Value::List(arr) => {
            output.push(if matches!(value, Value::Array(_)) {
                0x18
            } else {
                0x19
            });
            output.extend_from_slice(&(arr.len() as u32).to_be_bytes());
            for item in arr {
                encode_value_internal(item, output);
            }
        }
        Value::Map(m) => {
            output.push(0x1A);
            output.extend_from_slice(&(m.len() as u32).to_be_bytes());
            let mut sorted_keys: Vec<_> = m.keys().collect();
            sorted_keys.sort();
            for key in sorted_keys {
                let key_bytes = key.as_bytes();
                output.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
                output.extend_from_slice(key_bytes);
                encode_value_internal(&m[key], output);
            }
        }
        Value::Struct(fields) => {
            output.push(0x1B);
            output.extend_from_slice(&(fields.len() as u32).to_be_bytes());
            for (name, value) in fields {
                let name_bytes = name.as_bytes();
                output.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
                output.extend_from_slice(name_bytes);
                encode_value_internal(value, output);
            }
        }
        Value::Json(j) => {
            output.push(0x1C);
            let json_str = j.to_string();
            let bytes = json_str.as_bytes();
            output.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            output.extend_from_slice(bytes);
        }
    }
}

// ============================================================================
// Decoding
// ============================================================================

/// Decode a value from sortable binary format
pub fn decode_value(bytes: &[u8]) -> Result<Value> {
    let mut cursor = Cursor::new(bytes);
    decode_value_internal(&mut cursor)
}

fn decode_value_internal(cursor: &mut Cursor<&[u8]>) -> Result<Value> {
    let mut tag = [0u8; 1];
    cursor.read_exact(&mut tag)?;

    match tag[0] {
        0x00 => Ok(Value::Null),
        0x01 => {
            let mut b = [0u8; 1];
            cursor.read_exact(&mut b)?;
            Ok(Value::Bool(b[0] != 0))
        }
        0x02 => {
            let mut b = [0u8; 1];
            cursor.read_exact(&mut b)?;
            let u = b[0] ^ (1u8 << 7);
            Ok(Value::I8(u as i8))
        }
        0x03 => {
            let mut b = [0u8; 2];
            cursor.read_exact(&mut b)?;
            let u = u16::from_be_bytes(b) ^ (1u16 << 15);
            Ok(Value::I16(u as i16))
        }
        0x04 => {
            let mut b = [0u8; 4];
            cursor.read_exact(&mut b)?;
            let u = u32::from_be_bytes(b) ^ (1u32 << 31);
            Ok(Value::I32(u as i32))
        }
        0x05 => {
            let mut b = [0u8; 8];
            cursor.read_exact(&mut b)?;
            let u = u64::from_be_bytes(b) ^ (1u64 << 63);
            Ok(Value::I64(u as i64))
        }
        0x06 => {
            let mut b = [0u8; 16];
            cursor.read_exact(&mut b)?;
            let u = u128::from_be_bytes(b) ^ (1u128 << 127);
            Ok(Value::I128(u as i128))
        }
        0x07 => {
            let mut b = [0u8; 1];
            cursor.read_exact(&mut b)?;
            Ok(Value::U8(b[0]))
        }
        0x08 => {
            let mut b = [0u8; 2];
            cursor.read_exact(&mut b)?;
            Ok(Value::U16(u16::from_be_bytes(b)))
        }
        0x09 => {
            let mut b = [0u8; 4];
            cursor.read_exact(&mut b)?;
            Ok(Value::U32(u32::from_be_bytes(b)))
        }
        0x0A => {
            let mut b = [0u8; 8];
            cursor.read_exact(&mut b)?;
            Ok(Value::U64(u64::from_be_bytes(b)))
        }
        0x0B => {
            let mut b = [0u8; 16];
            cursor.read_exact(&mut b)?;
            Ok(Value::U128(u128::from_be_bytes(b)))
        }
        0x0C => {
            let mut b = [0u8; 4];
            cursor.read_exact(&mut b)?;
            let sortable = u32::from_be_bytes(b);
            let bits = if sortable & (1u32 << 31) != 0 {
                sortable ^ (1u32 << 31)
            } else {
                !sortable
            };
            Ok(Value::F32(f32::from_bits(bits)))
        }
        0x0D => {
            let mut b = [0u8; 8];
            cursor.read_exact(&mut b)?;
            let sortable = u64::from_be_bytes(b);
            let bits = if sortable & (1u64 << 63) != 0 {
                sortable ^ (1u64 << 63)
            } else {
                !sortable
            };
            Ok(Value::F64(f64::from_bits(bits)))
        }
        0x0E => {
            let mut mantissa_bytes = [0u8; 16];
            cursor.read_exact(&mut mantissa_bytes)?;
            let sortable = u128::from_be_bytes(mantissa_bytes);
            let mantissa = (sortable ^ (1u128 << 127)) as i128;

            let mut scale_bytes = [0u8; 4];
            cursor.read_exact(&mut scale_bytes)?;
            let scale = u32::from_be_bytes(scale_bytes);

            Ok(Value::Decimal(rust_decimal::Decimal::from_i128_with_scale(
                mantissa, scale,
            )))
        }
        0x0F => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_be_bytes(len_bytes) as usize;

            let mut str_bytes = vec![0u8; len];
            cursor.read_exact(&mut str_bytes)?;

            let s = String::from_utf8(str_bytes)
                .map_err(|e| Error::Decoding(format!("Invalid UTF-8: {}", e)))?;
            Ok(Value::Str(s))
        }
        0x10 => {
            let mut b = [0u8; 8];
            cursor.read_exact(&mut b)?;
            let days = i64::from_be_bytes(b);
            let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + chrono::Duration::days(days);
            Ok(Value::Date(date))
        }
        0x11 => {
            let mut b = [0u8; 8];
            cursor.read_exact(&mut b)?;
            let nanos = i64::from_be_bytes(b);
            let secs = (nanos / 1_000_000_000) as u32;
            let nsecs = (nanos % 1_000_000_000) as u32;
            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nsecs)
                .ok_or_else(|| Error::Decoding("Invalid time".to_string()))?;
            Ok(Value::Time(time))
        }
        0x12 => {
            let mut ts_bytes = [0u8; 8];
            cursor.read_exact(&mut ts_bytes)?;
            let timestamp = i64::from_be_bytes(ts_bytes);

            let mut nanos_bytes = [0u8; 4];
            cursor.read_exact(&mut nanos_bytes)?;
            let nanos = u32::from_be_bytes(nanos_bytes);

            let ts = chrono::DateTime::from_timestamp(timestamp, nanos)
                .ok_or_else(|| Error::Decoding("Invalid timestamp".to_string()))?
                .naive_utc();
            Ok(Value::Timestamp(ts))
        }
        0x13 => {
            let mut months_bytes = [0u8; 4];
            cursor.read_exact(&mut months_bytes)?;
            let months = i32::from_be_bytes(months_bytes);

            let mut days_bytes = [0u8; 4];
            cursor.read_exact(&mut days_bytes)?;
            let days = i32::from_be_bytes(days_bytes);

            let mut micros_bytes = [0u8; 8];
            cursor.read_exact(&mut micros_bytes)?;
            let microseconds = i64::from_be_bytes(micros_bytes);

            Ok(Value::Interval(Interval {
                months,
                days,
                microseconds,
            }))
        }
        0x14 => {
            let mut uuid_bytes = [0u8; 16];
            cursor.read_exact(&mut uuid_bytes)?;
            let uuid = uuid::Uuid::from_bytes(uuid_bytes);
            Ok(Value::Uuid(uuid))
        }
        0x15 => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_be_bytes(len_bytes) as usize;

            let mut bytes = vec![0u8; len];
            cursor.read_exact(&mut bytes)?;
            Ok(Value::Bytea(bytes))
        }
        0x16 => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_be_bytes(len_bytes) as usize;

            let mut ip_bytes = vec![0u8; len];
            cursor.read_exact(&mut ip_bytes)?;

            let ip_str = String::from_utf8(ip_bytes)
                .map_err(|e| Error::Decoding(format!("Invalid IP string: {}", e)))?;
            let ip = ip_str
                .parse()
                .map_err(|e| Error::Decoding(format!("Invalid IP address: {}", e)))?;
            Ok(Value::Inet(ip))
        }
        0x17 => {
            let mut x_bytes = [0u8; 8];
            cursor.read_exact(&mut x_bytes)?;
            let x = f64::from_be_bytes(x_bytes);

            let mut y_bytes = [0u8; 8];
            cursor.read_exact(&mut y_bytes)?;
            let y = f64::from_be_bytes(y_bytes);

            Ok(Value::Point(Point { x, y }))
        }
        0x18 | 0x19 => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_be_bytes(len_bytes) as usize;

            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                items.push(decode_value_internal(cursor)?);
            }

            Ok(if tag[0] == 0x18 {
                Value::Array(items)
            } else {
                Value::List(items)
            })
        }
        0x1A => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_be_bytes(len_bytes) as usize;

            let mut map = std::collections::HashMap::new();
            for _ in 0..len {
                let mut key_len_bytes = [0u8; 4];
                cursor.read_exact(&mut key_len_bytes)?;
                let key_len = u32::from_be_bytes(key_len_bytes) as usize;

                let mut key_bytes = vec![0u8; key_len];
                cursor.read_exact(&mut key_bytes)?;
                let key = String::from_utf8(key_bytes)
                    .map_err(|e| Error::Decoding(format!("Invalid map key: {}", e)))?;

                let value = decode_value_internal(cursor)?;
                map.insert(key, value);
            }

            Ok(Value::Map(map))
        }
        0x1B => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_be_bytes(len_bytes) as usize;

            let mut fields = Vec::with_capacity(len);
            for _ in 0..len {
                let mut name_len_bytes = [0u8; 4];
                cursor.read_exact(&mut name_len_bytes)?;
                let name_len = u32::from_be_bytes(name_len_bytes) as usize;

                let mut name_bytes = vec![0u8; name_len];
                cursor.read_exact(&mut name_bytes)?;
                let name = String::from_utf8(name_bytes)
                    .map_err(|e| Error::Decoding(format!("Invalid field name: {}", e)))?;

                let value = decode_value_internal(cursor)?;
                fields.push((name, value));
            }

            Ok(Value::Struct(fields))
        }
        0x1C => {
            let mut len_bytes = [0u8; 4];
            cursor.read_exact(&mut len_bytes)?;
            let len = u32::from_be_bytes(len_bytes) as usize;

            let mut json_bytes = vec![0u8; len];
            cursor.read_exact(&mut json_bytes)?;

            let json_str = String::from_utf8(json_bytes)
                .map_err(|e| Error::Decoding(format!("Invalid JSON string: {}", e)))?;
            let json: serde_json::Value = serde_json::from_str(&json_str)
                .map_err(|e| Error::Decoding(format!("Invalid JSON: {}", e)))?;
            Ok(Value::Json(json))
        }
        _ => Err(Error::Decoding(format!(
            "Unknown type tag: 0x{:02X}",
            tag[0]
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_integer() {
        let value = Value::I64(42);
        let encoded = encode_value(&value);
        let decoded = decode_value(&encoded).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_string() {
        let value = Value::Str("hello world".to_string());
        let encoded = encode_value(&value);
        let decoded = decode_value(&encoded).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_array() {
        let value = Value::Array(vec![
            Value::I64(1),
            Value::I64(2),
            Value::Str("three".to_string()),
        ]);
        let encoded = encode_value(&value);
        let decoded = decode_value(&encoded).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_json() {
        let value = Value::Json(serde_json::json!({"key": "value", "number": 42}));
        let encoded = encode_value(&value);
        let decoded = decode_value(&encoded).unwrap();
        assert_eq!(value, decoded);
    }
}
