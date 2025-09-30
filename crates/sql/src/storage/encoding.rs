//! Encoding utilities for storage keys and values

use crate::types::value::Value;
use bincode;
use serde::{Deserialize, Serialize};

/// Encode a row key
pub fn encode_row_key(row_id: u64) -> Vec<u8> {
    row_id.to_be_bytes().to_vec()
}

/// Decode a row key
pub fn decode_row_key(bytes: &[u8]) -> Option<u64> {
    if bytes.len() == 8 {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(bytes);
        Some(u64::from_be_bytes(arr))
    } else {
        None
    }
}

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

/// Encode a value in a sortable binary format
fn encode_value_sortable(value: &Value, output: &mut Vec<u8>) {
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
            let bytes = bincode::serialize(d).unwrap();
            output.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            output.extend_from_slice(&bytes);
        }
        // String
        Value::Str(s) => {
            output.push(0x0F);
            let bytes = s.as_bytes();
            output.extend_from_slice(bytes);
            output.push(0x00); // Null terminator for sorting
        }
        // Date/Time types
        Value::Date(d) => {
            output.push(0x10);
            let bytes = bincode::serialize(d).unwrap();
            output.extend_from_slice(&bytes);
        }
        Value::Time(t) => {
            output.push(0x11);
            let bytes = bincode::serialize(t).unwrap();
            output.extend_from_slice(&bytes);
        }
        Value::Timestamp(ts) => {
            output.push(0x12);
            let bytes = bincode::serialize(ts).unwrap();
            output.extend_from_slice(&bytes);
        }
        Value::Interval(i) => {
            output.push(0x13);
            let bytes = bincode::serialize(i).unwrap();
            output.extend_from_slice(&bytes);
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
            let bytes = bincode::serialize(ip).unwrap();
            output.extend_from_slice(&bytes);
        }
        Value::Point(p) => {
            output.push(0x17);
            let bytes = bincode::serialize(p).unwrap();
            output.extend_from_slice(&bytes);
        }
        // Collection types
        Value::Array(arr) | Value::List(arr) => {
            output.push(0x18);
            let bytes = bincode::serialize(arr).unwrap();
            output.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            output.extend_from_slice(&bytes);
        }
        Value::Map(m) => {
            output.push(0x19);
            let bytes = bincode::serialize(m).unwrap();
            output.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            output.extend_from_slice(&bytes);
        }
        Value::Struct(s) => {
            output.push(0x1A);
            let bytes = bincode::serialize(s).unwrap();
            output.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            output.extend_from_slice(&bytes);
        }
    }
}

/// Decode an index key to extract the row_id
pub fn decode_row_id_from_index_key(bytes: &[u8]) -> Option<u64> {
    if bytes.len() >= 8 {
        let start = bytes.len() - 8;
        let mut row_id_bytes = [0u8; 8];
        row_id_bytes.copy_from_slice(&bytes[start..]);
        Some(u64::from_be_bytes(row_id_bytes))
    } else {
        None
    }
}

/// Serialize a value using bincode
pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(value)
}

/// Deserialize a value using bincode
pub fn deserialize<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, bincode::Error> {
    bincode::deserialize(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_key_encoding() {
        let row_id = 12345u64;
        let encoded = encode_row_key(row_id);
        let decoded = decode_row_key(&encoded);
        assert_eq!(decoded, Some(row_id));
    }

    #[test]
    fn test_index_key_sorting() {
        // Test that encoded keys maintain sort order
        let values1 = vec![Value::I64(10), Value::Str("apple".to_string())];
        let values2 = vec![Value::I64(10), Value::Str("banana".to_string())];
        let values3 = vec![Value::I64(20), Value::Str("apple".to_string())];

        let row_id = 1u64;

        let key1 = encode_index_key(&values1, row_id);
        let key2 = encode_index_key(&values2, row_id);
        let key3 = encode_index_key(&values3, row_id);

        assert!(key1 < key2); // Same number, different strings
        assert!(key1 < key3); // Different numbers
        assert!(key2 < key3);
    }

    #[test]
    fn test_null_sorting() {
        let null_val = vec![Value::Null];
        let int_val = vec![Value::I64(-100)];

        let row_id = 1u64;

        let null_key = encode_index_key(&null_val, row_id);
        let int_key = encode_index_key(&int_val, row_id);

        assert!(null_key < int_key); // NULL should sort first
    }

    #[test]
    fn test_negative_number_sorting() {
        let neg_val = vec![Value::I64(-100)];
        let zero_val = vec![Value::I64(0)];
        let pos_val = vec![Value::I64(100)];

        let row_id = 1u64;

        let neg_key = encode_index_key(&neg_val, row_id);
        let zero_key = encode_index_key(&zero_val, row_id);
        let pos_key = encode_index_key(&pos_val, row_id);

        assert!(neg_key < zero_key);
        assert!(zero_key < pos_key);
    }
}
