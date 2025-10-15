//! MVCC integration for Value
//!
//! Provides Encode/Decode implementations for the proven-mvcc crate.

use crate::codec::{decode_value, encode_value};
use crate::types::Value;
use proven_mvcc::{Decode, Encode, Error as MvccError};

type Result<T> = std::result::Result<T, MvccError>;

/// Implement MVCC Encode for Value using our efficient binary encoding
impl Encode for Value {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(encode_value(self))
    }
}

/// Implement MVCC Decode for Value using our efficient binary encoding
impl Decode for Value {
    fn decode(bytes: &[u8]) -> Result<Self> {
        decode_value(bytes).map_err(|e| MvccError::Encoding(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_mvcc_encode_decode() {
        let value = Value::Str("test".to_string());
        let encoded = value.encode().unwrap();
        let decoded = Value::decode(&encoded).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_mvcc_encode_decode_json() {
        let value = Value::Json(serde_json::json!({"key": "value"}));
        let encoded = value.encode().unwrap();
        let decoded = Value::decode(&encoded).unwrap();
        assert_eq!(value, decoded);
    }
}
