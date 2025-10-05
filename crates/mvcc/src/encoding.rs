//! Encoding/decoding traits for MVCC storage
//!
//! These traits replace Serde, allowing each crate to define custom,
//! efficient encodings for their types.

use crate::error::Result;

/// Encode a value to bytes
pub trait Encode {
    fn encode(&self) -> Result<Vec<u8>>;
}

/// Decode a value from bytes
pub trait Decode: Sized {
    fn decode(bytes: &[u8]) -> Result<Self>;
}

// Common type implementations
impl Encode for u64 {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.to_be_bytes().to_vec())
    }
}

impl Decode for u64 {
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 8 {
            return Err(crate::Error::Encoding(format!(
                "Expected 8 bytes for u64, got {}",
                bytes.len()
            )));
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(buf))
    }
}

impl Encode for Vec<u8> {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.clone())
    }
}

impl Decode for Vec<u8> {
    fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(bytes.to_vec())
    }
}

impl Encode for () {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(Vec::with_capacity(0))
    }
}

impl Decode for () {
    fn decode(_bytes: &[u8]) -> Result<Self> {
        Ok(())
    }
}
