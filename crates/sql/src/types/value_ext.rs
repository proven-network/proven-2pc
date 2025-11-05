//! SQL-specific extensions for proven_value::Value
//!
//! This module provides SQL-specific functionality as an extension trait
//! so we can keep proven-value generic and reusable across crates.

use crate::error::{Error, Result};
use crate::types::{DataType, Value};

/// Extension trait for SQL-specific Value operations
pub trait ValueExt {
    /// Check if this value matches the expected data type
    fn check_type(&self, expected: &DataType) -> Result<()>;

    /// Check if value is a signed integer type
    fn is_signed_integer(&self) -> bool;

    /// Get the DataType for this value
    fn data_type(&self) -> DataType;

    /// Convert value to boolean (for SQL boolean context)
    fn to_bool(&self) -> Result<bool>;
}

impl ValueExt for Value {
    fn check_type(&self, expected: &DataType) -> Result<()> {
        // Get the base type (unwrap Nullable)
        let base_type = expected.base_type();

        // Check if value matches the expected type
        match (self, base_type) {
            (Value::Null, DataType::Null) => Ok(()),
            (Value::Bool(_), DataType::Bool) => Ok(()),

            // Integer types
            (Value::I8(_), DataType::I8) => Ok(()),
            (Value::I16(_), DataType::I16) => Ok(()),
            (Value::I32(_), DataType::I32) => Ok(()),
            (Value::I64(_), DataType::I64) => Ok(()),
            (Value::I128(_), DataType::I128) => Ok(()),
            (Value::U8(_), DataType::U8) => Ok(()),
            (Value::U16(_), DataType::U16) => Ok(()),
            (Value::U32(_), DataType::U32) => Ok(()),
            (Value::U64(_), DataType::U64) => Ok(()),
            (Value::U128(_), DataType::U128) => Ok(()),

            // Float types
            (Value::F32(_), DataType::F32) => Ok(()),
            (Value::F64(_), DataType::F64) => Ok(()),

            // Decimal
            (Value::Decimal(_), DataType::Decimal(_, _)) => Ok(()),

            // String types
            (Value::Str(_), DataType::Str) => Ok(()),
            (Value::Str(_), DataType::Text) => Ok(()),

            // Date/Time types
            (Value::Date(_), DataType::Date) => Ok(()),
            (Value::Time(_), DataType::Time) => Ok(()),
            (Value::Timestamp(_), DataType::Timestamp) => Ok(()),
            (Value::Interval(_), DataType::Interval) => Ok(()),

            // Special types
            (Value::Uuid(_), DataType::Uuid) => Ok(()),
            (Value::Bytea(_), DataType::Bytea) => Ok(()),
            (Value::Inet(_), DataType::Inet) => Ok(()),
            (Value::Point(_), DataType::Point) => Ok(()),
            (Value::PrivateKey(_), DataType::PrivateKey) => Ok(()),
            (Value::PublicKey(_), DataType::PublicKey) => Ok(()),
            (Value::Identity(_), DataType::Identity) => Ok(()),
            (Value::Vault(_), DataType::Vault) => Ok(()),

            // Collection types
            (Value::Array(_), DataType::Array(_, _)) => Ok(()),
            (Value::List(_), DataType::List(_)) => Ok(()),
            (Value::Map(_), DataType::Map(_, _)) => Ok(()),
            (Value::Struct(_), DataType::Struct(_)) => Ok(()),

            // JSON
            (Value::Json(_), DataType::Json) => Ok(()),

            // Type mismatch
            _ => Err(Error::InvalidValue(format!(
                "Type mismatch: expected {:?}, got {}",
                expected,
                self.type_name()
            ))),
        }
    }

    fn is_signed_integer(&self) -> bool {
        matches!(
            self,
            Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_) | Value::I128(_)
        )
    }

    fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Bool(_) => DataType::Bool,
            Value::I8(_) => DataType::I8,
            Value::I16(_) => DataType::I16,
            Value::I32(_) => DataType::I32,
            Value::I64(_) => DataType::I64,
            Value::I128(_) => DataType::I128,
            Value::U8(_) => DataType::U8,
            Value::U16(_) => DataType::U16,
            Value::U32(_) => DataType::U32,
            Value::U64(_) => DataType::U64,
            Value::U128(_) => DataType::U128,
            Value::F32(_) => DataType::F32,
            Value::F64(_) => DataType::F64,
            Value::Decimal(_) => DataType::Decimal(None, None),
            Value::Str(_) => DataType::Str,
            Value::Date(_) => DataType::Date,
            Value::Time(_) => DataType::Time,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Interval(_) => DataType::Interval,
            Value::Uuid(_) => DataType::Uuid,
            Value::Bytea(_) => DataType::Bytea,
            Value::Inet(_) => DataType::Inet,
            Value::Point(_) => DataType::Point,
            Value::PrivateKey(_) => DataType::PrivateKey,
            Value::PublicKey(_) => DataType::PublicKey,
            Value::Identity(_) => DataType::Identity,
            Value::Vault(_) => DataType::Vault,
            Value::Array(arr) => {
                if let Some(first) = arr.first() {
                    DataType::Array(Box::new(first.data_type()), Some(arr.len()))
                } else {
                    DataType::Array(Box::new(DataType::Null), Some(0))
                }
            }
            Value::List(list) => {
                if let Some(first) = list.first() {
                    DataType::List(Box::new(first.data_type()))
                } else {
                    DataType::List(Box::new(DataType::Null))
                }
            }
            Value::Map(map) => {
                if let Some((_, v)) = map.iter().next() {
                    // Keys are always strings in our implementation
                    DataType::Map(Box::new(DataType::Str), Box::new(v.data_type()))
                } else {
                    DataType::Map(Box::new(DataType::Str), Box::new(DataType::Null))
                }
            }
            Value::Struct(fields) => {
                let field_types = fields
                    .iter()
                    .map(|(name, value)| (name.clone(), value.data_type()))
                    .collect();
                DataType::Struct(field_types)
            }
            Value::Json(_) => DataType::Json,
        }
    }

    fn to_bool(&self) -> Result<bool> {
        match self {
            Value::Bool(b) => Ok(*b),
            Value::Null => Ok(false),
            Value::I8(i) => Ok(*i != 0),
            Value::I16(i) => Ok(*i != 0),
            Value::I32(i) => Ok(*i != 0),
            Value::I64(i) => Ok(*i != 0),
            Value::I128(i) => Ok(*i != 0),
            Value::U8(i) => Ok(*i != 0),
            Value::U16(i) => Ok(*i != 0),
            Value::U32(i) => Ok(*i != 0),
            Value::U64(i) => Ok(*i != 0),
            Value::U128(i) => Ok(*i != 0),
            Value::Str(s) => Ok(!s.is_empty()),
            _ => Err(Error::InvalidValue(format!(
                "Cannot convert {} to boolean",
                self.type_name()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_type_integers() {
        let value = Value::I64(42);
        assert!(value.check_type(&DataType::I64).is_ok());
        assert!(value.check_type(&DataType::I32).is_err());
    }

    #[test]
    fn test_check_type_string() {
        let value = Value::Str("hello".to_string());
        assert!(value.check_type(&DataType::Str).is_ok());
        assert!(value.check_type(&DataType::Text).is_ok());
        assert!(value.check_type(&DataType::I64).is_err());
    }

    #[test]
    fn test_is_signed_integer() {
        assert!(Value::I64(42).is_signed_integer());
        assert!(Value::I32(-10).is_signed_integer());
        assert!(!Value::U64(42).is_signed_integer());
        assert!(!Value::Str("test".to_string()).is_signed_integer());
    }
}
