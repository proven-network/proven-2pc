//! SQL types and values with deterministic operations

use crate::error::{Error, Result};
use bytes::Bytes;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use uuid::Uuid;

/// A row of values in a table
pub type Row = Vec<Value>;

/// SQL data types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Decimal(u32, u32), // precision, scale
    String,
    Timestamp,
    Uuid,
    Blob,
    Nullable(Box<DataType>),
}

impl DataType {
    pub fn is_nullable(&self) -> bool {
        matches!(self, DataType::Nullable(_))
    }

    pub fn base_type(&self) -> &DataType {
        match self {
            DataType::Nullable(inner) => inner.base_type(),
            _ => self,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Decimal(p, s) => write!(f, "DECIMAL({}, {})", p, s),
            DataType::String => write!(f, "VARCHAR"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Uuid => write!(f, "UUID"),
            DataType::Blob => write!(f, "BLOB"),
            DataType::Nullable(inner) => write!(f, "{} NULL", inner),
        }
    }
}

/// SQL values with deterministic operations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Decimal(Decimal),
    String(String),
    Timestamp(u64), // Logical timestamp, not wall clock
    Uuid(Uuid),
    Blob(Bytes),
}

impl Value {
    /// Create a new UUID value with a v4 random UUID
    pub fn new_uuid() -> Self {
        Value::Uuid(Uuid::new_v4())
    }

    /// Create a UUID value from a string
    pub fn uuid_from_str(s: &str) -> Result<Self> {
        Uuid::parse_str(s)
            .map(Value::Uuid)
            .map_err(|e| Error::InvalidValue(format!("Invalid UUID: {}", e)))
    }

    /// Create a UUID value from bytes
    pub fn uuid_from_bytes(bytes: [u8; 16]) -> Self {
        Value::Uuid(Uuid::from_bytes(bytes))
    }

    /// Create a Blob value from a byte vector
    pub fn blob_from_vec(data: Vec<u8>) -> Self {
        Value::Blob(Bytes::from(data))
    }

    /// Create a Blob value from a string
    pub fn blob_from_str(s: &str) -> Self {
        Value::Blob(Bytes::from(s.to_string()))
    }

    /// Get bytes from a Blob value
    pub fn as_blob_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(b) => Some(b.as_ref()),
            _ => None,
        }
    }

    /// Get UUID as string
    pub fn as_uuid_string(&self) -> Option<String> {
        match self {
            Value::Uuid(u) => Some(u.to_string()),
            _ => None,
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Nullable(Box::new(DataType::String)), // Default nullable type
            Value::Boolean(_) => DataType::Boolean,
            Value::Integer(_) => DataType::Integer,
            Value::Decimal(_) => DataType::Decimal(38, 10), // Default precision/scale
            Value::String(_) => DataType::String,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Uuid(_) => DataType::Uuid,
            Value::Blob(_) => DataType::Blob,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn check_type(&self, expected: &DataType) -> Result<()> {
        match (self, expected) {
            (Value::Null, DataType::Nullable(_)) => Ok(()),
            (Value::Boolean(_), DataType::Boolean) => Ok(()),
            (Value::Integer(_), DataType::Integer) => Ok(()),
            (Value::Decimal(_), DataType::Decimal(_, _)) => Ok(()),
            (Value::String(_), DataType::String) => Ok(()),
            (Value::Timestamp(_), DataType::Timestamp) => Ok(()),
            (Value::Uuid(_), DataType::Uuid) => Ok(()),
            (Value::Blob(_), DataType::Blob) => Ok(()),
            (_, DataType::Nullable(inner)) => self.check_type(inner),
            _ => Err(Error::TypeMismatch {
                expected: expected.to_string(),
                found: self.data_type().to_string(),
            }),
        }
    }

    /// Compare two values for ordering
    pub fn compare(&self, other: &Value) -> Result<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Ok(Ordering::Equal),
            (Value::Null, _) => Ok(Ordering::Less),
            (_, Value::Null) => Ok(Ordering::Greater),

            (Value::Boolean(a), Value::Boolean(b)) => Ok(a.cmp(b)),
            (Value::Integer(a), Value::Integer(b)) => Ok(a.cmp(b)),
            (Value::Decimal(a), Value::Decimal(b)) => Ok(a.cmp(b)),
            (Value::String(a), Value::String(b)) => Ok(a.cmp(b)),
            (Value::Timestamp(a), Value::Timestamp(b)) => Ok(a.cmp(b)),
            (Value::Uuid(a), Value::Uuid(b)) => Ok(a.cmp(b)),
            (Value::Blob(a), Value::Blob(b)) => Ok(a.cmp(b)),

            // Allow comparison between integers and decimals
            (Value::Integer(i), Value::Decimal(d)) => {
                let i_dec = Decimal::from(*i);
                Ok(i_dec.cmp(d))
            }
            (Value::Decimal(d), Value::Integer(i)) => {
                let i_dec = Decimal::from(*i);
                Ok(d.cmp(&i_dec))
            }

            _ => Err(Error::TypeMismatch {
                expected: self.data_type().to_string(),
                found: other.data_type().to_string(),
            }),
        }
    }

    /// Add two values (deterministic)
    pub fn add(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a
                .checked_add(*b)
                .map(Value::Integer)
                .ok_or_else(|| Error::InvalidValue("Integer overflow".into())),
            (Value::Decimal(a), Value::Decimal(b)) => Ok(Value::Decimal(a + b)),
            (Value::Integer(i), Value::Decimal(d)) | (Value::Decimal(d), Value::Integer(i)) => {
                let i_dec = Decimal::from(*i);
                Ok(Value::Decimal(i_dec + d))
            }
            (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
            _ => Err(Error::TypeMismatch {
                expected: "numeric or string".into(),
                found: format!("{:?} and {:?}", self, other),
            }),
        }
    }

    /// Subtract two values (deterministic)
    pub fn subtract(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a
                .checked_sub(*b)
                .map(Value::Integer)
                .ok_or_else(|| Error::InvalidValue("Integer underflow".into())),
            (Value::Decimal(a), Value::Decimal(b)) => Ok(Value::Decimal(a - b)),
            (Value::Integer(i), Value::Decimal(d)) => {
                let i_dec = Decimal::from(*i);
                Ok(Value::Decimal(i_dec - d))
            }
            (Value::Decimal(d), Value::Integer(i)) => {
                let i_dec = Decimal::from(*i);
                Ok(Value::Decimal(d - i_dec))
            }
            _ => Err(Error::TypeMismatch {
                expected: "numeric".into(),
                found: format!("{:?} and {:?}", self, other),
            }),
        }
    }

    /// Multiply two values (deterministic)
    pub fn multiply(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a
                .checked_mul(*b)
                .map(Value::Integer)
                .ok_or_else(|| Error::InvalidValue("Integer overflow".into())),
            (Value::Decimal(a), Value::Decimal(b)) => Ok(Value::Decimal(a * b)),
            (Value::Integer(i), Value::Decimal(d)) | (Value::Decimal(d), Value::Integer(i)) => {
                let i_dec = Decimal::from(*i);
                Ok(Value::Decimal(i_dec * d))
            }
            _ => Err(Error::TypeMismatch {
                expected: "numeric".into(),
                found: format!("{:?} and {:?}", self, other),
            }),
        }
    }

    /// Divide two values (deterministic)
    pub fn divide(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (_, Value::Integer(0)) => Err(Error::InvalidValue("Division by zero".into())),
            (_, Value::Decimal(d)) if d.is_zero() => {
                Err(Error::InvalidValue("Division by zero".into()))
            }

            (Value::Integer(a), Value::Integer(b)) => {
                // Integer division truncates
                Ok(Value::Integer(a / b))
            }
            (Value::Decimal(a), Value::Decimal(b)) => Ok(Value::Decimal(a / b)),
            (Value::Integer(i), Value::Decimal(d)) => {
                let i_dec = Decimal::from(*i);
                Ok(Value::Decimal(i_dec / d))
            }
            (Value::Decimal(d), Value::Integer(i)) => {
                let i_dec = Decimal::from(*i);
                Ok(Value::Decimal(d / i_dec))
            }
            _ => Err(Error::TypeMismatch {
                expected: "numeric".into(),
                found: format!("{:?} and {:?}", self, other),
            }),
        }
    }

    /// Logical AND
    pub fn and(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Boolean(a), Value::Boolean(b)) => Ok(Value::Boolean(*a && *b)),
            (Value::Null, Value::Boolean(false)) | (Value::Boolean(false), Value::Null) => {
                Ok(Value::Boolean(false))
            }
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "boolean".into(),
                found: format!("{:?} and {:?}", self, other),
            }),
        }
    }

    /// Logical OR
    pub fn or(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Boolean(a), Value::Boolean(b)) => Ok(Value::Boolean(*a || *b)),
            (Value::Null, Value::Boolean(true)) | (Value::Boolean(true), Value::Null) => {
                Ok(Value::Boolean(true))
            }
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "boolean".into(),
                found: format!("{:?} and {:?}", self, other),
            }),
        }
    }

    /// Logical NOT
    pub fn not(&self) -> Result<Value> {
        match self {
            Value::Boolean(b) => Ok(Value::Boolean(!b)),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "boolean".into(),
                found: self.data_type().to_string(),
            }),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Decimal(d) => write!(f, "{}", d),
            Value::String(s) => write!(f, "'{}'", s),
            Value::Timestamp(t) => write!(f, "{}", t),
            Value::Uuid(u) => write!(f, "'{}'", u),
            Value::Blob(b) => write!(f, "x'{}'", hex::encode(b)),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.compare(other).ok()
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other).unwrap_or(Ordering::Equal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_comparison() {
        assert_eq!(
            Value::Integer(1).compare(&Value::Integer(2)).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            Value::String("a".into())
                .compare(&Value::String("b".into()))
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            Value::Null.compare(&Value::Integer(1)).unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn test_value_arithmetic() {
        assert_eq!(
            Value::Integer(2).add(&Value::Integer(3)).unwrap(),
            Value::Integer(5)
        );
        assert_eq!(
            Value::Decimal(Decimal::from(10))
                .multiply(&Value::Decimal(Decimal::from(2)))
                .unwrap(),
            Value::Decimal(Decimal::from(20))
        );
    }

    #[test]
    fn test_type_checking() {
        assert!(Value::Integer(42).check_type(&DataType::Integer).is_ok());
        assert!(Value::Integer(42).check_type(&DataType::String).is_err());
        assert!(
            Value::Null
                .check_type(&DataType::Nullable(Box::new(DataType::Integer)))
                .is_ok()
        );
    }

    #[test]
    fn test_uuid_type() {
        // Test UUID creation and comparison
        let uuid1 = Value::new_uuid();
        let uuid2 = Value::new_uuid();

        assert_eq!(uuid1.data_type(), DataType::Uuid);
        assert!(uuid1.check_type(&DataType::Uuid).is_ok());
        assert!(uuid1.check_type(&DataType::String).is_err());

        // UUIDs should be different
        assert_ne!(uuid1, uuid2);

        // Test UUID from string
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let uuid_val = Value::uuid_from_str(uuid_str).unwrap();
        assert_eq!(uuid_val.as_uuid_string().unwrap(), uuid_str);

        // Test UUID from bytes
        let bytes = [0u8; 16];
        let uuid_from_bytes = Value::uuid_from_bytes(bytes);
        assert_eq!(uuid_from_bytes.data_type(), DataType::Uuid);

        // Test ordering
        let uuid3 = Value::uuid_from_str("00000000-0000-0000-0000-000000000000").unwrap();
        let uuid4 = Value::uuid_from_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();
        assert_eq!(uuid3.compare(&uuid4).unwrap(), Ordering::Less);
    }

    #[test]
    fn test_blob_type() {
        // Test Blob creation
        let data = vec![1, 2, 3, 4, 5];
        let blob = Value::blob_from_vec(data.clone());

        assert_eq!(blob.data_type(), DataType::Blob);
        assert!(blob.check_type(&DataType::Blob).is_ok());
        assert!(blob.check_type(&DataType::String).is_err());

        // Test getting bytes back
        assert_eq!(blob.as_blob_bytes(), Some(data.as_slice()));

        // Test Blob from string
        let text = "Hello, World!";
        let text_blob = Value::blob_from_str(text);
        assert_eq!(text_blob.as_blob_bytes(), Some(text.as_bytes()));

        // Test ordering
        let blob1 = Value::blob_from_vec(vec![1, 2, 3]);
        let blob2 = Value::blob_from_vec(vec![1, 2, 4]);
        assert_eq!(blob1.compare(&blob2).unwrap(), Ordering::Less);

        // Test Display format
        let blob_display = Value::blob_from_vec(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(blob_display.to_string(), "x'deadbeef'");
    }

    #[test]
    fn test_mixed_type_comparisons() {
        let uuid = Value::new_uuid();
        let blob = Value::blob_from_vec(vec![1, 2, 3]);
        let string = Value::String("test".to_string());

        // Different types should not be comparable (except with Null)
        assert!(uuid.compare(&blob).is_err());
        assert!(uuid.compare(&string).is_err());
        assert!(blob.compare(&string).is_err());

        // Null comparisons should work
        assert_eq!(Value::Null.compare(&uuid).unwrap(), Ordering::Less);
        assert_eq!(uuid.compare(&Value::Null).unwrap(), Ordering::Greater);
    }
}
