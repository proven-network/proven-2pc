//! SQL types and values with deterministic operations

use crate::error::{Error, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

/// SQL data types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Decimal(u32, u32), // precision, scale
    String,
    Timestamp,
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
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Nullable(Box::new(DataType::String)), // Default nullable type
            Value::Boolean(_) => DataType::Boolean,
            Value::Integer(_) => DataType::Integer,
            Value::Decimal(_) => DataType::Decimal(38, 10), // Default precision/scale
            Value::String(_) => DataType::String,
            Value::Timestamp(_) => DataType::Timestamp,
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
            (Value::Integer(a), Value::Integer(b)) => {
                a.checked_add(*b)
                    .map(Value::Integer)
                    .ok_or_else(|| Error::InvalidValue("Integer overflow".into()))
            }
            (Value::Decimal(a), Value::Decimal(b)) => {
                Ok(Value::Decimal(a + b))
            }
            (Value::Integer(i), Value::Decimal(d)) | (Value::Decimal(d), Value::Integer(i)) => {
                let i_dec = Decimal::from(*i);
                Ok(Value::Decimal(i_dec + d))
            }
            (Value::String(a), Value::String(b)) => {
                Ok(Value::String(format!("{}{}", a, b)))
            }
            _ => Err(Error::TypeMismatch {
                expected: "numeric or string".into(),
                found: format!("{:?} and {:?}", self, other),
            }),
        }
    }
    
    /// Subtract two values (deterministic)
    pub fn subtract(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => {
                a.checked_sub(*b)
                    .map(Value::Integer)
                    .ok_or_else(|| Error::InvalidValue("Integer underflow".into()))
            }
            (Value::Decimal(a), Value::Decimal(b)) => {
                Ok(Value::Decimal(a - b))
            }
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
            (Value::Integer(a), Value::Integer(b)) => {
                a.checked_mul(*b)
                    .map(Value::Integer)
                    .ok_or_else(|| Error::InvalidValue("Integer overflow".into()))
            }
            (Value::Decimal(a), Value::Decimal(b)) => {
                Ok(Value::Decimal(a * b))
            }
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
            (Value::Decimal(a), Value::Decimal(b)) => {
                Ok(Value::Decimal(a / b))
            }
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
        assert_eq!(Value::Integer(1).compare(&Value::Integer(2)).unwrap(), Ordering::Less);
        assert_eq!(Value::String("a".into()).compare(&Value::String("b".into())).unwrap(), Ordering::Less);
        assert_eq!(Value::Null.compare(&Value::Integer(1)).unwrap(), Ordering::Less);
    }
    
    #[test]
    fn test_value_arithmetic() {
        assert_eq!(
            Value::Integer(2).add(&Value::Integer(3)).unwrap(),
            Value::Integer(5)
        );
        assert_eq!(
            Value::Decimal(Decimal::from(10)).multiply(&Value::Decimal(Decimal::from(2))).unwrap(),
            Value::Decimal(Decimal::from(20))
        );
    }
    
    #[test]
    fn test_type_checking() {
        assert!(Value::Integer(42).check_type(&DataType::Integer).is_ok());
        assert!(Value::Integer(42).check_type(&DataType::String).is_err());
        assert!(Value::Null.check_type(&DataType::Nullable(Box::new(DataType::Integer))).is_ok());
    }
}