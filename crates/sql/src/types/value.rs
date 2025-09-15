//! SQL values with GlueSQL-compatible format
//!
//! This module provides SQL value types that match GlueSQL's format
//! for better test compatibility and SQL compliance.

use super::data_type::{Interval, Point};
use crate::error::{Error, Result};
use crate::types::DataType;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use uuid::Uuid;

/// A row of values in a table
pub type Row = Vec<Value>;

/// SQL values with GlueSQL-compatible format
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    // Null
    Null,
    // Boolean
    Bool(bool),
    // Integer types
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    // Float types
    F32(f32),
    F64(f64),
    // Decimal
    Decimal(Decimal),
    // String (using Str for GlueSQL compatibility)
    Str(String),
    // Date/Time types
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(NaiveDateTime),
    Interval(Interval),
    // Special types
    Uuid(Uuid),
    Bytea(Vec<u8>),
    Inet(IpAddr),
    Point(Point),
    // Collection types
    List(Vec<Value>),
    Map(HashMap<String, Value>),
}

// Backward compatibility - map old names to new format
impl Value {
    /// Create an I64 value (most common integer type)
    pub fn integer(i: i64) -> Self {
        Value::I64(i)
    }

    /// Create a string value
    pub fn string(s: String) -> Self {
        Value::Str(s)
    }

    /// Create a boolean value
    pub fn boolean(b: bool) -> Self {
        Value::Bool(b)
    }

    /// Check if value is any integer type
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            Value::I8(_)
                | Value::I16(_)
                | Value::I32(_)
                | Value::I64(_)
                | Value::I128(_)
                | Value::U8(_)
                | Value::U16(_)
                | Value::U32(_)
                | Value::U64(_)
                | Value::U128(_)
        )
    }

    /// Convert any integer to i128 for comparison
    pub fn to_i128(&self) -> Result<i128> {
        match self {
            Value::I8(v) => Ok(*v as i128),
            Value::I16(v) => Ok(*v as i128),
            Value::I32(v) => Ok(*v as i128),
            Value::I64(v) => Ok(*v as i128),
            Value::I128(v) => Ok(*v),
            Value::U8(v) => Ok(*v as i128),
            Value::U16(v) => Ok(*v as i128),
            Value::U32(v) => Ok(*v as i128),
            Value::U64(v) => Ok(*v as i128),
            Value::U128(v) => (*v)
                .try_into()
                .map_err(|_| Error::InvalidValue("U128 too large for I128".into())),
            _ => Err(Error::TypeMismatch {
                expected: "integer".into(),
                found: format!("{:?}", self),
            }),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Convert value to boolean
    pub fn to_bool(&self) -> Result<bool> {
        match self {
            Value::Bool(b) => Ok(*b),
            Value::Null => Ok(false),
            // Non-zero numbers are true
            Value::I8(n) => Ok(*n != 0),
            Value::I16(n) => Ok(*n != 0),
            Value::I32(n) => Ok(*n != 0),
            Value::I64(n) => Ok(*n != 0),
            Value::I128(n) => Ok(*n != 0),
            Value::U8(n) => Ok(*n != 0),
            Value::U16(n) => Ok(*n != 0),
            Value::U32(n) => Ok(*n != 0),
            Value::U64(n) => Ok(*n != 0),
            Value::U128(n) => Ok(*n != 0),
            Value::F32(n) => Ok(*n != 0.0 && !n.is_nan()),
            Value::F64(n) => Ok(*n != 0.0 && !n.is_nan()),
            _ => Err(Error::TypeMismatch {
                expected: "boolean".into(),
                found: format!("{:?}", self),
            }),
        }
    }

    /// Get the data type of this value
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Nullable(Box::new(DataType::I64)), // Default nullable type
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
            Value::List(_) => DataType::List(Box::new(DataType::I64)), // TODO: track element type
            Value::Map(_) => DataType::Map(Box::new(DataType::Str), Box::new(DataType::I64)), // TODO: track types
        }
    }

    /// Check if this value matches the expected data type
    pub fn check_type(&self, expected: &DataType) -> Result<()> {
        // Handle nullable types
        if let DataType::Nullable(inner) = expected {
            if self.is_null() {
                return Ok(());
            }
            return self.check_type(inner);
        }

        // Check if the value's type matches the expected type
        let actual = self.data_type();
        if actual == *expected {
            return Ok(());
        }

        // Allow integer type conversions
        if self.is_integer()
            && matches!(
                expected,
                DataType::I64
                    | DataType::I32
                    | DataType::I16
                    | DataType::I8
                    | DataType::U64
                    | DataType::U32
                    | DataType::U16
                    | DataType::U8
            )
        {
            return Ok(());
        }

        // Allow string/text interchangeability
        if matches!(self, Value::Str(_)) && matches!(expected, DataType::Str | DataType::Text) {
            return Ok(());
        }

        // Allow Decimal values to match any Decimal type specification
        if matches!(self, Value::Decimal(_)) && matches!(expected, DataType::Decimal(_, _)) {
            return Ok(());
        }

        Err(Error::TypeMismatch {
            expected: expected.to_string(),
            found: actual.to_string(),
        })
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::I8(i) => write!(f, "{}", i),
            Value::I16(i) => write!(f, "{}", i),
            Value::I32(i) => write!(f, "{}", i),
            Value::I64(i) => write!(f, "{}", i),
            Value::I128(i) => write!(f, "{}", i),
            Value::U8(i) => write!(f, "{}", i),
            Value::U16(i) => write!(f, "{}", i),
            Value::U32(i) => write!(f, "{}", i),
            Value::U64(i) => write!(f, "{}", i),
            Value::U128(i) => write!(f, "{}", i),
            Value::F32(v) => write!(f, "{}", v),
            Value::F64(v) => write!(f, "{}", v),
            Value::Decimal(d) => write!(f, "{}", d),
            Value::Str(s) => write!(f, "'{}'", s),
            Value::Date(d) => write!(f, "{}", d),
            Value::Time(t) => write!(f, "{}", t),
            Value::Timestamp(ts) => write!(f, "{}", ts),
            Value::Interval(i) => write!(
                f,
                "INTERVAL {} months {} days {} microseconds",
                i.months, i.days, i.microseconds
            ),
            Value::Uuid(u) => write!(f, "'{}'", u),
            Value::Bytea(b) => write!(f, "x'{}'", hex::encode(b)),
            Value::Inet(ip) => write!(f, "{}", ip),
            Value::Point(p) => write!(f, "POINT({} {})", p.x, p.y),
            Value::List(l) => write!(f, "{:?}", l),
            Value::Map(m) => write!(f, "{:?}", m),
        }
    }
}

// Implement Debug for Value to have nicer test output
impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "Null"),
            Value::Bool(b) => write!(f, "Bool({})", b),
            Value::I8(i) => write!(f, "I8({})", i),
            Value::I16(i) => write!(f, "I16({})", i),
            Value::I32(i) => write!(f, "I32({})", i),
            Value::I64(i) => write!(f, "I64({})", i),
            Value::I128(i) => write!(f, "I128({})", i),
            Value::U8(i) => write!(f, "U8({})", i),
            Value::U16(i) => write!(f, "U16({})", i),
            Value::U32(i) => write!(f, "U32({})", i),
            Value::U64(i) => write!(f, "U64({})", i),
            Value::U128(i) => write!(f, "U128({})", i),
            Value::F32(v) => write!(f, "F32({})", v),
            Value::F64(v) => write!(f, "F64({})", v),
            Value::Decimal(d) => write!(f, "Decimal({})", d),
            Value::Str(s) => write!(f, "Str({})", s),
            Value::Date(d) => write!(f, "Date({})", d),
            Value::Time(t) => write!(f, "Time({})", t),
            Value::Timestamp(ts) => {
                // Format timestamp in ISO format, removing fractional seconds if they're zero
                let formatted = ts.format("%Y-%m-%dT%H:%M:%S%.f").to_string();
                // Only trim fractional seconds, not seconds themselves
                let trimmed = if formatted.contains('.') {
                    formatted.trim_end_matches('0').trim_end_matches('.')
                } else {
                    &formatted
                };
                write!(f, "Timestamp({})", trimmed)
            }
            Value::Interval(i) => write!(f, "Interval({})", i),
            Value::Uuid(u) => write!(f, "Uuid({})", u),
            Value::Bytea(b) => write!(f, "Bytea({})", hex::encode(b)),
            Value::Inet(ip) => write!(f, "Inet({})", ip),
            Value::Point(p) => write!(f, "Point({}, {})", p.x, p.y),
            Value::List(l) => {
                write!(f, "List[")?;
                for (i, v) in l.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", v)?;
                }
                write!(f, "]")
            }
            Value::Map(m) => write!(f, "Map({:?})", m),
        }
    }
}

// Implement Hash for Value
impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => 0.hash(state),
            Value::Bool(b) => b.hash(state),
            Value::I8(i) => i.hash(state),
            Value::I16(i) => i.hash(state),
            Value::I32(i) => i.hash(state),
            Value::I64(i) => i.hash(state),
            Value::I128(i) => i.hash(state),
            Value::U8(i) => i.hash(state),
            Value::U16(i) => i.hash(state),
            Value::U32(i) => i.hash(state),
            Value::U64(i) => i.hash(state),
            Value::U128(i) => i.hash(state),
            Value::F32(f) => f.to_bits().hash(state),
            Value::F64(f) => f.to_bits().hash(state),
            Value::Decimal(d) => d.hash(state),
            Value::Str(s) => s.hash(state),
            Value::Date(d) => d.hash(state),
            Value::Time(t) => t.hash(state),
            Value::Timestamp(ts) => ts.hash(state),
            Value::Interval(i) => i.hash(state),
            Value::Uuid(u) => u.hash(state),
            Value::Bytea(b) => b.hash(state),
            Value::Inet(ip) => ip.hash(state),
            Value::Point(p) => {
                p.x.to_bits().hash(state);
                p.y.to_bits().hash(state);
            }
            Value::List(l) => l.hash(state),
            Value::Map(m) => {
                // Hash map keys in sorted order for determinism
                let mut pairs: Vec<_> = m.iter().collect();
                pairs.sort_by_key(|(k, _)| k.as_str());
                for (k, v) in pairs {
                    k.hash(state);
                    v.hash(state);
                }
            }
        }
    }
}

impl Eq for Value {}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (self, other) {
            // Null comparisons
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            // Boolean comparisons
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),

            // Numeric comparisons - convert to decimal for cross-type comparison
            (a, b) if a.is_integer() && b.is_integer() => {
                // Compare as i128 for integer types
                match (a.to_i128(), b.to_i128()) {
                    (Ok(a_val), Ok(b_val)) => a_val.cmp(&b_val),
                    _ => Ordering::Equal,
                }
            }
            (Value::F32(a), Value::F32(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::F64(a), Value::F64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Decimal(a), Value::Decimal(b)) => a.cmp(b),

            // Mixed numeric types - use evaluator's compare
            (a, b)
                if (a.is_integer()
                    || matches!(a, Value::F32(_) | Value::F64(_) | Value::Decimal(_)))
                    && (b.is_integer()
                        || matches!(b, Value::F32(_) | Value::F64(_) | Value::Decimal(_))) =>
            {
                crate::types::evaluator::compare(a, b).unwrap_or(Ordering::Equal)
            }

            // String comparisons
            (Value::Str(a), Value::Str(b)) => a.cmp(b),

            // Date/Time comparisons
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Time(a), Value::Time(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::Interval(a), Value::Interval(b)) => a.cmp(b),

            // UUID comparisons
            (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),

            // Bytea comparisons
            (Value::Bytea(a), Value::Bytea(b)) => a.cmp(b),

            // IP address comparisons
            (Value::Inet(a), Value::Inet(b)) => a.cmp(b),

            // Point comparisons
            (Value::Point(a), Value::Point(b)) => a.cmp(b),

            // List comparisons (lexicographic)
            (Value::List(a), Value::List(b)) => a.cmp(b),

            // Map comparisons - compare sorted key-value pairs
            (Value::Map(a), Value::Map(b)) => {
                let mut a_pairs: Vec<_> = a.iter().collect();
                let mut b_pairs: Vec<_> = b.iter().collect();
                a_pairs.sort_by_key(|(k, _)| k.as_str());
                b_pairs.sort_by_key(|(k, _)| k.as_str());
                a_pairs.cmp(&b_pairs)
            }

            // Different types - consider them equal for total ordering
            _ => Ordering::Equal,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_types() {
        assert!(Value::I8(10).is_integer());
        assert!(Value::U64(1000).is_integer());
        assert!(!Value::Str("not integer".into()).is_integer());

        // Test conversion to i128
        assert_eq!(Value::I8(10).to_i128().unwrap(), 10i128);
        assert_eq!(Value::U32(1000).to_i128().unwrap(), 1000i128);
    }
}
