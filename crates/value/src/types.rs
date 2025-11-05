//! Value types for Proven database
//!
//! Comprehensive value representation supporting SQL types and collections.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use uuid::Uuid;

use crate::identity::Identity;
use crate::interval::Interval;
use crate::point::Point;
use crate::private_key::PrivateKey;
use crate::public_key::PublicKey;
use crate::vault::Vault;

/// Universal value type for Proven database components
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
    // String
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
    PrivateKey(PrivateKey),
    PublicKey(PublicKey),
    Identity(Identity),
    Vault(Vault),
    // Collection types
    Array(Vec<Value>),            // Fixed-size array
    List(Vec<Value>),             // Variable-size list
    Map(HashMap<String, Value>),  // Key-value pairs
    Struct(Vec<(String, Value)>), // Named fields
    // JSON type (schemaless)
    Json(serde_json::Value),
}

impl Value {
    // ========================================================================
    // Constructors
    // ========================================================================

    /// Create a null value
    pub fn null() -> Self {
        Value::Null
    }

    /// Create an I64 value (most common integer type)
    pub fn integer(i: i64) -> Self {
        Value::I64(i)
    }

    /// Create a string value
    pub fn string<S: Into<String>>(s: S) -> Self {
        Value::Str(s.into())
    }

    /// Create a boolean value
    pub fn boolean(b: bool) -> Self {
        Value::Bool(b)
    }

    /// Create a float value
    pub fn float(f: f64) -> Self {
        Value::F64(f)
    }

    /// Create bytes value
    pub fn bytes(b: Vec<u8>) -> Self {
        Value::Bytea(b)
    }

    // ========================================================================
    // JSON Parsing Utilities
    // ========================================================================

    /// Convert serde_json::Value to our Value type
    fn from_json_value(json: serde_json::Value) -> Value {
        match json {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::I64(i)
                } else if let Some(f) = n.as_f64() {
                    Value::F64(f)
                } else {
                    // Fallback for very large numbers
                    Value::Json(serde_json::Value::Number(n))
                }
            }
            serde_json::Value::String(s) => Value::Str(s),
            serde_json::Value::Array(arr) => {
                let values: Vec<Value> = arr.into_iter().map(Value::from_json_value).collect();
                Value::List(values)
            }
            serde_json::Value::Object(obj) => {
                let map: std::collections::HashMap<String, Value> = obj
                    .into_iter()
                    .map(|(k, v)| (k, Value::from_json_value(v)))
                    .collect();
                Value::Map(map)
            }
        }
    }

    /// Parse a JSON array from a string, returning a Value::List
    pub fn parse_json_array(s: &str) -> Result<Value, String> {
        let json_value: serde_json::Value =
            serde_json::from_str(s).map_err(|e| format!("Failed to parse JSON array: {}", e))?;

        match json_value {
            serde_json::Value::Array(arr) => {
                let values: Vec<Value> = arr.into_iter().map(Value::from_json_value).collect();
                Ok(Value::List(values))
            }
            _ => Err("Expected JSON array".to_string()),
        }
    }

    /// Parse a JSON object from a string, returning a Value::Map
    pub fn parse_json_object(s: &str) -> Result<Value, String> {
        let json_value: serde_json::Value =
            serde_json::from_str(s).map_err(|e| format!("Failed to parse JSON object: {}", e))?;

        match json_value {
            serde_json::Value::Object(obj) => {
                let map: std::collections::HashMap<String, Value> = obj
                    .into_iter()
                    .map(|(k, v)| (k, Value::from_json_value(v)))
                    .collect();
                Ok(Value::Map(map))
            }
            _ => Err("Expected JSON object".to_string()),
        }
    }

    // ========================================================================
    // Type Checks
    // ========================================================================

    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
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

    /// Check if value is a float type
    pub fn is_float(&self) -> bool {
        matches!(self, Value::F32(_) | Value::F64(_))
    }

    /// Check if value is numeric (integer, float, or decimal)
    pub fn is_numeric(&self) -> bool {
        self.is_integer() || self.is_float() || matches!(self, Value::Decimal(_))
    }

    /// Check if value is a string
    pub fn is_string(&self) -> bool {
        matches!(self, Value::Str(_))
    }

    /// Check if value is a boolean
    pub fn is_boolean(&self) -> bool {
        matches!(self, Value::Bool(_))
    }

    /// Check if value is bytes
    pub fn is_bytes(&self) -> bool {
        matches!(self, Value::Bytea(_))
    }

    /// Check if value is JSON
    pub fn is_json(&self) -> bool {
        matches!(self, Value::Json(_))
    }

    /// Check if value is a collection (array, list, map, or struct)
    pub fn is_collection(&self) -> bool {
        matches!(
            self,
            Value::Array(_) | Value::List(_) | Value::Map(_) | Value::Struct(_)
        )
    }

    // ========================================================================
    // Type Name and Tag
    // ========================================================================

    /// Get a numeric tag for this value type (for Ord implementation)
    /// Note: NULL has the highest tag (255) so it sorts last in ASC order,
    /// which follows SQL standard behavior.
    fn type_tag(&self) -> u8 {
        match self {
            Value::Bool(_) => 0,
            Value::I8(_) => 1,
            Value::I16(_) => 2,
            Value::I32(_) => 3,
            Value::I64(_) => 4,
            Value::I128(_) => 5,
            Value::U8(_) => 6,
            Value::U16(_) => 7,
            Value::U32(_) => 8,
            Value::U64(_) => 9,
            Value::U128(_) => 10,
            Value::F32(_) => 11,
            Value::F64(_) => 12,
            Value::Decimal(_) => 13,
            Value::Str(_) => 14,
            Value::Date(_) => 15,
            Value::Time(_) => 16,
            Value::Timestamp(_) => 17,
            Value::Interval(_) => 18,
            Value::Uuid(_) => 19,
            Value::Bytea(_) => 20,
            Value::Inet(_) => 21,
            Value::Point(_) => 22,
            Value::PrivateKey(_) => 23,
            Value::PublicKey(_) => 24,
            Value::Identity(_) => 25,
            Value::Vault(_) => 26,
            Value::Array(_) => 27,
            Value::List(_) => 28,
            Value::Map(_) => 29,
            Value::Struct(_) => 30,
            Value::Json(_) => 31,
            Value::Null => 255, // NULL sorts last (SQL standard)
        }
    }

    /// Get the type name of this value
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::I8(_) => "i8",
            Value::I16(_) => "i16",
            Value::I32(_) => "i32",
            Value::I64(_) => "i64",
            Value::I128(_) => "i128",
            Value::U8(_) => "u8",
            Value::U16(_) => "u16",
            Value::U32(_) => "u32",
            Value::U64(_) => "u64",
            Value::U128(_) => "u128",
            Value::F32(_) => "f32",
            Value::F64(_) => "f64",
            Value::Decimal(_) => "decimal",
            Value::Str(_) => "string",
            Value::Date(_) => "date",
            Value::Time(_) => "time",
            Value::Timestamp(_) => "timestamp",
            Value::Interval(_) => "interval",
            Value::Uuid(_) => "uuid",
            Value::Bytea(_) => "bytea",
            Value::Inet(_) => "inet",
            Value::Point(_) => "point",
            Value::PrivateKey(_) => "private_key",
            Value::PublicKey(_) => "public_key",
            Value::Identity(_) => "identity",
            Value::Vault(_) => "vault",
            Value::Array(_) => "array",
            Value::List(_) => "list",
            Value::Map(_) => "map",
            Value::Struct(_) => "struct",
            Value::Json(_) => "json",
        }
    }
}

// ============================================================================
// Display
// ============================================================================

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "Null"),
            Value::Bool(b) => write!(f, "Bool({:?})", b),
            Value::I8(i) => write!(f, "I8({:?})", i),
            Value::I16(i) => write!(f, "I16({:?})", i),
            Value::I32(i) => write!(f, "I32({:?})", i),
            Value::I64(i) => write!(f, "I64({:?})", i),
            Value::I128(i) => write!(f, "I128({:?})", i),
            Value::U8(u) => write!(f, "U8({:?})", u),
            Value::U16(u) => write!(f, "U16({:?})", u),
            Value::U32(u) => write!(f, "U32({:?})", u),
            Value::U64(u) => write!(f, "U64({:?})", u),
            Value::U128(u) => write!(f, "U128({:?})", u),
            Value::F32(fl) => {
                // Format whole numbers without decimal point (2.0 -> 2)
                if fl.fract() == 0.0 && fl.is_finite() {
                    write!(f, "F32({})", *fl as i64)
                } else {
                    write!(f, "F32({:?})", fl)
                }
            }
            Value::F64(fl) => {
                // Format whole numbers without decimal point (1.0 -> 1)
                if fl.fract() == 0.0 && fl.is_finite() {
                    write!(f, "F64({})", *fl as i64)
                } else {
                    write!(f, "F64({:?})", fl)
                }
            }
            Value::Decimal(d) => write!(f, "Decimal({:?})", d),
            Value::Str(s) => write!(f, "Str({})", s),
            Value::Date(d) => write!(f, "Date({:?})", d),
            Value::Time(t) => write!(f, "Time({:?})", t),
            Value::Timestamp(ts) => write!(f, "Timestamp({:?})", ts),
            Value::Interval(i) => write!(
                f,
                "Interval({} months, {} days, {} us)",
                i.months, i.days, i.microseconds
            ),
            Value::Uuid(u) => write!(f, "Uuid({:?})", u),
            Value::Bytea(b) => write!(f, "Bytea({} bytes)", b.len()),
            Value::Inet(ip) => write!(f, "Inet({:?})", ip),
            Value::Point(p) => write!(f, "Point({:?})", p),
            Value::PrivateKey(k) => write!(f, "PrivateKey({:?})", k),
            Value::PublicKey(k) => write!(f, "PublicKey({:?})", k),
            Value::Identity(i) => write!(f, "Identity({:?})", i),
            Value::Vault(v) => write!(f, "Vault({:?})", v),
            Value::Array(arr) => write!(f, "Array({:?})", arr),
            Value::List(list) => write!(f, "List({:?})", list),
            Value::Map(map) => write!(f, "Map({:?})", map),
            Value::Struct(fields) => write!(f, "Struct({:?})", fields),
            Value::Json(j) => write!(f, "Json({:?})", j),
        }
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
            Value::U8(u) => write!(f, "{}", u),
            Value::U16(u) => write!(f, "{}", u),
            Value::U32(u) => write!(f, "{}", u),
            Value::U64(u) => write!(f, "{}", u),
            Value::U128(u) => write!(f, "{}", u),
            Value::F32(fl) => write!(f, "{}", fl),
            Value::F64(fl) => write!(f, "{}", fl),
            Value::Decimal(d) => write!(f, "{}", d),
            Value::Str(s) => write!(f, "{}", s),
            Value::Date(d) => write!(f, "{}", d),
            Value::Time(t) => write!(f, "{}", t),
            Value::Timestamp(ts) => write!(f, "{}", ts),
            Value::Interval(_) => write!(f, "{:?}", self), // Use Debug for complex types
            Value::Uuid(u) => write!(f, "{}", u),
            Value::Bytea(b) => write!(f, "<{} bytes>", b.len()),
            Value::Inet(ip) => write!(f, "{}", ip),
            Value::Point(p) => write!(f, "({}, {})", p.x, p.y),
            Value::PrivateKey(k) => write!(f, "{}", k),
            Value::PublicKey(k) => write!(f, "{}", k),
            Value::Identity(i) => write!(f, "{}", i),
            Value::Vault(v) => write!(f, "{}", v),
            Value::Array(_) | Value::List(_) | Value::Map(_) | Value::Struct(_) => {
                write!(f, "{:?}", self)
            }
            Value::Json(j) => write!(f, "{}", j),
        }
    }
}

// ============================================================================
// Trait Implementations (Eq, Hash, Ord)
// ============================================================================

// Implement Eq even though we have floats - we use bitwise comparison for floats
impl Eq for Value {}

// Implement Hash using bitwise representation for floats
impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the discriminant first
        std::mem::discriminant(self).hash(state);

        match self {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::I8(i) => i.hash(state),
            Value::I16(i) => i.hash(state),
            Value::I32(i) => i.hash(state),
            Value::I64(i) => i.hash(state),
            Value::I128(i) => i.hash(state),
            Value::U8(u) => u.hash(state),
            Value::U16(u) => u.hash(state),
            Value::U32(u) => u.hash(state),
            Value::U64(u) => u.hash(state),
            Value::U128(u) => u.hash(state),
            Value::F32(f) => f.to_bits().hash(state),
            Value::F64(f) => f.to_bits().hash(state),
            Value::Decimal(d) => {
                d.mantissa().hash(state);
                d.scale().hash(state);
            }
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
            Value::PrivateKey(k) => k.hash(state),
            Value::PublicKey(k) => k.hash(state),
            Value::Identity(i) => i.hash(state),
            Value::Vault(v) => v.hash(state),
            Value::Array(arr) => arr.hash(state),
            Value::List(list) => list.hash(state),
            Value::Map(map) => {
                // Hash map as sorted key-value pairs for consistency
                let mut pairs: Vec<_> = map.iter().collect();
                pairs.sort_by_key(|(k, _)| *k);
                pairs.hash(state);
            }
            Value::Struct(fields) => fields.hash(state),
            Value::Json(j) => j.to_string().hash(state),
        }
    }
}

// Implement PartialOrd with special handling for floats
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Implement Ord with consistent ordering even for floats
impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        // First compare type tags for consistent ordering across types
        let self_tag = self.type_tag();
        let other_tag = other.type_tag();
        match self_tag.cmp(&other_tag) {
            Ordering::Equal => {}
            other => return other,
        }

        // Same type, compare values
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::I8(a), Value::I8(b)) => a.cmp(b),
            (Value::I16(a), Value::I16(b)) => a.cmp(b),
            (Value::I32(a), Value::I32(b)) => a.cmp(b),
            (Value::I64(a), Value::I64(b)) => a.cmp(b),
            (Value::I128(a), Value::I128(b)) => a.cmp(b),
            (Value::U8(a), Value::U8(b)) => a.cmp(b),
            (Value::U16(a), Value::U16(b)) => a.cmp(b),
            (Value::U32(a), Value::U32(b)) => a.cmp(b),
            (Value::U64(a), Value::U64(b)) => a.cmp(b),
            (Value::U128(a), Value::U128(b)) => a.cmp(b),
            // For floats, use total ordering via bits
            (Value::F32(a), Value::F32(b)) => {
                let a_bits = a.to_bits();
                let b_bits = b.to_bits();
                // Handle sign: negative floats sort before positive
                let a_signed = if a.is_sign_negative() {
                    !a_bits
                } else {
                    a_bits ^ (1u32 << 31)
                };
                let b_signed = if b.is_sign_negative() {
                    !b_bits
                } else {
                    b_bits ^ (1u32 << 31)
                };
                a_signed.cmp(&b_signed)
            }
            (Value::F64(a), Value::F64(b)) => {
                let a_bits = a.to_bits();
                let b_bits = b.to_bits();
                let a_signed = if a.is_sign_negative() {
                    !a_bits
                } else {
                    a_bits ^ (1u64 << 63)
                };
                let b_signed = if b.is_sign_negative() {
                    !b_bits
                } else {
                    b_bits ^ (1u64 << 63)
                };
                a_signed.cmp(&b_signed)
            }
            (Value::Decimal(a), Value::Decimal(b)) => a.cmp(b),
            (Value::Str(a), Value::Str(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Time(a), Value::Time(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::Interval(a), Value::Interval(b)) => a.cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
            (Value::Bytea(a), Value::Bytea(b)) => a.cmp(b),
            (Value::Inet(a), Value::Inet(b)) => a.cmp(b),
            (Value::Point(a), Value::Point(b)) => match a.x.partial_cmp(&b.x) {
                Some(Ordering::Equal) | None => a.y.partial_cmp(&b.y).unwrap_or(Ordering::Equal),
                Some(ord) => ord,
            },
            (Value::PrivateKey(a), Value::PrivateKey(b)) => a.cmp(b),
            (Value::PublicKey(a), Value::PublicKey(b)) => a.cmp(b),
            (Value::Identity(a), Value::Identity(b)) => a.cmp(b),
            (Value::Vault(a), Value::Vault(b)) => a.cmp(b),
            (Value::Array(a), Value::Array(b)) => a.cmp(b),
            (Value::List(a), Value::List(b)) => a.cmp(b),
            (Value::Map(a), Value::Map(b)) => {
                // Compare maps as sorted key-value pairs
                let mut a_pairs: Vec<_> = a.iter().collect();
                let mut b_pairs: Vec<_> = b.iter().collect();
                a_pairs.sort_by_key(|(k, _)| *k);
                b_pairs.sort_by_key(|(k, _)| *k);
                a_pairs.cmp(&b_pairs)
            }
            (Value::Struct(a), Value::Struct(b)) => a.cmp(b),
            (Value::Json(a), Value::Json(b)) => a.to_string().cmp(&b.to_string()),
            _ => Ordering::Equal, // Should never reach here due to discriminant check
        }
    }
}
