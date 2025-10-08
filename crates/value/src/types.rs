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

/// A row of values (useful for SQL contexts)
pub type Row = Vec<Value>;

/// Point type for geometric data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }
}

/// Interval type for time durations
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

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
    // Type Name
    // ========================================================================

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
            Value::F32(fl) => write!(f, "F32({:?})", fl),
            Value::F64(fl) => write!(f, "F64({:?})", fl),
            Value::Decimal(d) => write!(f, "Decimal({:?})", d),
            Value::Str(s) => write!(f, "Str({:?})", s),
            Value::Date(d) => write!(f, "Date({:?})", d),
            Value::Time(t) => write!(f, "Time({:?})", t),
            Value::Timestamp(ts) => write!(f, "Timestamp({:?})", ts),
            Value::Interval(i) => write!(f, "Interval({:?})", i),
            Value::Uuid(u) => write!(f, "Uuid({:?})", u),
            Value::Bytea(b) => write!(f, "Bytea({} bytes)", b.len()),
            Value::Inet(ip) => write!(f, "Inet({:?})", ip),
            Value::Point(p) => write!(f, "Point({:?})", p),
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
            Value::Array(_) | Value::List(_) | Value::Map(_) | Value::Struct(_) => {
                write!(f, "{:?}", self)
            }
            Value::Json(j) => write!(f, "{}", j),
        }
    }
}
