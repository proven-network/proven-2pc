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
use std::str::FromStr;
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
    Array(Vec<Value>),            // Fixed-size array
    List(Vec<Value>),             // Variable-size list
    Map(HashMap<String, Value>),  // Key-value pairs
    Struct(Vec<(String, Value)>), // Named fields
    // JSON type (schemaless)
    Json(serde_json::Value), // Schemaless JSON
}

// Backward compatibility - map old names to new format
impl Value {
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

    /// Check if value is a signed integer type
    pub fn is_signed_integer(&self) -> bool {
        matches!(
            self,
            Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_) | Value::I128(_)
        )
    }

    /// Check if value is an unsigned integer type
    pub fn is_unsigned_integer(&self) -> bool {
        matches!(
            self,
            Value::U8(_) | Value::U16(_) | Value::U32(_) | Value::U64(_) | Value::U128(_)
        )
    }

    /// Check if value is any numeric type (integer, float, or decimal)
    pub fn is_numeric(&self) -> bool {
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
                | Value::F32(_)
                | Value::F64(_)
                | Value::Decimal(_)
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
            Value::Array(vals) => {
                // TODO: Infer element type from values
                let elem_type = if vals.is_empty() {
                    DataType::I64
                } else {
                    vals[0].data_type()
                };
                DataType::Array(Box::new(elem_type), Some(vals.len()))
            }
            Value::List(vals) => {
                // TODO: Infer element type from values
                let elem_type = if vals.is_empty() {
                    DataType::I64
                } else {
                    vals[0].data_type()
                };
                DataType::List(Box::new(elem_type))
            }
            Value::Map(map) => {
                // Infer value type from the map contents
                let value_type = if map.is_empty() {
                    DataType::I64 // Default if empty
                } else {
                    // Get the first value's type as representative
                    map.values().next().unwrap().data_type()
                };
                DataType::Map(Box::new(DataType::Str), Box::new(value_type))
            }
            Value::Struct(fields) => {
                let field_types = fields
                    .iter()
                    .map(|(name, val)| (name.clone(), val.data_type()))
                    .collect();
                DataType::Struct(field_types)
            }
            Value::Json(_) => DataType::Json,
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

        // Allow empty maps to match any Map type
        if let Value::Map(m) = self
            && m.is_empty()
            && matches!(expected, DataType::Map(_, _))
        {
            return Ok(());
        }

        // Allow empty lists/arrays to match any List/Array type
        if let Value::List(l) = self
            && l.is_empty()
            && matches!(expected, DataType::List(_))
        {
            return Ok(());
        }
        if let Value::Array(a) = self
            && a.is_empty()
            && matches!(expected, DataType::Array(_, _))
        {
            return Ok(());
        }

        // Special handling for structs with null fields
        if let (Value::Struct(fields), DataType::Struct(schema_fields)) = (self, expected) {
            // Check if struct fields match, ignoring NULL type mismatches
            if fields.len() == schema_fields.len() {
                let mut all_match = true;
                for ((field_name, field_val), (schema_name, schema_type)) in
                    fields.iter().zip(schema_fields.iter())
                {
                    if field_name != schema_name {
                        all_match = false;
                        break;
                    }
                    // NULL values are compatible with any type
                    if !field_val.is_null() {
                        // Recursively check non-null field types
                        if field_val.check_type(schema_type).is_err() {
                            all_match = false;
                            break;
                        }
                    }
                }
                if all_match {
                    return Ok(());
                }
            }
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
            Value::Array(a) => {
                write!(f, "[")?;
                for (i, val) in a.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", val)?;
                }
                write!(f, "]")
            }
            Value::List(l) => {
                write!(f, "[")?;
                for (i, val) in l.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", val)?;
                }
                write!(f, "]")
            }
            Value::Map(m) => {
                write!(f, "{{")?;
                let mut first = true;
                for (key, val) in m.iter() {
                    if !first {
                        write!(f, ", ")?;
                    }
                    write!(f, "'{}': {}", key, val)?;
                    first = false;
                }
                write!(f, "}}")
            }
            Value::Struct(s) => {
                write!(f, "{{")?;
                for (i, (name, val)) in s.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", name, val)?;
                }
                write!(f, "}}")
            }
            Value::Json(j) => write!(f, "{}", j),
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
            Value::Array(a) => {
                write!(f, "Array[")?;
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", v)?;
                }
                write!(f, "]")
            }
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
            Value::Struct(s) => {
                write!(f, "Struct{{")?;
                for (i, (name, val)) in s.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {:?}", name, val)?;
                }
                write!(f, "}}")
            }
            Value::Json(j) => write!(f, "Json({})", j),
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
            Value::Array(a) => a.hash(state),
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
            Value::Struct(s) => s.hash(state),
            Value::Json(j) => j.to_string().hash(state), // Hash JSON as string for consistency
        }
    }
}

impl Eq for Value {}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (self, other) {
            // Null comparisons - SQL semantics: NULL sorts last in ASC, first in DESC
            // To achieve this, NULL should be Greater than any non-NULL value
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater, // NULL is greater (sorts last in ASC)
            (_, Value::Null) => Ordering::Less,    // Non-NULL is less than NULL

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
                crate::operators::compare(a, b).unwrap_or(Ordering::Equal)
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
            (Value::Array(a), Value::Array(b)) => a.cmp(b),
            (Value::List(a), Value::List(b)) => a.cmp(b),

            // Map comparisons - compare sorted key-value pairs
            (Value::Map(a), Value::Map(b)) => {
                let mut a_pairs: Vec<_> = a.iter().collect();
                let mut b_pairs: Vec<_> = b.iter().collect();
                a_pairs.sort_by_key(|(k, _)| k.as_str());
                b_pairs.sort_by_key(|(k, _)| k.as_str());
                a_pairs.cmp(&b_pairs)
            }

            // Struct comparisons - compare fields in order
            (Value::Struct(a), Value::Struct(b)) => a.cmp(b),

            // JSON comparisons - compare as strings
            (Value::Json(a), Value::Json(b)) => a.to_string().cmp(&b.to_string()),

            // Different types - consider them equal for total ordering
            _ => Ordering::Equal,
        }
    }
}

impl Value {
    // Collection access methods

    /// Parse JSON string to array/list value
    pub fn parse_json_array(s: &str) -> Result<Value> {
        let parsed: serde_json::Value =
            serde_json::from_str(s).map_err(|e| Error::InvalidJsonString(e.to_string()))?;

        if let serde_json::Value::Array(arr) = parsed {
            let values: Result<Vec<Value>> = arr.into_iter().map(Self::from_json_value).collect();
            Ok(Value::List(values?))
        } else {
            Err(Error::JsonArrayTypeRequired)
        }
    }

    /// Parse JSON string to map/object value
    pub fn parse_json_object(s: &str) -> Result<Value> {
        let parsed: serde_json::Value = serde_json::from_str(s)
            .map_err(|e| Error::ParseError(format!("Invalid JSON object: {}", e)))?;

        if let serde_json::Value::Object(obj) = parsed {
            let mut map = HashMap::new();
            for (key, val) in obj {
                map.insert(key, Self::from_json_value(val)?);
            }
            Ok(Value::Map(map))
        } else {
            Err(Error::ParseError("Expected JSON object".into()))
        }
    }

    /// Convert JSON value to SQL Value
    fn from_json_value(json: serde_json::Value) -> Result<Value> {
        match json {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
            serde_json::Value::Number(n) => {
                // Parse all JSON numbers as Decimal for maximum flexibility
                // This allows proper coercion to the target type later
                use rust_decimal::Decimal;

                if let Some(i) = n.as_i64() {
                    Ok(Value::Decimal(Decimal::from(i)))
                } else if let Some(u) = n.as_u64() {
                    Ok(Value::Decimal(Decimal::from(u)))
                } else if n.as_f64().is_some() {
                    // For floats, use string parsing to avoid precision loss
                    Decimal::from_str(&n.to_string())
                        .map(Value::Decimal)
                        .map_err(|_| Error::ParseError("Invalid JSON number".into()))
                } else {
                    Err(Error::ParseError("Invalid JSON number".into()))
                }
            }
            serde_json::Value::String(s) => Ok(Value::Str(s)),
            serde_json::Value::Array(arr) => {
                let values: Result<Vec<Value>> =
                    arr.into_iter().map(Self::from_json_value).collect();
                Ok(Value::List(values?))
            }
            serde_json::Value::Object(obj) => {
                let mut map = HashMap::new();
                for (key, val) in obj {
                    map.insert(key, Self::from_json_value(val)?);
                }
                Ok(Value::Map(map))
            }
        }
    }

    /// Convert Value to JSON string for display
    pub fn to_json_string(&self) -> String {
        match self {
            Value::Array(vals) | Value::List(vals) => {
                let items: Vec<String> = vals.iter().map(|v| v.to_json_string()).collect();
                format!("[{}]", items.join(", "))
            }
            Value::Map(map) => {
                let items: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("\"{}\": {}", k, v.to_json_string()))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
            Value::Struct(fields) => {
                let items: Vec<String> = fields
                    .iter()
                    .map(|(k, v)| format!("\"{}\": {}", k, v.to_json_string()))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
            Value::Json(j) => j.to_string(),
            Value::Null => "null".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Str(s) => format!("\"{}\"", s.replace('\"', "\\\"")),
            _ => self.to_string(),
        }
    }

    /// Access array/list element by index
    pub fn get_index(&self, index: usize) -> Option<&Value> {
        match self {
            Value::Array(vals) | Value::List(vals) => vals.get(index),
            _ => None,
        }
    }

    /// Access struct field by name
    pub fn get_field(&self, field: &str) -> Option<&Value> {
        match self {
            Value::Struct(fields) => fields
                .iter()
                .find(|(name, _)| name == field)
                .map(|(_, val)| val),
            _ => None,
        }
    }

    /// Access map value by key
    pub fn get_key(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Map(map) => map.get(key),
            _ => None,
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

    #[test]
    fn test_collection_values() {
        // Test JSON array parsing
        let arr = Value::parse_json_array("[1, 2, 3]").unwrap();
        match arr {
            Value::List(vals) => {
                assert_eq!(vals.len(), 3);
                assert_eq!(vals[0], Value::Decimal(Decimal::from(1)));
                assert_eq!(vals[1], Value::Decimal(Decimal::from(2)));
                assert_eq!(vals[2], Value::Decimal(Decimal::from(3)));
            }
            _ => panic!("Expected List"),
        }

        // Test JSON object parsing
        let obj = Value::parse_json_object(r#"{"name": "Alice", "age": 30}"#).unwrap();
        match obj {
            Value::Map(map) => {
                assert_eq!(map.get("name"), Some(&Value::Str("Alice".to_string())));
                assert_eq!(map.get("age"), Some(&Value::Decimal(Decimal::from(30))));
            }
            _ => panic!("Expected Map"),
        }

        // Test array access
        let list = Value::List(vec![
            Value::Decimal(Decimal::from(10)),
            Value::Decimal(Decimal::from(20)),
            Value::Decimal(Decimal::from(30)),
        ]);
        assert_eq!(list.get_index(0), Some(&Value::Decimal(Decimal::from(10))));
        assert_eq!(list.get_index(2), Some(&Value::Decimal(Decimal::from(30))));
        assert_eq!(list.get_index(5), None);

        // Test struct field access
        let structure = Value::Struct(vec![
            ("name".to_string(), Value::Str("Bob".to_string())),
            ("age".to_string(), Value::Decimal(Decimal::from(25))),
        ]);
        assert_eq!(
            structure.get_field("name"),
            Some(&Value::Str("Bob".to_string()))
        );
        assert_eq!(
            structure.get_field("age"),
            Some(&Value::Decimal(Decimal::from(25)))
        );
        assert_eq!(structure.get_field("unknown"), None);

        // Test map key access
        let mut map = HashMap::new();
        map.insert("key1".to_string(), Value::Str("value1".to_string()));
        map.insert("key2".to_string(), Value::Decimal(Decimal::from(42)));
        let map_val = Value::Map(map);
        assert_eq!(
            map_val.get_key("key1"),
            Some(&Value::Str("value1".to_string()))
        );
        assert_eq!(
            map_val.get_key("key2"),
            Some(&Value::Decimal(Decimal::from(42)))
        );
        assert_eq!(map_val.get_key("key3"), None);

        // Test Display formatting
        let list = Value::List(vec![
            Value::Decimal(Decimal::from(1)),
            Value::Decimal(Decimal::from(2)),
        ]);
        assert_eq!(list.to_string(), "[1, 2]");

        let mut map = HashMap::new();
        map.insert("a".to_string(), Value::Decimal(Decimal::from(1)));
        let map_val = Value::Map(map);
        assert_eq!(map_val.to_string(), "{'a': 1}");

        // Test to_json_string
        let nested = Value::List(vec![
            Value::Decimal(Decimal::from(1)),
            Value::Str("test".to_string()),
            Value::Bool(true),
            Value::Null,
        ]);
        assert_eq!(nested.to_json_string(), r#"[1, "test", true, null]"#);
    }
}
