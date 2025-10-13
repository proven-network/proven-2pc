//! SQL data types with GlueSQL-compatible format

use serde::{Deserialize, Serialize};
use std::fmt;

/// SQL data types matching GlueSQL format
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    // Boolean
    Bool,
    // Integer types
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    // Float types
    F32,
    F64,
    // Decimal with precision and scale
    Decimal(Option<u32>, Option<u32>), // precision, scale (optional for flexibility)
    // String types
    Str,
    Text,
    // Date/Time types
    Date,
    Time,
    Timestamp,
    Interval,
    // Special types
    Uuid,
    Bytea,
    Inet,
    Point,
    // Collection types
    Array(Box<DataType>, Option<usize>), // Fixed-size array (e.g., INTEGER[3])
    List(Box<DataType>),                 // Variable-size list (e.g., INTEGER[])
    Map(Box<DataType>, Box<DataType>),   // Key-value pairs
    Struct(Vec<(String, DataType)>),     // Named fields like records
    // JSON type (schemaless)
    Json, // Schemaless JSON type
    // Null handling
    Nullable(Box<DataType>),
    // Explicit Null type (for NULL literals)
    Null,
}

impl DataType {
    pub fn base_type(&self) -> &DataType {
        match self {
            DataType::Nullable(inner) => inner.base_type(),
            _ => self,
        }
    }

    /// Check if this type is numeric (integer, float, or decimal)
    pub fn is_numeric(&self) -> bool {
        matches!(
            self.base_type(),
            DataType::I8
                | DataType::I16
                | DataType::I32
                | DataType::I64
                | DataType::I128
                | DataType::U8
                | DataType::U16
                | DataType::U32
                | DataType::U64
                | DataType::U128
                | DataType::F32
                | DataType::F64
                | DataType::Decimal(_, _)
        )
    }

    /// Check if this type is an integer (signed or unsigned)
    pub fn is_integer(&self) -> bool {
        matches!(
            self.base_type(),
            DataType::I8
                | DataType::I16
                | DataType::I32
                | DataType::I64
                | DataType::I128
                | DataType::U8
                | DataType::U16
                | DataType::U32
                | DataType::U64
                | DataType::U128
        )
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Bool => write!(f, "BOOLEAN"),
            DataType::I8 => write!(f, "TINYINT"),
            DataType::I16 => write!(f, "SMALLINT"),
            DataType::I32 => write!(f, "INT"),
            DataType::I64 => write!(f, "BIGINT"),
            DataType::I128 => write!(f, "HUGEINT"),
            DataType::U8 => write!(f, "TINYINT UNSIGNED"),
            DataType::U16 => write!(f, "SMALLINT UNSIGNED"),
            DataType::U32 => write!(f, "INT UNSIGNED"),
            DataType::U64 => write!(f, "BIGINT UNSIGNED"),
            DataType::U128 => write!(f, "HUGEINT UNSIGNED"),
            DataType::F32 => write!(f, "REAL"),
            DataType::F64 => write!(f, "DOUBLE PRECISION"),
            DataType::Decimal(p, s) => match (p, s) {
                (Some(p), Some(s)) => write!(f, "DECIMAL({}, {})", p, s),
                (Some(p), None) => write!(f, "DECIMAL({})", p),
                _ => write!(f, "DECIMAL"),
            },
            DataType::Str | DataType::Text => write!(f, "VARCHAR"),
            DataType::Date => write!(f, "DATE"),
            DataType::Time => write!(f, "TIME"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::Uuid => write!(f, "UUID"),
            DataType::Bytea => write!(f, "BYTEA"),
            DataType::Inet => write!(f, "INET"),
            DataType::Point => write!(f, "POINT"),
            DataType::Array(inner, Some(size)) => write!(f, "{}[{}]", inner, size),
            DataType::Array(inner, None) => write!(f, "{}[]", inner),
            DataType::List(inner) => write!(f, "{}[]", inner),
            DataType::Map(key, value) => write!(f, "MAP({}, {})", key, value),
            DataType::Struct(fields) => {
                let field_strs: Vec<String> = fields
                    .iter()
                    .map(|(name, dtype)| format!("{} {}", name, dtype))
                    .collect();
                write!(f, "STRUCT({})", field_strs.join(", "))
            }
            DataType::Json => write!(f, "JSON"),
            DataType::Nullable(inner) => write!(f, "{} NULL", inner),
            DataType::Null => write!(f, "NULL"),
        }
    }
}
