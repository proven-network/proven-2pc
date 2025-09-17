//! SQL data types with GlueSQL-compatible format

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
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
    // Null handling
    Nullable(Box<DataType>),
    // Explicit Null type (for NULL literals)
    Null,
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

    /// Check if this type is an unsigned integer
    pub fn is_unsigned_integer(&self) -> bool {
        matches!(
            self.base_type(),
            DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 | DataType::U128
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
            DataType::Nullable(inner) => write!(f, "{} NULL", inner),
            DataType::Null => write!(f, "NULL"),
        }
    }
}

/// Point type for geometric coordinates
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

impl Eq for Point {}

impl PartialOrd for Point {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Point {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.x.partial_cmp(&other.x) {
            Some(Ordering::Equal) | None => self.y.partial_cmp(&other.y).unwrap_or(Ordering::Equal),
            Some(other) => other,
        }
    }
}

impl std::hash::Hash for Point {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.x.to_bits().hash(state);
        self.y.to_bits().hash(state);
    }
}

/// Interval type for time durations
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

impl std::fmt::Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();

        if self.months != 0 {
            if self.months == 1 {
                parts.push("1 month".to_string());
            } else {
                parts.push(format!("{} months", self.months));
            }
        }

        if self.days != 0 {
            if self.days == 1 {
                parts.push("1 day".to_string());
            } else {
                parts.push(format!("{} days", self.days));
            }
        }

        if self.microseconds != 0 {
            // Convert microseconds to appropriate units
            let abs_micros = self.microseconds.abs();
            let sign = if self.microseconds < 0 { "-" } else { "" };

            if abs_micros % 1_000_000 == 0 {
                // Full seconds
                let seconds = abs_micros / 1_000_000;
                if seconds % 60 == 0 {
                    // Full minutes
                    let minutes = seconds / 60;
                    if minutes % 60 == 0 {
                        // Full hours
                        let hours = minutes / 60;
                        if hours == 1 {
                            parts.push(format!("{}1 hour", sign));
                        } else {
                            parts.push(format!("{}{} hours", sign, hours));
                        }
                    } else if minutes == 1 {
                        parts.push(format!("{}1 minute", sign));
                    } else {
                        parts.push(format!("{}{} minutes", sign, minutes));
                    }
                } else if seconds == 1 {
                    parts.push(format!("{}1 second", sign));
                } else {
                    parts.push(format!("{}{} seconds", sign, seconds));
                }
            } else {
                // Has fractional seconds, show as microseconds
                parts.push(format!("{}{} microseconds", sign, abs_micros));
            }
        }

        if parts.is_empty() {
            write!(f, "0")
        } else {
            write!(f, "{}", parts.join(" "))
        }
    }
}
