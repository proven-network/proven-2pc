//! SQL data types with GlueSQL-compatible format

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

/// SQL data types matching GlueSQL format
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    List(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
    // Null handling
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
            DataType::List(_) => write!(f, "ARRAY"),
            DataType::Map(_, _) => write!(f, "MAP"),
            DataType::Nullable(inner) => write!(f, "{} NULL", inner),
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
