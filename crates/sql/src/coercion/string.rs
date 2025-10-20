//! String coercions and parsing

use crate::error::{Error, Result};
use crate::types::{DataType, Value};

/// Parse string to signed integer
pub fn parse_string_to_signed(s: &str, target: &DataType) -> Result<Value> {
    match target {
        DataType::I8 => s
            .parse::<i8>()
            .map(Value::I8)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to TINYINT", s))),
        DataType::I16 => s
            .parse::<i16>()
            .map(Value::I16)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to SMALLINT", s))),
        DataType::I32 => s
            .parse::<i32>()
            .map(Value::I32)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to INT", s))),
        DataType::I64 => s
            .parse::<i64>()
            .map(Value::I64)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to BIGINT", s))),
        DataType::I128 => s
            .parse::<i128>()
            .map(Value::I128)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to HUGEINT", s))),
        _ => Err(Error::TypeMismatch {
            expected: "signed integer type".into(),
            found: target.to_string(),
        }),
    }
}

/// Parse string to unsigned integer
pub fn parse_string_to_unsigned(s: &str, target: &DataType) -> Result<Value> {
    match target {
        DataType::U8 => s
            .parse::<u8>()
            .map(Value::U8)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to TINYINT UNSIGNED", s))),
        DataType::U16 => s.parse::<u16>().map(Value::U16).map_err(|_| {
            Error::ExecutionError(format!("Cannot cast '{}' to SMALLINT UNSIGNED", s))
        }),
        DataType::U32 => s
            .parse::<u32>()
            .map(Value::U32)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to INT UNSIGNED", s))),
        DataType::U64 => s
            .parse::<u64>()
            .map(Value::U64)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to BIGINT UNSIGNED", s))),
        DataType::U128 => s
            .parse::<u128>()
            .map(Value::U128)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to HUGEINT UNSIGNED", s))),
        _ => Err(Error::TypeMismatch {
            expected: "unsigned integer type".into(),
            found: target.to_string(),
        }),
    }
}

/// Parse string to float
pub fn parse_string_to_float(s: &str, target: &DataType) -> Result<Value> {
    match target {
        DataType::F32 => s
            .parse::<f32>()
            .map(Value::F32)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to REAL", s))),
        DataType::F64 => s
            .parse::<f64>()
            .map(Value::F64)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to DOUBLE PRECISION", s))),
        _ => Err(Error::TypeMismatch {
            expected: "float type".into(),
            found: target.to_string(),
        }),
    }
}

/// Parse string to boolean
pub fn parse_string_to_bool(s: &str) -> Result<Value> {
    match s.to_uppercase().as_str() {
        "TRUE" | "T" | "YES" | "Y" | "1" => Ok(Value::Bool(true)),
        "FALSE" | "F" | "NO" | "N" | "0" => Ok(Value::Bool(false)),
        _ => Err(Error::ExecutionError(format!(
            "Cannot cast '{}' to BOOLEAN",
            s
        ))),
    }
}

/// Parse string to decimal
pub fn parse_string_to_decimal(s: &str) -> Result<Value> {
    use rust_decimal::Decimal;
    s.parse::<Decimal>()
        .map(Value::Decimal)
        .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to DECIMAL", s)))
}
