//! Numeric type coercions (integers, floats, decimals)

use crate::error::{Error, Result};
use crate::types::{DataType, Value};

/// Coerce signed integer to another signed integer (widening or narrowing with bounds check)
pub fn coerce_signed_widen(value: i128, target: &DataType) -> Result<Value> {
    match target {
        DataType::I8 => i8::try_from(value)
            .map(Value::I8)
            .map_err(|_| Error::TypeMismatch {
                expected: "TINYINT".into(),
                found: format!("value {}", value),
            }),
        DataType::I16 => i16::try_from(value)
            .map(Value::I16)
            .map_err(|_| Error::TypeMismatch {
                expected: "SMALLINT".into(),
                found: format!("value {}", value),
            }),
        DataType::I32 => i32::try_from(value)
            .map(Value::I32)
            .map_err(|_| Error::TypeMismatch {
                expected: "INT".into(),
                found: format!("value {}", value),
            }),
        DataType::I64 => i64::try_from(value)
            .map(Value::I64)
            .map_err(|_| Error::TypeMismatch {
                expected: "BIGINT".into(),
                found: format!("value {}", value),
            }),
        DataType::I128 => Ok(Value::I128(value)),
        _ => Err(Error::TypeMismatch {
            expected: "signed integer type".into(),
            found: target.to_string(),
        }),
    }
}

/// Coerce signed integer to unsigned integer (with bounds checking)
pub fn coerce_signed_to_unsigned(value: i128, target: &DataType) -> Result<Value> {
    match target {
        DataType::U8 => u8::try_from(value).map(Value::U8).map_err(|_| {
            Error::InvalidValue(format!("Cannot convert {} to TINYINT UNSIGNED", value))
        }),
        DataType::U16 => u16::try_from(value).map(Value::U16).map_err(|_| {
            Error::InvalidValue(format!("Cannot convert {} to SMALLINT UNSIGNED", value))
        }),
        DataType::U32 => u32::try_from(value)
            .map(Value::U32)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to INT UNSIGNED", value))),
        DataType::U64 => u64::try_from(value).map(Value::U64).map_err(|_| {
            Error::InvalidValue(format!("Cannot convert {} to BIGINT UNSIGNED", value))
        }),
        DataType::U128 => u128::try_from(value).map(Value::U128).map_err(|_| {
            Error::InvalidValue(format!("Cannot convert {} to HUGEINT UNSIGNED", value))
        }),
        _ => Err(Error::TypeMismatch {
            expected: "unsigned integer type".into(),
            found: target.to_string(),
        }),
    }
}

/// Coerce unsigned integer to signed integer (widening where safe)
pub fn coerce_unsigned_to_signed(value: u128, target: &DataType) -> Result<Value> {
    match target {
        DataType::I8 => i8::try_from(value)
            .map(Value::I8)
            .map_err(|_| Error::TypeMismatch {
                expected: "TINYINT".into(),
                found: format!("unsigned value {}", value),
            }),
        DataType::I16 => i16::try_from(value)
            .map(Value::I16)
            .map_err(|_| Error::TypeMismatch {
                expected: "SMALLINT".into(),
                found: format!("unsigned value {}", value),
            }),
        DataType::I32 => i32::try_from(value)
            .map(Value::I32)
            .map_err(|_| Error::TypeMismatch {
                expected: "INT".into(),
                found: format!("unsigned value {}", value),
            }),
        DataType::I64 => i64::try_from(value)
            .map(Value::I64)
            .map_err(|_| Error::TypeMismatch {
                expected: "BIGINT".into(),
                found: format!("unsigned value {}", value),
            }),
        DataType::I128 => i128::try_from(value)
            .map(Value::I128)
            .map_err(|_| Error::TypeMismatch {
                expected: "HUGEINT".into(),
                found: format!("unsigned value {}", value),
            }),
        _ => Err(Error::TypeMismatch {
            expected: "signed integer type".into(),
            found: target.to_string(),
        }),
    }
}

/// Coerce unsigned integer to another unsigned integer
pub fn coerce_unsigned_widen(value: u128, target: &DataType) -> Result<Value> {
    match target {
        DataType::U8 => u8::try_from(value)
            .map(Value::U8)
            .map_err(|_| Error::TypeMismatch {
                expected: "TINYINT UNSIGNED".into(),
                found: format!("value {}", value),
            }),
        DataType::U16 => u16::try_from(value)
            .map(Value::U16)
            .map_err(|_| Error::TypeMismatch {
                expected: "SMALLINT UNSIGNED".into(),
                found: format!("value {}", value),
            }),
        DataType::U32 => u32::try_from(value)
            .map(Value::U32)
            .map_err(|_| Error::TypeMismatch {
                expected: "INT UNSIGNED".into(),
                found: format!("value {}", value),
            }),
        DataType::U64 => u64::try_from(value)
            .map(Value::U64)
            .map_err(|_| Error::TypeMismatch {
                expected: "BIGINT UNSIGNED".into(),
                found: format!("value {}", value),
            }),
        DataType::U128 => Ok(Value::U128(value)),
        _ => Err(Error::TypeMismatch {
            expected: "unsigned integer type".into(),
            found: target.to_string(),
        }),
    }
}

/// Coerce integer to float
pub fn coerce_int_to_float(value: i128, target: &DataType) -> Result<Value> {
    match target {
        DataType::F32 => Ok(Value::F32(value as f32)),
        DataType::F64 => Ok(Value::F64(value as f64)),
        _ => Err(Error::TypeMismatch {
            expected: "float type".into(),
            found: target.to_string(),
        }),
    }
}

/// Coerce float to integer (truncating)
pub fn coerce_float_to_int(value: f64, target: &DataType) -> Result<Value> {
    let truncated = value.trunc();

    match target {
        DataType::U8 => {
            if truncated >= 0.0 && truncated <= u8::MAX as f64 {
                Ok(Value::U8(truncated as u8))
            } else {
                Err(Error::ExecutionError(format!(
                    "Float {} out of range for TINYINT UNSIGNED",
                    value
                )))
            }
        }
        DataType::U16 => {
            if truncated >= 0.0 && truncated <= u16::MAX as f64 {
                Ok(Value::U16(truncated as u16))
            } else {
                Err(Error::ExecutionError(format!(
                    "Float {} out of range for SMALLINT UNSIGNED",
                    value
                )))
            }
        }
        DataType::U32 => {
            if truncated >= 0.0 && truncated <= u32::MAX as f64 {
                Ok(Value::U32(truncated as u32))
            } else {
                Err(Error::ExecutionError(format!(
                    "Float {} out of range for INT UNSIGNED",
                    value
                )))
            }
        }
        DataType::U64 => {
            if truncated >= 0.0 && truncated <= u64::MAX as f64 {
                Ok(Value::U64(truncated as u64))
            } else {
                Err(Error::ExecutionError(format!(
                    "Float {} out of range for BIGINT UNSIGNED",
                    value
                )))
            }
        }
        DataType::U128 => {
            if truncated >= 0.0 {
                Ok(Value::U128(truncated as u128))
            } else {
                Err(Error::ExecutionError(format!(
                    "Float {} out of range for HUGEINT UNSIGNED",
                    value
                )))
            }
        }
        DataType::I8 => {
            if truncated >= i8::MIN as f64 && truncated <= i8::MAX as f64 {
                Ok(Value::I8(truncated as i8))
            } else {
                Err(Error::ExecutionError(format!(
                    "Float {} out of range for TINYINT",
                    value
                )))
            }
        }
        DataType::I16 => {
            if truncated >= i16::MIN as f64 && truncated <= i16::MAX as f64 {
                Ok(Value::I16(truncated as i16))
            } else {
                Err(Error::ExecutionError(format!(
                    "Float {} out of range for SMALLINT",
                    value
                )))
            }
        }
        DataType::I32 => Ok(Value::I32(truncated as i32)),
        DataType::I64 => Ok(Value::I64(truncated as i64)),
        DataType::I128 => Ok(Value::I128(truncated as i128)),
        _ => Err(Error::TypeMismatch {
            expected: "integer type".into(),
            found: target.to_string(),
        }),
    }
}

/// Coerce decimal to integer
pub fn coerce_decimal_to_int(decimal: &rust_decimal::Decimal, target: &DataType) -> Result<Value> {
    use rust_decimal::prelude::ToPrimitive;

    if !decimal.fract().is_zero() {
        return Err(Error::InvalidValue(format!(
            "Cannot convert decimal {} with fractional part to integer",
            decimal
        )));
    }

    match target {
        DataType::U8 => decimal.to_u8().map(Value::U8).ok_or_else(|| {
            Error::InvalidValue(format!("Cannot convert {} to TINYINT UNSIGNED", decimal))
        }),
        DataType::U16 => decimal.to_u16().map(Value::U16).ok_or_else(|| {
            Error::InvalidValue(format!("Cannot convert {} to SMALLINT UNSIGNED", decimal))
        }),
        DataType::U32 => decimal.to_u32().map(Value::U32).ok_or_else(|| {
            Error::InvalidValue(format!("Cannot convert {} to INT UNSIGNED", decimal))
        }),
        DataType::U64 => decimal.to_u64().map(Value::U64).ok_or_else(|| {
            Error::InvalidValue(format!("Cannot convert {} to BIGINT UNSIGNED", decimal))
        }),
        DataType::U128 => decimal.to_u128().map(Value::U128).ok_or_else(|| {
            Error::InvalidValue(format!("Cannot convert {} to HUGEINT UNSIGNED", decimal))
        }),
        DataType::I8 => decimal
            .to_i8()
            .map(Value::I8)
            .ok_or_else(|| Error::InvalidValue(format!("Cannot convert {} to TINYINT", decimal))),
        DataType::I16 => decimal
            .to_i16()
            .map(Value::I16)
            .ok_or_else(|| Error::InvalidValue(format!("Cannot convert {} to SMALLINT", decimal))),
        DataType::I32 => decimal
            .to_i32()
            .map(Value::I32)
            .ok_or_else(|| Error::InvalidValue(format!("Cannot convert {} to INT", decimal))),
        DataType::I64 => decimal
            .to_i64()
            .map(Value::I64)
            .ok_or_else(|| Error::InvalidValue(format!("Cannot convert {} to BIGINT", decimal))),
        DataType::I128 => decimal
            .to_i128()
            .map(Value::I128)
            .ok_or_else(|| Error::InvalidValue(format!("Cannot convert {} to HUGEINT", decimal))),
        _ => Err(Error::TypeMismatch {
            expected: "integer type".into(),
            found: target.to_string(),
        }),
    }
}

/// Coerce integer to decimal
pub fn coerce_int_to_decimal(value: i128) -> Result<Value> {
    Ok(Value::Decimal(rust_decimal::Decimal::from(value)))
}

/// Coerce float to decimal
pub fn coerce_float_to_decimal(value: f64) -> Result<Value> {
    use rust_decimal::prelude::FromPrimitive;
    rust_decimal::Decimal::from_f64(value)
        .ok_or_else(|| Error::InvalidValue(format!("Cannot convert float {} to decimal", value)))
        .map(Value::Decimal)
}

/// Coerce decimal to float
pub fn coerce_decimal_to_float(
    decimal: &rust_decimal::Decimal,
    target: &DataType,
) -> Result<Value> {
    use rust_decimal::prelude::ToPrimitive;
    match target {
        DataType::F32 => decimal.to_f32().map(Value::F32).ok_or_else(|| {
            Error::InvalidValue(format!("Cannot convert decimal {} to float", decimal))
        }),
        DataType::F64 => decimal.to_f64().map(Value::F64).ok_or_else(|| {
            Error::InvalidValue(format!("Cannot convert decimal {} to double", decimal))
        }),
        _ => Err(Error::TypeMismatch {
            expected: "float type".into(),
            found: target.to_string(),
        }),
    }
}

/// Enforce decimal precision and scale
pub fn enforce_decimal_precision(
    decimal: rust_decimal::Decimal,
    precision: Option<u32>,
    scale: Option<u32>,
) -> Result<Value> {
    use rust_decimal::prelude::ToPrimitive;

    if let Some(scale_val) = scale {
        // Round to the specified scale
        let scaled = decimal.round_dp(scale_val);

        if let Some(precision_val) = precision {
            // Check if the value fits within precision
            let whole_digits = precision_val.saturating_sub(scale_val);
            let max_whole = 10_i128.pow(whole_digits);

            if let Some(whole_part) = scaled.trunc().to_i128()
                && whole_part.abs() >= max_whole
            {
                return Err(Error::InvalidValue(format!(
                    "Decimal {} exceeds precision({}, {})",
                    decimal, precision_val, scale_val
                )));
            }
        }

        Ok(Value::Decimal(scaled))
    } else if precision.is_some() {
        // Only precision specified, use default scale of 0
        Ok(Value::Decimal(decimal.round_dp(0)))
    } else {
        // No constraints, return as-is
        Ok(Value::Decimal(decimal))
    }
}
