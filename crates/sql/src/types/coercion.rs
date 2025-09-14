//! Type coercion for SQL values
//! Handles automatic type conversion to match schema requirements

use crate::error::{Error, Result};
use crate::types::{DataType, Value};

/// Coerce a value to match the target data type
/// This handles implicit type conversions that are safe and expected in SQL
pub fn coerce_value(value: Value, target_type: &DataType) -> Result<Value> {
    // If types already match, return as-is
    if value.data_type() == *target_type {
        return Ok(value);
    }

    match (&value, target_type) {
        // NULL can be coerced to any nullable type
        (Value::Null, _) => Ok(Value::Null),

        // Integer type coercions (widening is safe, narrowing checks bounds)
        (Value::I8(v), DataType::I16) => Ok(Value::I16(*v as i16)),
        (Value::I8(v), DataType::I32) => Ok(Value::I32(*v as i32)),
        (Value::I8(v), DataType::I64) => Ok(Value::I64(*v as i64)),
        (Value::I8(v), DataType::I128) => Ok(Value::I128(*v as i128)),

        (Value::I16(v), DataType::I8) => {
            i8::try_from(*v)
                .map(Value::I8)
                .map_err(|_| Error::TypeMismatch {
                    expected: "TINYINT".into(),
                    found: format!("SMALLINT value {}", v),
                })
        }
        (Value::I16(v), DataType::I32) => Ok(Value::I32(*v as i32)),
        (Value::I16(v), DataType::I64) => Ok(Value::I64(*v as i64)),
        (Value::I16(v), DataType::I128) => Ok(Value::I128(*v as i128)),

        (Value::I32(v), DataType::I8) => {
            i8::try_from(*v)
                .map(Value::I8)
                .map_err(|_| Error::TypeMismatch {
                    expected: "TINYINT".into(),
                    found: format!("INT value {}", v),
                })
        }
        (Value::I32(v), DataType::I16) => {
            i16::try_from(*v)
                .map(Value::I16)
                .map_err(|_| Error::TypeMismatch {
                    expected: "SMALLINT".into(),
                    found: format!("INT value {}", v),
                })
        }
        (Value::I32(v), DataType::I64) => Ok(Value::I64(*v as i64)),
        (Value::I32(v), DataType::I128) => Ok(Value::I128(*v as i128)),

        (Value::I64(v), DataType::I8) => {
            i8::try_from(*v)
                .map(Value::I8)
                .map_err(|_| Error::TypeMismatch {
                    expected: "TINYINT".into(),
                    found: format!("BIGINT value {}", v),
                })
        }
        (Value::I64(v), DataType::I16) => {
            i16::try_from(*v)
                .map(Value::I16)
                .map_err(|_| Error::TypeMismatch {
                    expected: "SMALLINT".into(),
                    found: format!("BIGINT value {}", v),
                })
        }
        (Value::I64(v), DataType::I32) => {
            i32::try_from(*v)
                .map(Value::I32)
                .map_err(|_| Error::TypeMismatch {
                    expected: "INT".into(),
                    found: format!("BIGINT value {}", v),
                })
        }
        (Value::I64(v), DataType::I128) => Ok(Value::I128(*v as i128)),

        (Value::I128(v), DataType::I8) => {
            i8::try_from(*v)
                .map(Value::I8)
                .map_err(|_| Error::TypeMismatch {
                    expected: "TINYINT".into(),
                    found: format!("HUGEINT value {}", v),
                })
        }
        (Value::I128(v), DataType::I16) => {
            i16::try_from(*v)
                .map(Value::I16)
                .map_err(|_| Error::TypeMismatch {
                    expected: "SMALLINT".into(),
                    found: format!("HUGEINT value {}", v),
                })
        }
        (Value::I128(v), DataType::I32) => {
            i32::try_from(*v)
                .map(Value::I32)
                .map_err(|_| Error::TypeMismatch {
                    expected: "INT".into(),
                    found: format!("HUGEINT value {}", v),
                })
        }
        (Value::I128(v), DataType::I64) => {
            i64::try_from(*v)
                .map(Value::I64)
                .map_err(|_| Error::TypeMismatch {
                    expected: "BIGINT".into(),
                    found: format!("HUGEINT value {}", v),
                })
        }

        // Unsigned integer coercions
        (Value::U8(v), DataType::U16) => Ok(Value::U16(*v as u16)),
        (Value::U8(v), DataType::U32) => Ok(Value::U32(*v as u32)),
        (Value::U8(v), DataType::U64) => Ok(Value::U64(*v as u64)),
        (Value::U8(v), DataType::U128) => Ok(Value::U128(*v as u128)),
        (Value::U8(v), DataType::I16) => Ok(Value::I16(*v as i16)),
        (Value::U8(v), DataType::I32) => Ok(Value::I32(*v as i32)),
        (Value::U8(v), DataType::I64) => Ok(Value::I64(*v as i64)),
        (Value::U8(v), DataType::I128) => Ok(Value::I128(*v as i128)),

        // Float to float coercions
        (Value::F32(v), DataType::F64) => Ok(Value::F64(*v as f64)),
        (Value::F64(v), DataType::F32) => Ok(Value::F32(*v as f32)),

        // Integer to float coercions (always safe but may lose precision)
        (Value::I8(v), DataType::F32) => Ok(Value::F32(*v as f32)),
        (Value::I8(v), DataType::F64) => Ok(Value::F64(*v as f64)),
        (Value::I16(v), DataType::F32) => Ok(Value::F32(*v as f32)),
        (Value::I16(v), DataType::F64) => Ok(Value::F64(*v as f64)),
        (Value::I32(v), DataType::F32) => Ok(Value::F32(*v as f32)),
        (Value::I32(v), DataType::F64) => Ok(Value::F64(*v as f64)),
        (Value::I64(v), DataType::F64) => Ok(Value::F64(*v as f64)),

        // Decimal to float coercions
        (Value::Decimal(d), DataType::F32) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_f32().map(Value::F32).ok_or_else(|| {
                Error::InvalidValue(format!("Cannot convert decimal {} to float", d))
            })
        }
        (Value::Decimal(d), DataType::F64) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_f64().map(Value::F64).ok_or_else(|| {
                Error::InvalidValue(format!("Cannot convert decimal {} to double", d))
            })
        }

        // Decimal to Decimal (with potentially different precision/scale)
        (Value::Decimal(_), DataType::Decimal(_, _)) => {
            // For now, just pass through the decimal value
            // TODO: In the future, we may want to check and enforce precision/scale
            Ok(value)
        }

        // Integer to Decimal coercions
        (Value::I8(v), DataType::Decimal(_, _)) => {
            Ok(Value::Decimal(rust_decimal::Decimal::from(*v)))
        }
        (Value::I16(v), DataType::Decimal(_, _)) => {
            Ok(Value::Decimal(rust_decimal::Decimal::from(*v)))
        }
        (Value::I32(v), DataType::Decimal(_, _)) => {
            Ok(Value::Decimal(rust_decimal::Decimal::from(*v)))
        }
        (Value::I64(v), DataType::Decimal(_, _)) => {
            Ok(Value::Decimal(rust_decimal::Decimal::from(*v)))
        }
        (Value::I128(v), DataType::Decimal(_, _)) => {
            Ok(Value::Decimal(rust_decimal::Decimal::from(*v)))
        }

        // String coercions (only between string types)
        (Value::Str(s), DataType::Text) => Ok(Value::Str(s.clone())),

        // Boolean remains strict - no implicit coercion
        (Value::Bool(_), DataType::Bool) => Ok(value),

        // No coercion possible
        _ => Err(Error::TypeMismatch {
            expected: target_type.to_string(),
            found: value.data_type().to_string(),
        }),
    }
}

/// Coerce a row of values to match a table schema
pub fn coerce_row(row: Vec<Value>, schema: &crate::types::schema::Table) -> Result<Vec<Value>> {
    if row.len() != schema.columns.len() {
        return Err(Error::ExecutionError(format!(
            "Row has {} values but table has {} columns",
            row.len(),
            schema.columns.len()
        )));
    }

    row.into_iter()
        .zip(&schema.columns)
        .map(|(value, column)| coerce_value(value, &column.datatype))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_widening() {
        // I64 to I128 (widening - always safe)
        let value = Value::I64(1000000000000);
        let result = coerce_value(value, &DataType::I128).unwrap();
        assert_eq!(result, Value::I128(1000000000000));

        // I32 to I64 (widening - always safe)
        let value = Value::I32(42);
        let result = coerce_value(value, &DataType::I64).unwrap();
        assert_eq!(result, Value::I64(42));
    }

    #[test]
    fn test_integer_narrowing() {
        // I64 to I32 (narrowing - may fail)
        let value = Value::I64(42);
        let result = coerce_value(value, &DataType::I32).unwrap();
        assert_eq!(result, Value::I32(42));

        // I64 to I32 with overflow should fail
        let value = Value::I64(i64::MAX);
        let result = coerce_value(value, &DataType::I32);
        assert!(result.is_err());
    }

    #[test]
    fn test_null_coercion() {
        // NULL can be coerced to any type
        let value = Value::Null;
        let result = coerce_value(value.clone(), &DataType::I128).unwrap();
        assert_eq!(result, Value::Null);

        let result = coerce_value(value.clone(), &DataType::Str).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_integer_to_decimal() {
        let value = Value::I64(42);
        let result = coerce_value(value, &DataType::Decimal(Some(10), Some(2))).unwrap();
        match result {
            Value::Decimal(d) => assert_eq!(d.to_string(), "42"),
            _ => panic!("Expected Decimal"),
        }
    }
}
