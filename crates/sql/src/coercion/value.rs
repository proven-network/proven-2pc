//! Value coercion implementation

use crate::error::{Error, Result};
use crate::types::{DataType, Value, ValueExt};
use std::collections::HashMap;

/// Coerce a value to match the target data type
/// This handles implicit type conversions that are safe and expected in SQL
pub fn coerce_value_impl(value: Value, target_type: &DataType) -> Result<Value> {
    if !matches!(value, Value::Struct(_)) && value.data_type() == *target_type {
        return Ok(value);
    }

    // Special handling for Map values that need element coercion
    if let (Value::Map(map), DataType::Map(_key_type, value_type)) = (&value, target_type) {
        // Empty maps can be coerced to any map type
        if map.is_empty() {
            return Ok(Value::Map(HashMap::new()));
        }

        let mut coerced_map = HashMap::new();
        for (key, val) in map.clone() {
            // Coerce the value to the expected type
            let coerced_val = coerce_value_impl(val, value_type)?;
            coerced_map.insert(key, coerced_val);
        }
        return Ok(Value::Map(coerced_map));
    }

    match (&value, target_type) {
        // NULL can be coerced to any nullable type
        (Value::Null, _) => Ok(Value::Null),

        // Non-null value to Nullable type - unwrap the Nullable and try to coerce
        (_, DataType::Nullable(inner_type)) if !matches!(value, Value::Null) => {
            coerce_value_impl(value, inner_type)
        }

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

        // Signed to unsigned coercions (with bounds checking)
        (Value::I8(v), DataType::U8) => u8::try_from(*v)
            .map(Value::U8)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to TINYINT UNSIGNED", v))),
        (Value::I16(v), DataType::U8) => u8::try_from(*v)
            .map(Value::U8)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to TINYINT UNSIGNED", v))),
        (Value::I32(v), DataType::U8) => u8::try_from(*v)
            .map(Value::U8)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to TINYINT UNSIGNED", v))),
        (Value::I64(v), DataType::U8) => u8::try_from(*v)
            .map(Value::U8)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to TINYINT UNSIGNED", v))),

        (Value::I8(v), DataType::U16) => u16::try_from(*v)
            .map(Value::U16)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to SMALLINT UNSIGNED", v))),
        (Value::I16(v), DataType::U16) => u16::try_from(*v)
            .map(Value::U16)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to SMALLINT UNSIGNED", v))),
        (Value::I32(v), DataType::U16) => u16::try_from(*v)
            .map(Value::U16)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to SMALLINT UNSIGNED", v))),
        (Value::I64(v), DataType::U16) => u16::try_from(*v)
            .map(Value::U16)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to SMALLINT UNSIGNED", v))),

        (Value::I8(v), DataType::U32) => u32::try_from(*v)
            .map(Value::U32)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to INT UNSIGNED", v))),
        (Value::I16(v), DataType::U32) => u32::try_from(*v)
            .map(Value::U32)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to INT UNSIGNED", v))),
        (Value::I32(v), DataType::U32) => u32::try_from(*v)
            .map(Value::U32)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to INT UNSIGNED", v))),
        (Value::I64(v), DataType::U32) => u32::try_from(*v)
            .map(Value::U32)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to INT UNSIGNED", v))),

        (Value::I8(v), DataType::U64) => u64::try_from(*v)
            .map(Value::U64)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to BIGINT UNSIGNED", v))),
        (Value::I16(v), DataType::U64) => u64::try_from(*v)
            .map(Value::U64)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to BIGINT UNSIGNED", v))),
        (Value::I32(v), DataType::U64) => u64::try_from(*v)
            .map(Value::U64)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to BIGINT UNSIGNED", v))),
        (Value::I64(v), DataType::U64) => u64::try_from(*v)
            .map(Value::U64)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to BIGINT UNSIGNED", v))),
        (Value::I128(v), DataType::U8) => u8::try_from(*v)
            .map(Value::U8)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to TINYINT UNSIGNED", v))),
        (Value::I128(v), DataType::U16) => u16::try_from(*v)
            .map(Value::U16)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to SMALLINT UNSIGNED", v))),
        (Value::I128(v), DataType::U32) => u32::try_from(*v)
            .map(Value::U32)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to INT UNSIGNED", v))),
        (Value::I128(v), DataType::U64) => u64::try_from(*v)
            .map(Value::U64)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to BIGINT UNSIGNED", v))),

        (Value::I8(v), DataType::U128) => u128::try_from(*v)
            .map(Value::U128)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to HUGEINT UNSIGNED", v))),
        (Value::I16(v), DataType::U128) => u128::try_from(*v)
            .map(Value::U128)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to HUGEINT UNSIGNED", v))),
        (Value::I32(v), DataType::U128) => u128::try_from(*v)
            .map(Value::U128)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to HUGEINT UNSIGNED", v))),
        (Value::I64(v), DataType::U128) => u128::try_from(*v)
            .map(Value::U128)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to HUGEINT UNSIGNED", v))),
        (Value::I128(v), DataType::U128) => u128::try_from(*v)
            .map(Value::U128)
            .map_err(|_| Error::InvalidValue(format!("Cannot convert {} to HUGEINT UNSIGNED", v))),

        // Float to float coercions
        (Value::F32(v), DataType::F64) => Ok(Value::F64(*v as f64)),
        (Value::F64(v), DataType::F32) => Ok(Value::F32(*v as f32)),

        // Float to unsigned integer coercions (for large u128 values stored as float)
        (Value::F64(v), DataType::U128) if *v >= 0.0 && v.fract() == 0.0 => {
            // Note: We can't reliably check if v <= u128::MAX because of float precision
            // but the cast will saturate at u128::MAX if needed
            Ok(Value::U128(*v as u128))
        }
        (Value::F64(v), DataType::U64)
            if *v >= 0.0 && *v <= u64::MAX as f64 && v.fract() == 0.0 =>
        {
            Ok(Value::U64(*v as u64))
        }
        (Value::F64(v), DataType::U32)
            if *v >= 0.0 && *v <= u32::MAX as f64 && v.fract() == 0.0 =>
        {
            Ok(Value::U32(*v as u32))
        }
        (Value::F64(v), DataType::U16)
            if *v >= 0.0 && *v <= u16::MAX as f64 && v.fract() == 0.0 =>
        {
            Ok(Value::U16(*v as u16))
        }
        (Value::F64(v), DataType::U8) if *v >= 0.0 && *v <= u8::MAX as f64 && v.fract() == 0.0 => {
            Ok(Value::U8(*v as u8))
        }

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

        // Float to Decimal coercions
        (Value::F32(v), DataType::Decimal(_, _)) => {
            use rust_decimal::prelude::FromPrimitive;
            rust_decimal::Decimal::from_f32(*v)
                .ok_or_else(|| {
                    Error::InvalidValue(format!("Cannot convert float {} to decimal", v))
                })
                .map(Value::Decimal)
        }
        (Value::F64(v), DataType::Decimal(_, _)) => {
            use rust_decimal::prelude::FromPrimitive;
            rust_decimal::Decimal::from_f64(*v)
                .ok_or_else(|| {
                    Error::InvalidValue(format!("Cannot convert double {} to decimal", v))
                })
                .map(Value::Decimal)
        }

        // Decimal to signed integer coercions
        (Value::Decimal(d), DataType::I8) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_i8()
                .filter(|_| d.fract().is_zero())
                .map(Value::I8)
                .ok_or_else(|| Error::InvalidValue(format!("Cannot convert {} to TINYINT", d)))
        }
        (Value::Decimal(d), DataType::I16) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_i16()
                .filter(|_| d.fract().is_zero())
                .map(Value::I16)
                .ok_or_else(|| Error::InvalidValue(format!("Cannot convert {} to SMALLINT", d)))
        }
        (Value::Decimal(d), DataType::I32) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_i32()
                .filter(|_| d.fract().is_zero())
                .map(Value::I32)
                .ok_or_else(|| Error::InvalidValue(format!("Cannot convert {} to INT", d)))
        }
        (Value::Decimal(d), DataType::I64) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_i64()
                .filter(|_| d.fract().is_zero())
                .map(Value::I64)
                .ok_or_else(|| Error::InvalidValue(format!("Cannot convert {} to BIGINT", d)))
        }
        (Value::Decimal(d), DataType::I128) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_i128()
                .filter(|_| d.fract().is_zero())
                .map(Value::I128)
                .ok_or_else(|| Error::InvalidValue(format!("Cannot convert {} to HUGEINT", d)))
        }

        // Decimal to unsigned integer coercions
        (Value::Decimal(d), DataType::U8) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_u8()
                .filter(|_| d.fract().is_zero())
                .map(Value::U8)
                .ok_or_else(|| {
                    Error::InvalidValue(format!("Cannot convert {} to TINYINT UNSIGNED", d))
                })
        }
        (Value::Decimal(d), DataType::U16) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_u16()
                .filter(|_| d.fract().is_zero())
                .map(Value::U16)
                .ok_or_else(|| {
                    Error::InvalidValue(format!("Cannot convert {} to SMALLINT UNSIGNED", d))
                })
        }
        (Value::Decimal(d), DataType::U32) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_u32()
                .filter(|_| d.fract().is_zero())
                .map(Value::U32)
                .ok_or_else(|| Error::InvalidValue(format!("Cannot convert {} to INT UNSIGNED", d)))
        }
        (Value::Decimal(d), DataType::U64) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_u64()
                .filter(|_| d.fract().is_zero())
                .map(Value::U64)
                .ok_or_else(|| {
                    Error::InvalidValue(format!("Cannot convert {} to BIGINT UNSIGNED", d))
                })
        }
        (Value::Decimal(d), DataType::U128) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_u128()
                .filter(|_| d.fract().is_zero())
                .map(Value::U128)
                .ok_or_else(|| {
                    Error::InvalidValue(format!("Cannot convert {} to HUGEINT UNSIGNED", d))
                })
        }

        // String coercions (only between string types)
        (Value::Str(s), DataType::Text) => Ok(Value::Str(s.clone())),

        // UUID coercions
        (Value::Str(s), DataType::Uuid) => {
            use uuid::Uuid;
            // Try to parse the UUID string (supports standard, URN, and hex formats)
            Uuid::parse_str(s)
                .map(Value::Uuid)
                .map_err(|_| Error::InvalidValue(format!("Failed to parse UUID: {}", s)))
        }
        (Value::Uuid(_), DataType::Uuid) => Ok(value),

        // INET coercions
        (Value::Str(s), DataType::Inet) => {
            use std::net::IpAddr;
            // Try to parse the IP address string
            s.parse::<IpAddr>()
                .map(Value::Inet)
                .map_err(|_| Error::InvalidValue(format!("Failed to parse IP address: {}", s)))
        }

        // Integer to INET conversions
        (Value::I8(v), DataType::Inet) => {
            use std::net::{IpAddr, Ipv4Addr};
            if *v >= 0 {
                Ok(Value::Inet(IpAddr::V4(Ipv4Addr::from(*v as u32))))
            } else {
                Err(Error::InvalidValue(format!(
                    "Cannot convert negative value {} to INET",
                    v
                )))
            }
        }
        (Value::I16(v), DataType::Inet) => {
            use std::net::{IpAddr, Ipv4Addr};
            if *v >= 0 {
                Ok(Value::Inet(IpAddr::V4(Ipv4Addr::from(*v as u32))))
            } else {
                Err(Error::InvalidValue(format!(
                    "Cannot convert negative value {} to INET",
                    v
                )))
            }
        }
        (Value::I32(v), DataType::Inet) => {
            use std::net::{IpAddr, Ipv4Addr};
            if *v >= 0 {
                Ok(Value::Inet(IpAddr::V4(Ipv4Addr::from(*v as u32))))
            } else {
                Err(Error::InvalidValue(format!(
                    "Cannot convert negative value {} to INET",
                    v
                )))
            }
        }
        (Value::I64(v), DataType::Inet) => {
            use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
            // Convert integer to IP address
            // For values in u32 range, convert to IPv4
            // For larger values, convert to IPv6
            if *v >= 0 && *v <= u32::MAX as i64 {
                let ipv4 = Ipv4Addr::from(*v as u32);
                Ok(Value::Inet(IpAddr::V4(ipv4)))
            } else if *v >= 0 {
                let ipv6 = Ipv6Addr::from(*v as u128);
                Ok(Value::Inet(IpAddr::V6(ipv6)))
            } else {
                Err(Error::InvalidValue(format!(
                    "Cannot convert negative value {} to INET",
                    v
                )))
            }
        }
        (Value::I128(v), DataType::Inet) => {
            use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
            if *v >= 0 && *v <= u32::MAX as i128 {
                let ipv4 = Ipv4Addr::from(*v as u32);
                Ok(Value::Inet(IpAddr::V4(ipv4)))
            } else if *v >= 0 {
                let ipv6 = Ipv6Addr::from(*v as u128);
                Ok(Value::Inet(IpAddr::V6(ipv6)))
            } else {
                Err(Error::InvalidValue(format!(
                    "Cannot convert negative value {} to INET",
                    v
                )))
            }
        }
        (Value::U8(v), DataType::Inet) => {
            use std::net::{IpAddr, Ipv4Addr};
            Ok(Value::Inet(IpAddr::V4(Ipv4Addr::from(*v as u32))))
        }
        (Value::U16(v), DataType::Inet) => {
            use std::net::{IpAddr, Ipv4Addr};
            Ok(Value::Inet(IpAddr::V4(Ipv4Addr::from(*v as u32))))
        }
        (Value::U32(v), DataType::Inet) => {
            use std::net::{IpAddr, Ipv4Addr};
            Ok(Value::Inet(IpAddr::V4(Ipv4Addr::from(*v))))
        }
        (Value::U64(v), DataType::Inet) => {
            use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
            if *v <= u32::MAX as u64 {
                Ok(Value::Inet(IpAddr::V4(Ipv4Addr::from(*v as u32))))
            } else {
                Ok(Value::Inet(IpAddr::V6(Ipv6Addr::from(*v as u128))))
            }
        }
        (Value::U128(v), DataType::Inet) => {
            use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
            if *v <= u32::MAX as u128 {
                Ok(Value::Inet(IpAddr::V4(Ipv4Addr::from(*v as u32))))
            } else {
                Ok(Value::Inet(IpAddr::V6(Ipv6Addr::from(*v))))
            }
        }
        (Value::Inet(_), DataType::Inet) => Ok(value),

        // POINT coercions
        (Value::Str(s), DataType::Point) => {
            // Parse POINT string format: "POINT(x y)" or "POINT(x, y)"
            let s = s.trim();
            if !s.to_uppercase().starts_with("POINT(") || !s.ends_with(')') {
                return Err(Error::InvalidValue(format!("Failed to parse POINT: {}", s)));
            }

            // Extract coordinates from "POINT(x y)" or "POINT(x, y)"
            let coords = &s[6..s.len() - 1]; // Remove "POINT(" and ")"
            let parts: Vec<&str> = coords
                .split(|c: char| c == ',' || c.is_whitespace())
                .filter(|s| !s.is_empty())
                .collect();

            if parts.len() != 2 {
                return Err(Error::InvalidValue(format!(
                    "Failed to parse POINT: expected 2 coordinates, found {}",
                    parts.len()
                )));
            }

            let x = parts[0].parse::<f64>().map_err(|_| {
                Error::InvalidValue(format!("Failed to parse POINT x coordinate: {}", parts[0]))
            })?;
            let y = parts[1].parse::<f64>().map_err(|_| {
                Error::InvalidValue(format!("Failed to parse POINT y coordinate: {}", parts[1]))
            })?;

            Ok(Value::Point(crate::types::Point::new(x, y)))
        }
        (Value::Point(_), DataType::Point) => Ok(value),

        // Boolean remains strict - no implicit coercion
        (Value::Bool(_), DataType::Bool) => Ok(value),

        // Integer literals to UUID are not allowed
        (
            Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_) | Value::I128(_),
            DataType::Uuid,
        ) => Err(Error::InvalidValue(format!(
            "Incompatible literal for UUID: {}",
            value.data_type()
        ))),

        // String to Date conversion
        (Value::Str(s), DataType::Date) => {
            use chrono::NaiveDate;
            NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(Value::Date)
                .map_err(|_| Error::TypeMismatch {
                    expected: "DATE".into(),
                    found: format!("Invalid date string: '{}'", s),
                })
        }

        // String to Time conversion
        (Value::Str(s), DataType::Time) => {
            use chrono::NaiveTime;
            // Try multiple time formats
            NaiveTime::parse_from_str(s, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
                .map(Value::Time)
                .map_err(|_| Error::TypeMismatch {
                    expected: "TIME".into(),
                    found: format!("Invalid time string: '{}'", s),
                })
        }

        // String to collection type conversions (JSON parsing)
        (Value::Str(s), DataType::List(elem_type)) => {
            // Parse JSON array string to List
            match Value::parse_json_array(s) {
                Ok(Value::List(items)) => {
                    // Special handling: if elem_type is I64 (the default) and coercion fails,
                    // just use the values as-is (type inference from data)
                    if **elem_type == DataType::I64 && !items.is_empty() {
                        // Try to coerce to I64 first
                        let mut coerced_items = Vec::new();
                        let mut all_coercible = true;
                        for item in &items {
                            match coerce_value_impl(item.clone(), elem_type) {
                                Ok(coerced) => coerced_items.push(coerced),
                                Err(_) => {
                                    all_coercible = false;
                                    break;
                                }
                            }
                        }

                        if all_coercible {
                            Ok(Value::List(coerced_items))
                        } else {
                            // Can't coerce to I64, use values as-is (inferred type)
                            Ok(Value::List(items))
                        }
                    } else {
                        // Not the default type, strict coercion
                        let mut coerced_items = Vec::new();
                        for item in items {
                            coerced_items.push(coerce_value_impl(item, elem_type)?);
                        }
                        Ok(Value::List(coerced_items))
                    }
                }
                Ok(_) => Err(Error::TypeMismatch {
                    expected: "LIST".into(),
                    found: format!("Invalid JSON array: '{}'", s),
                }),
                Err(e) => Err(Error::InvalidValue(e)),
            }
        }
        (Value::Str(s), DataType::Array(elem_type, size)) => {
            // Parse JSON array string to Array
            match Value::parse_json_array(s) {
                Ok(Value::List(items)) => {
                    // Check size constraint if specified
                    if let Some(expected_size) = size
                        && items.len() != *expected_size
                    {
                        return Err(Error::ArraySizeMismatch {
                            expected: *expected_size,
                            found: items.len(),
                        });
                    }
                    // Coerce each element to the expected type
                    let mut coerced_items = Vec::new();
                    for item in items {
                        coerced_items.push(coerce_value_impl(item, elem_type)?);
                    }
                    Ok(Value::Array(coerced_items))
                }
                Ok(_) => Err(Error::TypeMismatch {
                    expected: "ARRAY".into(),
                    found: format!("Invalid JSON array: '{}'", s),
                }),
                Err(e) => Err(Error::InvalidValue(e)),
            }
        }
        (Value::Str(s), DataType::Map(key_type, value_type)) => {
            // Parse JSON object string to Map
            match Value::parse_json_object(s) {
                Ok(Value::Map(mut map)) => {
                    // Coerce map values to match expected types
                    let mut coerced_map = HashMap::new();
                    for (key, val) in map.drain() {
                        // Coerce the value to the expected type
                        let coerced_val = coerce_value_impl(val, value_type)?;
                        coerced_map.insert(key, coerced_val);
                    }
                    Ok(Value::Map(coerced_map))
                }
                Ok(_) => Err(Error::TypeMismatch {
                    expected: format!("MAP({:?}, {:?})", key_type, value_type),
                    found: format!("Invalid JSON object: '{}'", s),
                }),
                Err(e) => Err(Error::InvalidValue(e)),
            }
        }
        (Value::Str(s), DataType::Struct(schema_fields)) => {
            // Parse JSON object string to Struct
            match Value::parse_json_object(s) {
                Ok(Value::Map(mut map)) => {
                    // Convert Map to Struct with type coercion based on schema
                    let mut fields = Vec::new();
                    for (field_name, field_type) in schema_fields {
                        if let Some(val) = map.remove(field_name) {
                            // Coerce the value to the expected field type
                            let coerced_val =
                                coerce_value_impl(val.clone(), field_type).map_err(|e| {
                                    // If coercion fails, provide more specific error for struct fields
                                    match e {
                                        Error::TypeMismatch { expected, found } => {
                                            Error::StructFieldTypeMismatch {
                                                field: field_name.clone(),
                                                expected,
                                                found,
                                            }
                                        }
                                        Error::ExecutionError(msg) => {
                                            // Convert ExecutionError to StructFieldTypeMismatch
                                            Error::StructFieldTypeMismatch {
                                                field: field_name.clone(),
                                                expected: field_type.to_string(),
                                                found: format!("conversion failed: {}", msg),
                                            }
                                        }
                                        _ => e,
                                    }
                                })?;
                            fields.push((field_name.clone(), coerced_val));
                        } else {
                            return Err(Error::StructFieldMissing(field_name.clone()));
                        }
                    }
                    // Check for extra fields
                    if !map.is_empty() {
                        let extra_fields: Vec<String> = map.keys().cloned().collect();
                        return Err(Error::TypeMismatch {
                            expected: "STRUCT".into(),
                            found: format!("extra fields: {:?}", extra_fields),
                        });
                    }
                    Ok(Value::Struct(fields))
                }
                Ok(_) => Err(Error::TypeMismatch {
                    expected: "STRUCT".into(),
                    found: format!("Invalid JSON object: '{}'", s),
                }),
                Err(e) => Err(Error::InvalidValue(e)),
            }
        }

        // Allow Struct to be coerced to Struct (for field type coercion)
        (Value::Struct(fields), DataType::Struct(schema_fields)) => {
            // First check if the struct is already compatible (considering NULLs)
            if fields.len() == schema_fields.len() {
                let mut all_match = true;
                for ((field_name, _field_val), (schema_name, _schema_type)) in
                    fields.iter().zip(schema_fields.iter())
                {
                    if field_name != schema_name {
                        all_match = false;
                        break;
                    }
                    // NULL values are compatible with any type - don't check their types
                    // Non-NULL values will be coerced below if needed
                }
                if all_match {
                    // Struct fields match by name, return as-is
                    // NULL values are acceptable in any field
                    return Ok(Value::Struct(fields.clone()));
                }
            }

            // Coerce each field to match the schema
            let mut coerced_fields = Vec::new();
            for (field_name, field_type) in schema_fields {
                // Find the corresponding field in the struct
                let field_value = fields
                    .iter()
                    .find(|(name, _)| name == field_name)
                    .map(|(_, val)| val.clone())
                    .ok_or_else(|| Error::StructFieldMissing(field_name.clone()))?;

                // Coerce the field value to the expected type
                let coerced_val = coerce_value_impl(field_value, field_type).map_err(|e| {
                    // If coercion fails, provide more specific error for struct fields
                    if let Error::TypeMismatch { expected, found } = e {
                        Error::StructFieldTypeMismatch {
                            field: field_name.clone(),
                            expected,
                            found,
                        }
                    } else {
                        e
                    }
                })?;
                coerced_fields.push((field_name.clone(), coerced_val));
            }

            // Check that we don't have extra fields
            if fields.len() != schema_fields.len() {
                return Err(Error::TypeMismatch {
                    expected: "STRUCT".into(),
                    found: format!(
                        "struct with {} fields, expected {}",
                        fields.len(),
                        schema_fields.len()
                    ),
                });
            }

            Ok(Value::Struct(coerced_fields))
        }

        // Allow Map to be coerced to Struct
        (Value::Map(map), DataType::Struct(schema_fields)) => {
            // Convert Map to Struct with type coercion based on schema
            let mut fields = Vec::new();
            let mut used_keys = Vec::new();
            for (field_name, field_type) in schema_fields {
                if let Some(val) = map.get(field_name) {
                    used_keys.push(field_name.clone());
                    // Coerce the value to the expected field type
                    let coerced_val = coerce_value_impl(val.clone(), field_type).map_err(|e| {
                        // If coercion fails, provide more specific error for struct fields
                        if let Error::TypeMismatch { expected, found } = e {
                            Error::StructFieldTypeMismatch {
                                field: field_name.clone(),
                                expected,
                                found,
                            }
                        } else {
                            e
                        }
                    })?;
                    fields.push((field_name.clone(), coerced_val));
                } else {
                    return Err(Error::StructFieldMissing(field_name.clone()));
                }
            }
            // Check for extra fields
            let all_keys: Vec<String> = map.keys().cloned().collect();
            let extra_fields: Vec<String> = all_keys
                .into_iter()
                .filter(|k| !used_keys.contains(k))
                .collect();
            if !extra_fields.is_empty() {
                return Err(Error::TypeMismatch {
                    expected: "STRUCT".into(),
                    found: format!("extra fields: {:?}", extra_fields),
                });
            }
            Ok(Value::Struct(fields))
        }

        // Allow List to be used where Array is expected
        (Value::List(items), DataType::Array(elem_type, size)) => {
            // Check size constraint if specified
            if let Some(expected_size) = size
                && items.len() != *expected_size
            {
                return Err(Error::ArraySizeMismatch {
                    expected: *expected_size,
                    found: items.len(),
                });
            }
            // Coerce each element to the expected type
            let mut coerced_items = Vec::new();
            for item in items {
                coerced_items.push(coerce_value_impl(item.clone(), elem_type)?);
            }
            Ok(Value::Array(coerced_items))
        }

        // Allow Array to be used where List is expected
        (Value::Array(items), DataType::List(elem_type)) => {
            // Coerce each element to the expected type
            let mut coerced_items = Vec::new();
            for item in items {
                coerced_items.push(coerce_value_impl(item.clone(), elem_type)?);
            }
            Ok(Value::List(coerced_items))
        }

        // Allow List to List coercion with element type conversion
        (Value::List(items), DataType::List(elem_type)) => {
            // Coerce each element to the expected type
            let mut coerced_items = Vec::new();
            for item in items {
                coerced_items.push(coerce_value_impl(item.clone(), elem_type)?);
            }
            Ok(Value::List(coerced_items))
        }

        // String to Timestamp conversion
        (Value::Str(s), DataType::Timestamp) => {
            use chrono::{NaiveDate, NaiveDateTime};
            // Try multiple timestamp formats
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
                .or_else(|_| {
                    // Try date-only format (add 00:00:00 time)
                    NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
                })
                .map(Value::Timestamp)
                .map_err(|_| Error::TypeMismatch {
                    expected: "TIMESTAMP".into(),
                    found: format!("Invalid timestamp string: '{}'", s),
                })
        }

        // String to JSON (parse JSON string)
        (Value::Str(s), DataType::Json) => serde_json::from_str(s)
            .map(Value::Json)
            .map_err(|e| Error::InvalidValue(format!("Invalid JSON: {}", e))),

        // JSON to String (serialize JSON to string)
        (Value::Json(j), DataType::Str | DataType::Text) => Ok(Value::Str(j.to_string())),

        // String to signed integer types (parsing)
        (Value::Str(s), DataType::I8) => s
            .parse::<i8>()
            .map(Value::I8)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to TINYINT", s))),
        (Value::Str(s), DataType::I16) => s
            .parse::<i16>()
            .map(Value::I16)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to SMALLINT", s))),
        (Value::Str(s), DataType::I32) => s
            .parse::<i32>()
            .map(Value::I32)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to INT", s))),
        (Value::Str(s), DataType::I64) => s
            .parse::<i64>()
            .map(Value::I64)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to BIGINT", s))),
        (Value::Str(s), DataType::I128) => s
            .parse::<i128>()
            .map(Value::I128)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to HUGEINT", s))),

        // String to unsigned integer types (parsing)
        (Value::Str(s), DataType::U8) => s
            .parse::<u8>()
            .map(Value::U8)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to TINYINT UNSIGNED", s))),
        (Value::Str(s), DataType::U16) => s.parse::<u16>().map(Value::U16).map_err(|_| {
            Error::ExecutionError(format!("Cannot cast '{}' to SMALLINT UNSIGNED", s))
        }),
        (Value::Str(s), DataType::U32) => s
            .parse::<u32>()
            .map(Value::U32)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to INT UNSIGNED", s))),
        (Value::Str(s), DataType::U64) => s
            .parse::<u64>()
            .map(Value::U64)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to BIGINT UNSIGNED", s))),
        (Value::Str(s), DataType::U128) => s
            .parse::<u128>()
            .map(Value::U128)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to HUGEINT UNSIGNED", s))),

        // String to float types (parsing)
        (Value::Str(s), DataType::F32) => s
            .parse::<f32>()
            .map(Value::F32)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to REAL", s))),
        (Value::Str(s), DataType::F64) => s
            .parse::<f64>()
            .map(Value::F64)
            .map_err(|_| Error::ExecutionError(format!("Cannot cast '{}' to DOUBLE PRECISION", s))),

        // String to boolean (parsing)
        (Value::Str(s), DataType::Bool) => match s.to_uppercase().as_str() {
            "TRUE" | "T" | "YES" | "Y" | "1" => Ok(Value::Bool(true)),
            "FALSE" | "F" | "NO" | "N" | "0" => Ok(Value::Bool(false)),
            _ => Err(Error::ExecutionError(format!(
                "Cannot cast '{}' to BOOLEAN",
                s
            ))),
        },

        // Float to signed integer types (truncating CAST)
        (Value::F32(v), DataType::I8) => {
            let truncated = v.trunc();
            if truncated >= i8::MIN as f32 && truncated <= i8::MAX as f32 {
                Ok(Value::I8(truncated as i8))
            } else {
                Err(Error::ExecutionError(format!(
                    "Float {} out of range for TINYINT",
                    v
                )))
            }
        }
        (Value::F32(v), DataType::I16) => {
            let truncated = v.trunc();
            if truncated >= i16::MIN as f32 && truncated <= i16::MAX as f32 {
                Ok(Value::I16(truncated as i16))
            } else {
                Err(Error::ExecutionError(format!(
                    "Float {} out of range for SMALLINT",
                    v
                )))
            }
        }
        (Value::F32(v), DataType::I32) => Ok(Value::I32(v.trunc() as i32)),
        (Value::F32(v), DataType::I64) => Ok(Value::I64(v.trunc() as i64)),
        (Value::F32(v), DataType::I128) => Ok(Value::I128(v.trunc() as i128)),

        (Value::F64(v), DataType::I8) => {
            let truncated = v.trunc();
            if truncated >= i8::MIN as f64 && truncated <= i8::MAX as f64 {
                Ok(Value::I8(truncated as i8))
            } else {
                Err(Error::ExecutionError(format!(
                    "Double {} out of range for TINYINT",
                    v
                )))
            }
        }
        (Value::F64(v), DataType::I16) => {
            let truncated = v.trunc();
            if truncated >= i16::MIN as f64 && truncated <= i16::MAX as f64 {
                Ok(Value::I16(truncated as i16))
            } else {
                Err(Error::ExecutionError(format!(
                    "Double {} out of range for SMALLINT",
                    v
                )))
            }
        }
        (Value::F64(v), DataType::I32) => Ok(Value::I32(v.trunc() as i32)),
        (Value::F64(v), DataType::I64) => Ok(Value::I64(v.trunc() as i64)),
        (Value::F64(v), DataType::I128) => Ok(Value::I128(v.trunc() as i128)),

        // Boolean to integer types
        (Value::Bool(b), DataType::I8) => Ok(Value::I8(if *b { 1 } else { 0 })),
        (Value::Bool(b), DataType::I16) => Ok(Value::I16(if *b { 1 } else { 0 })),
        (Value::Bool(b), DataType::I32) => Ok(Value::I32(if *b { 1 } else { 0 })),
        (Value::Bool(b), DataType::I64) => Ok(Value::I64(if *b { 1 } else { 0 })),
        (Value::Bool(b), DataType::I128) => Ok(Value::I128(if *b { 1 } else { 0 })),

        // Integer to boolean
        (Value::I8(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),
        (Value::I16(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),
        (Value::I32(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),
        (Value::I64(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),
        (Value::I128(v), DataType::Bool) => Ok(Value::Bool(*v != 0)),

        // Any type to string (using Display)
        (_, DataType::Str | DataType::Text) => Ok(Value::Str(value.to_string())),

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
        .map(|(value, column)| coerce_value_impl(value, &column.data_type))
        .collect()
}
