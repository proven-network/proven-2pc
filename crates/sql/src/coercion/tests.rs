use super::*;
use crate::types::{DataType, Value};

// ========== NULL TESTS ==========

#[test]
fn test_null_coercion() {
    // NULL can be coerced to any type
    let value = Value::Null;
    assert_eq!(
        coerce_value(value.clone(), &DataType::I128).unwrap(),
        Value::Null
    );
    assert_eq!(
        coerce_value(value.clone(), &DataType::Str).unwrap(),
        Value::Null
    );
    assert_eq!(
        coerce_value(value.clone(), &DataType::Bool).unwrap(),
        Value::Null
    );
}

// ========== INTEGER WIDENING TESTS ==========

#[test]
fn test_signed_integer_widening() {
    // I8 -> I16/I32/I64/I128
    assert_eq!(
        coerce_value(Value::I8(42), &DataType::I16).unwrap(),
        Value::I16(42)
    );
    assert_eq!(
        coerce_value(Value::I8(42), &DataType::I32).unwrap(),
        Value::I32(42)
    );
    assert_eq!(
        coerce_value(Value::I8(42), &DataType::I64).unwrap(),
        Value::I64(42)
    );
    assert_eq!(
        coerce_value(Value::I8(42), &DataType::I128).unwrap(),
        Value::I128(42)
    );

    // I16 -> I32/I64/I128
    assert_eq!(
        coerce_value(Value::I16(1000), &DataType::I32).unwrap(),
        Value::I32(1000)
    );
    assert_eq!(
        coerce_value(Value::I16(1000), &DataType::I64).unwrap(),
        Value::I64(1000)
    );
    assert_eq!(
        coerce_value(Value::I16(1000), &DataType::I128).unwrap(),
        Value::I128(1000)
    );

    // I32 -> I64/I128
    assert_eq!(
        coerce_value(Value::I32(100000), &DataType::I64).unwrap(),
        Value::I64(100000)
    );
    assert_eq!(
        coerce_value(Value::I32(100000), &DataType::I128).unwrap(),
        Value::I128(100000)
    );

    // I64 -> I128
    assert_eq!(
        coerce_value(Value::I64(1000000000000), &DataType::I128).unwrap(),
        Value::I128(1000000000000)
    );
}

#[test]
fn test_unsigned_integer_widening() {
    // U8 -> U16/U32/U64/U128
    assert_eq!(
        coerce_value(Value::U8(42), &DataType::U16).unwrap(),
        Value::U16(42)
    );
    assert_eq!(
        coerce_value(Value::U8(42), &DataType::U32).unwrap(),
        Value::U32(42)
    );
    assert_eq!(
        coerce_value(Value::U8(42), &DataType::U64).unwrap(),
        Value::U64(42)
    );
    assert_eq!(
        coerce_value(Value::U8(42), &DataType::U128).unwrap(),
        Value::U128(42)
    );

    // U16 -> U32/U64/U128
    assert_eq!(
        coerce_value(Value::U16(1000), &DataType::U32).unwrap(),
        Value::U32(1000)
    );
    assert_eq!(
        coerce_value(Value::U16(1000), &DataType::U64).unwrap(),
        Value::U64(1000)
    );
    assert_eq!(
        coerce_value(Value::U16(1000), &DataType::U128).unwrap(),
        Value::U128(1000)
    );
}

// ========== INTEGER NARROWING TESTS ==========

#[test]
fn test_integer_narrowing() {
    // Success cases (value fits)
    assert_eq!(
        coerce_value(Value::I64(42), &DataType::I32).unwrap(),
        Value::I32(42)
    );
    assert_eq!(
        coerce_value(Value::I32(10), &DataType::I8).unwrap(),
        Value::I8(10)
    );

    // Failure cases (overflow)
    assert!(coerce_value(Value::I64(i64::MAX), &DataType::I32).is_err());
    assert!(coerce_value(Value::I32(1000), &DataType::I8).is_err());
    assert!(coerce_value(Value::I16(200), &DataType::I8).is_err());
}

// ========== SIGNED/UNSIGNED CONVERSION TESTS ==========

#[test]
fn test_unsigned_to_signed() {
    // Success: U8 -> I16 (safe, needs extra bit)
    assert_eq!(
        coerce_value(Value::U8(200), &DataType::I16).unwrap(),
        Value::I16(200)
    );

    // Success: U16 -> I32 (safe)
    assert_eq!(
        coerce_value(Value::U16(50000), &DataType::I32).unwrap(),
        Value::I32(50000)
    );

    // Failure: U8 -> I8 (overflow)
    assert!(coerce_value(Value::U8(200), &DataType::I8).is_err());
}

#[test]
fn test_signed_to_unsigned() {
    // Success: positive value
    assert_eq!(
        coerce_value(Value::I8(10), &DataType::U8).unwrap(),
        Value::U8(10)
    );
    assert_eq!(
        coerce_value(Value::I32(100), &DataType::U32).unwrap(),
        Value::U32(100)
    );

    // Failure: negative value
    assert!(coerce_value(Value::I8(-5), &DataType::U8).is_err());
    assert!(coerce_value(Value::I64(-100), &DataType::U64).is_err());
}

// ========== FLOAT TESTS ==========

#[test]
fn test_float_conversions() {
    // F32 -> F64 (widening)
    assert_eq!(
        coerce_value(Value::F32(std::f32::consts::PI), &DataType::F64).unwrap(),
        Value::F64(std::f32::consts::PI as f64)
    );

    // F64 -> F32 (narrowing)
    assert_eq!(
        coerce_value(Value::F64(2.5), &DataType::F32).unwrap(),
        Value::F32(2.5f32)
    );
}

#[test]
fn test_integer_to_float() {
    assert_eq!(
        coerce_value(Value::I8(42), &DataType::F32).unwrap(),
        Value::F32(42.0)
    );
    assert_eq!(
        coerce_value(Value::I32(100), &DataType::F64).unwrap(),
        Value::F64(100.0)
    );
    assert_eq!(
        coerce_value(Value::U16(200), &DataType::F32).unwrap(),
        Value::F32(200.0)
    );
}

#[test]
fn test_float_to_integer() {
    // Truncation (no rounding)
    assert_eq!(
        coerce_value(Value::F64(3.9), &DataType::I32).unwrap(),
        Value::I32(3)
    );
    assert_eq!(
        coerce_value(Value::F32(10.1), &DataType::I8).unwrap(),
        Value::I8(10)
    );

    // Range checks
    assert_eq!(
        coerce_value(Value::F64(127.0), &DataType::I8).unwrap(),
        Value::I8(127)
    );
    assert!(coerce_value(Value::F64(200.0), &DataType::I8).is_err());

    // Unsigned
    assert_eq!(
        coerce_value(Value::F64(255.0), &DataType::U8).unwrap(),
        Value::U8(255)
    );
    assert!(coerce_value(Value::F64(-1.0), &DataType::U8).is_err());
}

// ========== DECIMAL TESTS ==========

#[test]
fn test_integer_to_decimal() {
    let value = Value::I64(42);
    let result = coerce_value(value, &DataType::Decimal(Some(10), Some(2))).unwrap();
    match result {
        Value::Decimal(d) => assert_eq!(d.to_string(), "42"),
        _ => panic!("Expected Decimal"),
    }

    // Unsigned
    let value = Value::U32(100);
    let result = coerce_value(value, &DataType::Decimal(None, None)).unwrap();
    match result {
        Value::Decimal(d) => assert_eq!(d.to_string(), "100"),
        _ => panic!("Expected Decimal"),
    }
}

#[test]
fn test_float_to_decimal() {
    let value = Value::F64(std::f64::consts::PI);
    let result = coerce_value(value, &DataType::Decimal(Some(10), Some(5))).unwrap();
    match result {
        Value::Decimal(_) => {} // Success
        _ => panic!("Expected Decimal"),
    }
}

#[test]
fn test_decimal_to_integer() {
    use rust_decimal::Decimal;
    use std::str::FromStr;

    // Success: no fractional part
    let value = Value::Decimal(Decimal::from_str("42").unwrap());
    assert_eq!(coerce_value(value, &DataType::I32).unwrap(), Value::I32(42));

    // Failure: has fractional part
    let value = Value::Decimal(Decimal::from_str("42.5").unwrap());
    assert!(coerce_value(value, &DataType::I32).is_err());
}

// ========== BOOLEAN TESTS ==========

#[test]
fn test_bool_to_integer() {
    assert_eq!(
        coerce_value(Value::Bool(true), &DataType::I8).unwrap(),
        Value::I8(1)
    );
    assert_eq!(
        coerce_value(Value::Bool(false), &DataType::I8).unwrap(),
        Value::I8(0)
    );
    assert_eq!(
        coerce_value(Value::Bool(true), &DataType::U32).unwrap(),
        Value::U32(1)
    );
}

#[test]
fn test_integer_to_bool() {
    assert_eq!(
        coerce_value(Value::I32(0), &DataType::Bool).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        coerce_value(Value::I32(1), &DataType::Bool).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        coerce_value(Value::I32(-5), &DataType::Bool).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        coerce_value(Value::U8(0), &DataType::Bool).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        coerce_value(Value::U8(255), &DataType::Bool).unwrap(),
        Value::Bool(true)
    );
}

// ========== STRING PARSING TESTS ==========

#[test]
fn test_string_to_integer() {
    assert_eq!(
        coerce_value(Value::Str("42".into()), &DataType::I32).unwrap(),
        Value::I32(42)
    );
    assert_eq!(
        coerce_value(Value::Str("-100".into()), &DataType::I64).unwrap(),
        Value::I64(-100)
    );
    assert_eq!(
        coerce_value(Value::Str("255".into()), &DataType::U8).unwrap(),
        Value::U8(255)
    );

    // Invalid
    assert!(coerce_value(Value::Str("not a number".into()), &DataType::I32).is_err());
}

#[test]
fn test_string_to_float() {
    assert_eq!(
        coerce_value(Value::Str("3.69".into()), &DataType::F64).unwrap(),
        Value::F64(3.69)
    );
    assert_eq!(
        coerce_value(Value::Str("-0.5".into()), &DataType::F32).unwrap(),
        Value::F32(-0.5)
    );

    // Invalid
    assert!(coerce_value(Value::Str("invalid".into()), &DataType::F64).is_err());
}

#[test]
fn test_string_to_bool() {
    // True values
    assert_eq!(
        coerce_value(Value::Str("TRUE".into()), &DataType::Bool).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        coerce_value(Value::Str("true".into()), &DataType::Bool).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        coerce_value(Value::Str("T".into()), &DataType::Bool).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        coerce_value(Value::Str("YES".into()), &DataType::Bool).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        coerce_value(Value::Str("Y".into()), &DataType::Bool).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        coerce_value(Value::Str("1".into()), &DataType::Bool).unwrap(),
        Value::Bool(true)
    );

    // False values
    assert_eq!(
        coerce_value(Value::Str("FALSE".into()), &DataType::Bool).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        coerce_value(Value::Str("false".into()), &DataType::Bool).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        coerce_value(Value::Str("F".into()), &DataType::Bool).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        coerce_value(Value::Str("NO".into()), &DataType::Bool).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        coerce_value(Value::Str("N".into()), &DataType::Bool).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        coerce_value(Value::Str("0".into()), &DataType::Bool).unwrap(),
        Value::Bool(false)
    );

    // Invalid
    assert!(coerce_value(Value::Str("maybe".into()), &DataType::Bool).is_err());
}

// ========== TEMPORAL TYPE TESTS ==========

#[test]
fn test_string_to_date() {
    let result = coerce_value(Value::Str("2024-01-15".into()), &DataType::Date);
    assert!(result.is_ok());

    // Invalid
    assert!(coerce_value(Value::Str("not-a-date".into()), &DataType::Date).is_err());
}

#[test]
fn test_string_to_time() {
    let result = coerce_value(Value::Str("14:30:00".into()), &DataType::Time);
    assert!(result.is_ok());

    let result = coerce_value(Value::Str("14:30:00.123".into()), &DataType::Time);
    assert!(result.is_ok());

    // Invalid
    assert!(coerce_value(Value::Str("25:00:00".into()), &DataType::Time).is_err());
}

#[test]
fn test_string_to_timestamp() {
    let result = coerce_value(
        Value::Str("2024-01-15 14:30:00".into()),
        &DataType::Timestamp,
    );
    assert!(result.is_ok());

    let result = coerce_value(Value::Str("2024-01-15".into()), &DataType::Timestamp);
    assert!(result.is_ok()); // Should add 00:00:00

    // Invalid
    assert!(coerce_value(Value::Str("not-a-timestamp".into()), &DataType::Timestamp).is_err());
}

#[test]
fn test_string_to_interval() {
    // Various units
    let result = coerce_value(Value::Str("1 day".into()), &DataType::Interval);
    assert!(result.is_ok());

    let result = coerce_value(Value::Str("2 hours".into()), &DataType::Interval);
    assert!(result.is_ok());

    let result = coerce_value(Value::Str("30 seconds".into()), &DataType::Interval);
    assert!(result.is_ok());

    let result = coerce_value(Value::Str("3 months".into()), &DataType::Interval);
    assert!(result.is_ok());

    // Invalid
    assert!(coerce_value(Value::Str("invalid interval".into()), &DataType::Interval).is_err());
    assert!(coerce_value(Value::Str("5 fortnights".into()), &DataType::Interval).is_err());
}

// ========== SPECIAL TYPE TESTS ==========

#[test]
fn test_string_to_uuid() {
    let result = coerce_value(
        Value::Str("550e8400-e29b-41d4-a716-446655440000".into()),
        &DataType::Uuid,
    );
    assert!(result.is_ok());

    // Invalid
    assert!(coerce_value(Value::Str("not-a-uuid".into()), &DataType::Uuid).is_err());
}

#[test]
fn test_string_to_inet() {
    // IPv4
    let result = coerce_value(Value::Str("192.168.1.1".into()), &DataType::Inet);
    assert!(result.is_ok());

    // IPv6
    let result = coerce_value(Value::Str("::1".into()), &DataType::Inet);
    assert!(result.is_ok());

    // Invalid
    assert!(coerce_value(Value::Str("not.an.ip".into()), &DataType::Inet).is_err());
}

#[test]
fn test_integer_to_inet() {
    // Small values -> IPv4
    let result = coerce_value(Value::U32(0x7F000001), &DataType::Inet); // 127.0.0.1
    assert!(result.is_ok());

    // Negative should fail
    assert!(coerce_value(Value::I32(-1), &DataType::Inet).is_err());
}

#[test]
fn test_string_to_point() {
    let result = coerce_value(Value::Str("POINT(1.5 2.5)".into()), &DataType::Point);
    assert!(result.is_ok());

    let result = coerce_value(Value::Str("POINT(10, 20)".into()), &DataType::Point);
    assert!(result.is_ok());

    // Invalid
    assert!(coerce_value(Value::Str("not a point".into()), &DataType::Point).is_err());
    assert!(coerce_value(Value::Str("POINT(1)".into()), &DataType::Point).is_err());
}

#[test]
fn test_string_to_bytea() {
    // Hex with \x prefix
    let result = coerce_value(Value::Str("\\x48656c6c6f".into()), &DataType::Bytea);
    assert!(result.is_ok());

    // Hex with 0x prefix
    let result = coerce_value(Value::Str("0x48656c6c6f".into()), &DataType::Bytea);
    assert!(result.is_ok());

    // Plain hex
    let result = coerce_value(Value::Str("48656c6c6f".into()), &DataType::Bytea);
    assert!(result.is_ok());

    // Invalid hex
    assert!(coerce_value(Value::Str("\\xGGGG".into()), &DataType::Bytea).is_err());
}

#[test]
fn test_bytea_to_string() {
    let bytes = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello"
    let result = coerce_value(Value::Bytea(bytes), &DataType::Str).unwrap();
    match result {
        Value::Str(s) => assert!(s.starts_with("\\x")),
        _ => panic!("Expected string"),
    }
}

// ========== ANY TYPE TO STRING TESTS ==========

#[test]
fn test_any_to_string() {
    // Integers
    assert_eq!(
        coerce_value(Value::I32(42), &DataType::Str).unwrap(),
        Value::Str("42".into())
    );

    // Float
    let result = coerce_value(Value::F64(std::f64::consts::PI), &DataType::Str).unwrap();
    match result {
        Value::Str(s) => assert!(s.contains("3.14")),
        _ => panic!("Expected string"),
    }

    // Bool
    assert_eq!(
        coerce_value(Value::Bool(true), &DataType::Str).unwrap(),
        Value::Str("true".into())
    );
}

// ========== ERROR CASES ==========

#[test]
fn test_incompatible_coercions() {
    // UUID from integer (not allowed)
    assert!(coerce_value(Value::I32(123), &DataType::Uuid).is_err());

    // Date from integer (not allowed)
    assert!(coerce_value(Value::I64(20240115), &DataType::Date).is_err());
}

#[test]
fn test_type_mismatch_error() {
    let result = coerce_value(Value::Str("not a number".into()), &DataType::I32);
    assert!(result.is_err());
}
